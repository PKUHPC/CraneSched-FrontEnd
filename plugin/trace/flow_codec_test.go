package main

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestInvalidFlowPointBecomesQueryableSanitizedFault(t *testing.T) {
	pipeline, err := newTracePointPipeline("run-1.shard-0", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatalf("newTracePointPipeline() error = %v", err)
	}
	span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id": "DO-NOT-LEAK-THIS-VALUE",
	})

	normalized, err := pipeline.Process(rawTracePoint{span: span})
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	point := influxPoint(normalized)
	tags := pointTags(point)
	wantFaultName := generatedExecutionFlowCatalog.PipelineFaultPoint()
	if tags["name"] != wantFaultName {
		t.Fatalf("fault name = %q, want %q", tags["name"], wantFaultName)
	}
	if tags["flow_environment_id"] != "run-1.shard-0" {
		t.Fatalf("fault environment = %q", tags["flow_environment_id"])
	}
	if _, exists := tags["flow_id"]; exists {
		t.Fatal("invalid flow_id was promoted on pipeline fault")
	}
	if slot, err := strconv.Atoi(tags["flow_slot"]); err != nil ||
		slot < 0 || slot >= flowCollisionSlots {
		t.Fatalf("fault flow_slot = %q", tags["flow_slot"])
	}
	fields := pointFields(point)
	if fields["reason_code"] != "invalid_flow_id" {
		t.Fatalf("fault reason = %#v, want invalid_flow_id", fields["reason_code"])
	}
	if !flowSpanIDPattern.MatchString(fields["span_id"].(string)) {
		t.Fatalf("synthetic span ID is invalid: %#v", fields["span_id"])
	}
	if got := fields["event_sequence"]; got != int64(1) {
		t.Fatalf("fault event_sequence = %#v, want int64(1)", got)
	}
	line := write.PointToLineProtocol(point, time.Nanosecond)
	if strings.Contains(line, "DO-NOT-LEAK-THIS-VALUE") {
		t.Fatalf("pipeline fault leaked rejected data: %s", line)
	}
}

func TestPipelineFaultUsesObservationTimeForRejectedPastAndFutureEvents(t *testing.T) {
	observedAt := time.Date(2026, time.August, 4, 12, 0, 0, 123, time.UTC)
	for _, test := range []struct {
		name       string
		rejectedAt time.Time
	}{
		{name: "past", rejectedAt: observedAt.Add(-24 * time.Hour)},
		{name: "future", rejectedAt: observedAt.Add(24 * time.Hour)},
	} {
		t.Run(test.name, func(t *testing.T) {
			pipeline, err := newTracePointPipeline("run-1.shard-0", generatedExecutionFlowCatalog)
			if err != nil {
				t.Fatal(err)
			}
			pipeline.now = func() time.Time { return observedAt }
			span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
				"flow_id": "invalid",
			})
			span.StartTime = timestamppb.New(test.rejectedAt)
			span.EndTime = timestamppb.New(test.rejectedAt)

			encoded, err := pipeline.Process(rawTracePoint{span: span})
			if err != nil {
				t.Fatal(err)
			}
			if !encoded.time.Equal(observedAt) {
				t.Fatalf("fault storage time = %s, want observation time %s", encoded.time, observedAt)
			}
			if got := encoded.fields["event_time_unix_nano"]; got != observedAt.UnixNano() {
				t.Fatalf("fault event time = %#v, want %d", got, observedAt.UnixNano())
			}
		})
	}
}

func TestPipelineFaultIdentityDoesNotDependOnRejectedData(t *testing.T) {
	observedAt := time.Date(2026, time.August, 4, 12, 0, 0, 123, time.UTC)
	makeFault := func(flowID, traceID, spanID string) encodedTracePoint {
		t.Helper()
		pipeline, err := newTracePointPipeline("run-1.shard-0", generatedExecutionFlowCatalog)
		if err != nil {
			t.Fatal(err)
		}
		pipeline.now = func() time.Time { return observedAt }
		span := testSpan("flow/v1/ctld/job/accepted", map[string]string{"flow_id": flowID})
		span.TraceId = traceID
		span.SpanId = spanID
		point, err := pipeline.Process(rawTracePoint{span: span})
		if err != nil {
			t.Fatal(err)
		}
		return point
	}

	first := makeFault("secret-a", "trace-a", "0000000000000001")
	second := makeFault("secret-b", "trace-b", "0000000000000002")
	if first.fields["span_id"] != second.fields["span_id"] {
		t.Fatalf(
			"sanitized fault identity depends on rejected data: first=%v second=%v",
			first.fields["span_id"],
			second.fields["span_id"],
		)
	}
}

func TestFlowSlotsUseCanonicalEventSequenceModulo(t *testing.T) {
	pipeline, err := newTracePointPipeline("run-1.shard-0", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatalf("newTracePointPipeline() error = %v", err)
	}
	seen := make(map[string]struct{}, flowCollisionSlots)
	for index := 0; index < flowCollisionSlots; index++ {
		span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
			"flow_id":        "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
			"event_sequence": strconv.Itoa(index),
		})
		point, err := pipeline.Process(rawTracePoint{span: span})
		if err != nil {
			t.Fatalf("encode point %d: %v", index, err)
		}
		slot := point.tags["flow_slot"]
		if slot != strconv.Itoa(index) {
			t.Fatalf("event sequence %d produced flow_slot %q", index, slot)
		}
		if _, duplicate := seen[slot]; duplicate {
			t.Fatalf("duplicate allocated flow_slot %s", slot)
		}
		seen[slot] = struct{}{}
	}

	wrapped := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id":        "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
		"event_sequence": strconv.Itoa(flowCollisionSlots),
	})
	point, err := pipeline.Process(rawTracePoint{span: wrapped})
	if err != nil {
		t.Fatalf("encode wrapped event sequence: %v", err)
	}
	if point.tags["flow_slot"] != "0" {
		t.Fatalf("wrapped flow_slot = %q, want 0", point.tags["flow_slot"])
	}
}

func TestEqualNanosecondFlowPointsUseDistinctSequenceSlots(t *testing.T) {
	pipeline, err := newTracePointPipeline("run-1.shard-0", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatalf("newTracePointPipeline() error = %v", err)
	}

	first := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4", "event_sequence": "7",
	})
	second := testSpan(first.Name, map[string]string{
		"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4", "event_sequence": "8",
	})
	second.SpanId = "fedcba9876543210"
	firstPoint, err := pipeline.Process(rawTracePoint{span: first})
	if err != nil {
		t.Fatal(err)
	}
	secondPoint, err := pipeline.Process(rawTracePoint{span: second})
	if err != nil {
		t.Fatal(err)
	}
	if !firstPoint.time.Equal(secondPoint.time) {
		t.Fatal("test points do not share an event timestamp")
	}
	if firstPoint.tags["flow_slot"] == secondPoint.tags["flow_slot"] {
		t.Fatalf("equal-nanosecond points share slot %q", firstPoint.tags["flow_slot"])
	}
}

func TestFlowSlotCardinalityIsBoundedWithoutMovingEventTime(t *testing.T) {
	pipeline, err := newTracePointPipeline("run-1.shard-0", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatalf("newTracePointPipeline() error = %v", err)
	}

	slots := make(map[string]struct{}, flowCollisionSlots)
	wantEventTime := testSpan("flow/v1/ctld/job/accepted", nil).EndTime.AsTime()
	for sequence := 0; sequence < flowCollisionSlots*3; sequence++ {
		span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
			"flow_id":        "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
			"event_sequence": strconv.Itoa(sequence),
		})
		point, processErr := pipeline.Process(rawTracePoint{span: span})
		if processErr != nil {
			t.Fatalf("process sequence %d: %v", sequence, processErr)
		}
		slots[point.tags["flow_slot"]] = struct{}{}
		if !point.time.Equal(wantEventTime) {
			t.Fatalf("sequence %d moved storage time to %s, want %s", sequence, point.time, wantEventTime)
		}
		if got := point.fields["event_time_unix_nano"]; got != wantEventTime.UnixNano() {
			t.Fatalf("sequence %d event time = %#v", sequence, got)
		}
	}
	if len(slots) != flowCollisionSlots {
		t.Fatalf("flow slot cardinality = %d, want %d", len(slots), flowCollisionSlots)
	}
}
