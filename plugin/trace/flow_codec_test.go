package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
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
	if slot, err := strconv.Atoi(tags["flow_instance_slot"]); err != nil ||
		slot < 0 || slot >= flowInstanceSlots {
		t.Fatalf("fault flow_instance_slot = %q", tags["flow_instance_slot"])
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

func TestFlowInstanceSlotIsStableAndBounded(t *testing.T) {
	pipeline, err := newTracePointPipeline("run-1.shard-0", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatalf("newTracePointPipeline() error = %v", err)
	}
	encode := func(serviceInstance string, sequence int) encodedTracePoint {
		t.Helper()
		span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
			"flow_id":          "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
			"service_instance": serviceInstance,
			"event_sequence":   strconv.Itoa(sequence),
		})
		point, err := pipeline.Process(rawTracePoint{span: span})
		if err != nil {
			t.Fatalf("encode instance %q sequence %d: %v", serviceInstance, sequence, err)
		}
		return point
	}

	first := encode("ctld#instance-a", 7)
	repeated := encode("ctld#instance-a", 7)
	secondInstance := encode("ctld#instance-b", 7)
	secondSequence := encode("ctld#instance-a", 8)
	if first.tags["flow_instance_slot"] != repeated.tags["flow_instance_slot"] {
		t.Fatalf("identical service instance is unstable: %v != %v", first.tags, repeated.tags)
	}
	if first.tags["flow_instance_slot"] != secondSequence.tags["flow_instance_slot"] {
		t.Fatalf("same service instance changed slot across sequences: %v != %v", first.tags, secondSequence.tags)
	}
	if first.tags["flow_slot"] != "7" || secondInstance.tags["flow_slot"] != "7" ||
		secondSequence.tags["flow_slot"] != "8" {
		t.Fatalf("event sequence slots are not canonical modulo values: first=%v second_instance=%v second_sequence=%v",
			first.tags, secondInstance.tags, secondSequence.tags)
	}
	if _, promoted := first.tags["service_instance"]; promoted {
		t.Fatal("unbounded service_instance must remain a field, not a tag")
	}
	if !first.time.Equal(secondInstance.time) {
		t.Fatal("test points do not share an event timestamp")
	}
	seen := make(map[string]struct{}, flowInstanceSlots)
	for instance := 0; instance < flowInstanceSlots*4; instance++ {
		point := encode("ctld#sample-"+strconv.Itoa(instance), 7)
		slot, parseErr := strconv.Atoi(point.tags["flow_instance_slot"])
		if parseErr != nil || slot < 0 || slot >= flowInstanceSlots {
			t.Fatalf("instance %d produced out-of-domain slot %q",
				instance, point.tags["flow_instance_slot"])
		}
		seen[point.tags["flow_instance_slot"]] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("service identity hash did not separate the sample set: %v", seen)
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

func TestFlowCollisionTagCardinalityHasStrictUpperBoundWithoutMovingEventTime(t *testing.T) {
	pipeline, err := newTracePointPipeline("run-1.shard-0", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatalf("newTracePointPipeline() error = %v", err)
	}

	tagPairs := make(map[string]struct{}, flowCollisionSlots*flowInstanceSlots)
	wantEventTime := testSpan("flow/v1/ctld/job/accepted", nil).EndTime.AsTime()
	for instance := 0; instance < flowInstanceSlots*4; instance++ {
		for sequence := 0; sequence < flowCollisionSlots; sequence++ {
			span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
				"flow_id":          "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
				"service_instance": "ctld#instance-" + strconv.Itoa(instance),
				"event_sequence":   strconv.Itoa(sequence),
			})
			point, processErr := pipeline.Process(rawTracePoint{span: span})
			if processErr != nil {
				t.Fatalf("process instance %d sequence %d: %v", instance, sequence, processErr)
			}
			sequenceSlot, parseErr := strconv.Atoi(point.tags["flow_slot"])
			if parseErr != nil || sequenceSlot != sequence {
				t.Fatalf("sequence %d produced flow_slot %q", sequence, point.tags["flow_slot"])
			}
			instanceSlot, parseErr := strconv.Atoi(point.tags["flow_instance_slot"])
			if parseErr != nil || instanceSlot < 0 || instanceSlot >= flowInstanceSlots {
				t.Fatalf("instance %d produced out-of-domain flow_instance_slot %q",
					instance, point.tags["flow_instance_slot"])
			}
			tagPairs[point.tags["flow_instance_slot"]+"/"+point.tags["flow_slot"]] = struct{}{}
			if !point.time.Equal(wantEventTime) {
				t.Fatalf("instance %d sequence %d moved storage time to %s, want %s",
					instance, sequence, point.time, wantEventTime)
			}
			if got := point.fields["event_time_unix_nano"]; got != wantEventTime.UnixNano() {
				t.Fatalf("instance %d sequence %d event time = %#v", instance, sequence, got)
			}
		}
	}
	maxPairs := flowCollisionSlots * flowInstanceSlots
	if len(tagPairs) > maxPairs {
		t.Fatalf("collision tag pair cardinality = %d, want at most %d", len(tagPairs), maxPairs)
	}
	if len(tagPairs) <= flowCollisionSlots {
		t.Fatalf("instance dimension did not separate the sample set: pairs=%d", len(tagPairs))
	}
	if _, promoted := testSpan("flow/v1/ctld/job/accepted", nil).Attributes["flow_instance_slot"]; promoted {
		t.Fatal("storage-only flow_instance_slot leaked into the wire schema")
	}
}

func TestFlowInstanceSlotHasNoProcessLifetimeCapacity(t *testing.T) {
	pipeline, err := newTracePointPipeline("run-1.shard-0", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatal(err)
	}
	const flows = 16*1024 + 1
	for flow := 0; flow < flows; flow++ {
		span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
			"flow_id":          fmt.Sprintf("%032x", flow),
			"service_instance": "ctld#instance-" + strconv.Itoa(flow%97),
		})
		point, processErr := pipeline.Process(rawTracePoint{span: span})
		if processErr != nil {
			t.Fatalf("flow %d failed: %v", flow, processErr)
		}
		if point.tags["name"] == generatedExecutionFlowCatalog.PipelineFaultPoint() {
			t.Fatalf("flow %d became a pipeline fault", flow)
		}
	}
}

func TestFlowInstanceSlotIsConcurrentAndConsistent(t *testing.T) {
	pipeline, err := newTracePointPipeline("run-1.shard-0", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		slot string
		err  error
	}
	const goroutines = 256
	results := make(chan result, goroutines)
	var wg sync.WaitGroup
	for instance := 0; instance < goroutines; instance++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
				"flow_id":          "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
				"service_instance": "ctld#concurrent-stable",
				"event_sequence":   "9",
			})
			point, processErr := pipeline.Process(rawTracePoint{span: span})
			results <- result{slot: point.tags["flow_instance_slot"], err: processErr}
		}()
	}
	wg.Wait()
	close(results)

	var wantSlot string
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent encoding failed: %v", result.err)
		}
		if wantSlot == "" {
			wantSlot = result.slot
		} else if result.slot != wantSlot {
			t.Fatalf("same identity mapped inconsistently: got %q, want %q", result.slot, wantSlot)
		}
	}
	slot, parseErr := strconv.Atoi(wantSlot)
	if parseErr != nil || slot < 0 || slot >= flowInstanceSlots {
		t.Fatalf("concurrent identity produced out-of-domain slot %q", wantSlot)
	}
}

func TestUnboundPipelinePointsUseBoundedInstanceSlotWithoutExhaustion(t *testing.T) {
	for _, test := range []struct {
		name     string
		producer string
	}{
		{name: generatedExecutionFlowCatalog.HeartbeatPoint(), producer: "craned"},
		{name: generatedExecutionFlowCatalog.PipelineFaultPoint(), producer: "frontend"},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoder := &influxTracePointEncoder{}
			// Heartbeat and fault points carry no flow_id, so they used to fall
			// back to a single fixed slot. That is where cross-instance
			// collisions are most likely, not least: every Supervisor on one
			// node shares the "service" tag. Require the slot to stay bounded
			// and stateless, but also to actually separate producer instances.
			unboundSlots := make(map[string]struct{}, flowInstanceSlots)
			for instance := 0; instance < flowInstanceSlots*2; instance++ {
				point := typedTracePoint{
					name:      test.name,
					service:   test.producer,
					eventTime: time.Unix(100, 25_000),
					flow: &executionFlowEnvelope{
						environmentID:          "run-1.shard-0",
						producer:               test.producer,
						serviceLogicalInstance: test.producer,
						serviceInstance:        test.producer + "#" + strconv.Itoa(instance),
						eventSequence:          int64(instance),
						pipelineFault:          test.name == generatedExecutionFlowCatalog.PipelineFaultPoint(),
					},
				}
				encoded, err := encoder.Encode(routedTracePoint{point: point})
				if err != nil {
					t.Fatalf("unbound instance %d failed: %v", instance, err)
				}
				slot, parseErr := strconv.Atoi(encoded.tags["flow_instance_slot"])
				if parseErr != nil || slot < 0 || slot >= flowInstanceSlots {
					t.Fatalf("unbound instance %d slot = %q is out of domain",
						instance, encoded.tags["flow_instance_slot"])
				}
				unboundSlots[encoded.tags["flow_instance_slot"]] = struct{}{}
			}
			// A hash-independent floor: distinct producer instances must not all
			// collapse onto one series.
			if len(unboundSlots) < flowInstanceSlots/4 {
				t.Fatalf("unbound points used only %d of %d instance slots",
					len(unboundSlots), flowInstanceSlots)
			}

			business := typedTracePoint{
				name:      "flow/v1/ctld/job/accepted",
				service:   test.producer,
				eventTime: time.Unix(100, 25_000),
				flow: &executionFlowEnvelope{
					environmentID:          "run-1.shard-0",
					flowID:                 "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
					producer:               test.producer,
					serviceLogicalInstance: test.producer,
					serviceInstance:        test.producer + "#business",
				},
			}
			encoded, err := encoder.Encode(routedTracePoint{point: business})
			if err != nil {
				t.Fatalf("unbound points consumed business capacity: %v", err)
			}
			slot, parseErr := strconv.Atoi(encoded.tags["flow_instance_slot"])
			if parseErr != nil || slot < 0 || slot >= flowInstanceSlots {
				t.Fatalf("business instance slot = %q", encoded.tags["flow_instance_slot"])
			}
		})
	}
}

// TestConcurrentInstanceHeartbeatsKeepDistinctPointIdentity covers the exact
// overwrite this slot dimension exists to prevent. Influx identifies a point by
// measurement, tag set, and timestamp. Two Supervisors on one node share the
// "service" tag, heartbeats carry no flow_id, and their per-process sequence
// counters both restart at zero, so a same-nanosecond pair agrees on every
// other tag. If the instance slot did not separate them, one heartbeat would
// silently replace the other and the validator would report a pipeline gap.
func TestConcurrentInstanceHeartbeatsKeepDistinctPointIdentity(t *testing.T) {
	encoder := &influxTracePointEncoder{}
	heartbeat := func(serviceInstance string) encodedTracePoint {
		t.Helper()
		encoded, err := encoder.Encode(routedTracePoint{point: typedTracePoint{
			name:      generatedExecutionFlowCatalog.HeartbeatPoint(),
			service:   "Supervisor@node1",
			eventTime: time.Unix(100, 25_000),
			flow: &executionFlowEnvelope{
				environmentID:          "run-1.shard-0",
				producer:               "supervisor",
				serviceLogicalInstance: "node1",
				serviceInstance:        serviceInstance,
				eventSequence:          3,
			},
		}})
		if err != nil {
			t.Fatalf("encode heartbeat for %q: %v", serviceInstance, err)
		}
		return encoded
	}

	firstInstance := "node1#pid=101:start=1700000000000000000"
	secondInstance := "node1#pid=202:start=1700000000000000001"
	first := heartbeat(firstInstance)
	second := heartbeat(secondInstance)

	if !first.time.Equal(second.time) {
		t.Fatal("test must compare two heartbeats at the same timestamp")
	}
	if first.tags["flow_slot"] != second.tags["flow_slot"] {
		t.Fatal("test must compare two heartbeats sharing a sequence slot")
	}
	if first.measurement != second.measurement {
		t.Fatal("test must compare two heartbeats in the same measurement")
	}
	if first.tags["flow_instance_slot"] == second.tags["flow_instance_slot"] {
		t.Fatalf(
			"heartbeats from service instances %q and %q share instance slot %q; "+
				"their Influx point identity collides and one would overwrite the other",
			firstInstance, secondInstance, first.tags["flow_instance_slot"],
		)
	}
}

func TestMissingEnvironmentRejectsWithoutInventingFaultEnvironment(t *testing.T) {
	pipeline, err := newTracePointPipeline("", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatal(err)
	}
	span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_environment_id": "producer-claimed-environment",
	})

	point, err := pipeline.Process(rawTracePoint{span: span})
	if err == nil || !strings.Contains(err.Error(), flowEnvironmentIDEnv) {
		t.Fatalf("Process() error = %v, want missing environment failure", err)
	}
	if len(point.tags) != 0 || len(point.fields) != 0 {
		t.Fatalf("unscoped rejection produced a persisted point: %+v", point)
	}
	if got := pipeline.rejected.Load(); got != 1 {
		t.Fatalf("unscoped rejection count = %d, want 1", got)
	}
}
