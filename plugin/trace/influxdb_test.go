package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFlowSpansUseCoreShardBuckets(t *testing.T) {
	store := &InfluxTraceStore{
		traceBucket:       "trace",
		traceCoreBucket:   "core",
		traceDetailBucket: "detail",
		traceErrorBucket:  "error",
		traceShardBuckets: []string{"core-0", "core-1"},
	}
	span := &protos.SpanInfo{
		Name:       "flow/v1/ctld/job/accepted",
		SpanId:     "span-1",
		Attributes: map[string]string{"job_id": "42"},
	}

	wantPrimary := store.traceShardBuckets[stableTraceShardKey(span)%uint32(len(store.traceShardBuckets))]
	if got := store.TraceBucketsForSpan(span); len(got) != 1 || got[0] != wantPrimary {
		t.Fatalf("flow buckets = %v, want [%s]", got, wantPrimary)
	}

	span.Status = protos.SpanStatus_SPAN_STATUS_ERROR
	got := store.TraceBucketsForSpan(span)
	if len(got) != 2 || got[0] != wantPrimary || got[1] != "error" {
		t.Fatalf("failed flow buckets = %v, want [%s error]", got, wantPrimary)
	}
	if span.SpanId != "span-1" {
		t.Fatalf("span ID changed during bucket routing: %q", span.SpanId)
	}
}

func TestFlowSpansUseCoreBucketWithoutShards(t *testing.T) {
	store := &InfluxTraceStore{
		traceBucket:       "trace",
		traceCoreBucket:   "core",
		traceDetailBucket: "detail",
		traceErrorBucket:  "error",
	}

	got := store.TraceBucketsForSpan(&protos.SpanInfo{
		Name: "flow/v1/supervisor/task/spawned",
	})
	if len(got) != 1 || got[0] != "core" {
		t.Fatalf("flow buckets = %v, want [core]", got)
	}
}

func TestExistingErrorBucketRoutingIsPreserved(t *testing.T) {
	store := &InfluxTraceStore{
		traceBucket:       "trace",
		traceCoreBucket:   "core",
		traceDetailBucket: "detail",
		traceErrorBucket:  "error",
	}
	tests := []struct {
		name string
		span *protos.SpanInfo
		want []string
	}{
		{
			name: "legacy core error",
			span: &protos.SpanInfo{
				Name:   "job/end",
				Status: protos.SpanStatus_SPAN_STATUS_ERROR,
			},
			want: []string{"core", "error"},
		},
		{
			name: "detail error",
			span: &protos.SpanInfo{
				Name:   "step/prepare",
				Status: protos.SpanStatus_SPAN_STATUS_ERROR,
			},
			want: []string{"error"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := store.TraceBucketsForSpan(test.span)
			if len(got) != len(test.want) {
				t.Fatalf("buckets = %v, want %v", got, test.want)
			}
			for index := range test.want {
				if got[index] != test.want[index] {
					t.Fatalf("buckets = %v, want %v", got, test.want)
				}
			}
		})
	}
}

func TestFlowPointPromotesValidatedAttributesToTags(t *testing.T) {
	point, err := influxPointForSpanWithEnvironment(
		testSpan("flow/v1/ctld/job/accepted", map[string]string{
			"flow_id":             "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
			"flow_environment_id": "gh-123_ABC.1",
			"span_id":             "caller-supplied-id",
			"job_id":              "42",
		}),
		"gh-123_ABC.1",
	)
	if err != nil {
		t.Fatalf("influxPointForSpanWithEnvironment() error = %v", err)
	}

	tags := pointTags(point)
	if got := tags["flow_id"]; got != "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4" {
		t.Fatalf("flow_id tag = %q, want a normalized 32-character ID", got)
	}
	if got := tags["flow_environment_id"]; got != "gh-123_ABC.1" {
		t.Fatalf("flow_environment_id tag = %q, want %q", got, "gh-123_ABC.1")
	}
	if got := tags["span_id"]; got != "0123456789abcdef" {
		t.Fatalf("span_id tag = %q, want %q", got, "0123456789abcdef")
	}
	if _, ok := tags["flow_point_id"]; ok {
		t.Fatal("flow span ID must use the canonical span_id tag")
	}
	fields := pointFields(point)
	if _, ok := fields["flow_id"]; ok {
		t.Fatal("promoted flow_id must not remain a field")
	}
	if _, ok := fields["flow_environment_id"]; ok {
		t.Fatal("promoted flow_environment_id must not remain a field")
	}
	if _, ok := fields["span_id"]; ok {
		t.Fatal("promoted span_id must not remain a field")
	}
	if got := fields["job_id"]; got != "42" {
		t.Fatalf("job_id field = %#v, want %q", got, "42")
	}
}

func TestExecutionFlowEnvironmentIDFromEnv(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		unsetEnvironmentForTest(t, flowEnvironmentIDEnv)

		got, err := executionFlowEnvironmentIDFromEnv()
		if err != nil {
			t.Fatalf("executionFlowEnvironmentIDFromEnv() error = %v", err)
		}
		if got != "" {
			t.Fatalf("executionFlowEnvironmentIDFromEnv() = %q, want empty", got)
		}
	})

	t.Run("valid", func(t *testing.T) {
		t.Setenv(flowEnvironmentIDEnv, "run-935.shard_3")

		got, err := executionFlowEnvironmentIDFromEnv()
		if err != nil {
			t.Fatalf("executionFlowEnvironmentIDFromEnv() error = %v", err)
		}
		if got != "run-935.shard_3" {
			t.Fatalf("executionFlowEnvironmentIDFromEnv() = %q, want %q", got, "run-935.shard_3")
		}
	})
}

func TestNewInfluxTraceStoreRejectsInvalidFlowEnvironmentBeforeConnecting(t *testing.T) {
	t.Setenv(flowEnvironmentIDEnv, "run/935")

	_, err := NewInfluxTraceStore(&Config{})
	if err == nil {
		t.Fatal("NewInfluxTraceStore() accepted an invalid execution-flow environment ID")
	}
	if !strings.Contains(err.Error(), flowEnvironmentIDEnv) {
		t.Fatalf("NewInfluxTraceStore() error = %q, want environment variable name", err)
	}
}

func TestFlowPointInjectsCanonicalEnvironment(t *testing.T) {
	for _, spanName := range []string{
		"flow/v1/ctld/job/accepted",
		"flow/v1/pipeline/heartbeat",
	} {
		t.Run(spanName, func(t *testing.T) {
			point, err := influxPointForSpanWithEnvironment(
				testSpan(spanName, map[string]string{
					"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
				}),
				"run-935.shard_3",
			)
			if err != nil {
				t.Fatalf("influxPointForSpanWithEnvironment() error = %v", err)
			}
			if got := pointTags(point)["flow_environment_id"]; got != "run-935.shard_3" {
				t.Fatalf("flow_environment_id tag = %q, want %q", got, "run-935.shard_3")
			}
			if _, ok := pointFields(point)["flow_environment_id"]; ok {
				t.Fatal("injected flow_environment_id must not be stored as a field")
			}
		})
	}
}

func TestFlowPointRejectsCallerEnvironmentMismatch(t *testing.T) {
	_, err := influxPointForSpanWithEnvironment(
		testSpan("flow/v1/ctld/job/accepted", map[string]string{
			"flow_environment_id": "caller-environment",
		}),
		"canonical-environment",
	)
	if err == nil {
		t.Fatal("influxPointForSpanWithEnvironment() accepted a caller environment mismatch")
	}
	if !strings.Contains(err.Error(), "does not match process environment") {
		t.Fatalf("mismatch error = %q", err)
	}
}

func TestFlowPointAcceptsMatchingCanonicalEnvironment(t *testing.T) {
	point, err := influxPointForSpanWithEnvironment(
		testSpan("flow/v1/ctld/job/accepted", map[string]string{
			"flow_environment_id": "canonical-environment",
		}),
		"canonical-environment",
	)
	if err != nil {
		t.Fatalf("influxPointForSpanWithEnvironment() error = %v", err)
	}
	if got := pointTags(point)["flow_environment_id"]; got != "canonical-environment" {
		t.Fatalf("flow_environment_id tag = %q, want %q", got, "canonical-environment")
	}
}

func TestFlowPointRequiresCanonicalEnvironment(t *testing.T) {
	tests := []struct {
		name       string
		attributes map[string]string
	}{
		{name: "attribute absent"},
		{
			name: "producer supplies environment",
			attributes: map[string]string{
				"flow_environment_id": "producer-environment",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := influxPointForSpanWithEnvironment(
				testSpan("flow/v1/ctld/job/accepted", test.attributes),
				"",
			)
			if err == nil {
				t.Fatal("flow point accepted an absent canonical environment")
			}
			if !strings.Contains(err.Error(), flowEnvironmentIDEnv) {
				t.Fatalf("missing environment error = %q, want %s", err, flowEnvironmentIDEnv)
			}
		})
	}
}

func TestNonFlowPointDoesNotInjectCanonicalEnvironment(t *testing.T) {
	point, err := influxPointForSpanWithEnvironment(
		testSpan("job/lifecycle", map[string]string{"job_id": "42"}),
		"canonical-environment",
	)
	if err != nil {
		t.Fatalf("influxPointForSpanWithEnvironment() error = %v", err)
	}
	if _, ok := pointTags(point)["flow_environment_id"]; ok {
		t.Fatal("non-flow span received a flow_environment_id tag")
	}
	if _, ok := pointFields(point)["flow_environment_id"]; ok {
		t.Fatal("non-flow span received a flow_environment_id field")
	}
}

func TestNonFlowPointKeepsFlowAttributesAsFields(t *testing.T) {
	point := influxPointForSpan(testSpan("job/lifecycle", map[string]string{
		"flow_id":             "A1b2C3d4",
		"flow_environment_id": "gh-123",
	}))

	tags := pointTags(point)
	if _, ok := tags["flow_id"]; ok {
		t.Fatal("non-flow span must not promote flow_id")
	}
	if _, ok := tags["flow_environment_id"]; ok {
		t.Fatal("non-flow span must not promote flow_environment_id")
	}
	fields := pointFields(point)
	if got := fields["flow_id"]; got != "A1b2C3d4" {
		t.Fatalf("flow_id field = %#v, want %q", got, "A1b2C3d4")
	}
	if got := fields["flow_environment_id"]; got != "gh-123" {
		t.Fatalf("flow_environment_id field = %#v, want %q", got, "gh-123")
	}
}

func TestNonFlowPointKeepsSpanIDAsField(t *testing.T) {
	point, err := influxPointForSpanWithEnvironment(
		testSpan("job/lifecycle", nil),
		"",
	)
	if err != nil {
		t.Fatalf("influxPointForSpanWithEnvironment() error = %v", err)
	}
	if _, ok := pointTags(point)["span_id"]; ok {
		t.Fatal("non-flow span_id must not be promoted")
	}
	if _, ok := pointTags(point)["flow_point_id"]; ok {
		t.Fatal("non-flow span must not receive a flow_point_id tag")
	}
	if got := pointFields(point)["span_id"]; got != "0123456789abcdef" {
		t.Fatalf("span_id field = %#v, want %q", got, "0123456789abcdef")
	}
}

func TestFlowPointRejectsInvalidFlowIDs(t *testing.T) {
	tests := []string{
		"abcdef0123456789abcdef012345678",
		"abcdef0123456789abcdef01234567890",
		"abcdef0123456789abcdef012345678g",
		"ABCDEF0123456789ABCDEF0123456789",
	}

	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			point, err := influxPointForSpanWithEnvironment(testSpan(
				"flow/v1/ctld/job/accepted",
				map[string]string{"flow_id": value},
			), "canonical-environment")
			if err != nil {
				t.Fatalf("influxPointForSpanWithEnvironment() error = %v", err)
			}
			if _, ok := pointTags(point)["flow_id"]; ok {
				t.Fatalf("invalid flow_id value %q was promoted", value)
			}
			if got := pointFields(point)["flow_id"]; got != value {
				t.Fatalf("invalid flow_id field = %#v, want %q", got, value)
			}
		})
	}
}

func TestFlowPointRejectsInvalidSpanID(t *testing.T) {
	tests := []string{
		"",
		"0123456789abcde",
		"0123456789abcdef0",
		"0123456789abcdeg",
		"0123456789ABCDEF",
	}

	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			span := testSpan("flow/v1/ctld/job/accepted", nil)
			span.SpanId = value
			_, err := influxPointForSpanWithEnvironment(span, "canonical-environment")
			if err == nil {
				t.Fatalf("accepted invalid flow span_id %q", value)
			}
			if !strings.Contains(err.Error(), "span_id") {
				t.Fatalf("invalid span_id error = %q", err)
			}
		})
	}
}

func TestFlowPointsWithCollidingLegacySlotsRemainDistinct(t *testing.T) {
	firstSpan := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
	})
	// These IDs differ by 1,000,000,000 and collided when their numeric values
	// were reduced modulo the number of nanoseconds in one second.
	firstSpan.SpanId = "0000000000000001"
	secondSpan := testSpan(firstSpan.Name, firstSpan.Attributes)
	secondSpan.SpanId = "000000003b9aca01"

	firstPoint, err := influxPointForSpanWithEnvironment(firstSpan, "canonical-environment")
	if err != nil {
		t.Fatalf("first influxPointForSpanWithEnvironment() error = %v", err)
	}
	secondPoint, err := influxPointForSpanWithEnvironment(secondSpan, "canonical-environment")
	if err != nil {
		t.Fatalf("second influxPointForSpanWithEnvironment() error = %v", err)
	}

	firstTags := pointTags(firstPoint)
	secondTags := pointTags(secondPoint)
	if got := firstTags["span_id"]; got != firstSpan.SpanId {
		t.Fatalf("first span_id tag = %q, want %q", got, firstSpan.SpanId)
	}
	if got := secondTags["span_id"]; got != secondSpan.SpanId {
		t.Fatalf("second span_id tag = %q, want %q", got, secondSpan.SpanId)
	}
	if firstTags["span_id"] == secondTags["span_id"] {
		t.Fatalf("flow point tag identities collide: %v", firstTags)
	}
	if !firstPoint.Time().Equal(secondPoint.Time()) {
		t.Fatalf("real event times differ: %s != %s", firstPoint.Time(), secondPoint.Time())
	}
	firstLine := write.PointToLineProtocol(firstPoint, time.Nanosecond)
	secondLine := write.PointToLineProtocol(secondPoint, time.Nanosecond)
	if firstLine == secondLine {
		t.Fatalf("flow point line-protocol identities collide: %s", firstLine)
	}
	wantEventTime := firstSpan.EndTime.AsTime().UnixNano()
	for index, point := range []*write.Point{firstPoint, secondPoint} {
		fields := pointFields(point)
		if _, ok := fields["span_id"]; ok {
			t.Fatalf("point %d retained span_id as a field", index)
		}
		if got := fields["event_time_unix_nano"]; got != wantEventTime {
			t.Fatalf("point %d event time = %#v, want %d", index, got, wantEventTime)
		}
		if got := fields["duration_us"]; got != int64(0) {
			t.Fatalf("point %d duration = %#v, want zero", index, got)
		}
	}
}

func TestFlowPointRetainsEventTimeAtQueryStartBoundary(t *testing.T) {
	span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
	})
	boundary := time.Unix(100, 987654321)
	span.StartTime = timestamppb.New(boundary)
	span.EndTime = timestamppb.New(boundary)
	span.SpanId = "0000000000000001"

	point, err := influxPointForSpanWithEnvironment(span, "canonical-environment")
	if err != nil {
		t.Fatalf("influxPointForSpanWithEnvironment() error = %v", err)
	}
	if !point.Time().Equal(boundary) {
		t.Fatalf("point time = %s, want exact query boundary %s", point.Time(), boundary)
	}
	if point.Time().Before(boundary) {
		t.Fatalf("point time %s moved before query boundary %s", point.Time(), boundary)
	}
}

func TestFlowPrimaryAndErrorCopiesHaveIdenticalPointData(t *testing.T) {
	store := &InfluxTraceStore{
		traceCoreBucket:  "core",
		traceErrorBucket: "error",
	}
	span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
	})
	span.Status = protos.SpanStatus_SPAN_STATUS_ERROR
	buckets := store.TraceBucketsForSpan(span)
	if len(buckets) != 2 {
		t.Fatalf("flow error buckets = %v, want primary and error copies", buckets)
	}

	encoded := make([]string, 0, len(buckets))
	for range buckets {
		point, err := influxPointForSpanWithEnvironment(span, "canonical-environment")
		if err != nil {
			t.Fatalf("influxPointForSpanWithEnvironment() error = %v", err)
		}
		encoded = append(encoded, write.PointToLineProtocol(point, time.Nanosecond))
	}
	if encoded[0] != encoded[1] {
		t.Fatalf("primary and error copies differ:\nprimary: %serror: %s", encoded[0], encoded[1])
	}
}

func TestInvalidFlowSpanDoesNotBlockValidSpanInSameBatch(t *testing.T) {
	store, writes := recordingInfluxStore(t, "canonical-environment")
	invalid := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
		"job_id":  "1",
	})
	invalid.SpanId = "invalid-span-id"
	valid := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id": "fedcba98fedcba98fedcba98fedcba98",
		"job_id":  "2",
	})
	valid.SpanId = "fedcba9876543210"

	if err := store.SaveSpansToBucket("core", []*protos.SpanInfo{invalid, valid}); err != nil {
		t.Fatalf("SaveSpansToBucket() error = %v", err)
	}

	bodies := writes()
	if len(bodies) != 1 {
		t.Fatalf("Influx write count = %d, want 1", len(bodies))
	}
	if !strings.Contains(bodies[0], `span_id=fedcba9876543210`) ||
		!strings.Contains(bodies[0], `job_id="2"`) {
		t.Fatalf("valid flow span was not written: %s", bodies[0])
	}
	if strings.Contains(bodies[0], `job_id="1"`) {
		t.Fatalf("invalid flow span reached InfluxDB: %s", bodies[0])
	}
	if got := store.rejectedSpanWrites.Load(); got != 1 {
		t.Fatalf("rejected span writes = %d, want 1", got)
	}
}

func TestInvalidFlowSpanDoesNotBlockLaterBatch(t *testing.T) {
	store, writes := recordingInfluxStore(t, "canonical-environment")
	invalid := testSpan("flow/v1/ctld/job/accepted", nil)
	invalid.SpanId = "invalid-span-id"

	if err := store.SaveSpansToBucket("core", []*protos.SpanInfo{invalid}); err != nil {
		t.Fatalf("invalid-only SaveSpansToBucket() error = %v", err)
	}
	if got := len(writes()); got != 0 {
		t.Fatalf("invalid-only batch made %d Influx writes, want 0", got)
	}

	valid := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id": "fedcba98fedcba98fedcba98fedcba98",
	})
	if err := store.SaveSpansToBucket("core", []*protos.SpanInfo{valid}); err != nil {
		t.Fatalf("later SaveSpansToBucket() error = %v", err)
	}
	if got := len(writes()); got != 1 {
		t.Fatalf("Influx write count after valid batch = %d, want 1", got)
	}
}

func recordingInfluxStore(
	t *testing.T,
	flowEnvironmentID string,
) (*InfluxTraceStore, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read Influx request body: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		bodies = append(bodies, string(body))
		mu.Unlock()
		writer.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	client := influxdb2.NewClient(server.URL, "test-token")
	t.Cleanup(client.Close)
	store := &InfluxTraceStore{
		client:            client,
		org:               "test-org",
		flowEnvironmentID: flowEnvironmentID,
	}
	return store, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), bodies...)
	}
}

func testSpan(name string, attributes map[string]string) *protos.SpanInfo {
	start := time.Unix(100, 0)
	return &protos.SpanInfo{
		TraceId:      "trace-1",
		SpanId:       "0123456789abcdef",
		ParentSpanId: "parent-1",
		Name:         name,
		StartTime:    timestamppb.New(start),
		EndTime:      timestamppb.New(start.Add(25 * time.Microsecond)),
		Attributes:   attributes,
		ServiceName:  "CraneCtld",
	}
}

func pointTags(point *write.Point) map[string]string {
	tags := make(map[string]string, len(point.TagList()))
	for _, tag := range point.TagList() {
		tags[tag.Key] = tag.Value
	}
	return tags
}

func pointFields(point *write.Point) map[string]interface{} {
	fields := make(map[string]interface{}, len(point.FieldList()))
	for _, field := range point.FieldList() {
		fields[field.Key] = field.Value
	}
	return fields
}

func unsetEnvironmentForTest(t *testing.T, key string) {
	t.Helper()
	value, present := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("os.Unsetenv(%q): %v", key, err)
	}
	t.Cleanup(func() {
		if present {
			if err := os.Setenv(key, value); err != nil {
				t.Errorf("os.Setenv(%q): %v", key, err)
			}
			return
		}
		if err := os.Unsetenv(key); err != nil {
			t.Errorf("os.Unsetenv(%q): %v", key, err)
		}
	})
}
