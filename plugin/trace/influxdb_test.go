package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxhttp "github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFlowSpansUseCoreShardBuckets(t *testing.T) {
	router := &traceBucketRouter{
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

	decision := routedDecision(span)
	wantPrimary := router.traceShardBuckets[decision.shard%uint32(len(router.traceShardBuckets))]
	if got := router.TraceBucketsForDecision(decision); len(got) != 1 || got[0] != wantPrimary {
		t.Fatalf("flow buckets = %v, want [%s]", got, wantPrimary)
	}

	span.Status = protos.SpanStatus_SPAN_STATUS_ERROR
	got := router.TraceBucketsForDecision(routedDecision(span))
	if len(got) != 2 || got[0] != wantPrimary || got[1] != "error" {
		t.Fatalf("failed flow buckets = %v, want [%s error]", got, wantPrimary)
	}
	if span.SpanId != "span-1" {
		t.Fatalf("span ID changed during bucket routing: %q", span.SpanId)
	}
}

func TestFlowSpansUseCoreBucketWithoutShards(t *testing.T) {
	router := &traceBucketRouter{
		traceBucket:       "trace",
		traceCoreBucket:   "core",
		traceDetailBucket: "detail",
		traceErrorBucket:  "error",
	}

	got := router.TraceBucketsForDecision(routedDecision(&protos.SpanInfo{
		Name: "flow/v1/supervisor/task/spawned",
	}))
	if len(got) != 1 || got[0] != "core" {
		t.Fatalf("flow buckets = %v, want [core]", got)
	}
}

func TestGenericErrorBucketRouting(t *testing.T) {
	router := &traceBucketRouter{
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
			name: "detail error",
			span: &protos.SpanInfo{
				Name:   "step/prepare",
				Status: protos.SpanStatus_SPAN_STATUS_ERROR,
			},
			want: []string{"detail", "error"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := router.TraceBucketsForDecision(routedDecision(test.span))
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

func TestRoutingDoesNotInterpretBusinessNamesOrStatuses(t *testing.T) {
	router := &traceBucketRouter{
		traceBucket: "trace", traceCoreBucket: "core",
		traceDetailBucket: "detail", traceErrorBucket: "error",
	}
	span := &protos.SpanInfo{
		Name:       "job/end",
		Attributes: map[string]string{"final_status": "Failed"},
	}
	got := router.TraceBucketsForDecision(routedDecision(span))
	if len(got) != 1 || got[0] != "detail" {
		t.Fatalf("business-shaped span buckets = %v, want [detail]", got)
	}
}

func TestInfluxLookupPropagatesTypedHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(writer, `{"code":"unavailable","message":"injected"}`)
	}))
	t.Cleanup(server.Close)
	client := influxdb2.NewClient(server.URL, "test-token")
	t.Cleanup(client.Close)

	_, err := findOrganizationByName(context.Background(), client, "crane")
	var httpErr *influxhttp.Error
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("organization lookup error = %#v, want typed HTTP 503", err)
	}
	_, err = findBucketByName(context.Background(), client, "crane", "trace")
	httpErr = nil
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("bucket lookup error = %#v, want typed HTTP 503", err)
	}
}

func TestInfluxLookupHonorsStartupContextDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	t.Cleanup(server.Close)
	client := influxdb2.NewClient(server.URL, "test-token")
	t.Cleanup(client.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := findOrganizationByName(ctx, client, "crane")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lookup error = %v, want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("lookup exceeded bounded deadline: %s", elapsed)
	}
}

func TestInfluxWriteHonorsContextDeadlineAndReportsRetryBatch(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {
		case <-request.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(server.Close)
	client := influxdb2.NewClient(server.URL, "test-token")
	t.Cleanup(client.Close)
	t.Cleanup(func() { close(release) })
	store := &InfluxTraceStore{
		client: client,
		org:    "test-org",
		router: &traceBucketRouter{traceBucket: "trace", traceDetailBucket: "detail"},
	}
	point := encodedTracePoint{
		tags:    map[string]string{"name": "step/detail"},
		fields:  map[string]interface{}{"span_id": "span-1"},
		time:    time.Unix(100, 0),
		routing: traceRoutingDecision{destinations: []traceDestination{traceDestinationDetail}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result, err := store.WriteBatch(ctx, []encodedTracePoint{point})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("write error = %v, want context deadline", err)
	}
	if len(result.failed) != 1 {
		t.Fatalf("failed retry points = %d, want 1", len(result.failed))
	}
}

func TestInfluxCloseIsIdempotentAndRejectsLaterWrites(t *testing.T) {
	client := influxdb2.NewClient("http://127.0.0.1:1", "test-token")
	store := &InfluxTraceStore{
		client: client,
		org:    "test-org",
		router: &traceBucketRouter{traceBucket: "trace", traceDetailBucket: "detail"},
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
	point := encodedTracePoint{routing: traceRoutingDecision{
		destinations: []traceDestination{traceDestinationDetail},
	}}
	result, err := store.WriteBatch(context.Background(), []encodedTracePoint{point})
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("post-close write error = %v", err)
	}
	if len(result.failed) != 1 {
		t.Fatalf("post-close retry points = %d, want 1", len(result.failed))
	}
}

func TestInfluxCloseDeadlineWaitsForSingleClientCloseWithoutMarkingClosed(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	closeCalls := 0
	store := &InfluxTraceStore{
		closeClient: func() {
			closeCalls++
			close(started)
			<-release
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := store.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close error = %v, want deadline exceeded", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("client Close did not start")
	}
	store.closeMu.Lock()
	closed, closing := store.closed, store.closing
	store.closeMu.Unlock()
	if closed || !closing {
		t.Fatalf("timed-out close state: closed=%t closing=%t", closed, closing)
	}
	point := encodedTracePoint{routing: traceRoutingDecision{
		destinations: []traceDestination{traceDestinationDetail},
	}}
	result, writeErr := store.WriteBatch(context.Background(), []encodedTracePoint{point})
	if writeErr == nil || !strings.Contains(writeErr.Error(), "closing") {
		t.Fatalf("write during close error = %v", writeErr)
	}
	if len(result.failed) != 1 {
		t.Fatalf("write during close retry points = %d, want 1", len(result.failed))
	}

	unblock()
	retryCtx, retryCancel := context.WithTimeout(context.Background(), time.Second)
	defer retryCancel()
	if err := store.Close(retryCtx); err != nil {
		t.Fatalf("retry Close failed: %v", err)
	}
	if closeCalls != 1 {
		t.Fatalf("client Close calls = %d, want 1", closeCalls)
	}
	store.closeMu.Lock()
	closed, closing = store.closed, store.closing
	store.closeMu.Unlock()
	if !closed || closing {
		t.Fatalf("completed close state: closed=%t closing=%t", closed, closing)
	}
}

func TestFlowPointPromotesValidatedAttributesToTags(t *testing.T) {
	point, err := influxPointForSpanWithEnvironment(
		testSpan("flow/v1/ctld/job/accepted", map[string]string{
			"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
			"job_id":  "42",
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
	if got := tags["flow_slot"]; got == "" {
		t.Fatal("flow_slot tag is missing")
	}
	if got := tags["flow_instance_slot"]; got == "" {
		t.Fatal("flow_instance_slot tag is missing")
	}
	if _, ok := tags["span_id"]; ok {
		t.Fatal("unique span_id must not be an Influx tag")
	}
	fields := pointFields(point)
	if _, ok := fields["flow_id"]; ok {
		t.Fatal("promoted flow_id must not remain a field")
	}
	if _, ok := fields["flow_environment_id"]; ok {
		t.Fatal("promoted flow_environment_id must not remain a field")
	}
	if got := fields["span_id"]; got != "0123456789abcdef" {
		t.Fatalf("span_id field = %#v, want %q", got, "0123456789abcdef")
	}
	if got := fields["job_id"]; got != int64(42) {
		t.Fatalf("job_id field = %#v, want int64(42)", got)
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

func TestNewTracePointCodecRejectsInvalidFlowEnvironmentBeforeConnecting(t *testing.T) {
	t.Setenv(flowEnvironmentIDEnv, "run/935")

	_, err := newTracePointPipelineFromEnv()
	if err == nil {
		t.Fatal("newTracePointPipelineFromEnv() accepted an invalid execution-flow environment ID")
	}
	if !strings.Contains(err.Error(), flowEnvironmentIDEnv) {
		t.Fatalf("newTracePointPipelineFromEnv() error = %q, want environment variable name", err)
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

func TestFlowPointRejectsProducerControlledEnvironment(t *testing.T) {
	_, err := influxPointForSpanWithEnvironment(
		testSpan("flow/v1/ctld/job/accepted", map[string]string{
			"flow_environment_id": "canonical-environment",
		}),
		"canonical-environment",
	)
	if err == nil {
		t.Fatal("influxPointForSpanWithEnvironment() accepted producer-controlled storage metadata")
	}
	if !strings.Contains(err.Error(), "trusted storage metadata") {
		t.Fatalf("producer environment error = %q", err)
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
			_, err := influxPointForSpanWithEnvironment(testSpan(
				"flow/v1/ctld/job/accepted",
				map[string]string{"flow_id": value},
			), "canonical-environment")
			if err == nil {
				t.Fatalf("invalid flow_id value %q was accepted", value)
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

func TestFlowPointsWithDifferentSequencesKeepOriginalStorageTime(t *testing.T) {
	firstSpan := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id":        "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
		"event_sequence": "1",
	})
	firstSpan.SpanId = "0000000000000001"
	secondSpan := testSpan(firstSpan.Name, firstSpan.Attributes)
	secondSpan.Attributes["event_sequence"] = "65"
	secondSpan.SpanId = "000000003b9aca01"

	pipeline, err := newTracePointPipeline("canonical-environment", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatalf("newTracePointPipeline() error = %v", err)
	}
	firstEncoded, err := pipeline.Process(rawTracePoint{span: firstSpan})
	if err != nil {
		t.Fatalf("first codec.encode() error = %v", err)
	}
	secondEncoded, err := pipeline.Process(rawTracePoint{span: secondSpan})
	if err != nil {
		t.Fatalf("second codec.encode() error = %v", err)
	}
	firstPoint := influxPoint(firstEncoded)
	secondPoint := influxPoint(secondEncoded)

	firstTags := pointTags(firstPoint)
	secondTags := pointTags(secondPoint)
	if firstTags["flow_slot"] != secondTags["flow_slot"] {
		t.Fatalf("wrapped sequence slots differ: %v != %v", firstTags, secondTags)
	}
	if firstTags["flow_instance_slot"] != secondTags["flow_instance_slot"] {
		t.Fatalf("same service instance changed slots: %v != %v", firstTags, secondTags)
	}
	if !firstPoint.Time().Equal(secondPoint.Time()) {
		t.Fatalf("flow points moved storage time: %s != %s", firstPoint.Time(), secondPoint.Time())
	}
	wantEventTime := firstSpan.EndTime.AsTime().UnixNano()
	for index, point := range []*write.Point{firstPoint, secondPoint} {
		if got := point.Time().UnixNano(); got != wantEventTime {
			t.Fatalf("point %d storage time = %d, want %d", index, got, wantEventTime)
		}
		fields := pointFields(point)
		if got := fields["span_id"]; got == "" {
			t.Fatalf("point %d lost span_id field", index)
		}
		if got := fields["event_time_unix_nano"]; got != wantEventTime {
			t.Fatalf("point %d event time = %#v, want %d", index, got, wantEventTime)
		}
		if got := fields["duration_us"]; got != int64(0) {
			t.Fatalf("point %d duration = %#v, want zero", index, got)
		}
	}
	if got := pointFields(firstPoint)["event_sequence"]; got != int64(1) {
		t.Fatalf("first event sequence = %#v, want 1", got)
	}
	if got := pointFields(secondPoint)["event_sequence"]; got != int64(65) {
		t.Fatalf("second event sequence = %#v, want 65", got)
	}
}

func TestMaximumFlowSequenceDoesNotDriftPastQueryStop(t *testing.T) {
	span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id":        "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
		"event_sequence": strconv.FormatInt(int64(^uint64(0)>>1), 10),
	})
	queryStop := time.Date(2026, time.August, 4, 12, 0, 0, 123, time.UTC)
	span.StartTime = timestamppb.New(queryStop)
	span.EndTime = timestamppb.New(queryStop)

	point, err := influxPointForSpanWithEnvironment(span, "canonical-environment")
	if err != nil {
		t.Fatalf("influxPointForSpanWithEnvironment() error = %v", err)
	}
	if !point.Time().Equal(queryStop) {
		t.Fatalf("maximum sequence moved point time to %s, want %s", point.Time(), queryStop)
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
	router := &traceBucketRouter{
		traceCoreBucket:  "core",
		traceErrorBucket: "error",
	}
	span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
	})
	span.Status = protos.SpanStatus_SPAN_STATUS_ERROR
	buckets := router.TraceBucketsForDecision(routedDecision(span))
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
	store, codec, writes := recordingInfluxStore(t, "canonical-environment")
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

	points := normalizeTestSpans(t, codec, invalid, valid)
	if _, err := store.WriteBatch(context.Background(), points); err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}

	bodies := writes()
	if len(bodies) != 1 {
		t.Fatalf("Influx write count = %d, want 1", len(bodies))
	}
	if !strings.Contains(bodies[0], `span_id="fedcba9876543210"`) ||
		!strings.Contains(bodies[0], `job_id=2i`) {
		t.Fatalf("valid flow span was not written: %s", bodies[0])
	}
	if strings.Contains(bodies[0], `job_id=1i`) {
		t.Fatalf("invalid flow span reached InfluxDB: %s", bodies[0])
	}
	if !strings.Contains(bodies[0], `name=flow/v1/pipeline/fault`) ||
		!strings.Contains(bodies[0], `reason_code="invalid_span_id"`) {
		t.Fatalf("sanitized pipeline fault was not written: %s", bodies[0])
	}
	if strings.Contains(bodies[0], "invalid-span-id") {
		t.Fatalf("pipeline fault leaked the rejected value: %s", bodies[0])
	}
	if got := codec.rejected.Load(); got != 1 {
		t.Fatalf("rejected span writes = %d, want 1", got)
	}
}

func TestInvalidFlowSpanDoesNotBlockLaterBatch(t *testing.T) {
	store, codec, writes := recordingInfluxStore(t, "canonical-environment")
	invalid := testSpan("flow/v1/ctld/job/accepted", nil)
	invalid.SpanId = "invalid-span-id"

	if _, err := store.WriteBatch(
		context.Background(), normalizeTestSpans(t, codec, invalid),
	); err != nil {
		t.Fatalf("invalid-only WriteBatch() error = %v", err)
	}
	initialWrites := writes()
	if got := len(initialWrites); got != 1 {
		t.Fatalf("invalid-only batch made %d Influx writes, want one fault write", got)
	}
	if !strings.Contains(initialWrites[0], `name=flow/v1/pipeline/fault`) {
		t.Fatalf("invalid-only batch did not persist a pipeline fault: %s", initialWrites[0])
	}

	valid := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id": "fedcba98fedcba98fedcba98fedcba98",
	})
	if _, err := store.WriteBatch(
		context.Background(), normalizeTestSpans(t, codec, valid),
	); err != nil {
		t.Fatalf("later WriteBatch() error = %v", err)
	}
	if got := len(writes()); got != 2 {
		t.Fatalf("Influx write count after valid batch = %d, want 2", got)
	}
}

func recordingInfluxStore(
	t *testing.T,
	flowEnvironmentID string,
) (*InfluxTraceStore, *tracePointPipeline, func() []string) {
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
	codec, err := newTracePointPipeline(flowEnvironmentID, generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatalf("newTracePointPipeline(): %v", err)
	}
	store := &InfluxTraceStore{
		client: client,
		org:    "test-org",
		router: &traceBucketRouter{traceBucket: "core", traceCoreBucket: "core", traceErrorBucket: "core"},
	}
	return store, codec, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), bodies...)
	}
}

func normalizeTestSpans(
	t *testing.T,
	codec *tracePointPipeline,
	spans ...*protos.SpanInfo,
) []encodedTracePoint {
	t.Helper()
	points := make([]encodedTracePoint, 0, len(spans))
	for _, span := range spans {
		point, err := codec.Process(rawTracePoint{span: span})
		if err != nil {
			t.Fatalf("Normalize(%q): %v", span.GetName(), err)
		}
		points = append(points, point)
	}
	return points
}

func testSpan(name string, attributes map[string]string) *protos.SpanInfo {
	start := time.Unix(100, 0)
	wirePrefix := generatedExecutionFlowCatalog.WirePrefix()
	if strings.HasPrefix(name, wirePrefix) {
		flowAttributes := map[string]string{
			"flow_schema":              generatedExecutionFlowCatalog.SchemaVersion(),
			"point":                    strings.TrimPrefix(name, wirePrefix),
			"producer":                 "cranectld",
			"service_logical_instance": "ctld",
			"service_instance":         "ctld#test",
			"event_sequence":           "1",
		}
		if spec, ok := generatedExecutionFlowCatalog.Point(strings.TrimPrefix(name, wirePrefix)); ok {
			flowAttributes["producer"] = spec.Producer
			for _, key := range spec.RequiredAttributes {
				switch key {
				case "job_id":
					flowAttributes[key] = "1"
				case "step_id", "task_id", "attempt":
					flowAttributes[key] = "0"
				case "node_id":
					flowAttributes[key] = "craned1"
				case "operation":
					flowAttributes[key] = "submit"
				case "outcome":
					flowAttributes[key] = "success"
				case "reason_code":
					flowAttributes[key] = "non-batch-job"
				case "status":
					flowAttributes[key] = "2"
				}
			}
		}
		for key, value := range attributes {
			flowAttributes[key] = value
		}
		attributes = flowAttributes
	}
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

func routedDecision(span *protos.SpanInfo) traceRoutingDecision {
	point, err := (protobufTracePointDecoder{}).Decode(rawTracePoint{span: span})
	if err != nil {
		panic(err)
	}
	if generatedExecutionFlowCatalog != nil &&
		strings.HasPrefix(point.name, generatedExecutionFlowCatalog.WirePrefix()) {
		point.flow = &executionFlowEnvelope{}
	}
	return NewTracePointRouter().Route(validatedTracePoint{point: point}).routing
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
