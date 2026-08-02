package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"CraneFrontEnd/generated/protos"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

const (
	maxRetries           = 3
	retryInterval        = 5 * time.Second
	flowTraceSpanPrefix  = "flow/v1/"
	flowEnvironmentIDEnv = "CRANE_EXECUTION_FLOW_ENVIRONMENT_ID"
)

var (
	flowIDTagPattern            = regexp.MustCompile(`^[0-9a-f]{32}$`)
	flowSpanIDTagPattern        = regexp.MustCompile(`^[0-9a-f]{16}$`)
	flowEnvironmentIDTagPattern = regexp.MustCompile(
		`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`,
	)
)

type TraceStore interface {
	SaveSpansToBucket(string, []*protos.SpanInfo) error
	TraceBucketsForSpan(*protos.SpanInfo) []string
	Close() error
}

type InfluxTraceStore struct {
	client             influxdb2.Client
	org                string
	traceBucket        string
	traceCoreBucket    string
	traceDetailBucket  string
	traceErrorBucket   string
	traceShardBuckets  []string
	flowEnvironmentID  string
	rejectedSpanWrites atomic.Uint64
}

type flowPointValidationError struct {
	reason  string
	message string
}

func (e *flowPointValidationError) Error() string { return e.message }

func newFlowPointValidationError(reason, message string) error {
	return &flowPointValidationError{reason: reason, message: message}
}

var coreTraceSpanNames = map[string]struct{}{
	"job/pending":   {},
	"job/lifecycle": {},
	"step/execute":  {},
	"job/end":       {},
}

func NewTraceStore(cfg *Config) (TraceStore, error) {
	switch cfg.DB.Type {
	case "influxdb":
		return NewInfluxTraceStore(cfg)
	default:
		return nil, fmt.Errorf("unsupported trace database type: %s", cfg.DB.Type)
	}
}

func NewInfluxTraceStore(cfg *Config) (*InfluxTraceStore, error) {
	flowEnvironmentID, err := executionFlowEnvironmentIDFromEnv()
	if err != nil {
		return nil, err
	}

	var client influxdb2.Client

	for i := 0; i < maxRetries; i++ {
		client = influxdb2.NewClient(cfg.DB.InfluxDB.URL, cfg.DB.InfluxDB.Token)
		_, err = client.Ping(context.Background())

		if err == nil {
			break
		}

		log.Warnf("Failed to connect to InfluxDB (attempt %d/%d): %v", i+1, maxRetries, err)
		client.Close()

		if i < maxRetries-1 {
			time.Sleep(retryInterval)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to ping InfluxDB after %d attempts: %v", maxRetries, err)
	}

	store := &InfluxTraceStore{
		client:            client,
		org:               cfg.DB.InfluxDB.Org,
		traceBucket:       cfg.DB.InfluxDB.TraceBucket,
		traceCoreBucket:   cfg.DB.InfluxDB.TraceCoreBucket,
		traceDetailBucket: cfg.DB.InfluxDB.TraceDetailBucket,
		traceErrorBucket:  cfg.DB.InfluxDB.TraceErrorBucket,
		traceShardBuckets: append([]string(nil), cfg.DB.InfluxDB.TraceShardBuckets...),
		flowEnvironmentID: flowEnvironmentID,
	}

	for _, bucket := range store.traceBuckets() {
		if err := store.createBucketIfNotExists(bucket); err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to create trace bucket %s: %v", bucket, err)
		}
	}

	return store, nil
}

func (s *InfluxTraceStore) SaveSpansToBucket(bucket string, spans []*protos.SpanInfo) error {
	if len(spans) == 0 {
		return nil
	}
	if bucket == "" {
		bucket = s.traceBucket
	}

	points, err := s.influxPointsForSpans(bucket, spans)
	if err != nil {
		return err
	}
	if len(points) == 0 {
		return nil
	}

	start := time.Now()
	writeAPI := s.client.WriteAPIBlocking(s.org, bucket)
	ctx := context.Background()
	if err = writeAPI.WritePoint(ctx, points...); err != nil {
		log.Errorf("Failed to write %d spans to InfluxDB bucket=%s: %v", len(points), bucket, err)
		return fmt.Errorf("failed to write spans to bucket %s: %v", bucket, err)
	}

	elapsed := time.Since(start)
	log.Debugf("Saved %d trace spans to InfluxDB bucket=%s in %s", len(points), bucket, elapsed)
	if elapsed > time.Second {
		log.Warnf("Slow trace span write: saved %d spans to InfluxDB bucket=%s in %s",
			len(points), bucket, elapsed)
	}
	return nil
}

func (s *InfluxTraceStore) influxPointsForSpans(
	bucket string,
	spans []*protos.SpanInfo,
) ([]*write.Point, error) {
	points := make([]*write.Point, 0, len(spans))
	for _, span := range spans {
		point, err := influxPointForSpanWithEnvironment(span, s.flowEnvironmentID)
		if err == nil {
			points = append(points, point)
			continue
		}

		var validationError *flowPointValidationError
		if !errors.As(err, &validationError) {
			return nil, fmt.Errorf("failed to construct InfluxDB point: %w", err)
		}

		rejected := s.rejectedSpanWrites.Add(1)
		if rejected == 1 || rejected%128 == 0 {
			// Do not log span attributes or rejected values. Point validation is
			// permanent, so retrying the same span would block this writer shard.
			log.Warnf(
				"Rejected invalid execution-flow span write bucket=%s reason=%s rejected_writes=%d",
				bucket,
				validationError.reason,
				rejected,
			)
		}
	}
	return points, nil
}

func influxPointForSpan(span *protos.SpanInfo) *write.Point {
	point, err := influxPointForSpanWithEnvironment(span, "")
	if err != nil {
		panic(err)
	}
	return point
}

func influxPointForSpanWithEnvironment(
	span *protos.SpanInfo,
	flowEnvironmentID string,
) (*write.Point, error) {
	tags := map[string]string{
		"name": span.Name,
	}
	if span.ServiceName != "" {
		tags["service"] = span.ServiceName
	}

	startTime := span.StartTime.AsTime()
	endTime := span.EndTime.AsTime()
	isFlowSpan := strings.HasPrefix(span.Name, flowTraceSpanPrefix)
	duration := endTime.Sub(startTime).Microseconds()
	if isFlowSpan {
		duration = 0
	}
	fields := map[string]interface{}{
		"trace_id":       span.TraceId,
		"span_id":        span.SpanId,
		"parent_span_id": span.ParentSpanId,
		"duration_us":    duration,
	}
	if isFlowSpan {
		if flowEnvironmentID == "" {
			return nil, newFlowPointValidationError(
				"missing_flow_environment_id",
				fmt.Sprintf("%s is required for execution-flow spans", flowEnvironmentIDEnv),
			)
		}
		if !flowEnvironmentIDTagPattern.MatchString(flowEnvironmentID) {
			return nil, newFlowPointValidationError(
				"invalid_flow_environment_id",
				fmt.Sprintf(
					"%s must match %s",
					flowEnvironmentIDEnv,
					flowEnvironmentIDTagPattern.String(),
				),
			)
		}
		if !flowSpanIDTagPattern.MatchString(span.SpanId) {
			return nil, newFlowPointValidationError(
				"invalid_span_id",
				fmt.Sprintf("flow span_id must match %s", flowSpanIDTagPattern.String()),
			)
		}
		if callerValue, ok := span.Attributes["flow_environment_id"]; ok &&
			callerValue != flowEnvironmentID {
			return nil, newFlowPointValidationError(
				"flow_environment_id_mismatch",
				"flow_environment_id does not match process environment",
			)
		}
		tags["flow_environment_id"] = flowEnvironmentID
		// Influx identifies a point by measurement, tag set, and timestamp.
		// Preserve the real event time and use the validated span ID to
		// distinguish flow points emitted at the same instant.
		tags["span_id"] = span.SpanId
		delete(fields, "span_id")
		fields["event_time_unix_nano"] = endTime.UnixNano()
	}

	for key, value := range span.Attributes {
		if isFlowSpan && (key == "flow_environment_id" || key == "span_id") {
			continue
		}
		if tagValue, ok := flowAttributeTag(span.Name, key, value); ok {
			tags[key] = tagValue
			continue
		}
		fields[key] = value
	}

	return influxdb2.NewPoint("spans", tags, fields, endTime), nil
}

func executionFlowEnvironmentIDFromEnv() (string, error) {
	value, present := os.LookupEnv(flowEnvironmentIDEnv)
	if !present {
		return "", nil
	}
	if !flowEnvironmentIDTagPattern.MatchString(value) {
		return "", fmt.Errorf(
			"%s must match %s",
			flowEnvironmentIDEnv,
			flowEnvironmentIDTagPattern.String(),
		)
	}
	return value, nil
}

func flowAttributeTag(spanName, key, value string) (string, bool) {
	if !strings.HasPrefix(spanName, flowTraceSpanPrefix) {
		return "", false
	}
	switch key {
	case "flow_id":
		if flowIDTagPattern.MatchString(value) {
			return value, true
		}
	}
	return "", false
}

func (s *InfluxTraceStore) TraceBucketsForSpan(span *protos.SpanInfo) []string {
	if span == nil {
		return []string{s.traceBucket}
	}

	primary := s.traceBucket
	if isCoreTraceSpanName(span.Name) {
		if len(s.traceShardBuckets) > 0 {
			primary = s.traceShardBuckets[stableTraceShardKey(span)%uint32(len(s.traceShardBuckets))]
		} else if s.traceCoreBucket != "" {
			primary = s.traceCoreBucket
		}

		buckets := []string{primary}
		if spanShouldWriteErrorBucket(span) {
			errorBucket := s.traceErrorBucket
			if errorBucket == "" {
				errorBucket = s.traceBucket
			}
			if errorBucket != "" && errorBucket != primary {
				buckets = append(buckets, errorBucket)
			}
		}
		return buckets
	}

	if spanShouldWriteErrorBucket(span) {
		if s.traceErrorBucket != "" {
			primary = s.traceErrorBucket
		}
	} else if s.traceDetailBucket != "" {
		primary = s.traceDetailBucket
	}
	return []string{primary}
}

func isCoreTraceSpanName(name string) bool {
	if strings.HasPrefix(name, flowTraceSpanPrefix) {
		return true
	}
	_, ok := coreTraceSpanNames[name]
	return ok
}

func spanShouldWriteErrorBucket(span *protos.SpanInfo) bool {
	if span == nil {
		return false
	}
	if span.Status == protos.SpanStatus_SPAN_STATUS_ERROR {
		return true
	}
	if v, ok := span.Attributes["final_status"]; ok && v != "" &&
		v != "2" && v != "Completed" && v != "completed" {
		return true
	}
	return false
}

func stableTraceShardKey(span *protos.SpanInfo) uint32 {
	key := ""
	if span != nil {
		if jobID := span.Attributes["job_id"]; jobID != "" {
			key = jobID
		} else if span.TraceId != "" {
			key = span.TraceId
		} else {
			key = span.SpanId
		}
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func (s *InfluxTraceStore) traceBuckets() []string {
	seen := make(map[string]struct{})
	add := func(bucket string) {
		if bucket == "" {
			return
		}
		seen[bucket] = struct{}{}
	}
	add(s.traceBucket)
	add(s.traceCoreBucket)
	add(s.traceDetailBucket)
	add(s.traceErrorBucket)
	for _, bucket := range s.traceShardBuckets {
		add(bucket)
	}
	buckets := make([]string, 0, len(seen))
	for bucket := range seen {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)
	return buckets
}

func (s *InfluxTraceStore) Close() error {
	s.client.Close()
	return nil
}

func (s *InfluxTraceStore) createBucketIfNotExists(bucketName string) error {
	ctx := context.Background()

	if err := s.createOrgIfNotExists(); err != nil {
		return fmt.Errorf("failed to ensure organization exists: %v", err)
	}

	bucketsAPI := s.client.BucketsAPI()
	bucket, _ := bucketsAPI.FindBucketByName(ctx, bucketName)

	if bucket != nil {
		log.Infof("Bucket already exists: %s", bucketName)
		return nil
	}

	log.Infof("Creating bucket: %s", bucketName)
	orgAPI := s.client.OrganizationsAPI()
	org, err := orgAPI.FindOrganizationByName(ctx, s.org)
	if err != nil {
		return fmt.Errorf("failed to find organization: %v", err)
	}

	_, err = bucketsAPI.CreateBucketWithName(ctx, org, bucketName)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %v", err)
	}

	log.Infof("Successfully created bucket: %s", bucketName)
	return nil
}

func (s *InfluxTraceStore) createOrgIfNotExists() error {
	ctx := context.Background()
	orgAPI := s.client.OrganizationsAPI()

	org, _ := orgAPI.FindOrganizationByName(ctx, s.org)

	if org != nil {
		log.Infof("Organization already exists: %s", s.org)
		return nil
	}

	log.Infof("Creating organization: %s", s.org)
	_, err := orgAPI.CreateOrganizationWithName(ctx, s.org)
	if err != nil {
		return fmt.Errorf("failed to create organization: %v", err)
	}

	log.Infof("Successfully created organization: %s", s.org)
	return nil
}
