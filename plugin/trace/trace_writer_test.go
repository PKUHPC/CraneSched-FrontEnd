package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"
)

type fakeTraceDB struct {
	mu            sync.Mutex
	writes        map[string]int
	failOnce      map[string]bool
	writeSequence []string
}

func newFakeTraceDB() *fakeTraceDB {
	return &fakeTraceDB{writes: make(map[string]int), failOnce: make(map[string]bool)}
}

func (f *fakeTraceDB) WriteBatch(
	ctx context.Context,
	points []encodedTracePoint,
) (traceSinkBatchResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	router := testBucketRouter()
	var failed []encodedTracePoint
	for _, point := range points {
		for _, bucket := range router.TraceBucketsForDecision(point.routing) {
			if f.failOnce[bucket] {
				f.failOnce[bucket] = false
				retry := point
				retry.routing.destinations = destinationsForBucket(router, point.routing, bucket)
				failed = append(failed, retry)
				continue
			}
			f.writes[bucket]++
			f.writeSequence = append(f.writeSequence, bucket)
		}
	}
	if len(failed) > 0 {
		return traceSinkBatchResult{failed: failed}, errors.New("injected write failure")
	}
	return traceSinkBatchResult{}, nil
}
func (f *fakeTraceDB) Close(context.Context) error { return nil }

type fakeTraceProcessor struct{}

func (fakeTraceProcessor) Process(raw rawTracePoint) (encodedTracePoint, error) {
	point, err := (protobufTracePointDecoder{}).Decode(raw)
	if err != nil {
		return encodedTracePoint{}, err
	}
	if strings.HasPrefix(point.name, generatedExecutionFlowCatalog.WirePrefix()) {
		flowID, _ := stringTraceAttribute(point, "flow_id")
		point.flow = &executionFlowEnvelope{flowID: flowID}
	}
	return (influxTracePointEncoder{}).Encode(
		NewTracePointRouter().Route(validatedTracePoint{point: point}),
	)
}

func testTraceWriter(db TraceSink, cfg TraceWriterConfig) *TraceWriter {
	return NewTraceWriter(db, fakeTraceProcessor{}, cfg)
}

func testBucketRouter() *traceBucketRouter {
	return &traceBucketRouter{
		traceBucket:       "trace",
		traceCoreBucket:   "core",
		traceDetailBucket: "detail",
		traceErrorBucket:  "error",
	}
}

func destinationsForBucket(
	router TraceBucketRouter,
	decision traceRoutingDecision,
	bucket string,
) []traceDestination {
	var destinations []traceDestination
	for _, destination := range decision.destinations {
		candidate := traceRoutingDecision{destinations: []traceDestination{destination}, shard: decision.shard}
		buckets := router.TraceBucketsForDecision(candidate)
		if len(buckets) == 1 && buckets[0] == bucket {
			destinations = append(destinations, destination)
		}
	}
	return destinations
}

func TestTraceWriterWritesFailedCoreSpanToCoreAndErrorBuckets(t *testing.T) {
	db := newFakeTraceDB()
	writer := testTraceWriter(db, TraceWriterConfig{
		Shards:          1,
		BatchSpans:      8,
		QueueBatches:    8,
		FlushIntervalMs: 1,
		RetryBackoffMs:  1,
	})

	err := writer.Enqueue(context.Background(), []*protos.SpanInfo{{
		Name:       "flow/v1/ctld/job/accepted",
		Status:     protos.SpanStatus_SPAN_STATUS_ERROR,
		Attributes: map[string]string{"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4"},
	}})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	if got := db.writes["core"]; got != 1 {
		t.Fatalf("core writes = %d, want 1", got)
	}
	if got := db.writes["error"]; got != 1 {
		t.Fatalf("error writes = %d, want 1", got)
	}
}

func TestTraceWriterRetriesOnlyFailedBucket(t *testing.T) {
	db := newFakeTraceDB()
	db.failOnce["error"] = true
	writer := testTraceWriter(db, TraceWriterConfig{
		Shards:            1,
		BatchSpans:        8,
		QueueBatches:      8,
		FlushIntervalMs:   1,
		RetryBackoffMs:    1,
		MaxRetryBackoffMs: 2,
	})

	if err := writer.Enqueue(context.Background(), []*protos.SpanInfo{{
		Name:       "flow/v1/ctld/job/accepted",
		Status:     protos.SpanStatus_SPAN_STATUS_ERROR,
		Attributes: map[string]string{"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4"},
	}}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := writer.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	if got := db.writes["core"]; got != 1 {
		t.Fatalf("core writes = %d, want 1", got)
	}
	if got := db.writes["error"]; got != 1 {
		t.Fatalf("error writes = %d, want 1 after retry", got)
	}
}

func TestTraceWriterKeepsValidSiblingWhenOnePointIsMalformed(t *testing.T) {
	db := newFakeTraceDB()
	writer := testTraceWriter(db, TraceWriterConfig{
		Shards: 1, BatchSpans: 8, QueueBatches: 8, FlushIntervalMs: 1,
	})
	err := writer.Enqueue(context.Background(), []*protos.SpanInfo{
		nil,
		{Name: "step/detail", SpanId: "valid-span"},
	})
	if err == nil || !strings.Contains(err.Error(), "trace span is nil") {
		t.Fatalf("enqueue error = %v, want isolated malformed-point error", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if got := db.writes["detail"]; got != 1 {
		t.Fatalf("valid sibling writes = %d, want 1", got)
	}
}

type transientTraceSink struct {
	attempts atomic.Int64
	failures int64
}

func (s *transientTraceSink) WriteBatch(
	_ context.Context,
	points []encodedTracePoint,
) (traceSinkBatchResult, error) {
	if s.attempts.Add(1) <= s.failures {
		return traceSinkBatchResult{failed: points}, errors.New("transient write failure")
	}
	return traceSinkBatchResult{}, nil
}

func (*transientTraceSink) Close(context.Context) error { return nil }

func TestTraceWriterRetriesTransientFailureDuringBoundedClose(t *testing.T) {
	sink := &transientTraceSink{failures: 2}
	writer := testTraceWriter(sink, TraceWriterConfig{
		Shards: 1, BatchSpans: 8, QueueBatches: 8, FlushIntervalMs: 1000,
		RetryBackoffMs: 1, MaxRetryBackoffMs: 2, CloseTimeoutMs: 100,
	})
	if err := writer.Enqueue(context.Background(), []*protos.SpanInfo{{Name: "step/detail"}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("bounded close retry failed: %v", err)
	}
	if got := sink.attempts.Load(); got != 3 {
		t.Fatalf("write attempts = %d, want 3", got)
	}
}

type blockingTraceStore struct {
	started     chan struct{}
	release     chan struct{}
	writeExited chan struct{}
	startOnce   sync.Once
	exitOnce    sync.Once
}

func newBlockingTraceStore(release chan struct{}) *blockingTraceStore {
	return &blockingTraceStore{
		started:     make(chan struct{}),
		release:     release,
		writeExited: make(chan struct{}),
	}
}

func (s *blockingTraceStore) WriteBatch(
	ctx context.Context,
	points []encodedTracePoint,
) (traceSinkBatchResult, error) {
	s.startOnce.Do(func() { close(s.started) })
	defer s.exitOnce.Do(func() { close(s.writeExited) })
	if s.release == nil {
		<-ctx.Done()
		return traceSinkBatchResult{failed: points}, ctx.Err()
	}
	select {
	case <-s.release:
		return traceSinkBatchResult{}, nil
	case <-ctx.Done():
		return traceSinkBatchResult{failed: points}, ctx.Err()
	}
}

func (s *blockingTraceStore) Close(context.Context) error { return nil }

func TestTraceWriterWriteDeadlineIsSurfacedOnClose(t *testing.T) {
	store := newBlockingTraceStore(nil)
	writer := testTraceWriter(store, TraceWriterConfig{
		Shards:            1,
		BatchSpans:        8,
		QueueBatches:      8,
		FlushIntervalMs:   1,
		RetryBackoffMs:    1,
		MaxRetryBackoffMs: 5,
		WriteTimeoutMs:    20,
		CloseTimeoutMs:    80,
	})
	if err := writer.Enqueue(context.Background(), []*protos.SpanInfo{{Name: "step/detail"}}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	err := writer.Close()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close error = %v, want context deadline exceeded", err)
	}
	select {
	case <-store.writeExited:
	default:
		t.Fatal("Close returned before the timed-out store write exited")
	}
}

func TestTraceWriterCloseJoinsInFlightWrite(t *testing.T) {
	release := make(chan struct{})
	store := newBlockingTraceStore(release)
	writer := testTraceWriter(store, TraceWriterConfig{
		Shards:          1,
		BatchSpans:      8,
		QueueBatches:    8,
		FlushIntervalMs: 1,
		WriteTimeoutMs:  1000,
	})
	if err := writer.Enqueue(context.Background(), []*protos.SpanInfo{{Name: "step/detail"}}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	closed := make(chan error, 1)
	go func() { closed <- writer.Close() }()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("store write did not start")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before store write was released: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not join the released store write")
	}
}

type uncooperativeTraceSink struct {
	started    chan struct{}
	release    chan struct{}
	startOnce  sync.Once
	writes     atomic.Int64
	closeCalls atomic.Int64
	closeErr   atomic.Bool
}

func newUncooperativeTraceSink() *uncooperativeTraceSink {
	return &uncooperativeTraceSink{started: make(chan struct{}), release: make(chan struct{})}
}

func (s *uncooperativeTraceSink) WriteBatch(
	context.Context,
	[]encodedTracePoint,
) (traceSinkBatchResult, error) {
	s.startOnce.Do(func() { close(s.started) })
	<-s.release
	s.writes.Add(1)
	return traceSinkBatchResult{}, nil
}

func (s *uncooperativeTraceSink) Close(context.Context) error {
	s.closeCalls.Add(1)
	if s.closeErr.Swap(false) {
		return errors.New("injected close failure")
	}
	return nil
}

func TestTraceWriterCloseTimeoutCanBeRetried(t *testing.T) {
	sink := newUncooperativeTraceSink()
	writer := testTraceWriter(sink, TraceWriterConfig{
		Shards: 1, BatchSpans: 1, QueueBatches: 1, FlushIntervalMs: 1,
		WriteTimeoutMs: 1000, CloseTimeoutMs: 20,
	})
	if err := writer.Enqueue(context.Background(), []*protos.SpanInfo{{Name: "step/detail"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sink.started:
	case <-time.After(time.Second):
		t.Fatal("sink write did not start")
	}

	err := writer.Close()
	if err == nil || !strings.Contains(err.Error(), "did not drain") {
		t.Fatalf("first Close error = %v", err)
	}
	if writer.Drained() {
		t.Fatal("writer reported drained while sink was blocked")
	}
	close(sink.release)
	if err := writer.Close(); err != nil {
		t.Fatalf("retry Close failed: %v", err)
	}
	if !writer.Drained() {
		t.Fatal("writer did not drain after sink release")
	}
}

func TestTraceWriterCloseUnblocksBlockedEnqueue(t *testing.T) {
	sink := newUncooperativeTraceSink()
	writer := testTraceWriter(sink, TraceWriterConfig{
		Shards: 1, BatchSpans: 1, QueueBatches: 1, FlushIntervalMs: 1,
		WriteTimeoutMs: 1000, CloseTimeoutMs: 1000,
	})
	if err := writer.Enqueue(context.Background(), []*protos.SpanInfo{{Name: "first"}}); err != nil {
		t.Fatal(err)
	}
	<-sink.started
	if err := writer.Enqueue(context.Background(), []*protos.SpanInfo{{Name: "second"}}); err != nil {
		t.Fatal(err)
	}
	enqueueDone := make(chan error, 1)
	go func() {
		enqueueDone <- writer.Enqueue(context.Background(), []*protos.SpanInfo{{Name: "third"}})
	}()
	closeDone := make(chan error, 1)
	go func() { closeDone <- writer.Close() }()
	select {
	case err := <-enqueueDone:
		if err == nil || !strings.Contains(err.Error(), "stopping") {
			t.Fatalf("blocked Enqueue error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Enqueue")
	}
	close(sink.release)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after sink release")
	}
}

func TestTraceRuntimeDoesNotCloseActiveSinkAndRetries(t *testing.T) {
	sink := newUncooperativeTraceSink()
	writer := testTraceWriter(sink, TraceWriterConfig{
		Shards: 1, BatchSpans: 1, QueueBatches: 1, FlushIntervalMs: 1,
		WriteTimeoutMs: 1000, CloseTimeoutMs: 20,
	})
	runtime := newTraceRuntime(writer, sink)
	if err := writer.Enqueue(context.Background(), []*protos.SpanInfo{{Name: "step/detail"}}); err != nil {
		t.Fatal(err)
	}
	<-sink.started
	if err := runtime.Close(); err == nil {
		t.Fatal("runtime Close succeeded while writer was blocked")
	}
	if got := sink.closeCalls.Load(); got != 0 {
		t.Fatalf("sink Close called %d times while writer was active", got)
	}
	close(sink.release)
	if err := runtime.Close(); err != nil {
		t.Fatalf("runtime Close retry failed: %v", err)
	}
	if got := sink.closeCalls.Load(); got != 1 {
		t.Fatalf("sink Close calls = %d, want 1", got)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("idempotent runtime Close failed: %v", err)
	}
	if got := sink.closeCalls.Load(); got != 1 {
		t.Fatalf("idempotent Close called sink %d times", got)
	}
}

func TestTraceRuntimeRetriesSinkCloseFailure(t *testing.T) {
	sink := newUncooperativeTraceSink()
	close(sink.release)
	sink.closeErr.Store(true)
	writer := testTraceWriter(sink, TraceWriterConfig{Shards: 1, CloseTimeoutMs: 100})
	runtime := newTraceRuntime(writer, sink)
	if err := runtime.Close(); err == nil || !strings.Contains(err.Error(), "close trace sink") {
		t.Fatalf("first runtime Close error = %v", err)
	}
	if runtime.closed() {
		t.Fatal("runtime closed after sink close failure")
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("runtime Close retry failed: %v", err)
	}
	if got := sink.closeCalls.Load(); got != 2 {
		t.Fatalf("sink Close calls = %d, want 2", got)
	}
}

type deadlineCloseSink struct {
	allowClose atomic.Bool
	closeCalls atomic.Int64
}

func (*deadlineCloseSink) WriteBatch(
	context.Context,
	[]encodedTracePoint,
) (traceSinkBatchResult, error) {
	return traceSinkBatchResult{}, nil
}

func (s *deadlineCloseSink) Close(ctx context.Context) error {
	s.closeCalls.Add(1)
	if s.allowClose.Load() {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestTraceRuntimeBoundsAndRetriesSinkClose(t *testing.T) {
	sink := &deadlineCloseSink{}
	writer := testTraceWriter(sink, TraceWriterConfig{Shards: 1, CloseTimeoutMs: 20})
	runtime := newTraceRuntime(writer, sink)
	started := time.Now()
	err := runtime.Close()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first runtime Close error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("sink close exceeded bound: %s", elapsed)
	}
	if runtime.closed() {
		t.Fatal("runtime closed after timed-out sink close")
	}
	sink.allowClose.Store(true)
	if err := runtime.Close(); err != nil {
		t.Fatalf("runtime Close retry failed: %v", err)
	}
	if got := sink.closeCalls.Load(); got != 2 {
		t.Fatalf("sink close calls = %d, want 2", got)
	}
}

func TestGlobalTraceWriterSnapshotRacesWithClose(t *testing.T) {
	sink := newFakeTraceDB()
	writer := testTraceWriter(sink, TraceWriterConfig{
		Shards: 2, BatchSpans: 8, QueueBatches: 32, FlushIntervalMs: 1,
		CloseTimeoutMs: 1000,
	})
	var owner GlobalTrace
	if err := owner.install(newTraceRuntime(writer, sink)); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for index := 0; index < 16; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; attempt < 100; attempt++ {
				snapshot := owner.writerSnapshot()
				if snapshot == nil {
					return
				}
				_ = snapshot.Enqueue(context.Background(), []*protos.SpanInfo{{Name: "step/detail"}})
			}
		}()
	}
	if err := owner.close(); err != nil {
		t.Fatalf("owner Close failed: %v", err)
	}
	wg.Wait()
	if owner.writerSnapshot() != nil {
		t.Fatal("owner exposed writer after Close")
	}
}

func TestTraceWriterDrainsEveryAcceptedPointAtStopBoundary(t *testing.T) {
	sink := newFakeTraceDB()
	writer := testTraceWriter(sink, TraceWriterConfig{
		Shards: 4, BatchSpans: 16, QueueBatches: 128, FlushIntervalMs: 1,
		CloseTimeoutMs: 1000,
	})
	var accepted atomic.Int64
	for index := 0; index < 16; index++ {
		if err := writer.Enqueue(context.Background(), []*protos.SpanInfo{{Name: "step/detail"}}); err != nil {
			t.Fatal(err)
		}
		accepted.Add(1)
	}

	start := make(chan struct{})
	var enqueues sync.WaitGroup
	for index := 0; index < 128; index++ {
		enqueues.Add(1)
		go func() {
			defer enqueues.Done()
			<-start
			if err := writer.Enqueue(
				context.Background(), []*protos.SpanInfo{{Name: "step/detail"}},
			); err == nil {
				accepted.Add(1)
			}
		}()
	}
	closeDone := make(chan error, 1)
	go func() {
		<-start
		closeDone <- writer.Close()
	}()
	close(start)
	enqueues.Wait()
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}

	sink.mu.Lock()
	written := 0
	for _, count := range sink.writes {
		written += count
	}
	sink.mu.Unlock()
	if got, want := int64(written), accepted.Load(); got != want {
		t.Fatalf("written points = %d, accepted = %d", got, want)
	}
}
