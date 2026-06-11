package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/plugin/monitor/pkg/config"
	"CraneFrontEnd/plugin/monitor/pkg/types"
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

func (f *fakeTraceDB) SaveNodeEnergy(*types.NodeData) error { return nil }
func (f *fakeTraceDB) SaveJobEnergy(*types.JobData) error   { return nil }
func (f *fakeTraceDB) SaveNodeEvents([]*protos.CranedEventInfo) error {
	return nil
}
func (f *fakeTraceDB) SaveLicenseUsage([]*protos.LicenseInfo) error { return nil }
func (f *fakeTraceDB) SaveSpans(spans []*protos.SpanInfo) error {
	for _, span := range spans {
		for _, bucket := range f.TraceBucketsForSpan(span) {
			if err := f.SaveSpansToBucket(bucket, []*protos.SpanInfo{span}); err != nil {
				return err
			}
		}
	}
	return nil
}
func (f *fakeTraceDB) SaveSpansToBucket(bucket string, spans []*protos.SpanInfo) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOnce[bucket] {
		f.failOnce[bucket] = false
		return errors.New("injected write failure")
	}
	f.writes[bucket] += len(spans)
	f.writeSequence = append(f.writeSequence, bucket)
	return nil
}
func (f *fakeTraceDB) TraceBucketForSpan(span *protos.SpanInfo) string {
	buckets := f.TraceBucketsForSpan(span)
	if len(buckets) == 0 {
		return "legacy"
	}
	return buckets[0]
}
func (f *fakeTraceDB) TraceBucketsForSpan(span *protos.SpanInfo) []string {
	if span.GetName() == "job/end" {
		if span.GetStatus() == protos.SpanStatus_SPAN_STATUS_ERROR {
			return []string{"core", "error"}
		}
		return []string{"core"}
	}
	return []string{"detail"}
}
func (f *fakeTraceDB) Close() error { return nil }

func TestTraceWriterWritesFailedCoreSpanToCoreAndErrorBuckets(t *testing.T) {
	db := newFakeTraceDB()
	writer := NewTraceWriter(db, config.TraceWriterConfig{
		Shards:          1,
		BatchSpans:      8,
		QueueBatches:    8,
		FlushIntervalMs: 1,
		RetryBackoffMs:  1,
	})

	err := writer.Enqueue(context.Background(), []*protos.SpanInfo{{
		Name:   "job/end",
		Status: protos.SpanStatus_SPAN_STATUS_ERROR,
	}})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	writer.Close()

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
	writer := NewTraceWriter(db, config.TraceWriterConfig{
		Shards:            1,
		BatchSpans:        8,
		QueueBatches:      8,
		FlushIntervalMs:   1,
		RetryBackoffMs:    1,
		MaxRetryBackoffMs: 2,
	})

	if err := writer.Enqueue(context.Background(), []*protos.SpanInfo{{
		Name:   "job/end",
		Status: protos.SpanStatus_SPAN_STATUS_ERROR,
	}}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	writer.Close()

	if got := db.writes["core"]; got != 1 {
		t.Fatalf("core writes = %d, want 1", got)
	}
	if got := db.writes["error"]; got != 1 {
		t.Fatalf("error writes = %d, want 1 after retry", got)
	}
}
