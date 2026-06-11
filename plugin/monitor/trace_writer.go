package main

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"time"

	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/plugin/monitor/pkg/db"
)

const (
	defaultTraceQueueBatches = 4096
	maxTraceWriteBatchSpans  = 4096
	traceWriteFlushInterval  = 50 * time.Millisecond
	traceWriterStatsInterval = 5 * time.Second
	traceWriterCloseTimeout  = 10 * time.Second
)

type TraceWriter struct {
	db    db.DBInterface
	queue chan []*protos.SpanInfo
	stop  chan struct{}
	done  chan struct{}

	stopped         atomic.Bool
	enqueuedBatches atomic.Uint64
	enqueuedSpans   atomic.Uint64
	failedEnqueues  atomic.Uint64
}

func NewTraceWriter(database db.DBInterface) *TraceWriter {
	writer := &TraceWriter{
		db:    database,
		queue: make(chan []*protos.SpanInfo, defaultTraceQueueBatches),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	go writer.run()
	return writer
}

func (w *TraceWriter) Enqueue(ctx context.Context, spans []*protos.SpanInfo) error {
	if len(spans) == 0 || w == nil || w.stopped.Load() {
		return nil
	}

	batch := append([]*protos.SpanInfo(nil), spans...)
	select {
	case w.queue <- batch:
		w.enqueuedBatches.Add(1)
		w.enqueuedSpans.Add(uint64(len(batch)))
		return nil
	case <-ctx.Done():
		failed := w.failedEnqueues.Add(1)
		if failed == 1 || failed%128 == 0 {
			log.Warnf("Trace writer enqueue canceled %d times: %v", failed, ctx.Err())
		}
		return ctx.Err()
	case <-w.stop:
		return errors.New("trace writer is stopping")
	}
}

func (w *TraceWriter) Close() {
	if w == nil || w.stopped.Swap(true) {
		return
	}

	close(w.stop)

	timer := time.NewTimer(traceWriterCloseTimeout)
	defer timer.Stop()

	select {
	case <-w.done:
	case <-timer.C:
		log.Warnf("Trace writer did not finish draining within %s", traceWriterCloseTimeout)
	}
}

func (w *TraceWriter) run() {
	defer close(w.done)

	ticker := time.NewTicker(traceWriteFlushInterval)
	defer ticker.Stop()
	statsTicker := time.NewTicker(traceWriterStatsInterval)
	defer statsTicker.Stop()

	pending := make([]*protos.SpanInfo, 0, maxTraceWriteBatchSpans)
	stats := traceWriterStats{}
	var lastEnqueuedBatches uint64
	var lastEnqueuedSpans uint64
	var lastFailedEnqueues uint64

	logStats := func(final bool) {
		enqueuedBatches := w.enqueuedBatches.Load()
		enqueuedSpans := w.enqueuedSpans.Load()
		failedEnqueues := w.failedEnqueues.Load()
		snapshot := stats.snapshot()
		stats.reset()

		enqueueBatchesDelta := enqueuedBatches - lastEnqueuedBatches
		enqueueSpansDelta := enqueuedSpans - lastEnqueuedSpans
		failedEnqueuesDelta := failedEnqueues - lastFailedEnqueues
		lastEnqueuedBatches = enqueuedBatches
		lastEnqueuedSpans = enqueuedSpans
		lastFailedEnqueues = failedEnqueues

		queueLen := len(w.queue)
		shouldLog := final || enqueueBatchesDelta > 0 || snapshot.flushCount > 0 ||
			queueLen > 0 || len(pending) > 0 || snapshot.writeErrors > 0 ||
			failedEnqueuesDelta > 0
		if !shouldLog {
			return
		}

		msg := "TraceWriterStats final=%t queue_len_batches=%d queue_cap_batches=%d " +
			"pending_spans=%d enqueue_batches=%d enqueue_spans=%d flush_count=%d " +
			"flush_spans=%d flush_batch_spans_p50=%d flush_batch_spans_p95=%d " +
			"flush_batch_spans_p99=%d flush_batch_spans_max=%d " +
			"flush_elapsed_ms_p50=%d flush_elapsed_ms_p95=%d " +
			"flush_elapsed_ms_p99=%d flush_elapsed_ms_max=%d write_errors=%d " +
			"enqueue_canceled=%d"
		args := []any{
			final, queueLen, cap(w.queue), len(pending), enqueueBatchesDelta,
			enqueueSpansDelta, snapshot.flushCount, snapshot.flushSpans,
			snapshot.batchP50, snapshot.batchP95, snapshot.batchP99,
			snapshot.batchMax, snapshot.elapsedP50Ms, snapshot.elapsedP95Ms,
			snapshot.elapsedP99Ms, snapshot.elapsedMaxMs, snapshot.writeErrors,
			failedEnqueuesDelta,
		}
		if queueLen > cap(w.queue)*3/4 || snapshot.elapsedP95Ms >= 500 ||
			snapshot.writeErrors > 0 || failedEnqueuesDelta > 0 {
			log.Warnf(msg, args...)
		} else {
			log.Infof(msg, args...)
		}
	}

	flush := func() {
		for start := 0; start < len(pending); start += maxTraceWriteBatchSpans {
			end := start + maxTraceWriteBatchSpans
			if end > len(pending) {
				end = len(pending)
			}
			batch := pending[start:end]
			begin := time.Now()
			if err := w.db.SaveSpans(batch); err != nil {
				stats.writeErrors++
				log.Errorf("Failed to save async trace spans: %v", err)
			}
			stats.record(len(batch), time.Since(begin))
		}
		pending = pending[:0]
	}

	for {
		select {
		case spans := <-w.queue:
			pending = append(pending, spans...)
			if len(pending) >= maxTraceWriteBatchSpans {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-statsTicker.C:
			logStats(false)
		case <-w.stop:
			for {
				select {
				case spans := <-w.queue:
					pending = append(pending, spans...)
					if len(pending) >= maxTraceWriteBatchSpans {
						flush()
					}
				default:
					flush()
					logStats(true)
					return
				}
			}
		}
	}
}

type traceWriterStats struct {
	flushCount  uint64
	flushSpans  uint64
	writeErrors uint64
	batchSizes  []int
	elapsedMs   []int64
}

type traceWriterStatsSnapshot struct {
	flushCount   uint64
	flushSpans   uint64
	writeErrors  uint64
	batchP50     int
	batchP95     int
	batchP99     int
	batchMax     int
	elapsedP50Ms int64
	elapsedP95Ms int64
	elapsedP99Ms int64
	elapsedMaxMs int64
}

func (s *traceWriterStats) record(batchSize int, elapsed time.Duration) {
	if batchSize == 0 {
		return
	}
	s.flushCount++
	s.flushSpans += uint64(batchSize)
	s.batchSizes = append(s.batchSizes, batchSize)
	s.elapsedMs = append(s.elapsedMs, elapsed.Milliseconds())
}

func (s *traceWriterStats) reset() {
	s.flushCount = 0
	s.flushSpans = 0
	s.writeErrors = 0
	s.batchSizes = s.batchSizes[:0]
	s.elapsedMs = s.elapsedMs[:0]
}

func (s *traceWriterStats) snapshot() traceWriterStatsSnapshot {
	return traceWriterStatsSnapshot{
		flushCount:   s.flushCount,
		flushSpans:   s.flushSpans,
		writeErrors:  s.writeErrors,
		batchP50:     percentileInt(s.batchSizes, 50),
		batchP95:     percentileInt(s.batchSizes, 95),
		batchP99:     percentileInt(s.batchSizes, 99),
		batchMax:     percentileInt(s.batchSizes, 100),
		elapsedP50Ms: percentileInt64(s.elapsedMs, 50),
		elapsedP95Ms: percentileInt64(s.elapsedMs, 95),
		elapsedP99Ms: percentileInt64(s.elapsedMs, 99),
		elapsedMaxMs: percentileInt64(s.elapsedMs, 100),
	}
}

func percentileInt(values []int, percentile int) int {
	if len(values) == 0 {
		return 0
	}
	copied := append([]int(nil), values...)
	sort.Ints(copied)
	idx := percentileIndex(len(copied), percentile)
	return copied[idx]
}

func percentileInt64(values []int64, percentile int) int64 {
	if len(values) == 0 {
		return 0
	}
	copied := append([]int64(nil), values...)
	sort.Slice(copied, func(i, j int) bool { return copied[i] < copied[j] })
	idx := percentileIndex(len(copied), percentile)
	return copied[idx]
}

func percentileIndex(length int, percentile int) int {
	if length <= 1 {
		return 0
	}
	idx := (length*percentile+99)/100 - 1
	if idx < 0 {
		return 0
	}
	if idx >= length {
		return length - 1
	}
	return idx
}
