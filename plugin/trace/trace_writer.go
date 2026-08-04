package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"CraneFrontEnd/generated/protos"
)

const (
	traceWriterStatsInterval = 5 * time.Second
)

type TraceWriter struct {
	workers   []*traceBatchWorker
	processor TracePointProcessor

	stop         chan struct{}
	enqueuesDone chan struct{}
	done         chan struct{}
	workerErrors chan error
	lifecycle    sync.RWMutex
	stopped      atomic.Bool
	stopOnce     sync.Once
	resultOnce   sync.Once
	closeErr     error
	closeTimeout time.Duration
}

type traceBatchWorker struct {
	id    int
	sink  TraceSink
	cfg   TraceWriterConfig
	queue chan []encodedTracePoint

	enqueuedBatches atomic.Uint64
	enqueuedSpans   atomic.Uint64
	failedEnqueues  atomic.Uint64
}

func NewTraceWriter(
	sink TraceSink,
	processor TracePointProcessor,
	writerConfig TraceWriterConfig,
) *TraceWriter {
	normalizeTraceWriterConfig(&writerConfig)
	writer := &TraceWriter{
		workers:      make([]*traceBatchWorker, writerConfig.Shards),
		processor:    processor,
		stop:         make(chan struct{}),
		enqueuesDone: make(chan struct{}),
		done:         make(chan struct{}),
		workerErrors: make(chan error, writerConfig.Shards),
		closeTimeout: time.Duration(writerConfig.CloseTimeoutMs) * time.Millisecond,
	}
	var wg sync.WaitGroup
	wg.Add(writerConfig.Shards)
	for i := range writer.workers {
		worker := &traceBatchWorker{
			id:    i,
			sink:  sink,
			cfg:   writerConfig,
			queue: make(chan []encodedTracePoint, writerConfig.QueueBatches),
		}
		writer.workers[i] = worker
		go worker.run(writer.stop, writer.enqueuesDone, writer.workerErrors, &wg)
	}
	go func() {
		wg.Wait()
		close(writer.workerErrors)
		close(writer.done)
	}()
	return writer
}

func (w *TraceWriter) Enqueue(ctx context.Context, spans []*protos.SpanInfo) error {
	if w == nil || len(spans) == 0 {
		return nil
	}
	w.lifecycle.RLock()
	defer w.lifecycle.RUnlock()
	if w.stopped.Load() {
		return errors.New("trace writer is stopping")
	}
	if w.processor == nil {
		return errors.New("trace point processor is not configured")
	}

	byShard := make(map[int][]encodedTracePoint)
	var pointErrors []error
	for index, span := range spans {
		point, err := w.processor.Process(rawTracePoint{span: span})
		if err != nil {
			pointErrors = append(pointErrors, fmt.Errorf("normalize trace span %d: %w", index, err))
			continue
		}
		shardID := int(point.routing.shard % uint32(len(w.workers)))
		byShard[shardID] = append(byShard[shardID], point)
	}

	for shardID, shardPoints := range byShard {
		batch := append([]encodedTracePoint(nil), shardPoints...)
		worker := w.workers[shardID]
		select {
		case worker.queue <- batch:
			worker.enqueuedBatches.Add(1)
			worker.enqueuedSpans.Add(uint64(len(batch)))
		case <-ctx.Done():
			failed := worker.failedEnqueues.Add(1)
			if failed == 1 || failed%128 == 0 {
				log.Warnf("Trace writer enqueue canceled shard_id=%d count=%d err=%v",
					shardID, failed, ctx.Err())
			}
			return ctx.Err()
		case <-w.stop:
			return errors.New("trace writer is stopping")
		}
	}
	return errors.Join(pointErrors...)
}

func (w *TraceWriter) Close() error {
	if w == nil {
		return nil
	}
	w.stopped.Store(true)
	w.stopOnce.Do(func() {
		close(w.stop)
		w.lifecycle.Lock()
		close(w.enqueuesDone)
		w.lifecycle.Unlock()
	})

	timer := time.NewTimer(w.closeTimeout)
	defer timer.Stop()
	select {
	case <-w.done:
		w.resultOnce.Do(func() {
			var shardErrors []error
			for err := range w.workerErrors {
				shardErrors = append(shardErrors, err)
			}
			w.closeErr = errors.Join(shardErrors...)
		})
		return w.closeErr
	case <-timer.C:
		return fmt.Errorf("trace writer did not drain within %s", w.closeTimeout)
	}
}

func (w *TraceWriter) Drained() bool {
	if w == nil {
		return true
	}
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

func (s *traceBatchWorker) run(
	stop <-chan struct{},
	enqueuesDone <-chan struct{},
	shardErrors chan<- error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	var finalError error
	defer func() {
		if finalError != nil {
			shardErrors <- finalError
		}
	}()

	ticker := time.NewTicker(time.Duration(s.cfg.FlushIntervalMs) * time.Millisecond)
	defer ticker.Stop()
	statsTicker := time.NewTicker(traceWriterStatsInterval)
	defer statsTicker.Stop()

	pending := make([]encodedTracePoint, 0, s.cfg.BatchSpans)
	stats := traceWriterStats{}
	var lastEnqueuedBatches uint64
	var lastEnqueuedSpans uint64
	var lastFailedEnqueues uint64
	var retryBackoff time.Duration
	var nextRetry time.Time
	var oldestPending time.Time
	var lastWriteError error
	var shutdownDeadline time.Time

	logStats := func(final bool) {
		enqueuedBatches := s.enqueuedBatches.Load()
		enqueuedSpans := s.enqueuedSpans.Load()
		failedEnqueues := s.failedEnqueues.Load()
		snapshot := stats.snapshot()
		stats.reset()

		enqueueBatchesDelta := enqueuedBatches - lastEnqueuedBatches
		enqueueSpansDelta := enqueuedSpans - lastEnqueuedSpans
		failedEnqueuesDelta := failedEnqueues - lastFailedEnqueues
		lastEnqueuedBatches = enqueuedBatches
		lastEnqueuedSpans = enqueuedSpans
		lastFailedEnqueues = failedEnqueues

		queueLen := len(s.queue)
		oldestPendingMs := int64(0)
		if !oldestPending.IsZero() && len(pending) > 0 {
			oldestPendingMs = time.Since(oldestPending).Milliseconds()
		}
		shouldLog := final || enqueueBatchesDelta > 0 || snapshot.flushCount > 0 ||
			queueLen > 0 || len(pending) > 0 || snapshot.writeErrors > 0 ||
			failedEnqueuesDelta > 0 || snapshot.retryCount > 0
		if !shouldLog {
			return
		}

		msg := "TraceWriterStats final=%t shard_id=%d queue_len_batches=%d queue_cap_batches=%d " +
			"pending_spans=%d oldest_pending_ms=%d enqueue_batches=%d enqueue_spans=%d " +
			"flush_count=%d flush_spans=%d flush_batch_spans_p50=%d flush_batch_spans_p95=%d " +
			"flush_batch_spans_p99=%d flush_batch_spans_max=%d flush_elapsed_ms_p50=%d " +
			"flush_elapsed_ms_p95=%d flush_elapsed_ms_p99=%d flush_elapsed_ms_max=%d " +
			"write_errors=%d retry_count=%d retry_spans=%d dropped_spans=%d enqueue_canceled=%d"
		args := []any{
			final, s.id, queueLen, cap(s.queue), len(pending), oldestPendingMs,
			enqueueBatchesDelta, enqueueSpansDelta, snapshot.flushCount,
			snapshot.flushSpans, snapshot.batchP50, snapshot.batchP95,
			snapshot.batchP99, snapshot.batchMax, snapshot.elapsedP50Ms,
			snapshot.elapsedP95Ms, snapshot.elapsedP99Ms, snapshot.elapsedMaxMs,
			snapshot.writeErrors, snapshot.retryCount, snapshot.retrySpans,
			snapshot.droppedSpans, failedEnqueuesDelta,
		}
		if queueLen > cap(s.queue)*3/4 || snapshot.elapsedP95Ms >= 500 ||
			snapshot.writeErrors > 0 || failedEnqueuesDelta > 0 ||
			snapshot.retryCount > 0 || oldestPendingMs >= 5000 {
			log.Warnf(msg, args...)
		} else {
			log.Infof(msg, args...)
		}
	}

	flush := func(force bool) {
		if len(pending) == 0 {
			return
		}
		if !force && !nextRetry.IsZero() && time.Now().Before(nextRetry) {
			return
		}

		limit := s.cfg.BatchSpans
		if limit > len(pending) {
			limit = len(pending)
		}
		batch := pending[:limit]
		begin := time.Now()
		writeTimeout := time.Duration(s.cfg.WriteTimeoutMs) * time.Millisecond
		if !shutdownDeadline.IsZero() {
			remaining := time.Until(shutdownDeadline)
			if remaining <= 0 {
				return
			}
			if writeTimeout > remaining {
				writeTimeout = remaining
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
		result, err := s.sink.WriteBatch(ctx, batch)
		cancel()
		if err == nil && len(result.failed) > 0 {
			err = errors.New("trace sink returned failed points without an error")
		}
		if err != nil {
			lastWriteError = err
			failed := result.failed
			if len(failed) == 0 {
				failed = append([]encodedTracePoint(nil), batch...)
			}
			stats.writeErrors++
			stats.retryCount++
			stats.retrySpans += uint64(len(failed))
			if retryBackoff == 0 {
				retryBackoff = time.Duration(s.cfg.RetryBackoffMs) * time.Millisecond
			} else {
				retryBackoff *= 2
				maxBackoff := time.Duration(s.cfg.MaxRetryBackoffMs) * time.Millisecond
				if retryBackoff > maxBackoff {
					retryBackoff = maxBackoff
				}
			}
			nextRetry = time.Now().Add(retryBackoff)
			log.Errorf("Failed to save async trace spans shard_id=%d batch_spans=%d retry_backoff_ms=%d: %v",
				s.id, len(failed), retryBackoff.Milliseconds(), err)
			nextPending := make([]encodedTracePoint, 0, len(failed)+len(pending)-limit)
			nextPending = append(nextPending, failed...)
			nextPending = append(nextPending, pending[limit:]...)
			pending = nextPending
			stats.record(len(batch), time.Since(begin))
			return
		}

		stats.record(len(batch), time.Since(begin))
		lastWriteError = nil
		pending = pending[limit:]
		if len(pending) == 0 {
			oldestPending = time.Time{}
		}
		retryBackoff = 0
		nextRetry = time.Time{}
	}

	appendPending := func(points []encodedTracePoint) {
		if len(points) == 0 {
			return
		}
		if len(pending) == 0 {
			oldestPending = time.Now()
		}
		pending = append(pending, points...)
	}

	for {
		select {
		case spans := <-s.queue:
			appendPending(spans)
			for len(pending) >= s.cfg.BatchSpans {
				before := len(pending)
				flush(false)
				if len(pending) == before {
					break
				}
			}
		case <-ticker.C:
			flush(false)
		case <-statsTicker.C:
			logStats(false)
		case <-stop:
			shutdownBudget := time.Duration(s.cfg.CloseTimeoutMs) * time.Millisecond
			margin := 10 * time.Millisecond
			if margin >= shutdownBudget {
				margin = shutdownBudget / 4
			}
			shutdownDeadline = time.Now().Add(shutdownBudget - margin)
			// Close signals producers before waiting for their read locks. Keep
			// consuming until every in-flight Enqueue has either sent or observed
			// stop, then drain the remaining buffered batches.
			for {
				select {
				case points := <-s.queue:
					appendPending(points)
				case <-enqueuesDone:
					goto drainQueue
				}
			}
		drainQueue:
			for {
				select {
				case spans := <-s.queue:
					appendPending(spans)
					for len(pending) >= s.cfg.BatchSpans {
						before := len(pending)
						flush(true)
						if len(pending) == before {
							break
						}
					}
				default:
					for len(pending) > 0 {
						before := len(pending)
						flush(true)
						if len(pending) == before {
							if time.Now().Before(shutdownDeadline) {
								wait := time.Until(nextRetry)
								if wait <= 0 {
									wait = time.Millisecond
								}
								if remaining := time.Until(shutdownDeadline); wait > remaining {
									wait = remaining
								}
								time.Sleep(wait)
								continue
							}
							stats.droppedSpans += uint64(len(pending))
							log.Warnf("Trace writer shard_id=%d stopped with %d undrained spans", s.id, len(pending))
							finalError = fmt.Errorf(
								"trace writer shard %d dropped %d spans during shutdown: %w",
								s.id,
								len(pending),
								lastWriteError,
							)
							pending = pending[:0]
						}
					}
					logStats(true)
					return
				}
			}
		}
	}
}

type traceWriterStats struct {
	flushCount   uint64
	flushSpans   uint64
	writeErrors  uint64
	retryCount   uint64
	retrySpans   uint64
	droppedSpans uint64
	batchSizes   []int
	elapsedMs    []int64
}

type traceWriterStatsSnapshot struct {
	flushCount   uint64
	flushSpans   uint64
	writeErrors  uint64
	retryCount   uint64
	retrySpans   uint64
	droppedSpans uint64
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
	s.retryCount = 0
	s.retrySpans = 0
	s.droppedSpans = 0
	s.batchSizes = s.batchSizes[:0]
	s.elapsedMs = s.elapsedMs[:0]
}

func (s *traceWriterStats) snapshot() traceWriterStatsSnapshot {
	return traceWriterStatsSnapshot{
		flushCount:   s.flushCount,
		flushSpans:   s.flushSpans,
		writeErrors:  s.writeErrors,
		retryCount:   s.retryCount,
		retrySpans:   s.retrySpans,
		droppedSpans: s.droppedSpans,
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
	if percentile <= 0 {
		return 0
	}
	if percentile >= 100 {
		return length - 1
	}
	idx := (length*percentile + 99) / 100
	if idx <= 0 {
		return 0
	}
	if idx > length {
		return length - 1
	}
	return idx - 1
}
