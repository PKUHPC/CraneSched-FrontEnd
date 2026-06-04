package main

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/plugin/monitor/pkg/db"
)

const (
	defaultTraceQueueBatches = 4096
	maxTraceWriteBatchSpans  = 4096
	traceWriteFlushInterval  = 50 * time.Millisecond
	traceWriterCloseTimeout  = 10 * time.Second
)

type TraceWriter struct {
	db    db.DBInterface
	queue chan []*protos.SpanInfo
	stop  chan struct{}
	done  chan struct{}

	stopped        atomic.Bool
	failedEnqueues atomic.Uint64
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

	pending := make([]*protos.SpanInfo, 0, maxTraceWriteBatchSpans)
	flush := func() {
		for start := 0; start < len(pending); start += maxTraceWriteBatchSpans {
			end := start + maxTraceWriteBatchSpans
			if end > len(pending) {
				end = len(pending)
			}
			if err := w.db.SaveSpans(pending[start:end]); err != nil {
				log.Errorf("Failed to save async trace spans: %v", err)
			}
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
					return
				}
			}
		}
	}
}
