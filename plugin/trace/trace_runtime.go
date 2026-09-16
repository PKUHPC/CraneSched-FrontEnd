package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type traceRuntimeState uint8

const (
	traceRuntimeRunning traceRuntimeState = iota
	traceRuntimeStopping
	traceRuntimeWriterDrained
	traceRuntimeClosed
)

type traceRuntime struct {
	mu       sync.RWMutex
	closeMu  sync.Mutex
	writer   *TraceWriter
	sink     TraceSink
	state    traceRuntimeState
	closeErr error
}

func newTraceRuntime(writer *TraceWriter, sink TraceSink) *traceRuntime {
	return &traceRuntime{writer: writer, sink: sink, state: traceRuntimeRunning}
}

func (r *traceRuntime) writerSnapshot() *TraceWriter {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state != traceRuntimeRunning {
		return nil
	}
	return r.writer
}

func (r *traceRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.closeMu.Lock()
	defer r.closeMu.Unlock()

	r.mu.Lock()
	if r.state == traceRuntimeClosed {
		err := r.closeErr
		r.mu.Unlock()
		return err
	}
	if r.state == traceRuntimeRunning {
		r.state = traceRuntimeStopping
	}
	state := r.state
	writer := r.writer
	sink := r.sink
	r.mu.Unlock()

	if state == traceRuntimeStopping {
		var writerErr error
		if writer != nil {
			writerErr = writer.Close()
			if !writer.Drained() {
				return fmt.Errorf("drain trace writer: %w", writerErr)
			}
		}
		r.mu.Lock()
		r.closeErr = errors.Join(r.closeErr, wrapError("drain trace writer", writerErr))
		r.state = traceRuntimeWriterDrained
		r.mu.Unlock()
	}

	if sink != nil {
		closeTimeout := defaultTraceCloseTimeoutMs * time.Millisecond
		if writer != nil && writer.closeTimeout > 0 {
			closeTimeout = writer.closeTimeout
		}
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		err := sink.Close(ctx)
		cancel()
		if err != nil {
			return errors.Join(r.closeErr, fmt.Errorf("close trace sink: %w", err))
		}
	}
	r.mu.Lock()
	r.state = traceRuntimeClosed
	r.writer = nil
	r.sink = nil
	err := r.closeErr
	r.mu.Unlock()
	return err
}

func (r *traceRuntime) closed() bool {
	if r == nil {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state == traceRuntimeClosed
}

func wrapError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}

type GlobalTrace struct {
	mu      sync.RWMutex
	runtime *traceRuntime
}

func (g *GlobalTrace) loaded() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.runtime != nil
}

func (g *GlobalTrace) install(runtime *traceRuntime) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.runtime != nil {
		return errors.New("trace plugin is already loaded")
	}
	g.runtime = runtime
	return nil
}

func (g *GlobalTrace) writerSnapshot() *TraceWriter {
	g.mu.RLock()
	runtime := g.runtime
	g.mu.RUnlock()
	return runtime.writerSnapshot()
}

func (g *GlobalTrace) close() error {
	g.mu.RLock()
	runtime := g.runtime
	g.mu.RUnlock()
	if runtime == nil {
		return nil
	}
	err := runtime.Close()
	if runtime.closed() {
		g.mu.Lock()
		if g.runtime == runtime {
			g.runtime = nil
		}
		g.mu.Unlock()
	}
	return err
}
