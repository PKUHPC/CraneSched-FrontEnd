package main

import "context"

type traceSinkBatchResult struct {
	failed []encodedTracePoint
}

// TraceSink is the batched persistence boundary. Bucket selection and the
// concrete Influx representation stay behind this interface.
type TraceSink interface {
	WriteBatch(context.Context, []encodedTracePoint) (traceSinkBatchResult, error)
	Close(context.Context) error
}
