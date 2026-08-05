package main

import "context"

type traceSinkBatchResult struct {
	// failed contains destination-scoped points that are safe to retry.
	failed []encodedTracePoint
	// dropped contains destination-scoped points rejected permanently by the
	// sink. The writer reports them at shutdown but must not let them block
	// later points in the shard.
	dropped []encodedTracePoint
}

// TraceSink is the batched persistence boundary. Bucket selection and the
// concrete Influx representation stay behind this interface.
type TraceSink interface {
	WriteBatch(context.Context, []encodedTracePoint) (traceSinkBatchResult, error)
	Close(context.Context) error
}
