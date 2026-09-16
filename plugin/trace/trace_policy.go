package main

import (
	"hash/fnv"

	"CraneFrontEnd/generated/protos"
)

type traceDestination uint8

const (
	traceDestinationDetail traceDestination = iota
	traceDestinationCore
	traceDestinationError
)

type traceRoutingDecision struct {
	destinations []traceDestination
	shard        uint32
}

type TracePointRouter interface {
	Route(validatedTracePoint) routedTracePoint
}

type tracePointRouter struct{}

var coreTracePointNames = map[string]struct{}{
	"job/pending":   {},
	"job/lifecycle": {},
	"step/execute":  {},
	"job/end":       {},
}

func NewTracePointRouter() TracePointRouter {
	return tracePointRouter{}
}

func (tracePointRouter) Route(validated validatedTracePoint) routedTracePoint {
	point := validated.point
	_, legacyCore := coreTracePointNames[point.name]
	isCore := point.flow != nil || legacyCore
	isError := tracePointIsError(point)

	var destinations []traceDestination
	switch {
	case isCore:
		destinations = []traceDestination{traceDestinationCore}
		if isError {
			destinations = append(destinations, traceDestinationError)
		}
	case isError:
		destinations = []traceDestination{traceDestinationError}
	default:
		destinations = []traceDestination{traceDestinationDetail}
	}
	return routedTracePoint{
		point: point,
		routing: traceRoutingDecision{
			destinations: destinations,
			shard:        stableTraceShardKey(point),
		},
	}
}

func tracePointIsError(point typedTracePoint) bool {
	if point.status == protos.SpanStatus_SPAN_STATUS_ERROR ||
		(point.flow != nil && point.flow.pipelineFault) {
		return true
	}
	finalStatus, ok := stringTraceAttribute(point, "final_status")
	return ok && finalStatus != "" && finalStatus != "2" &&
		finalStatus != "Completed" && finalStatus != "completed"
}

func stableTraceShardKey(point typedTracePoint) uint32 {
	key := ""
	if point.flow != nil && point.flow.flowID != "" {
		key = point.flow.flowID
	}
	if key == "" {
		key, _ = stringTraceAttribute(point, "job_id")
	}
	if key == "" {
		key = point.traceID
	}
	if key == "" {
		key = point.spanID
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return hasher.Sum32()
}
