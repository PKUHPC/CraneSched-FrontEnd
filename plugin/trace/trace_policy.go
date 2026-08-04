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

func NewTracePointRouter() TracePointRouter {
	return tracePointRouter{}
}

func (tracePointRouter) Route(validated validatedTracePoint) routedTracePoint {
	point := validated.point
	destinations := []traceDestination{traceDestinationDetail}
	if point.flow != nil {
		destinations[0] = traceDestinationCore
	}
	if point.status == protos.SpanStatus_SPAN_STATUS_ERROR ||
		(point.flow != nil && point.flow.pipelineFault) {
		destinations = append(destinations, traceDestinationError)
	}
	return routedTracePoint{
		point: point,
		routing: traceRoutingDecision{
			destinations: destinations,
			shard:        stableTraceShardKey(point),
		},
	}
}

func stableTraceShardKey(point typedTracePoint) uint32 {
	key := point.traceID
	if point.flow != nil && point.flow.flowID != "" {
		key = point.flow.flowID
	}
	if key == "" {
		key = point.spanID
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return hasher.Sum32()
}
