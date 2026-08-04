package main

import (
	"fmt"
	"strconv"
)

const flowCollisionSlots = 64

type TracePointEncoder interface {
	Encode(routedTracePoint) (encodedTracePoint, error)
}

type influxTracePointEncoder struct{}

func (*influxTracePointEncoder) Encode(
	routed routedTracePoint,
) (encodedTracePoint, error) {
	point := routed.point
	tags := map[string]string{"name": point.name}
	if point.service != "" {
		tags["service"] = point.service
	}
	fields := map[string]interface{}{
		"trace_id":       point.traceID,
		"span_id":        point.spanID,
		"parent_span_id": point.parentSpanID,
		"duration_us":    point.durationUS,
	}
	if point.flow != nil {
		fields["duration_us"] = int64(0)
		fields[executionFlowStorageEventTimeUnixNano] = point.eventTime.UnixNano()
		tags[executionFlowStorageFlowEnvironmentID] = point.flow.environmentID
		if point.flow.flowID != "" {
			tags["flow_id"] = point.flow.flowID
		}
		if point.flow.eventSequence < 0 {
			return encodedTracePoint{}, fmt.Errorf("execution-flow sequence cannot be negative")
		}
		// Influx identifies points by measurement, tag set, and timestamp. Keep
		// both collision dimensions in fixed 64-value domains: sequence slots
		// separate same-instance events and a stateless service-identity hash
		// separates the practical cross-instance collision set. Both dimensions
		// are deliberately bounded; span_id remains a field instead of creating a
		// series per event. Unbound heartbeat and fault points use slot zero.
		tags[executionFlowStorageFlowSlot] = strconv.FormatInt(
			point.flow.eventSequence%flowCollisionSlots, 10,
		)
		tags[executionFlowStorageFlowInstanceSlot] = strconv.FormatUint(
			uint64(flowInstanceSlot(point.flow)), 10,
		)
	}

	for key, value := range point.attributes {
		if point.flow != nil && (key == executionFlowStorageFlowEnvironmentID ||
			key == executionFlowEnvelopeFlowID || key == "span_id") {
			continue
		}
		fields[key] = value
	}
	return encodedTracePoint{
		tags: tags, fields: fields, time: point.eventTime, routing: routed.routing,
	}, nil
}
