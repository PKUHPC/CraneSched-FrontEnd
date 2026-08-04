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

func (influxTracePointEncoder) Encode(
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
		fields["event_time_unix_nano"] = point.eventTime.UnixNano()
		tags["flow_environment_id"] = point.flow.environmentID
		if point.flow.flowID != "" {
			tags["flow_id"] = point.flow.flowID
		}
		if point.flow.eventSequence < 0 {
			return encodedTracePoint{}, fmt.Errorf("execution-flow sequence cannot be negative")
		}
		// Influx identifies points by measurement, tag set, and timestamp. The
		// bounded slot separates the practical same-nanosecond collision set
		// without turning a unique span identifier into an unbounded tag.
		tags["flow_slot"] = strconv.FormatInt(point.flow.eventSequence%flowCollisionSlots, 10)
	}

	for key, value := range point.attributes {
		if point.flow != nil && (key == "flow_environment_id" || key == "flow_id" || key == "span_id") {
			continue
		}
		fields[key] = value
	}
	return encodedTracePoint{
		tags: tags, fields: fields, time: point.eventTime, routing: routed.routing,
	}, nil
}
