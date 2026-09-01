package main

import (
	"fmt"
	"strconv"
)

const (
	flowCollisionSlots   = 64
	traceSpanMeasurement = "spans"
)

// reservedTraceFields are written from the span itself for every point, so a
// producer attribute of the same name must never replace them. Flow points
// cannot reach here with one -- the validator rejects any attribute outside the
// canonical catalog -- but legacy non-flow spans are unvalidated, and letting
// one overwrite span_id or duration_us would make a point misreport its own
// identity.
var reservedTraceFields = map[string]struct{}{
	"trace_id":       {},
	"span_id":        {},
	"parent_span_id": {},
	"duration_us":    {},
}

// reservedFlowFields are storage metadata this encoder owns on flow points only.
// On a non-flow point no such tag or field is written, so an attribute of the
// same name is ordinary data and must be kept rather than dropped.
var reservedFlowFields = map[string]struct{}{
	executionFlowStorageFlowEnvironmentID: {},
	executionFlowStorageEventTimeUnixNano: {},
	executionFlowEnvelopeFlowID:           {},
}

type TracePointEncoder interface {
	Encode(routedTracePoint) (encodedTracePoint, error)
}

type influxTracePointEncoder struct{}

func (*influxTracePointEncoder) Encode(
	routed routedTracePoint,
) (encodedTracePoint, error) {
	point := routed.point
	measurement := traceSpanMeasurement
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
		measurement = executionFlowStorageMeasurement
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
		if _, reserved := reservedTraceFields[key]; reserved {
			continue
		}
		if point.flow != nil {
			if _, reserved := reservedFlowFields[key]; reserved {
				continue
			}
		}
		fields[key] = value
	}
	return encodedTracePoint{
		measurement: measurement,
		tags:        tags,
		fields:      fields,
		time:        point.eventTime,
		routing:     routed.routing,
	}, nil
}
