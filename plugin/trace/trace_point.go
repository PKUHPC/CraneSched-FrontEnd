package main

import (
	"fmt"
	"time"

	"CraneFrontEnd/generated/protos"
)

type executionFlowEnvelope struct {
	environmentID          string
	flowID                 string
	schemaVersion          string
	point                  string
	producer               string
	serviceLogicalInstance string
	serviceInstance        string
	eventSequence          int64
	pipelineFault          bool
}

// rawTracePoint is the wire-format boundary. Only the decoder knows that the
// trace hook currently supplies protobuf messages.
type rawTracePoint struct {
	span *protos.SpanInfo
}

// typedTracePoint is the storage-independent representation consumed by
// routing and encoding. Flow attributes have their schema-defined Go types.
type typedTracePoint struct {
	name           string
	service        string
	traceID        string
	spanID         string
	parentSpanID   string
	status         protos.SpanStatus
	eventTime      time.Time
	eventTimeValid bool
	durationUS     int64
	attributes     map[string]any
	flow           *executionFlowEnvelope
}

type TracePointDecoder interface {
	Decode(rawTracePoint) (typedTracePoint, error)
}

type protobufTracePointDecoder struct{}

func (protobufTracePointDecoder) Decode(raw rawTracePoint) (typedTracePoint, error) {
	span := raw.span
	if span == nil {
		return typedTracePoint{}, fmt.Errorf("trace span is nil")
	}

	var startTime time.Time
	if span.StartTime != nil {
		startTime = span.StartTime.AsTime()
	}
	var eventTime time.Time
	eventTimeValid := span.EndTime != nil && span.EndTime.CheckValid() == nil
	if span.EndTime != nil {
		eventTime = span.EndTime.AsTime()
	}

	attributes := make(map[string]any, len(span.Attributes))
	for key, value := range span.Attributes {
		attributes[key] = value
	}
	return typedTracePoint{
		name:           span.Name,
		service:        span.ServiceName,
		traceID:        span.TraceId,
		spanID:         span.SpanId,
		parentSpanID:   span.ParentSpanId,
		status:         span.Status,
		eventTime:      eventTime,
		eventTimeValid: eventTimeValid,
		durationUS:     eventTime.Sub(startTime).Microseconds(),
		attributes:     attributes,
	}, nil
}

type validatedTracePoint struct {
	point typedTracePoint
}

type routedTracePoint struct {
	point   typedTracePoint
	routing traceRoutingDecision
}

func stringTraceAttribute(point typedTracePoint, key string) (string, bool) {
	value, ok := point.attributes[key]
	if !ok {
		return "", false
	}
	text, ok := value.(string)
	return text, ok
}

// encodedTracePoint is the Influx storage ABI. It is intentionally defined
// after routing and remains opaque to decoders and validators.
type encodedTracePoint struct {
	measurement string
	tags        map[string]string
	fields      map[string]interface{}
	time        time.Time
	routing     traceRoutingDecision
}

type TracePointProcessor interface {
	Process(rawTracePoint) (encodedTracePoint, error)
}
