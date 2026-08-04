package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"CraneFrontEnd/generated/protos"
)

type tracePointPipeline struct {
	decoder       TracePointDecoder
	flowValidator ExecutionFlowValidator
	router        TracePointRouter
	encoder       TracePointEncoder
	rejected      atomic.Uint64
	now           func() time.Time
}

func newTracePointPipeline(
	flowEnvironmentID string,
	catalog executionFlowSchemaCatalog,
) (*tracePointPipeline, error) {
	validator, err := newExecutionFlowValidator(flowEnvironmentID, catalog)
	if err != nil {
		return nil, err
	}
	return &tracePointPipeline{
		decoder:       protobufTracePointDecoder{},
		flowValidator: validator,
		router:        NewTracePointRouter(),
		encoder:       influxTracePointEncoder{},
		now:           time.Now,
	}, nil
}

func newTracePointPipelineFromEnv() (*tracePointPipeline, error) {
	value, present := os.LookupEnv(flowEnvironmentIDEnv)
	if !present {
		value = ""
	}
	return newTracePointPipeline(value, generatedExecutionFlowCatalog)
}

func executionFlowEnvironmentIDFromEnv() (string, error) {
	value, present := os.LookupEnv(flowEnvironmentIDEnv)
	if !present {
		return "", nil
	}
	if !flowEnvironmentIDTagPattern.MatchString(value) {
		return "", fmt.Errorf(
			"%s must match %s",
			flowEnvironmentIDEnv,
			flowEnvironmentIDTagPattern.String(),
		)
	}
	return value, nil
}

func (p *tracePointPipeline) Process(raw rawTracePoint) (encodedTracePoint, error) {
	point, err := p.decoder.Decode(raw)
	if err != nil {
		return encodedTracePoint{}, err
	}
	validated, err := p.flowValidator.Validate(point)
	if err != nil {
		var validationError *flowPointValidationError
		if !errors.As(err, &validationError) {
			return encodedTracePoint{}, err
		}
		validated, err = p.pipelineFault(validationError)
		if err != nil {
			return encodedTracePoint{}, err
		}
	}
	routed := p.router.Route(validated)
	return p.encoder.Encode(routed)
}

func (p *tracePointPipeline) pipelineFault(
	validationError *flowPointValidationError,
) (validatedTracePoint, error) {
	environmentID := p.flowValidator.EnvironmentID()
	if environmentID == "" {
		return validatedTracePoint{}, validationError
	}

	now := p.now
	if now == nil {
		now = time.Now
	}
	eventTime := now().UTC()
	rejected := p.rejected.Add(1)
	if rejected > uint64(^uint64(0)>>1) {
		return validatedTracePoint{}, fmt.Errorf("execution-flow rejection counter overflow")
	}
	sequence := int64(rejected)
	// A pipeline fault must not retain, log, or derive persisted identifiers
	// from the rejected point. The environment, reason, observation time, and
	// local sequence are sufficient to identify this sanitized fault.
	seed := fmt.Sprintf(
		"%s\x00%s\x00%d\x00%d",
		environmentID,
		validationError.reason,
		sequence,
		eventTime.UnixNano(),
	)
	digest := sha256.Sum256([]byte(seed))
	if rejected == 1 || rejected%128 == 0 {
		log.Warnf(
			"Rejected invalid execution-flow span reason=%s rejected_spans=%d",
			validationError.reason,
			rejected,
		)
	}
	pointID := strings.TrimPrefix(
		p.flowValidator.PipelineFaultPoint(),
		p.flowValidator.WirePrefix(),
	)
	fault := typedTracePoint{
		name:           p.flowValidator.PipelineFaultPoint(),
		service:        flowFrontendLogicalInstance,
		spanID:         fmt.Sprintf("%x", digest[:8]),
		status:         protos.SpanStatus_SPAN_STATUS_ERROR,
		eventTime:      eventTime,
		eventTimeValid: true,
		attributes: map[string]any{
			"flow_schema":              p.flowValidator.SchemaVersion(),
			"point":                    pointID,
			"producer":                 "frontend",
			"service_logical_instance": flowFrontendLogicalInstance,
			"service_instance":         flowFrontendLogicalInstance,
			"event_sequence":           strconv.FormatInt(sequence, 10),
			"reason_code":              string(validationError.reason),
		},
	}
	return p.flowValidator.Validate(fault)
}
