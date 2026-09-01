package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	flowEnvironmentIDEnv        = "CRANE_EXECUTION_FLOW_ENVIRONMENT_ID"
	flowFrontendLogicalInstance = "trace-plugin"
)

var (
	flowIDTagPattern            = regexp.MustCompile(`^[0-9a-f]{32}$`)
	flowSpanIDPattern           = regexp.MustCompile(`^[0-9a-f]{16}$`)
	flowEnvironmentIDTagPattern = regexp.MustCompile(
		`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`,
	)
)

// convertedFlowAttribute is one integer attribute decoded from its wire string,
// held until the whole point has validated.
type convertedFlowAttribute struct {
	key   string
	value int64
}

type flowPointValidationError struct {
	reason  executionFlowReasonCode
	message string
}

func (e *flowPointValidationError) Error() string { return e.message }

func newFlowPointValidationError(reason executionFlowReasonCode, message string) error {
	return &flowPointValidationError{reason: reason, message: message}
}

type ExecutionFlowValidator interface {
	IsFlowPoint(typedTracePoint) bool
	Validate(typedTracePoint) (validatedTracePoint, error)
	EnvironmentID() string
	SchemaVersion() string
	WirePrefix() string
	PipelineFaultPoint() string
}

type schemaExecutionFlowValidator struct {
	flowEnvironmentID  string
	catalog            executionFlowSchemaCatalog
	wirePrefix         string
	heartbeatPoint     string
	pipelineFaultPoint string
	schemaVersion      string
}

func newExecutionFlowValidator(
	flowEnvironmentID string,
	catalog executionFlowSchemaCatalog,
) (*schemaExecutionFlowValidator, error) {
	if flowEnvironmentID != "" && !flowEnvironmentIDTagPattern.MatchString(flowEnvironmentID) {
		return nil, fmt.Errorf(
			"%s must match %s",
			flowEnvironmentIDEnv,
			flowEnvironmentIDTagPattern.String(),
		)
	}
	if catalog == nil {
		return nil, fmt.Errorf("canonical execution-flow point catalog is not installed")
	}
	wirePrefix := catalog.WirePrefix()
	heartbeatPoint := catalog.HeartbeatPoint()
	pipelineFaultPoint := catalog.PipelineFaultPoint()
	schemaVersion := catalog.SchemaVersion()
	if wirePrefix == "" || heartbeatPoint == "" || pipelineFaultPoint == "" ||
		schemaVersion == "" || !strings.HasPrefix(heartbeatPoint, wirePrefix) ||
		!strings.HasPrefix(pipelineFaultPoint, wirePrefix) {
		return nil, fmt.Errorf("canonical execution-flow catalog metadata is invalid")
	}
	return &schemaExecutionFlowValidator{
		flowEnvironmentID:  flowEnvironmentID,
		catalog:            catalog,
		wirePrefix:         wirePrefix,
		heartbeatPoint:     heartbeatPoint,
		pipelineFaultPoint: pipelineFaultPoint,
		schemaVersion:      schemaVersion,
	}, nil
}

func (v *schemaExecutionFlowValidator) IsFlowPoint(point typedTracePoint) bool {
	return strings.HasPrefix(point.name, v.wirePrefix)
}

func (v *schemaExecutionFlowValidator) EnvironmentID() string { return v.flowEnvironmentID }

func (v *schemaExecutionFlowValidator) SchemaVersion() string { return v.schemaVersion }

func (v *schemaExecutionFlowValidator) WirePrefix() string { return v.wirePrefix }

func (v *schemaExecutionFlowValidator) PipelineFaultPoint() string {
	return v.pipelineFaultPoint
}

func (v *schemaExecutionFlowValidator) Validate(point typedTracePoint) (validatedTracePoint, error) {
	if !v.IsFlowPoint(point) {
		return validatedTracePoint{point: point}, nil
	}
	if v.flowEnvironmentID == "" {
		return validatedTracePoint{}, newFlowPointValidationError(
			executionFlowReasonMissingFlowEnvironmentId,
			fmt.Sprintf("%s is required for execution-flow spans", flowEnvironmentIDEnv),
		)
	}
	if !flowSpanIDPattern.MatchString(point.spanID) {
		return validatedTracePoint{}, newFlowPointValidationError(
			executionFlowReasonInvalidSpanId, "flow span_id is invalid",
		)
	}
	if !point.eventTimeValid || point.eventTime.UnixNano() <= 0 {
		return validatedTracePoint{}, newFlowPointValidationError(
			executionFlowReasonInvalidEventTime, "flow event time is invalid",
		)
	}
	if _, ok := point.attributes[executionFlowStorageFlowEnvironmentID]; ok {
		return validatedTracePoint{}, newFlowPointValidationError(
			executionFlowReasonUnexpectedAttribute,
			"flow_environment_id is trusted storage metadata and cannot be supplied by a producer",
		)
	}
	pointID := strings.TrimPrefix(point.name, v.wirePrefix)
	isHeartbeat := point.name == v.heartbeatPoint
	isPipelineFault := point.name == v.pipelineFaultPoint
	if err := v.validateRequiredEnvelope(point, !isHeartbeat && !isPipelineFault); err != nil {
		return validatedTracePoint{}, err
	}
	flowSchema, _ := stringTraceAttribute(point, executionFlowEnvelopeFlowSchema)
	if flowSchema != v.schemaVersion {
		return validatedTracePoint{}, newFlowPointValidationError(
			executionFlowReasonInvalidFlowSchema, "flow_schema is invalid",
		)
	}
	declaredPoint, _ := stringTraceAttribute(point, executionFlowEnvelopePoint)
	if pointID == "" || declaredPoint != pointID {
		return validatedTracePoint{}, newFlowPointValidationError(
			executionFlowReasonPointNameMismatch, "point does not match span name",
		)
	}
	producer, _ := stringTraceAttribute(point, executionFlowEnvelopeProducer)
	logicalInstance, _ := stringTraceAttribute(
		point, executionFlowEnvelopeServiceLogicalInstance,
	)
	serviceInstance, _ := stringTraceAttribute(
		point, executionFlowEnvelopeServiceInstance,
	)
	eventSequenceText, _ := stringTraceAttribute(
		point, executionFlowEnvelopeEventSequence,
	)
	eventSequence, err := parseCanonicalInt64(eventSequenceText, false)
	if err != nil {
		return validatedTracePoint{}, newFlowPointValidationError(
			executionFlowReasonInvalidEventSequence, "event_sequence is invalid",
		)
	}

	flowID, _ := stringTraceAttribute(point, executionFlowEnvelopeFlowID)
	if isHeartbeat {
		for key := range point.attributes {
			if !v.isFlowMetadataAttribute(key) {
				return validatedTracePoint{}, newFlowPointValidationError(
					executionFlowReasonUnexpectedAttribute,
					"pipeline heartbeat contains an unexpected attribute",
				)
			}
		}
	} else {
		spec, known := v.catalog.Point(pointID)
		if !known {
			return validatedTracePoint{}, newFlowPointValidationError(
				executionFlowReasonUnknownPoint, "execution-flow point is not in the canonical catalog",
			)
		}
		if producer != spec.Producer {
			return validatedTracePoint{}, newFlowPointValidationError(
				executionFlowReasonProducerMismatch, "producer does not match the canonical point catalog",
			)
		}
		for _, key := range spec.RequiredAttributes {
			value, ok := stringTraceAttribute(point, key)
			if !ok || value == "" {
				return validatedTracePoint{}, newFlowPointValidationError(
					executionFlowReasonMissingRequiredAttribute,
					"flow point is missing a required attribute",
				)
			}
		}
	}
	if flowID != "" && !flowIDTagPattern.MatchString(flowID) {
		return validatedTracePoint{}, newFlowPointValidationError(
			executionFlowReasonInvalidFlowId, "flow_id is invalid",
		)
	}

	// Validation rewrites integer attributes from their wire strings, but the
	// caller still owns this map. Rewriting as we go would leave a rejected point
	// half converted, and a second Validate on it would then report
	// invalid_attribute_type instead of the reason it actually failed for.
	// Collect the conversions and apply them only once every attribute passed.
	var converted []convertedFlowAttribute
	for key, value := range point.attributes {
		if v.isFlowMetadataAttribute(key) {
			continue
		}
		text, ok := value.(string)
		if !ok {
			return validatedTracePoint{}, newFlowPointValidationError(
				executionFlowReasonInvalidAttributeType,
				"flow point attribute has an invalid wire type",
			)
		}
		attributeType, known := v.catalog.AttributeType(key)
		if !known {
			return validatedTracePoint{}, newFlowPointValidationError(
				executionFlowReasonUnexpectedAttribute,
				"flow point contains an unexpected attribute",
			)
		}
		switch attributeType {
		case "string":
		case "int64":
			parsed, parseErr := parseCanonicalInt64(text, true)
			if parseErr != nil {
				return validatedTracePoint{}, parseErr
			}
			converted = append(converted, convertedFlowAttribute{key: key, value: parsed})
		case "enum":
			if text != "" && !v.catalog.AllowsEnumValue(key, text) {
				return validatedTracePoint{}, newFlowPointValidationError(
					executionFlowReasonInvalidEnumValue,
					"flow point contains a value outside the canonical enum",
				)
			}
		default:
			return validatedTracePoint{}, newFlowPointValidationError(
				executionFlowReasonInvalidAttributeType,
				"canonical flow schema contains an unsupported attribute type",
			)
		}
	}
	for _, attribute := range converted {
		point.attributes[attribute.key] = attribute.value
	}
	point.attributes[executionFlowEnvelopeEventSequence] = eventSequence
	point.durationUS = 0
	point.flow = &executionFlowEnvelope{
		environmentID:          v.flowEnvironmentID,
		flowID:                 flowID,
		schemaVersion:          flowSchema,
		point:                  pointID,
		producer:               producer,
		serviceLogicalInstance: logicalInstance,
		serviceInstance:        serviceInstance,
		eventSequence:          eventSequence,
		pipelineFault:          isPipelineFault,
	}
	return validatedTracePoint{point: point}, nil
}

func (v *schemaExecutionFlowValidator) isFlowMetadataAttribute(key string) bool {
	return v.catalog.AllowsEnvelopeAttribute(key)
}

func (v *schemaExecutionFlowValidator) validateRequiredEnvelope(
	point typedTracePoint,
	business bool,
) error {
	for _, attribute := range v.catalog.EnvelopeAttributes() {
		var required bool
		switch attribute.Requirement {
		case executionFlowEnvelopeRequiredAlways:
			required = true
		case executionFlowEnvelopeRequiredBusiness:
			required = business
		case executionFlowEnvelopeOptional:
			required = false
		default:
			return newFlowPointValidationError(
				executionFlowReasonInvalidAttributeType,
				"canonical flow schema contains an unsupported envelope requirement",
			)
		}
		if !required {
			continue
		}
		value, ok := stringTraceAttribute(point, attribute.Name)
		if ok && value != "" {
			continue
		}
		return newFlowPointValidationError(
			attribute.MissingReason,
			"required execution-flow envelope attribute "+attribute.Name+" is missing",
		)
	}
	return nil
}

func parseCanonicalInt64(value string, signed bool) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || (!signed && parsed < 0) || strconv.FormatInt(parsed, 10) != value {
		return 0, newFlowPointValidationError(
			executionFlowReasonInvalidIntegerAttribute,
			"flow point contains a non-canonical or out-of-range integer attribute",
		)
	}
	return parsed, nil
}
