package main

import (
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func TestGeneratedFlowCatalogMetadata(t *testing.T) {
	if got := generatedExecutionFlowCatalog.SchemaName(); got != "flow/v1" {
		t.Fatalf("schema name = %q", got)
	}
	if got := generatedExecutionFlowCatalog.SchemaVersion(); got != "v1" {
		t.Fatalf("schema version = %q", got)
	}
	if got := generatedExecutionFlowCatalog.WirePrefix(); got != "flow/v1/" {
		t.Fatalf("wire prefix = %q", got)
	}
	if got := generatedExecutionFlowCatalog.HeartbeatPoint(); got != "flow/v1/pipeline/heartbeat" {
		t.Fatalf("heartbeat point = %q", got)
	}
	if got := generatedExecutionFlowCatalog.PipelineFaultPoint(); got != "flow/v1/pipeline/fault" {
		t.Fatalf("pipeline fault point = %q", got)
	}
	// The digest value is generated from the Backend schema. Keep this test
	// structural; the cross-repository generator check verifies exact equality.
	if digest := generatedExecutionFlowCatalog.SchemaSHA256(); len(digest) != 64 {
		t.Fatalf("schema SHA256 length = %d, want 64", len(digest))
	} else if _, err := hex.DecodeString(digest); err != nil {
		t.Fatalf("schema SHA256 is not lowercase hexadecimal: %q", digest)
	} else if digest != strings.ToLower(digest) {
		t.Fatalf("schema SHA256 is not canonical lowercase hexadecimal: %q", digest)
	}
	point, ok := generatedExecutionFlowCatalog.Point("pipeline/fault")
	if !ok || point.Producer != "frontend" ||
		len(point.RequiredAttributes) != 1 || point.RequiredAttributes[0] != "reason_code" {
		t.Fatalf("pipeline fault catalog entry = %#v, present=%t", point, ok)
	}
	if !generatedExecutionFlowCatalog.AllowsEnumValue(
		"reason_code", string(executionFlowReasonInvalidFlowId),
	) {
		t.Fatal("generated catalog does not allow its invalid-flow-id reason")
	}
	for name, wantType := range map[string]string{
		executionFlowEnvelopeEventSequence:          "int64",
		executionFlowEnvelopeFlowID:                 "string",
		executionFlowEnvelopeFlowSchema:             "string",
		executionFlowEnvelopePoint:                  "string",
		executionFlowEnvelopeProducer:               "string",
		executionFlowEnvelopeServiceInstance:        "string",
		executionFlowEnvelopeServiceLogicalInstance: "string",
	} {
		gotType, ok := generatedExecutionFlowCatalog.EnvelopeAttributeType(name)
		if !ok || gotType != wantType ||
			!generatedExecutionFlowCatalog.AllowsEnvelopeAttribute(name) {
			t.Fatalf("envelope attribute %q = %q, present=%t", name, gotType, ok)
		}
		if generatedExecutionFlowCatalog.AllowsAttribute(name) {
			t.Fatalf("envelope attribute %q leaked into point attributes", name)
		}
	}
	envelope := generatedExecutionFlowCatalog.EnvelopeAttributes()
	if len(envelope) != 7 {
		t.Fatalf("envelope attribute count = %d, want 7", len(envelope))
	}
	for index := 1; index < len(envelope); index++ {
		if envelope[index-1].Name >= envelope[index].Name {
			t.Fatalf("envelope catalog is not ordered: %#v", envelope)
		}
	}
	for _, attribute := range envelope {
		if attribute.Name == executionFlowEnvelopeFlowID {
			if attribute.Requirement != executionFlowEnvelopeRequiredBusiness ||
				attribute.MissingReason != executionFlowReasonInvalidFlowId {
				t.Fatalf("flow_id requirement = %#v", attribute)
			}
		} else if attribute.Requirement != executionFlowEnvelopeRequiredAlways {
			t.Fatalf("always-required envelope attribute = %#v", attribute)
		}
	}
	for _, storageOnly := range []string{
		executionFlowStorageEventTimeUnixNano,
		executionFlowStorageFlowEnvironmentID,
		executionFlowStorageFlowSlot,
		executionFlowStorageFlowInstanceSlot,
	} {
		if generatedExecutionFlowCatalog.AllowsEnvelopeAttribute(storageOnly) ||
			generatedExecutionFlowCatalog.AllowsAttribute(storageOnly) {
			t.Fatalf("storage-only attribute %q leaked into the wire schema", storageOnly)
		}
	}
	storage := generatedExecutionFlowCatalog.StorageAttributes()
	if len(storage) != 4 {
		t.Fatalf("storage attribute count = %d, want 4", len(storage))
	}
	for index := 1; index < len(storage); index++ {
		if storage[index-1].Name >= storage[index].Name {
			t.Fatalf("storage catalog is not ordered: %#v", storage)
		}
	}
	for _, test := range []struct {
		name          string
		attributeType string
		kind          executionFlowStorageKind
		minimum       int64
		maximum       int64
		hasMinimum    bool
		hasMaximum    bool
	}{
		{executionFlowStorageEventTimeUnixNano, "int64", executionFlowStorageField, 1, 0, true, false},
		{executionFlowStorageFlowEnvironmentID, "string", executionFlowStorageTag, 0, 0, false, false},
		{executionFlowStorageFlowInstanceSlot, "int64", executionFlowStorageTag, 0, 63, true, true},
		{executionFlowStorageFlowSlot, "int64", executionFlowStorageTag, 0, 63, true, true},
	} {
		attribute, ok := generatedExecutionFlowCatalog.StorageAttribute(test.name)
		if !ok || !generatedExecutionFlowCatalog.AllowsStorageAttribute(test.name) {
			t.Fatalf("storage attribute %q is missing", test.name)
		}
		if attribute.Type != test.attributeType || attribute.Kind != test.kind ||
			attribute.Source != "frontend" || attribute.Wire ||
			attribute.Minimum != test.minimum || attribute.Maximum != test.maximum ||
			attribute.HasMinimum != test.hasMinimum || attribute.HasMaximum != test.hasMaximum {
			t.Fatalf("storage attribute %q = %#v", test.name, attribute)
		}
	}
}

func TestGeneratedFlowCatalogPointIsImmutable(t *testing.T) {
	first, ok := generatedExecutionFlowCatalog.Point("ctld/job/accepted")
	if !ok || len(first.RequiredAttributes) == 0 {
		t.Fatal("canonical point is missing")
	}
	first.RequiredAttributes[0] = "mutated"
	second, ok := generatedExecutionFlowCatalog.Point("ctld/job/accepted")
	if !ok || second.RequiredAttributes[0] == "mutated" {
		t.Fatal("catalog returned shared required-attribute storage")
	}
}

func TestExecutionFlowValidatorRejectsSchemaViolations(t *testing.T) {
	tests := []struct {
		name       string
		spanName   string
		attributes map[string]string
		remove     string
		reason     executionFlowReasonCode
	}{
		{
			name: "unknown point", spanName: "flow/v1/ctld/job/not_real",
			attributes: map[string]string{"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4"},
			reason:     executionFlowReasonUnknownPoint,
		},
		{
			name: "producer mismatch", spanName: "flow/v1/ctld/job/accepted",
			attributes: map[string]string{
				"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4", "producer": "craned",
			},
			reason: executionFlowReasonProducerMismatch,
		},
		{
			name: "missing required", spanName: "flow/v1/ctld/job/accepted",
			attributes: map[string]string{"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4"},
			remove:     "job_id", reason: executionFlowReasonMissingRequiredAttribute,
		},
		{
			name: "unknown attribute", spanName: "flow/v1/ctld/job/accepted",
			attributes: map[string]string{
				"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4", "not_in_schema": "value",
			},
			reason: executionFlowReasonUnexpectedAttribute,
		},
		{
			name: "invalid enum", spanName: "flow/v1/ctld/job/accepted",
			attributes: map[string]string{
				"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4", "operation": "not-an-operation",
			},
			reason: executionFlowReasonInvalidEnumValue,
		},
		{
			name: "integer overflow", spanName: "flow/v1/ctld/job/accepted",
			attributes: map[string]string{
				"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4", "job_id": "9223372036854775808",
			},
			reason: executionFlowReasonInvalidIntegerAttribute,
		},
		{
			name: "leading zero", spanName: "flow/v1/ctld/job/accepted",
			attributes: map[string]string{
				"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4", "job_id": "01",
			},
			reason: executionFlowReasonInvalidIntegerAttribute,
		},
		{
			name: "leading plus", spanName: "flow/v1/ctld/job/accepted",
			attributes: map[string]string{
				"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4", "job_id": "+1",
			},
			reason: executionFlowReasonInvalidIntegerAttribute,
		},
	}

	validator, err := newExecutionFlowValidator("run-1.shard-0", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			span := testSpan(test.spanName, test.attributes)
			delete(span.Attributes, test.remove)
			decoded, decodeErr := (protobufTracePointDecoder{}).Decode(rawTracePoint{span: span})
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			_, validateErr := validator.Validate(decoded)
			var flowErr *flowPointValidationError
			if !errors.As(validateErr, &flowErr) {
				t.Fatalf("validation error = %v", validateErr)
			}
			if flowErr.reason != test.reason {
				t.Fatalf("reason = %q, want %q", flowErr.reason, test.reason)
			}
		})
	}
}

func TestExecutionFlowValidatorAllowsGloballyDeclaredOptionalAttribute(t *testing.T) {
	span := testSpan("flow/v1/ctld/job/accepted", map[string]string{
		"flow_id": "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
		"step_id": "9",
	})
	validator, err := newExecutionFlowValidator("run-1.shard-0", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := (protobufTracePointDecoder{}).Decode(rawTracePoint{span: span})
	if err != nil {
		t.Fatal(err)
	}
	validated, err := validator.Validate(decoded)
	if err != nil {
		t.Fatalf("globally declared optional attribute was rejected: %v", err)
	}
	if got := validated.point.attributes["step_id"]; got != int64(9) {
		t.Fatalf("typed step_id = %#v, want int64(9)", got)
	}
}

func TestHeartbeatUsesGeneratedDescriptorAndTypedSequence(t *testing.T) {
	span := testSpan(generatedExecutionFlowCatalog.HeartbeatPoint(), map[string]string{
		"event_sequence": "42",
	})
	validator, err := newExecutionFlowValidator("run-1.shard-0", generatedExecutionFlowCatalog)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := (protobufTracePointDecoder{}).Decode(rawTracePoint{span: span})
	if err != nil {
		t.Fatal(err)
	}
	validated, err := validator.Validate(decoded)
	if err != nil {
		t.Fatal(err)
	}
	point := validated.point
	if point.flow == nil || point.flow.eventSequence != 42 {
		t.Fatalf("heartbeat flow envelope = %#v", point.flow)
	}
	if got := point.attributes["event_sequence"]; got != int64(42) {
		t.Fatalf("heartbeat event_sequence = %#v", got)
	}
	if !strings.HasPrefix(point.name, generatedExecutionFlowCatalog.WirePrefix()) {
		t.Fatalf("heartbeat name = %q", point.name)
	}
}
