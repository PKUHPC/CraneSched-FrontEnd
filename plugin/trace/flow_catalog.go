package main

// executionFlowPointSpec is populated only by the catalog generated from the
// Backend execution-flow schema. This package deliberately has no second YAML
// or hand-maintained list of product flow points.
type executionFlowPointSpec struct {
	Producer           string
	RequiredAttributes []string
}

type executionFlowEnvelopeRequirement string

const (
	executionFlowEnvelopeRequiredAlways   executionFlowEnvelopeRequirement = "always"
	executionFlowEnvelopeRequiredBusiness executionFlowEnvelopeRequirement = "business"
	executionFlowEnvelopeOptional         executionFlowEnvelopeRequirement = "optional"
)

type executionFlowEnvelopeAttributeSpec struct {
	Name          string
	Type          string
	Requirement   executionFlowEnvelopeRequirement
	MissingReason executionFlowReasonCode
}

type executionFlowStorageKind string

const (
	executionFlowStorageField executionFlowStorageKind = "field"
	executionFlowStorageTag   executionFlowStorageKind = "tag"
)

type executionFlowStorageAttributeSpec struct {
	Name       string
	Type       string
	Kind       executionFlowStorageKind
	Source     string
	Wire       bool
	Minimum    int64
	Maximum    int64
	HasMinimum bool
	HasMaximum bool
}

type executionFlowSchemaCatalog interface {
	SchemaName() string
	SchemaSHA256() string
	SchemaVersion() string
	WirePrefix() string
	HeartbeatPoint() string
	PipelineFaultPoint() string
	Point(string) (executionFlowPointSpec, bool)
	AllowsEnvelopeAttribute(string) bool
	EnvelopeAttributeType(string) (string, bool)
	EnvelopeAttributes() []executionFlowEnvelopeAttributeSpec
	AllowsStorageAttribute(string) bool
	StorageAttribute(string) (executionFlowStorageAttributeSpec, bool)
	StorageAttributes() []executionFlowStorageAttributeSpec
	AllowsAttribute(string) bool
	AttributeType(string) (string, bool)
	AllowsEnumValue(string, string) bool
}

// generatedExecutionFlowCatalog is defined by flow_catalog_generated.go.
// The generated catalog is a required build input so a missing or stale schema
// cannot silently degrade execution-flow validation.
