package main

// executionFlowPointSpec is populated only by the catalog generated from the
// Backend execution-flow schema. This package deliberately has no second YAML
// or hand-maintained list of product flow points.
type executionFlowPointSpec struct {
	Producer           string
	RequiredAttributes []string
}

type executionFlowSchemaCatalog interface {
	SchemaName() string
	SchemaSHA256() string
	SchemaVersion() string
	WirePrefix() string
	HeartbeatPoint() string
	PipelineFaultPoint() string
	Point(string) (executionFlowPointSpec, bool)
	AllowsAttribute(string) bool
	AttributeType(string) (string, bool)
	AllowsEnumValue(string, string) bool
}

// generatedExecutionFlowCatalog is defined by flow_catalog_generated.go.
// The generated catalog is a required build input so a missing or stale schema
// cannot silently degrade execution-flow validation.
