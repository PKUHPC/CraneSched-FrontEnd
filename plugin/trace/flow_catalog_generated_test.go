package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// expectedExecutionFlowSchemaSHA256 pins the Backend schema revision this
// package was generated from.
//
// The Backend runs a cross-repository check that regenerates this catalog and
// diffs it, but that job only fires on Backend pull requests that touch
// schemas/execution-flow/v1.yaml. Drift that originates here -- a hand edit of
// a "DO NOT EDIT" file, or a regeneration landed without the paired Backend
// change -- is invisible to it. Pinning the digest as a literal makes any
// regeneration edit this test too, so it cannot pass review unnoticed.
//
// To update: regenerate from the Backend schema, then replace this constant
// with the new digest in the same commit.
const expectedExecutionFlowSchemaSHA256 = "dc98e49f35bc54e1cb9eef7386bf111be6aeb7c786da53beb176b2493470dfec"

const executionFlowCatalogSourceFile = "flow_catalog_generated.go"

func TestGeneratedFlowCatalogMatchesPinnedSchemaRevision(t *testing.T) {
	if got := generatedExecutionFlowCatalog.SchemaSHA256(); got != expectedExecutionFlowSchemaSHA256 {
		t.Fatalf(
			"generated catalog schema SHA256 = %q, want %q;\n"+
				"the catalog was regenerated from a different Backend schema revision.\n"+
				"If that is intended, update expectedExecutionFlowSchemaSHA256 in the same commit\n"+
				"and land the paired Backend and AutoTest changes together.",
			got, expectedExecutionFlowSchemaSHA256,
		)
	}
}

// TestGeneratedFlowCatalogHeaderAgreesWithConstant guards the other half of a
// hand edit: the provenance header and the compiled constant must describe the
// same schema revision, otherwise the file lies about where it came from.
func TestGeneratedFlowCatalogHeaderAgreesWithConstant(t *testing.T) {
	source, err := os.ReadFile(executionFlowCatalogSourceFile)
	if err != nil {
		t.Fatalf("read %s: %v", executionFlowCatalogSourceFile, err)
	}
	header, _, found := strings.Cut(string(source), "\npackage ")
	if !found {
		t.Fatalf("%s has no package clause", executionFlowCatalogSourceFile)
	}
	if !strings.Contains(header, "DO NOT EDIT") {
		t.Fatalf("%s lost its DO NOT EDIT provenance header", executionFlowCatalogSourceFile)
	}
	match := regexp.MustCompile(`(?m)^// SHA256: ([0-9a-f]{64})$`).FindStringSubmatch(header)
	if match == nil {
		t.Fatalf("%s header has no canonical SHA256 line", executionFlowCatalogSourceFile)
	}
	if match[1] != expectedExecutionFlowSchemaSHA256 {
		t.Fatalf(
			"%s header SHA256 = %q, want %q",
			executionFlowCatalogSourceFile, match[1], expectedExecutionFlowSchemaSHA256,
		)
	}
}
