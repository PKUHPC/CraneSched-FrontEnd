package cwrapper

import (
	"bytes"
	"strings"
	"testing"
)

func TestConvertSqueueStateFormats(t *testing.T) {
	got, err := convertSqueueFormat("%t %T %State %JobType")
	if err != nil {
		t.Fatalf("convertSqueueFormat() error = %v", err)
	}
	const want = "%statecompact %state %State %JobType"
	if got != want {
		t.Fatalf("convertSqueueFormat() = %q, want %q", got, want)
	}
}

func TestSqueueHelpUsesSlurmStateFormats(t *testing.T) {
	cmd := squeue()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)

	cmd.Run(cmd, []string{"--help"})
	help := output.String()
	for _, expected := range []string{
		"%deadline/%Deadline",
		"%i/%StepId",
		"%j/%JobID",
		"%K/%Wckey",
		"%n/%Name",
		"%t/%StateCompact",
		"%T/%State",
		"%JobType",
	} {
		if !strings.Contains(help, expected) {
			t.Errorf("squeue --help does not contain %q", expected)
		}
	}
	if strings.Contains(help, "%T/%JobType") {
		t.Error("squeue --help still advertises the old state-to-job-type mapping")
	}
}
