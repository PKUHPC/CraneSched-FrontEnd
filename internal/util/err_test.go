package util

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/spf13/cobra"
)

func TestRunEWrapperSilencesCommandExitError(t *testing.T) {
	cmd := &cobra.Command{
		Use: "test-command",
		RunE: func(cmd *cobra.Command, args []string) error {
			return &CommandExitError{Code: ErrorCmdArg}
		},
	}
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	RunEWrapperForLeafCommand(cmd)

	err := cmd.Execute()
	var commandExitErr *CommandExitError
	if !errors.As(err, &commandExitErr) {
		t.Fatalf("Execute() error = %T, want *CommandExitError", err)
	}
	if commandExitErr.Code != ErrorCmdArg {
		t.Fatalf("command exit code = %d, want %d", commandExitErr.Code, ErrorCmdArg)
	}
	if !cmd.SilenceUsage {
		t.Fatal("usage was not silenced for a command exit status")
	}
	if !cmd.SilenceErrors {
		t.Fatal("Cobra error output was not silenced for a command exit status")
	}
	if output.Len() != 0 {
		t.Fatalf("Cobra printed output for a command exit status: %q", output.String())
	}
}

func TestRunAndHandleExitPreservesCommandExitCode(t *testing.T) {
	const exitCode = 7
	if os.Getenv("CRANE_TEST_COMMAND_EXIT_CODE") == "1" {
		cmd := &cobra.Command{
			Use: "test-command",
			RunE: func(cmd *cobra.Command, args []string) error {
				return &CommandExitError{Code: exitCode}
			},
		}
		RunEWrapperForLeafCommand(cmd)
		RunAndHandleExit(cmd)
		return
	}

	command := exec.Command(os.Args[0], "-test.run=^TestRunAndHandleExitPreservesCommandExitCode$")
	command.Env = append(os.Environ(), "CRANE_TEST_COMMAND_EXIT_CODE=1")
	output, err := command.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("subprocess error = %v, want exit status %d", err, exitCode)
	}
	if got := exitErr.ExitCode(); got != exitCode {
		t.Fatalf("subprocess exit code = %d, want %d; output: %q", got, exitCode, output)
	}
	if len(output) != 0 {
		t.Fatalf("subprocess printed output for command exit status: %q", output)
	}
}
