package ccontrol

import (
	"bytes"
	"reflect"
	"testing"
)

func TestParseShowHostnamesCommand(t *testing.T) {
	testCases := []struct {
		entity   string
		hostlist string
	}{
		{entity: "hostname", hostlist: "rack[01-02]"},
		{entity: "hostnames", hostlist: "rack[01-02]"},
		{entity: "hostnames", hostlist: "node01;node02"},
	}
	for _, testCase := range testCases {
		commandString := getCmdStringByArgs([]string{"show", testCase.entity, testCase.hostlist})
		command, err := ParseCControlCommand(commandString)
		if err != nil {
			t.Fatalf("ParseCControlCommand(%q, %q): %v", testCase.entity, testCase.hostlist, err)
		}
		if got := command.GetEntity(); got != "hostnames" {
			t.Fatalf("entity for %q = %q, want hostnames", testCase.entity, got)
		}
		if got := unquoteIfQuoted(command.GetID()); got != testCase.hostlist {
			t.Fatalf("hostlist for %q = %q, want %q", testCase.entity, got, testCase.hostlist)
		}
	}

	if _, err := ParseCControlCommand("show node node01;node02"); err == nil {
		t.Fatal("non-hostnames entity accepted a semicolon-delimited identifier")
	}
}

func TestExpandAndPrintHostlist(t *testing.T) {
	hostnames, err := expandHostlist("rack[0-1]_blade[02,04];node[01-02]-ib;login1, login2")
	if err != nil {
		t.Fatalf("expandHostlist: %v", err)
	}
	want := []string{
		"rack0_blade02", "rack0_blade04", "rack1_blade02", "rack1_blade04",
		"node01-ib", "node02-ib", "login1", "login2",
	}
	if !reflect.DeepEqual(hostnames, want) {
		t.Fatalf("hostnames = %q, want %q", hostnames, want)
	}

	var output bytes.Buffer
	if err := printHostnames(&output, hostnames); err != nil {
		t.Fatalf("printHostnames: %v", err)
	}
	if got, want := output.String(), "rack0_blade02\nrack0_blade04\nrack1_blade02\nrack1_blade04\nnode01-ib\nnode02-ib\nlogin1\nlogin2\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestExpandHostlistRejectsInvalidInput(t *testing.T) {
	for _, hostlist := range []string{"", "node[2-1]", "node[01-02", "node01,,node02"} {
		if _, err := expandHostlist(hostlist); err == nil {
			t.Fatalf("expandHostlist(%q) succeeded, want error", hostlist)
		}
	}
}

func TestResolveHostlistArgumentUsesCommandEnvironment(t *testing.T) {
	t.Setenv("CRANE_JOB_NODELIST", "crane01;crane02")
	t.Setenv("SLURM_JOB_NODELIST", "slurm[01-02]")
	t.Setenv("SLURM_NODELIST", "legacy[01-02]")

	if got, err := resolveHostlistArgument("explicit[1-2]", false); err != nil || got != "explicit[1-2]" {
		t.Fatalf("explicit hostlist = %q, %v", got, err)
	}
	if got, err := resolveHostlistArgument("", false); err != nil || got != "crane01;crane02" {
		t.Fatalf("native hostlist = %q, %v", got, err)
	}
	if got, err := resolveHostlistArgument("", true); err != nil || got != "slurm[01-02]" {
		t.Fatalf("Slurm hostlist = %q, %v", got, err)
	}
	t.Setenv("SLURM_JOB_NODELIST", "")
	if got, err := resolveHostlistArgument("", true); err != nil || got != "legacy[01-02]" {
		t.Fatalf("legacy Slurm hostlist = %q, %v", got, err)
	}
}
