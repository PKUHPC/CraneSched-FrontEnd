// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package cwrapper

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"CraneFrontEnd/internal/cacctmgr"
)

func TestNormalizeSacctmgrArgsAcceptsNameFilters(t *testing.T) {
	tests := []struct {
		args       []string
		wantArgs   []string
		wantFilter map[string]string
	}{
		{
			args:       []string{"show", "account", "Name=acct1"},
			wantArgs:   []string{"cacctmgr", "show", "account", "name=acct1"},
			wantFilter: map[string]string{"name": "acct1"},
		},
		{
			args:       []string{"list", "user", "Accounts=acct1", "Names=user1"},
			wantArgs:   []string{"cacctmgr", "show", "user", "account=acct1", "name=user1"},
			wantFilter: map[string]string{"name": "user1", "account": "acct1"},
		},
		{
			args:       []string{"show", "qos", "Name=normal"},
			wantArgs:   []string{"cacctmgr", "show", "qos", "name=normal"},
			wantFilter: map[string]string{"name": "normal"},
		},
		{
			args:       []string{"show", "user", "where", "Name=user1", "Account=acct1"},
			wantArgs:   []string{"cacctmgr", "show", "user", "where", "name=user1", "account=acct1"},
			wantFilter: map[string]string{"name": "user1", "account": "acct1"},
		},
	}

	for _, tt := range tests {
		got := normalizeSacctmgrArgs(tt.args)
		if !slices.Equal(got, tt.wantArgs) {
			t.Errorf("normalizeSacctmgrArgs(%v) = %v, want %v", tt.args, got, tt.wantArgs)
			continue
		}

		command, err := cacctmgr.ParseCAcctMgrCommand(strings.Join(got[1:], " "))
		if err != nil {
			t.Errorf("ParseCAcctMgrCommand(%v) returned error: %v", got[1:], err)
			continue
		}
		for key, want := range tt.wantFilter {
			if value := command.GetFilterValue(key); value != want {
				t.Errorf("%v filter %s = %q, want %q", tt.args, key, value, want)
			}
		}
	}
}

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
