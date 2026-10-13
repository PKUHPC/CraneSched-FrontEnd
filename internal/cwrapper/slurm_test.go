// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package cwrapper

import (
	"CraneFrontEnd/internal/cacct"
	"slices"
	"testing"
)

func TestConvertSacctArgsUsesSlurmShortOptions(t *testing.T) {
	tests := []struct {
		args          []string
		want          []string
		wantParsable  bool
		wantParsable2 bool
	}{
		{args: []string{"-n", "-j", "1"}, want: []string{"--noheader", "--job=1"}},
		{args: []string{"-nP", "-j1"}, want: []string{"--noheader", "--job=1"}, wantParsable2: true},
		{args: []string{"-p", "-r", "CPU"}, want: []string{"--partition=CPU"}, wantParsable: true},
		{args: []string{"--name", "job1", "--partition=GPU"}, want: []string{"--name=job1", "--partition=GPU"}},
		{args: []string{"-s", "r", "-S", "2024-01-01T00:00:00~"},
			want: []string{"--state=r", "--start-time=2024-01-01T00:00:00~"}},
		{args: []string{"--starttime", "a~b", "--endtime=c~d", "-E", "e~f"},
			want: []string{"--start-time=a~b", "--end-time=c~d", "--end-time=e~f"}},
		{args: []string{"-N", "crnd1", "-A", "acct", "-u", "user", "-q", "qos", "-m", "5"},
			want: []string{"--nodelist=crnd1", "--account=acct", "--user=user", "--qos=qos", "--max-lines=5"}},
		{args: []string{"--jobs", "1,2", "-o", "JobID,State"}, want: []string{"--job=1,2", "--format=%JobID %State"}},
		{args: []string{"-V"}, want: []string{"--version"}},
		{args: []string{"-v", "--json", "-F"}, want: []string{"--json", "--full"}},
	}

	for _, tt := range tests {
		cacct.FlagParsable, cacct.FlagParsable2 = false, false
		got, err := convertSacctArgs(tt.args)
		if err != nil {
			t.Errorf("convertSacctArgs(%q) error: %v", tt.args, err)
			continue
		}
		if !slices.Equal(got, tt.want) {
			t.Errorf("convertSacctArgs(%q) = %q, want %q", tt.args, got, tt.want)
		}
		if cacct.FlagParsable != tt.wantParsable || cacct.FlagParsable2 != tt.wantParsable2 {
			t.Errorf("convertSacctArgs(%q) parsable = %v/%v, want %v/%v", tt.args,
				cacct.FlagParsable, cacct.FlagParsable2, tt.wantParsable, tt.wantParsable2)
		}
	}
	cacct.FlagParsable, cacct.FlagParsable2 = false, false
}

func TestConvertSacctArgsRejectsUnknownOptions(t *testing.T) {
	for _, args := range [][]string{{"-w", "crnd1"}, {"-t", "r"}, {"--not-a-flag"}, {"-j"}, {"-nx"}} {
		if got, err := convertSacctArgs(args); err == nil {
			t.Errorf("convertSacctArgs(%q) = %q, want error", args, got)
		}
	}
}

func TestConvertSqueueFormatUsesSlurmStateAndReasonSemantics(t *testing.T) {
	tests := map[string]string{
		"%T":            "%slurmstate",
		"%t":            "%slurmstatecompact",
		"%R":            "%slurmnodelistreason",
		"%r":            "%R",
		"%.10i %.8T %R": "%.10j %.8slurmstate %slurmnodelistreason",
	}
	for format, want := range tests {
		got, err := convertSqueueFormat(format)
		if err != nil {
			t.Errorf("convertSqueueFormat(%q) error: %v", format, err)
			continue
		}
		if got != want {
			t.Errorf("convertSqueueFormat(%q) = %q, want %q", format, got, want)
		}
	}
}
