// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package cwrapper

import (
	"CraneFrontEnd/internal/cacctmgr"
	"slices"
	"strings"
	"testing"
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
