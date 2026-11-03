// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"bytes"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func testSnapshot() *snapshot {
	return &snapshot{
		Nodes: []node{
			{Name: "n01", Hostname: "n01", Address: "n01", State: nodeState{Base: "idle"}, CPUs: 4, Memory: 3072, Sockets: 1, Cores: 4, Threads: 1},
			{Name: "n02", Hostname: "n02", Address: "n02", State: nodeState{Base: "idle"}, CPUs: 4, Memory: 3072, Sockets: 1, Cores: 4, Threads: 1},
			{Name: "n03", Hostname: "n03", Address: "n03", State: nodeState{Base: "idle"}, CPUs: 2, Memory: 2048, Sockets: 1, Cores: 2, Threads: 1},
		},
		Partitions: []partition{
			{Name: "compute", Nodes: []string{"n01", "n02", "n03"}, Available: "up", TimeLimit: durationLimit{Infinite: true}},
			{Name: "batch", Nodes: []string{"n01", "n02", "n03"}, Default: true, Available: "up", TimeLimit: durationLimit{Infinite: true}},
			{Name: "short", Nodes: []string{"n01"}, Available: "up", TimeLimit: durationLimit{Seconds: 300}},
		},
	}
}

func renderTest(t *testing.T, s *snapshot, args ...string) (string, string) {
	t.Helper()
	o, err := parseOptions(append([]string{"sinfo"}, args...), nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := buildRows(s, o)
	sortRows(rows, o)
	var out bytes.Buffer
	if err := newTextRenderer().table(&out, rows, o); err != nil {
		t.Fatal(err)
	}
	return out.String(), strings.Join(o.Diagnostics, "")
}

func TestFormatBehavior(t *testing.T) {
	cases := []struct {
		long            bool
		input, out, err string
	}{
		{false, "%08N!", "n01     !\n", ""},
		{false, "%.09N!", "      n01!\n", ""},
		{false, "%010N!", "n01       !\n", ""},
		{false, " lead\t%N 'x' # // \" y ", " lead\tn01 'x' # // \" y \n", ""},
		{false, "节点=%N 尾", "节点=n01 尾\n", ""},
		{false, "", "\n", ""}, {false, "abc \t", "abc \t\n", ""},
		{false, "%", "\n", ""}, {false, "%%", "\n", ""}, {false, "%%N", "n01\n", ""}, {false, "%N%", "n01\n", ""},
		{false, "prefix:%Q!%N", "prefix:%Q!n01\n", "Invalid node format specification: Q\n"},
		{false, "%09", "0        \n", ""},
		{false, "%..2N", "%..2N\n", "Invalid node format specification: .\n"},
		{false, "%2N!", "n0!\n", ""}, {false, "%.2N!", "n0!\n", ""},
		{true, "NodeList:09!", "n01      !\n", ""},
		{true, "NodeList:.09!", "      n01!\n", ""},
		{true, "NodeList:010!", "n01       !\n", ""},
		{true, ",,nOdElIsT:09!,,", "n01      !\n", ""},
		{true, " NodeList:09!", "!\n", "sinfo: error: Invalid job format specification:  NodeList\n"},
		{true, "NodeList :09!", "!\n", "sinfo: error: Invalid job format specification: NodeList \n"},
		{true, "NodeList:", "n01\n", ""},
		{true, "NodeList:tail", "n01tail\n", ""},
		{true, "NotAField,NodeList:09!", "n01      !\n", "sinfo: error: Invalid job format specification: NotAField\n"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			flag := "-o"
			if tc.long {
				flag = "-O"
			}
			out, err := renderTest(t, testSnapshot(), "-h", "-p", "compute", "-n", "n01", flag, tc.input)
			if out != tc.out || err != tc.err {
				t.Fatalf("got (%q,%q), want (%q,%q)", out, err, tc.out, tc.err)
			}
		})
	}
}

func TestGroupingSelectionAndOrdering(t *testing.T) {
	cases := []struct {
		name string
		args []string
		out  string
	}{
		{"deduplicate", []string{"-h", "-o", "%D|%N"}, "3|n[01-03]\n"},
		{"counts", []string{"-h", "-o", "%F|%C|%N"}, "0/3/0/3|0/10/0/10|n[01-03]\n"},
		{"partition", []string{"-h", "-o", "%P|%t|%N"}, "compute|idle|n[01-03]\nbatch*|idle|n[01-03]\nshort|idle|n01\n"},
		{"inexact", []string{"-h", "-o", "%P|%c|%N"}, "compute|2+|n[01-03]\nbatch*|2+|n[01-03]\nshort|4|n01\n"},
		{"exact", []string{"-h", "-e", "-o", "%P|%c|%N"}, "compute|4|n[01-02]\ncompute|2|n03\nbatch*|4|n[01-02]\nbatch*|2|n03\nshort|4|n01\n"},
		{"alphabetic", []string{"-h", "-S", "P", "-o", "%P"}, "batch*\ncompute\nshort\n"},
		{"descending", []string{"-h", "-S", "-P", "-o", "%P"}, "short\ncompute\nbatch*\n"},
		{"no_nodes", []string{"-h", "-n", "missing", "-o", "%P|%D|%t|%N"}, "compute|0|n/a|\nbatch*|0|n/a|\nshort|0|n/a|\n"},
		{"no_partition", []string{"-h", "-p", "missing"}, ""},
		{"empty_state", []string{"-h", "-t", "", "-o", "%N"}, ""},
		{"all_state", []string{"-h", "-t", "all", "-o", "%N"}, "n[01-03]\n"},
		{"and", []string{"-h", "-t", "idle,down+idle", "-o", "%N"}, ""},
		{"or", []string{"-h", "-t", "IdL,down", "-o", "%N"}, "n[01-03]\n"},
		{"sticky_and", []string{"-h", "-t", "idle+down", "-t", "idle,down", "-o", "%N"}, ""},
		{"nodes", []string{"-h", "-N", "-o", "%N|%P"}, "n01|compute\nn01|batch*\nn01|short\nn02|compute\nn02|batch*\nn03|compute\nn03|batch*\n"},
		{"truncated_default", []string{"-h", "-o", "%3P"}, "com\nba*\nsho\n"},
		{"uncollected_hostname", []string{"-Nh", "-p", "compute", "-S", "-n", "-o", "%N"}, "n01\nn02\nn03\n"},
		{"uncollected_address", []string{"-Nh", "-p", "compute", "-S", "-o", "-o", "%N"}, "n01\nn02\nn03\n"},
		{"collected_hostname", []string{"-Nh", "-p", "compute", "-S", "-n", "-o", "%N|%n"}, "n03|n03\nn02|n02\nn01|n01\n"},
		{"collected_address", []string{"-Nh", "-p", "compute", "-S", "-o", "-O", "NodeList:0|,NodeAddr:0"}, "n03|n03\nn02|n02\nn01|n01\n"},
		{"reasons", []string{"-R"}, "REASON               USER      TIMESTAMP           NODELIST\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := renderTest(t, testSnapshot(), tc.args...)
			if out != tc.out || err != "" {
				t.Fatalf("got (%q,%q), want %q", out, err, tc.out)
			}
		})
	}
}

func TestNodeAndCPUClassification(t *testing.T) {
	s := testSnapshot()
	s.Nodes[0].State.Base = "mixed"
	s.Nodes[0].AllocCPUs = 1
	s.Nodes[1].State.Drain = true
	s.Nodes[2].State.Base = "down"
	out, _ := renderTest(t, s, "-h", "-o", "%F|%C")
	if out != "1/0/2/3|1/3/6/10\n" {
		t.Fatal(out)
	}
	s.Nodes[0].Reason = "warm"
	s.Nodes[1].Reason = "cold"
	s.Nodes[2].Reason = "cold"
	out, _ = renderTest(t, s, "-h", "-o", "%E|%D|%N")
	if out != "cold|2|n[02-03]\nwarm|1|n01\n" {
		t.Fatal(out)
	}
}

func TestOptionOrdering(t *testing.T) {
	for _, tc := range []struct {
		args   []string
		env    map[string]string
		action string
		err    string
	}{
		{[]string{"--version", "--unknown"}, nil, "version", ""},
		{[]string{"--unknown", "--version"}, nil, "", "sinfo: unrecognized option '--unknown'\nTry \"sinfo --help\" for more information\n"},
		{[]string{"--help"}, map[string]string{"SINFO_ALL": "", "SINFO_PARTITION": ""}, "", "sinfo: error: Conflicting options, SINFO_ALL and SINFO_PARTITION, specified. Please choose one or the other.\n"},
		{[]string{"operand", "-V"}, nil, "version", ""},
		{[]string{"operand", "-V"}, map[string]string{"POSIXLY_CORRECT": ""}, "", ""},
		{[]string{"--", "-V"}, nil, "", ""},
		{[]string{"--form=x"}, nil, "", ""},
		{[]string{"--hel"}, nil, "", "sinfo: option '--hel' is ambiguous; possibilities: '--help' '--helpformat' '--helpFormat' '--helpstate'\nTry \"sinfo --help\" for more information\n"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			o, err := parseOptions(append([]string{"sinfo"}, tc.args...), tc.env)
			got := ""
			if err != nil {
				got = err.Error()
			}
			if got != tc.err {
				t.Fatalf("error = %q, want %q", got, tc.err)
			}
			if err == nil && o.Action != tc.action {
				t.Fatalf("action = %q, want %q", o.Action, tc.action)
			}
		})
	}
	o, err := parseOptions([]string{"sinfo", "-O", "NodeList", "-o", "%N", "--json", "v0.0.42"}, nil)
	if err != nil || !o.LongFormat || o.Format != "%N" || o.DataParser != "" {
		t.Fatalf("%+v %v", o, err)
	}
	o, err = parseOptions([]string{"sinfo", "-hi03junk", "-pnamed"}, nil)
	if err != nil || !o.NoHeader || o.Iterate != 3 || o.Partition != "named" {
		t.Fatalf("%+v %v", o, err)
	}
}

func TestHostLists(t *testing.T) {
	for _, tc := range []struct {
		input   string
		yes, no []string
	}{
		{"n[4294967296]", []string{"n4294967296"}, []string{"n0"}},
		{"n[1-]", []string{"n1"}, []string{"n01"}},
		{"n[+1]", []string{"n01"}, []string{"n1"}},
		{"n[ 1]", []string{"n01"}, []string{"n1"}},
		{"n[\t01]", []string{"n001"}, []string{"n01"}},
		{"n[1-+2]", []string{"n1", "n2"}, []string{"n02"}},
		{"n[1- 2]", []string{"n1", "n2"}, []string{"n02"}},
		{"n[1a-2]", []string{"n01", "n02"}, []string{"n1"}},
		{"n[1-65536]", []string{"n1", "n65536"}, []string{"n65537"}},
		{"n[01-03]", []string{"n01", "n03"}, []string{"n1", "n04"}},
		{"n[1-03]", []string{"n1", "n3"}, []string{"n01", "n03"}},
		{"n[001-3]", []string{"n001", "n003"}, []string{"n1", "n004"}},
		{"n[09-10]", []string{"n09", "n10"}, []string{"n9", "n010"}},
		{"r[1-2]n[03,05-07]", []string{"r1n03", "r2n06"}, []string{"r3n03", "r1n04"}},
		{", n1,, n2 ,", []string{"n1", "n2"}, []string{"n3"}},
	} {
		t.Run(tc.input, func(t *testing.T) {
			h, err := parseHostList(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range tc.yes {
				if !h.Match(n) {
					t.Errorf("missing %s", n)
				}
			}
			for _, n := range tc.no {
				if h.Match(n) {
					t.Errorf("unexpected %s", n)
				}
			}
		})
	}
	for _, input := range []string{"n[]", "n[1,]", "n[3-1]", "n[1-65537]", "n[18446744073709551616]", "n[1", "n1]"} {
		if _, err := parseHostList(input); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
	if got := compressHosts([]string{"n03", "n01", "n02", "n02", "x"}); got != "n[01-03],x" {
		t.Fatal(got)
	}
	for input, want := range map[string]string{
		"n09,n10": "n[09-10]", "n9,n10": "n[9-10]", "n009,n010,n011": "n[009-011]",
		"n1,n01,n02,n2": "n[1-2,01-02]", "r1n01,r2n01,r1n02,r2n02": "r1n[01-02],r2n[01-02]",
		"n1x,n2x": "n1x,n2x", "n1x2,n1x3,n2x2,n2x3": "n1x[2-3],n2x[2-3]",
		"n9,n09,n10": "n[9,09-10]", "n1,n01,n10,n010": "n[1,01,10,010]",
		"n01,n2,n10": "n[2,01,10]", "n99,n001": "n[99,001]", "n099,n100,n101": "n[099-101]",
	} {
		if got := compressHosts(strings.Split(input, ",")); got != want {
			t.Errorf("%q: got %q, want %q", input, got, want)
		}
	}
}

func TestHostPrefixOrdering(t *testing.T) {
	s := testSnapshot()
	s.Nodes = nil
	s.Partitions = s.Partitions[:1]
	s.Partitions[0].Nodes = []string{"r02n02", "r2n01", "r002n01", "r02n01", "r10n01", "r0n01"}
	for _, name := range s.Partitions[0].Nodes {
		s.Nodes = append(s.Nodes, node{Name: name, State: nodeState{Base: "idle"}})
	}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"default_node_order", []string{"-N"}, "r0n01\nr002n01\nr02n01\nr02n02\nr2n01\nr10n01\n"},
		{"explicit_node_order", []string{"-N", "-S", "N"}, "r0n01\nr002n01\nr02n01\nr02n02\nr2n01\nr10n01\n"},
		{"reverse_node_order", []string{"-N", "-S", "-N"}, "r10n01\nr2n01\nr02n02\nr02n01\nr002n01\nr0n01\n"},
		{"compressed_list", nil, "r0n01,r002n01,r02n[01-02],r2n01,r10n01\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"-h", "-p", "compute", "-o", "%N"}, tc.args...)
			got, diagnostic := renderTest(t, s, args...)
			if got != tc.want || diagnostic != "" {
				t.Fatalf("stdout=%q stderr=%q; want stdout=%q", got, diagnostic, tc.want)
			}
		})
	}
}

func TestStateProjections(t *testing.T) {
	for _, tc := range []struct {
		state                   nodeState
		long, compact, complete string
	}{
		{nodeState{Base: "idle"}, "idle", "idle", "IDLE"},
		{nodeState{Base: "idle", NoRespond: true, Power: "powered_down"}, "idle~", "idle~", "IDLE+POWERED_DOWN+NOT_RESPONDING"},
		{nodeState{Base: "mixed", Drain: true, NoRespond: true, Maintenance: true}, "draining$", "drng$", "MIXED+DRAIN+MAINTENANCE+NOT_RESPONDING"},
		{nodeState{Base: "idle", Maintenance: true}, "maint", "maint", "IDLE+MAINTENANCE"},
		{nodeState{Base: "allocated", Completing: true}, "allocated+", "alloc+", "ALLOCATED+COMPLETING"},
		{nodeState{Base: "idle", Reserved: true}, "reserved", "resv", "IDLE+RESERVED"},
		{nodeState{Base: "mixed", Planned: true}, "mixed-", "mix-", "MIXED+PLANNED"},
	} {
		got := []string{tc.state.long(), tc.state.compact(), tc.state.complete()}
		if !reflect.DeepEqual(got, []string{tc.long, tc.compact, tc.complete}) {
			t.Fatalf("%+v: %q", tc.state, got)
		}
	}
	for _, value := range []string{"err", "(idle)", " idle", "!!down"} {
		if _, err := parseStates(value, false); err == nil {
			t.Errorf("accepted %q", value)
		}
	}
}

func TestReservationRendering(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	r := newTextRenderer()
	var out bytes.Buffer
	if err := r.reservations(&out, nil, true, now); err != nil {
		t.Fatal(err)
	}
	if out.String() != "No reservations in the system\n" {
		t.Fatal(out.String())
	}
	out.Reset()
	if err := r.reservations(&out, []reservation{{Name: "maint", Nodes: "n[1-2]", Start: now, End: now.Add(time.Minute)}}, true, now); err != nil {
		t.Fatal(err)
	}
	want := "maint        ACTIVE  2026-01-02T03:04:05  2026-01-02T03:05:05     00:01:00  n[1-2]\n"
	if out.String() != want {
		t.Fatalf("%q != %q", out.String(), want)
	}
}

func TestActions(t *testing.T) {
	for _, tc := range []struct{ action, value, want string }{
		{"version", "", "slurm 26.05.4\n"},
		{"usage", "", "Usage: sinfo [-abdelNRrsTv] [-i seconds] [-t states] [-p partition] [-n nodes]\n             [-S fields] [-o format] [-O Format] [--federation] [--local]\n"},
		{"autocomplete", "node", "--nodes=\n\n"},
		{"autocomplete", "", ""},
		{"autocomplete", "not-an-option", ""},
		{"helpformat", "", "%S    %a    %V    %Y    %c    %O    %C    %L    \n%d    %f    %b    %e    %G    %G    %g    %B    \n%m    %o    %A    %F    %n    %N    %D    %h    \n%P    %R    %M    %I    %p    %E    %i    %r    \n%s    %X    %z    %t    %T    %Z    %l    %H    \n%u    %U    %v    %w    \n"},
	} {
		t.Run(tc.action+tc.value, func(t *testing.T) {
			var out bytes.Buffer
			if err := writeAction(&out, &options{Action: tc.action, ActionValue: tc.value}); err != nil {
				t.Fatal(err)
			}
			if out.String() != tc.want {
				t.Fatalf("%q != %q", out.String(), tc.want)
			}
		})
	}
	var out bytes.Buffer
	if err := writeAction(&out, &options{Action: "helpFormat"}); err != nil {
		t.Fatal(err)
	}
	if len(strings.Fields(out.String())) != 49 || strings.Contains(out.String(), "Share") {
		t.Fatal(out.String())
	}
}

func TestParsersConcurrent(t *testing.T) {
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 32; j++ {
				o, err := parseOptions([]string{"sinfo", "-n", "r[01-02]n[1-4]", "-t", "idle,!down", "-o", "%.09N|%P"}, nil)
				if err != nil || len(o.Fields) != 2 || !o.NodeFilter.Match("r02n3") || !o.StateFilter.match(nodeState{Base: "idle"}, 0) {
					t.Errorf("parse failed: %v", err)
					return
				}
			}
		}()
	}
	workers.Wait()
}

func TestNumericRendering(t *testing.T) {
	for _, tc := range []struct {
		n    uint64
		text string
	}{
		{0, "0"}, {1023, "1023"}, {1024, "1K"}, {1536, "1.50K"}, {2048, "2K"}, {2560, "2.50K"}, {1024 * 1024, "1M"},
	} {
		if got := unitNumber(tc.n, false); got != tc.text {
			t.Errorf("%d: %q != %q", tc.n, got, tc.text)
		}
	}
	if got := unitNumber(1536, true); got != "1536" {
		t.Fatal(got)
	}
	if got := decimalPrefix("4294967297"); got != 1 {
		t.Fatal(got)
	}
	if got := decimalPrefix("99999999999999999999999"); got != -1 {
		t.Fatal(got)
	}
}
