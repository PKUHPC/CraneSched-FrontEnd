// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"slices"
	"testing"
)

func slurmSinfoTestReply() *protos.QueryClusterInfoReply {
	return &protos.QueryClusterInfoReply{
		Ok: true,
		Partitions: []*protos.TrimmedPartitionInfo{
			{
				Name:  "CPU*",
				State: protos.PartitionState_PARTITION_UP,
				CranedLists: []*protos.TrimmedPartitionInfo_TrimmedCranedInfo{
					{ResourceState: protos.CranedResourceState_CRANE_IDLE, Count: 2, CranedListRegex: "crnd[1-2]"},
					{ResourceState: protos.CranedResourceState_CRANE_MIX, Count: 0},
					{
						ResourceState: protos.CranedResourceState_CRANE_IDLE,
						ControlState:  protos.CranedControlState_CRANE_DRAIN,
						Count:         1, CranedListRegex: "crnd3", Reason: "maintenance",
					},
				},
			},
			{Name: "GPU", State: protos.PartitionState_PARTITION_DOWN},
		},
	}
}

func setSlurmSinfoFlags(t *testing.T, format, formatLong string, nodeView, noHeader bool) {
	t.Helper()
	oldFormat, oldLong, oldNode, oldNoHeader := FlagSlurmFormat, FlagSlurmFormatLong, FlagSlurmNodeView, FlagNoHeader
	util.SetOutputMode(util.OutputModeSlurm)
	t.Cleanup(func() {
		FlagSlurmFormat, FlagSlurmFormatLong, FlagSlurmNodeView, FlagNoHeader = oldFormat, oldLong, oldNode, oldNoHeader
		util.SetOutputMode(util.OutputModeNative)
	})
	FlagSlurmFormat, FlagSlurmFormatLong, FlagSlurmNodeView, FlagNoHeader = format, formatLong, nodeView, noHeader
}

func TestFormatSlurmSinfo(t *testing.T) {
	tests := []struct {
		name       string
		format     string
		formatLong string
		nodeView   bool
		noHeader   bool
		want       []string
	}{
		{
			name:   "format specifiers",
			format: "%P|%R|%a|%D|%t|%T|%N|%E|%F",
			want: []string{
				"PARTITION|PARTITION|AVAIL|NODES|STATE|STATE|NODELIST|REASON|NODES(A/I/O/T)",
				"CPU*|CPU|up|2|idle|idle|crnd[1-2]|none|0/2/0/2",
				"CPU*|CPU|up|1|drain|idle+drain|crnd3|maintenance|0/0/1/1",
				"GPU|GPU|down|0|n/a|n/a||none|0/0/0/0",
			},
		},
		{
			name:   "unsupported width syntax",
			format: "%.6R %-3D",
		},
		{
			name:   "partition default format",
			format: SlurmDefaultFormat,
			want: []string{
				"PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST",
				"CPU*         up   infinite      2   idle crnd[1-2]",
				"CPU*         up   infinite      1  drain crnd3",
				"GPU        down   infinite      0    n/a",
			},
		},
		{
			name:     "width and alignment",
			format:   "%.6R|%4D|",
			noHeader: true,
			want:     []string{"   CPU|2   |", "   CPU|1   |", "   GPU|0   |"},
		},
		{
			name:       "field names",
			formatLong: "PartitionName:5,StateCompact:.6,NodeList",
			want: []string{
				"PARTI  STATE NODELIST",
				"CPU     idle crnd[1-2]",
				"CPU    drain crnd3",
				"GPU      n/a",
			},
		},
		{
			name:     "node view default format",
			nodeView: true,
			want: []string{
				"NODELIST  NODES PARTITION STATE",
				"crnd1         1 CPU*      idle",
				"crnd2         1 CPU*      idle",
				"crnd3         1 CPU*      drain",
			},
		},
		{
			name:     "node view custom format",
			format:   "%N %P",
			nodeView: true,
			noHeader: true,
			want:     []string{"crnd1 CPU*", "crnd2 CPU*", "crnd3 CPU*"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setSlurmSinfoFlags(t, tt.format, tt.formatLong, tt.nodeView, tt.noHeader)
			got, err := FormatSlurmSinfo(slurmSinfoTestReply())
			if tt.want == nil {
				if err == nil {
					t.Fatalf("FormatSlurmSinfo() = %q, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("FormatSlurmSinfo() error: %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("FormatSlurmSinfo() =\n%q\nwant\n%q", got, tt.want)
			}
		})
	}
}

func TestFormatSlurmSinfoRejectsUnsupportedFields(t *testing.T) {
	for _, tc := range []struct{ format, formatLong string }{
		{format: "%P %Q"},
		{format: "no specifiers"},
		{formatLong: "Partition,NotAField"},
		{formatLong: "Partition:abc"},
	} {
		setSlurmSinfoFlags(t, tc.format, tc.formatLong, false, false)
		if _, err := FormatSlurmSinfo(slurmSinfoTestReply()); err == nil {
			t.Errorf("format %q / %q succeeded, want error", tc.format, tc.formatLong)
		}
	}
}

func TestUseSlurmFormatOnlyInSlurmMode(t *testing.T) {
	setSlurmSinfoFlags(t, "", "", false, false)
	if !UseSlurmFormat() {
		t.Fatal("UseSlurmFormat() = false for default sinfo in Slurm mode")
	}
	util.SetOutputMode(util.OutputModeNative)
	if UseSlurmFormat() {
		t.Fatal("UseSlurmFormat() = true in native mode")
	}
}

func TestFormatSlurmSinfoDefaults(t *testing.T) {
	tests := []struct {
		name       string
		noHeader   bool
		listReason bool
		want       []string
	}{
		{
			name: "default six columns",
			want: []string{
				"PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST",
				"CPU*         up   infinite      2   idle crnd[1-2]",
				"CPU*         up   infinite      1  drain crnd3",
				"GPU        down   infinite      0    n/a",
			},
		},
		{
			name:     "noheader keeps all columns",
			noHeader: true,
			want: []string{
				"CPU*         up   infinite      2   idle crnd[1-2]",
				"CPU*         up   infinite      1  drain crnd3",
				"GPU        down   infinite      0    n/a",
			},
		},
		{
			name:       "list reasons",
			listReason: true,
			want:       []string{"REASON      NODELIST", "maintenance crnd3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setSlurmSinfoFlags(t, "", "", false, tt.noHeader)
			oldListReason := FlagListReason
			t.Cleanup(func() { FlagListReason = oldListReason })
			FlagListReason = tt.listReason
			got, err := FormatSlurmSinfo(slurmSinfoTestReply())
			if err != nil {
				t.Fatalf("FormatSlurmSinfo() error: %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("FormatSlurmSinfo() =\n%q\nwant\n%q", got, tt.want)
			}
		})
	}
}

func TestFormatSlurmSinfoListReasonsDeduplicatesPartitions(t *testing.T) {
	setSlurmSinfoFlags(t, "", "", false, false)
	oldListReason := FlagListReason
	t.Cleanup(func() { FlagListReason = oldListReason })
	FlagListReason = true

	drained := &protos.TrimmedPartitionInfo_TrimmedCranedInfo{
		ResourceState: protos.CranedResourceState_CRANE_IDLE,
		ControlState:  protos.CranedControlState_CRANE_DRAIN,
		Count:         1, CranedListRegex: "crnd2", Reason: "add test reason",
	}
	reply := &protos.QueryClusterInfoReply{Ok: true, Partitions: []*protos.TrimmedPartitionInfo{
		{Name: "computing*", State: protos.PartitionState_PARTITION_UP, CranedLists: []*protos.TrimmedPartitionInfo_TrimmedCranedInfo{drained}},
		{Name: "CPU", State: protos.PartitionState_PARTITION_UP, CranedLists: []*protos.TrimmedPartitionInfo_TrimmedCranedInfo{drained}},
	}}
	got, err := FormatSlurmSinfo(reply)
	if err != nil {
		t.Fatalf("FormatSlurmSinfo() error: %v", err)
	}
	want := []string{"REASON          NODELIST", "add test reason crnd2"}
	if !slices.Equal(got, want) {
		t.Fatalf("FormatSlurmSinfo() = %q, want %q", got, want)
	}
}
