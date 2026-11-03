// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"fmt"
	"io"
	"strings"
)

func writeVerbose(w io.Writer, o *options) error {
	groups := make(map[string]bool)
	for _, item := range o.Fields {
		if item.Field != nil {
			groups[item.Field.Group] = true
		}
	}
	optional := func(value string, set bool, absent string) string {
		if set {
			return value
		}
		return absent
	}
	var out strings.Builder
	states := optional(o.States, o.HasStates || o.Reasons, "(null)")
	if o.Reasons && !o.HasStates {
		states = "down" // The reference tokenizes its implicit state string in place.
	}
	out.WriteString("-----------------------------\n")
	for _, item := range []struct {
		name  string
		value any
	}{
		{"dead", o.Dead}, {"exact", map[bool]int{true: 1}[o.Exact]},
		{"filtering", o.Dead || o.Responding || o.HasNodes || o.HasPartition || o.HasStates || o.Reasons},
		{"format", o.Format}, {"iterate", o.Iterate}, {"long", o.Long}, {"no_header", o.NoHeader},
		{"node_field", o.AutoNodeWidth}, {"node_format", o.Node},
		{"nodes", optional(o.Nodes, o.HasNodes, "n/a")}, {"part_field", o.AutoPartitionWidth},
		{"partition", optional(o.Partition, o.HasPartition, "n/a")}, {"responding", o.Responding},
		{"states", states}, {"sort", optional(o.Sort, o.HasSort, "(null)")},
		{"summarize", o.Summary}, {"verbose", o.Verbose},
	} {
		fmt.Fprintf(&out, "%-12s= %v\n", item.name, item.value)
	}
	out.WriteString("-----------------------------\n")
	groups["all"], groups["reservation_mode"] = o.All, o.Reservations
	for _, item := range []struct{ label, group string }{
		{"all_flag        = ", "all"}, {"alloc_mem_flag  = ", "allocmem"}, {"avail_flag      = ", "available"},
		{"comment_flag    = ", "comment"}, {"cpus_flag       = ", "cpus"}, {"default_time_flag =", "defaulttime"},
		{"disk_flag       = ", "disk"}, {"extra_flag      = ", "extra"}, {"features_flag   = ", "features"},
		{"features_flag_act = ", "activefeatures"}, {"groups_flag     = ", "groups"}, {"gres_flag       = ", "gres"},
		{"gres_used_flag  = ", "gresused"}, {"job_size_flag   = ", "size"}, {"max_time_flag   = ", "time"},
		{"memory_flag     = ", "memory"}, {"partition_flag  = ", "partition"}, {"port_flag       = ", "port"},
		{"priority_job_factor_flag   = ", "priorityfactor"}, {"priority_tier_flag   = ", "prioritytier"},
		{"reason_flag     = ", "reason"}, {"reason_timestamp_flag = ", "reasontime"}, {"reason_user_flag = ", "reasonuser"},
		{"reservation_flag = ", "reservation_mode"}, {"resv_name_flag   = ", "reservation"}, {"root_flag       = ", "root"},
		{"oversubscribe_flag      = ", "oversubscribe"}, {"state_flag      = ", "state"},
		{"statecomplete_flag = ", "statecomplete"}, {"weight_flag     = ", "weight"},
	} {
		fmt.Fprintf(&out, "%s%t\n", item.label, groups[item.group])
	}
	out.WriteString("-----------------------------\n\n")
	_, err := io.WriteString(w, out.String())
	return err
}
