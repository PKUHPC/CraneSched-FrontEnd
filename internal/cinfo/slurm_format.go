// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Slurm sinfo output options set by the sinfo wrapper. They are only used in
// Slurm output mode and select the Slurm-style renderer below.
var (
	FlagSlurmFormat     string
	FlagSlurmFormatLong string
	FlagSlurmNodeView   bool
)

// SlurmDefaultFormat and SlurmDefaultNodeFormat are Slurm's default sinfo
// formats for the partition and node-oriented views. "#" sizes a field to
// its widest value.
const (
	SlurmDefaultFormat     = "%#P %.5a %.10l %.6D %.6t %N"
	SlurmDefaultNodeFormat = "%#N %.6D %#P %6t"
	// Slurm's -R default also has USER and TIMESTAMP columns, which the
	// controller does not report.
	SlurmDefaultReasonFormat = "%#E %N"
)

const autoWidth = -2

// slurmSinfoRow is one output line: a group of nodes sharing a partition and
// state, or a single node in the node-oriented view.
type slurmSinfoRow struct {
	partition   string
	avail       string
	nodeCount   uint64
	hasNodes    bool
	resource    protos.CranedResourceState
	control     protos.CranedControlState
	power       protos.CranedPowerState
	nodeList    string
	reason      string
	partitionNo int
}

type slurmSinfoField struct {
	header string
	value  func(row *slurmSinfoRow) string
}

var slurmSinfoFields = map[string]slurmSinfoField{
	"partition":     {"PARTITION", func(r *slurmSinfoRow) string { return slurmPartitionName(r, true) }},
	"partitionname": {"PARTITION", func(r *slurmSinfoRow) string { return slurmPartitionName(r, false) }},
	"available":     {"AVAIL", func(r *slurmSinfoRow) string { return r.avail }},
	"timelimit":     {"TIMELIMIT", func(r *slurmSinfoRow) string { return "infinite" }},
	"nodes":         {"NODES", func(r *slurmSinfoRow) string { return strconv.FormatUint(r.nodeCount, 10) }},
	"statecompact":  {"STATE", func(r *slurmSinfoRow) string { return slurmNodeState(r, true) }},
	"statelong":     {"STATE", func(r *slurmSinfoRow) string { return slurmNodeState(r, false) }},
	"nodelist":      {"NODELIST", func(r *slurmSinfoRow) string { return r.nodeList }},
	"nodehost":      {"HOSTNAMES", func(r *slurmSinfoRow) string { return r.nodeList }},
	"reason":        {"REASON", func(r *slurmSinfoRow) string { return slurmReason(r) }},
	"nodeaiot":      {"NODES(A/I/O/T)", slurmNodeAIOT},
}

// Slurm single-letter specifiers accepted by sinfo -o.
var slurmSinfoFormatLetters = map[string]string{
	"P": "partition",
	"R": "partitionname",
	"a": "available",
	"l": "timelimit",
	"D": "nodes",
	"t": "statecompact",
	"T": "statelong",
	"N": "nodelist",
	"n": "nodehost",
	"E": "reason",
	"F": "nodeaiot",
}

// Slurm field names accepted by sinfo -O. Keys are lower case.
var slurmSinfoLongFields = map[string]string{
	"partition":     "partition",
	"partitionname": "partitionname",
	"available":     "available",
	"timelimit":     "timelimit",
	"nodes":         "nodes",
	"statecompact":  "statecompact",
	"statelong":     "statelong",
	"nodelist":      "nodelist",
	"nodehost":      "nodehost",
	"reason":        "reason",
	"nodeaiot":      "nodeaiot",
}

// SlurmSinfoFormatHelp lists the supported -o specifiers for the wrapper help.
const SlurmSinfoFormatHelp = `Specify the output format using Slurm format specifiers:
	%P - Partition name followed by "*" for the default partition.
	%R - Partition name.
	%a - State/availability of the partition.
	%l - Maximum time for any job, always "infinite".
	%D - Number of nodes.
	%t - Node state in compact form, e.g. idle, mix, alloc, drain, down.
	%T - Node state in extended form.
	%N - List of node names.
	%n - List of node hostnames.
	%E - Reason a node is unavailable.
	%F - Number of nodes by state in the form "allocated/idle/other/total".
Use %[.]size to set a field width; "." right-justifies the field.`

// SlurmSinfoFormatLongHelp lists the supported -O fields for the wrapper help.
const SlurmSinfoFormatLongHelp = `Specify the output format using Slurm field names, separated by commas.
Supported fields: Partition, PartitionName, Available, Timelimit, Nodes, StateCompact,
StateLong, NodeList, NodeHost, Reason, NodeAIOT. Append ":[.]size" to set a field width.`

type slurmSinfoColumn struct {
	text       string
	field      string
	width      int
	rightAlign bool
}

var slurmSinfoFormatRegex = regexp.MustCompile(`%(\.)?(#|\d+)?([a-zA-Z])`)

func parseSlurmSinfoFormat(format string) ([]slurmSinfoColumn, error) {
	matches := slurmSinfoFormatRegex.FindAllStringSubmatchIndex(format, -1)
	if matches == nil {
		return nil, fmt.Errorf("invalid format: %q", format)
	}
	var columns []slurmSinfoColumn
	addText := func(text string) error {
		if strings.Contains(text, "%") {
			return fmt.Errorf("invalid format specifier in %q", text)
		}
		columns = append(columns, slurmSinfoColumn{text: text, width: -1})
		return nil
	}
	last := 0
	for _, m := range matches {
		if m[0] > last {
			if err := addText(format[last:m[0]]); err != nil {
				return nil, err
			}
		}
		letter := format[m[6]:m[7]]
		field, ok := slurmSinfoFormatLetters[letter]
		if !ok {
			return nil, fmt.Errorf("format specifier %%%s is not supported", letter)
		}
		width := -1
		if m[4] != -1 && format[m[4]:m[5]] == "#" {
			width = autoWidth
		} else if m[4] != -1 {
			value, err := strconv.Atoi(format[m[4]:m[5]])
			if err != nil {
				return nil, fmt.Errorf("invalid width in format: %q", format)
			}
			width = value
		}
		columns = append(columns, slurmSinfoColumn{field: field, width: width, rightAlign: m[2] != -1})
		last = m[1]
	}
	if last < len(format) {
		if err := addText(format[last:]); err != nil {
			return nil, err
		}
	}
	return columns, nil
}

func parseSlurmSinfoFormatLong(format string) ([]slurmSinfoColumn, error) {
	var columns []slurmSinfoColumn
	for i, item := range strings.Split(format, ",") {
		name, size, hasSize := strings.Cut(strings.TrimSpace(item), ":")
		field, ok := slurmSinfoLongFields[strings.ToLower(name)]
		if !ok {
			return nil, fmt.Errorf("unsupported field for -O/--Format: %q", name)
		}
		column := slurmSinfoColumn{field: field, width: 20}
		if hasSize {
			column.rightAlign = strings.HasPrefix(size, ".")
			width, err := strconv.Atoi(strings.TrimPrefix(size, "."))
			if err != nil {
				return nil, fmt.Errorf("invalid field size in -O/--Format: %q", item)
			}
			column.width = width
		}
		if i > 0 {
			columns = append(columns, slurmSinfoColumn{text: " ", width: -1})
		}
		columns = append(columns, column)
	}
	return columns, nil
}

// slurmPartitionName keeps or strips the "*" the controller appends to the
// default partition name.
func slurmPartitionName(row *slurmSinfoRow, markDefault bool) string {
	if markDefault {
		return row.partition
	}
	return strings.TrimSuffix(row.partition, "*")
}

func slurmNodeState(row *slurmSinfoRow, compact bool) string {
	if !row.hasNodes {
		return "n/a"
	}
	state := util.FormatSlurmNodeState(row.resource, row.control, row.power)
	if !compact {
		return state
	}
	// Slurm compact states use a single base state, e.g. "drain" for an
	// idle node that is drained.
	if row.control == protos.CranedControlState_CRANE_DRAIN {
		return "drain"
	}
	base, _, _ := strings.Cut(state, "+")
	return base
}

func slurmReason(row *slurmSinfoRow) string {
	if row.reason == "" {
		return "none"
	}
	return row.reason
}

func slurmNodeAIOT(row *slurmSinfoRow) string {
	var allocated, idle, other uint64
	switch {
	case !row.hasNodes:
	case row.resource == protos.CranedResourceState_CRANE_DOWN ||
		row.control == protos.CranedControlState_CRANE_DRAIN:
		other = row.nodeCount
	case row.resource == protos.CranedResourceState_CRANE_IDLE:
		idle = row.nodeCount
	default:
		allocated = row.nodeCount
	}
	return fmt.Sprintf("%d/%d/%d/%d", allocated, idle, other, row.nodeCount)
}

func buildSlurmSinfoRows(reply *protos.QueryClusterInfoReply, nodeView bool) []*slurmSinfoRow {
	var rows []*slurmSinfoRow
	for idx, partition := range reply.Partitions {
		avail := strings.ToLower(strings.TrimPrefix(partition.State.String(), "PARTITION_"))
		hasNodes := false
		for _, craned := range partition.CranedLists {
			if craned.Count == 0 {
				continue
			}
			hasNodes = true
			base := slurmSinfoRow{
				partition:   partition.Name,
				avail:       avail,
				nodeCount:   uint64(craned.Count),
				hasNodes:    true,
				resource:    craned.ResourceState,
				control:     craned.ControlState,
				power:       craned.PowerState,
				nodeList:    craned.CranedListRegex,
				reason:      craned.Reason,
				partitionNo: idx,
			}
			if !nodeView {
				row := base
				rows = append(rows, &row)
				continue
			}
			nodes, ok := util.ParseHostList(craned.CranedListRegex)
			if !ok {
				nodes = []string{craned.CranedListRegex}
			}
			for _, node := range nodes {
				row := base
				row.nodeCount = 1
				row.nodeList = node
				rows = append(rows, &row)
			}
		}
		if !hasNodes && !nodeView {
			rows = append(rows, &slurmSinfoRow{
				partition: partition.Name, avail: avail, partitionNo: idx,
			})
		}
	}
	if nodeView {
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].nodeList != rows[j].nodeList {
				return rows[i].nodeList < rows[j].nodeList
			}
			return rows[i].partitionNo < rows[j].partitionNo
		})
	}
	return rows
}

// UseSlurmFormat reports whether the Slurm-style renderer should be used. All
// sinfo wrapper output uses it; -o/-O/-N/-R only select the columns and rows.
func UseSlurmFormat() bool {
	return util.IsSlurmOutputMode()
}

func slurmSinfoColumns() ([]slurmSinfoColumn, error) {
	switch {
	case FlagSlurmFormatLong != "":
		return parseSlurmSinfoFormatLong(FlagSlurmFormatLong)
	case FlagSlurmFormat != "":
		return parseSlurmSinfoFormat(FlagSlurmFormat)
	case FlagSlurmNodeView:
		return parseSlurmSinfoFormat(SlurmDefaultNodeFormat)
	case FlagListReason:
		return parseSlurmSinfoFormat(SlurmDefaultReasonFormat)
	default:
		return parseSlurmSinfoFormat(SlurmDefaultFormat)
	}
}

// FormatSlurmSinfo renders the reply with Slurm sinfo -o/-O/-N semantics.
func FormatSlurmSinfo(reply *protos.QueryClusterInfoReply) ([]string, error) {
	columns, err := slurmSinfoColumns()
	if err != nil {
		return nil, util.NewCraneErr(util.ErrorInvalidFormat, err.Error())
	}
	rows := buildSlurmSinfoRows(reply, FlagSlurmNodeView)
	if FlagListReason {
		rows = filterSlurmReasonRows(rows)
	}
	resolveAutoWidths(columns, rows)

	var lines []string
	if !FlagNoHeader {
		lines = append(lines, renderSlurmSinfoLine(columns, nil))
	}
	for _, row := range rows {
		lines = append(lines, renderSlurmSinfoLine(columns, row))
	}
	return lines, nil
}

// filterSlurmReasonRows keeps down or drained nodes for -R. A node set listed
// under several partitions is reported once, as in Slurm.
func filterSlurmReasonRows(rows []*slurmSinfoRow) []*slurmSinfoRow {
	seen := map[string]bool{}
	var filtered []*slurmSinfoRow
	for _, row := range rows {
		if !row.hasNodes || (row.resource != protos.CranedResourceState_CRANE_DOWN &&
			row.control != protos.CranedControlState_CRANE_DRAIN) {
			continue
		}
		key := row.reason + "\x00" + row.nodeList
		if seen[key] {
			continue
		}
		seen[key] = true
		filtered = append(filtered, row)
	}
	return filtered
}

func resolveAutoWidths(columns []slurmSinfoColumn, rows []*slurmSinfoRow) {
	for i := range columns {
		if columns[i].width != autoWidth {
			continue
		}
		field := slurmSinfoFields[columns[i].field]
		width := len(field.header)
		for _, row := range rows {
			width = max(width, len(field.value(row)))
		}
		columns[i].width = width
	}
}

// renderSlurmSinfoLine renders the header when row is nil.
func renderSlurmSinfoLine(columns []slurmSinfoColumn, row *slurmSinfoRow) string {
	var builder strings.Builder
	for _, column := range columns {
		if column.field == "" {
			builder.WriteString(column.text)
			continue
		}
		field := slurmSinfoFields[column.field]
		value := field.header
		if row != nil {
			value = field.value(row)
		}
		builder.WriteString(padSlurmField(value, column.width, column.rightAlign))
	}
	return strings.TrimRight(builder.String(), " ")
}

// padSlurmField pads or truncates value to width like Slurm's sinfo does.
func padSlurmField(value string, width int, rightAlign bool) string {
	if width < 0 {
		return value
	}
	if len(value) > width {
		return value[:width]
	}
	padding := strings.Repeat(" ", width-len(value))
	if rightAlign {
		return padding + value
	}
	return value + padding
}
