// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"fmt"
	"io"
	"math"
	"os"
	"os/user"
	"sort"
	"strconv"
	"strings"
)

type serializerOptions struct{ Compact, NoTag, Complex bool }

func parseSerializerEnvironment(env map[string]string) (map[string]serializerOptions, error) {
	settings := make(map[string]serializerOptions)
	for _, kind := range []string{"json", "yaml"} {
		flags := serializerOptions{Compact: env["SLURM_"+strings.ToUpper(kind)] != ""}
		for _, flag := range strings.Split(env["SLURM_"+strings.ToUpper(kind)], ",") {
			switch strings.ToLower(flag) {
			case "":
			case "compact":
				flags.Compact = true
			case "pretty":
				flags.Compact = false
			case "complex":
				flags.Complex = true
			case "no_tag":
				flags.NoTag = true
			default:
				return nil, &commandError{fmt.Sprintf("sinfo: fatal: Unable to parse serializer %q flags: Invalid argument\n", env["SLURM_"+strings.ToUpper(kind)])}
			}
		}
		settings[kind] = flags
	}
	return settings, nil
}

type schemaOptions struct {
	Version  int
	Complex  bool
	SpecOnly bool
	Plugin   string
}

func selectSchema(o *options, diagnostics io.Writer) (schemaOptions, error) {
	version, flags, _ := strings.Cut(o.DataParser, "+")
	if !o.HasDataParser {
		version = "v0.0.45"
	}
	version = strings.TrimPrefix(version, "data_parser/")
	result := schemaOptions{Plugin: "data_parser/" + version}
	if strings.Contains(o.DataParser, "+") {
		result.Plugin += "+" + flags
	}
	switch version {
	case "v0.0.42":
		result.Version = 42
	case "v0.0.43":
		result.Version = 43
	case "v0.0.44":
		result.Version = 44
	case "v0.0.45":
		result.Version = 45
	default:
		message := "sinfo: error: data_parser_g_new: invalid plugin \n"
		if version != "" {
			message = fmt.Sprintf("sinfo: error: load_plugins: unable to find plugin: data_parser/%s\nsinfo: error: data_parser_g_new: failure loading plugins: Plugin has invalid format or unable to be loaded\n", version)
		}
		return result, &commandError{message + fmt.Sprintf("sinfo: error: application/%s output not supported by data_parser/v0.0.45\n", o.Serialization)}
	}
	for _, flag := range strings.Split(flags, "+") {
		switch strings.ToLower(flag) {
		case "", "fast", "minimize_refs":
		case "spec_only":
			result.SpecOnly = true
		case "complex":
			result.Complex = true
		case "prefer_refs":
			fmt.Fprintf(diagnostics, "sinfo: data_parser/v0.0.%d ignoring default flag FLAG_PREFER_REFS\n", result.Version)
		default:
			fmt.Fprintf(diagnostics, "sinfo: warning: data_parser/v0.0.%d ignoring unknown flag %s\n", result.Version, flag)
		}
	}
	return result, nil
}

// An ordered object is shared by the JSON and YAML encoders. Ordering is part
// of the CLI contract; Go maps would silently reorder its output vocabulary.
type member struct {
	name  string
	value any
}
type object []member
type array []any

func stringsArray(values []string) array {
	a := make(array, len(values))
	for i, value := range values {
		a[i] = value
	}
	return a
}

func (s schemaOptions) number(n uint64, unset, infinite bool) any {
	if s.Complex {
		if unset {
			return nil
		}
		if infinite {
			return "Infinity"
		}
		return n
	}
	if unset || infinite {
		n = 0
	}
	return object{{"set", !unset && !infinite}, {"infinite", infinite}, {"number", n}}
}

func (s schemaOptions) limit(l durationLimit) any {
	return s.number(l.Seconds/60, l.Unset, l.Infinite)
}

func orderedNames(names []string) array {
	unique := make(map[string]bool)
	result := make([]string, 0, len(names))
	for _, name := range names {
		if name != "" && !unique[name] {
			result = append(result, name)
			unique[name] = true
		}
	}
	sort.Slice(result, func(i, j int) bool { return hostLess(result[i], result[j]) })
	return stringsArray(result)
}

func (s schemaOptions) row(r *row, o *options, userName func(uint32) (string, bool)) object {
	n := &node{}
	if len(r.Nodes) != 0 {
		n = r.Nodes[0]
	}
	countsNodes, countsCPUs := r.counts()
	rangeOf := func(group string) object {
		lo, hi := r.minmax(group)
		return object{{"minimum", lo}, {"maximum", hi}}
	}
	freeLo, freeHi := r.minmax("freemem")
	freeRange := object{{"minimum", s.number(freeLo, freeLo == 0xfffffffffffffffe, freeLo == 0xffffffffffffffff)}, {"maximum", s.number(freeHi, freeHi == 0xfffffffffffffffe, freeHi == 0xffffffffffffffff)}}
	var addresses, hostnames array
	for _, field := range o.Fields {
		if field.Field == nil {
			continue
		}
		switch field.Field.Group {
		case "address":
			addresses = orderedNames(r.names("address"))
		case "hostname":
			hostnames = orderedNames(r.names("hostname"))
		}
	}
	state := array{}
	if len(r.Nodes) != 0 {
		state = stringsArray(strings.Split(n.State.complete(), "+"))
	}
	reasonTime := int64(0)
	if !n.ReasonTime.IsZero() {
		reasonTime = n.ReasonTime.Unix()
	}
	reasonUser := ""
	if n.ReasonUID != nil {
		var ok bool
		reasonUser, ok = userName(*n.ReasonUID)
		if !ok {
			reasonUser = strconv.FormatUint(uint64(*n.ReasonUID), 10)
		}
	}
	reason := object{{"description", n.Reason}, {"time", reasonTime}}
	if s.Version < 45 {
		reason = append(reason, member{"user", reasonUser})
	}
	cpus := object{{"allocated", countsCPUs.Alloc}, {"idle", countsCPUs.Idle}, {"other", countsCPUs.Other}, {"total", countsCPUs.Total}}
	cpus = append(cpus, rangeOf("cpus")...)
	cpus = append(cpus, member{"load", rangeOf("load")}, member{"per_node", object{{"max", s.number(r.Partition.MaxCPUs, false, r.Partition.MaxCPUs == 0xffffffff)}}})
	memory := rangeOf("memory")
	allocatedMemory := uint64(0)
	if len(r.Nodes) != 0 {
		allocatedMemory = r.Nodes[len(r.Nodes)-1].AllocMemory
	}
	memory = append(memory, member{"free", freeRange}, member{"allocated", allocatedMemory})
	result := object{
		{"port", n.Port}, {"node", object{{"state", state}}},
		{"nodes", object{{"allocated", countsNodes.Alloc}, {"idle", countsNodes.Idle}, {"other", countsNodes.Other}, {"total", countsNodes.Total}, {"hostnames", hostnames}, {"addresses", addresses}, {"nodes", orderedNames(r.names(""))}}},
		{"cpus", cpus}, {"sockets", rangeOf("sockets")}, {"cores", rangeOf("cores")}, {"threads", rangeOf("threads")},
		{"disk", rangeOf("disk")}, {"memory", memory}, {"weight", rangeOf("weight")},
		{"features", object{{"total", n.Features}, {"active", n.ActiveFeatures}}},
		{"gres", object{{"total", n.Gres}, {"used", n.GresUsed}}},
		{"cluster", r.Cluster}, {"comment", n.Comment}, {"extra", n.Extra}, {"reason", reason}, {"reservation", n.Reservation},
	}
	if s.Version >= 45 {
		result = append(result, member{"reason_set_by_user", reasonUser})
	}
	return append(result, member{"resource_spec", object{{"cpus", ""}, {"memory", 0}}}, member{"partition", s.partition(r.Partition)})
}

func (s schemaOptions) partition(p *partition) object {
	if p == nil {
		return nil
	}
	allAsEmpty := func(v string) string {
		if v == "all" {
			return ""
		}
		return v
	}
	configured := p.ConfiguredNodes
	if configured == "" {
		configured = compressHosts(p.Nodes)
	}
	legacyMemory := func(cpu, node uint64) int64 {
		if s.Version >= 45 {
			return 0
		}
		if cpu != 0 {
			return int64(cpu | 1<<63)
		}
		return int64(node)
	}
	memoryFields := func(cpu, node uint64) object {
		return object{{"memory_per_cpu", legacyMemory(cpu, node)}, {"partition_memory_per_cpu", s.number(cpu, cpu == 0, false)}, {"partition_memory_per_node", s.number(node, node == 0, false)}}
	}
	defaults := memoryFields(p.DefaultMemCPU, p.DefaultMemNode)
	defaults = append(defaults, member{"time", s.limit(p.DefaultTime)}, member{"job", ""})
	var binding any = 0
	selectType := array{}
	if s.Version >= 45 {
		binding = array{}
		selectType = array{"NONE"}
	}
	tres := p.TRES
	if tres == "" {
		tres = fmt.Sprintf("cpu=%d,mem=%s,node=%d,billing=%d", p.TotalCPUs, memoryTRES(p.TotalMemory), len(p.Nodes), p.TotalCPUs)
	}
	result := object{
		{"nodes", object{{"allowed_allocation", allAsEmpty(p.AllocNodes)}, {"configured", configured}, {"total", len(p.Nodes)}}},
		{"accounts", object{{"allowed", strings.Join(p.AllowedAccounts, ",")}, {"deny", strings.Join(p.DeniedAccounts, ",")}}},
		{"groups", object{{"allowed", allAsEmpty(p.Groups)}}},
		{"qos", object{{"allowed", ""}, {"deny", ""}, {"assigned", p.DefaultQoS}}},
		{"alternate", ""}, {"tres", object{{"billing_weights", ""}, {"configured", tres}}}, {"cluster", ""},
		{"select_type", selectType}, {"cpus", object{{"task_binding", binding}, {"total", p.TotalCPUs}}}, {"defaults", defaults},
	}
	state := strings.ToUpper(p.Available)
	if s.Version >= 45 {
		flags := array{}
		if p.Default {
			flags = append(flags, "DEFAULT")
		}
		if p.Hidden {
			flags = append(flags, "HIDDEN")
		}
		if p.RootOnly {
			flags = append(flags, "ROOT_ONLY")
		}
		result = append(result, member{"flags", flags}, member{"partition", object{{"exclusive", "NO"}, {"oversubscribe", p.OverSubscribe}, {"state", array{state}}}})
	}
	maximums := object{{"cpus_per_node", s.number(p.MaxCPUs, false, p.MaxCPUs == 0xffffffff)}, {"cpus_per_socket", s.number(0, false, true)}}
	maximums = append(maximums, memoryFields(p.MaxMemCPU, p.MaxMemNode)...)
	shares := 1
	if s.Version >= 45 {
		shares = 0
	}
	maximums = append(maximums,
		member{"nodes", s.number(p.MaxNodes, false, p.MaxNodes == 0xffffffff)}, member{"shares", shares},
		member{"oversubscribe", object{{"jobs", shares}, {"flags", array{}}}}, member{"time", s.limit(p.TimeLimit)}, member{"over_time_limit", s.number(0, true, false)},
	)
	result = append(result, member{"grace_time", 0}, member{"maximums", maximums}, member{"minimums", object{{"nodes", p.MinNodes}}}, member{"name", p.Name}, member{"node_sets", p.NodeSets})
	if s.Version >= 45 {
		result = append(result, member{"preempt_mode", array{"CLUSTER_GLOBAL"}})
	}
	result = append(result,
		member{"priority", object{{"job_factor", p.PriorityFactor}, {"tier", p.PriorityTier}}},
		member{"timeouts", object{{"resume", s.number(0, true, false)}, {"suspend", s.number(0, true, false)}}},
	)
	if s.Version >= 43 {
		result = append(result, member{"topology", ""})
	}
	if s.Version < 45 {
		result = append(result, member{"partition", object{{"state", array{state}}}})
	}
	return append(result, member{"suspend_time", s.number(0, true, false)})
}

func memoryTRES(mib uint64) string {
	// TRES starts in MiB (unlike the table's unit-less exact conversion).
	value, units := float64(mib), "MGTPE"
	i := 0
	for value >= 1024 && i < len(units)-1 {
		value /= 1024
		i++
	}
	if value == math.Trunc(value) {
		return fmt.Sprintf("%.0f%c", value, units[i])
	}
	return fmt.Sprintf("%.2f%c", value, units[i])
}

func renderStructured(w, diagnostics io.Writer, argv []string, rows []*row, snap *snapshot, o *options, userName func(uint32) (string, bool)) error {
	if strings.EqualFold(o.DataParser, "list") {
		fmt.Fprintln(diagnostics, "Possible data_parser plugins:")
		// The reference distribution's discovery order is stable for this
		// compatibility baseline, and is not numerical version order.
		_, err := io.WriteString(w, "data_parser/v0.0.45\ndata_parser/v0.0.43\ndata_parser/v0.0.42\ndata_parser/v0.0.44\n")
		return err
	}
	schema, err := selectSchema(o, diagnostics)
	if err != nil {
		return err
	}
	entries := make(array, 0)
	if !schema.SpecOnly {
		// All supported sinfo schema versions implicitly request these fields,
		// even when -o selects a smaller text/grouping vocabulary. An absent
		// Crane feature must not turn into a plausible zero or empty JSON value.
		for _, field := range []string{"Disk", "Weight", "Features", "features_act", "Comment", "Extra", "MaxCPUsPerNode", "AllocNodes", "Groups", "DefaultTime", "OverSubscribe", "Root", "Size", "PriorityTier"} {
			if capability := snap.UnsupportedFields[field]; capability != "" {
				return unsupported("--"+o.Serialization+" field "+field, capability)
			}
		}
		if err := validateProjection(rows, o); err != nil {
			return err
		}
		for _, row := range rows {
			entries = append(entries, schema.row(row, o, userName))
		}
	}
	name, _ := userName(uint32(os.Getuid()))
	group := strconv.Itoa(os.Getgid())
	if g, err := user.LookupGroupId(group); err == nil {
		group = g.Name
	}
	root := object{
		{"sinfo", entries},
		{"meta", object{
			{"plugin", object{{"type", ""}, {"name", ""}, {"data_parser", schema.Plugin}, {"accounting_storage", snap.AccountingStorage}}},
			{"client", object{{"source", ""}, {"user", name}, {"group", group}}}, {"command", stringsArray(argv)},
			{"slurm", object{{"version", object{{"major", "26"}, {"micro", "4"}, {"minor", "05"}}}, {"release", compatibilityVersion}, {"cluster", snap.Cluster}}},
		}}, {"errors", array{}}, {"warnings", array{}},
	}
	if schema.SpecOnly {
		root, err = publicSchema(schema.Version)
		if err != nil {
			return err
		}
	} else if schema.Complex {
		root = complexValues(root, schema.Version, "")
	}
	if o.Serialization == "yaml" {
		return writeYAML(w, root, o.Serializer)
	}
	return writeJSON(w, root, o.Serializer)
}

// COMPLEX exposes absent optional values as null. These paths are fixed by
// the public versioned schema, not inferred by decoding a text table.
func complexValues(value object, version int, path string) object {
	for i := range value {
		field := &value[i]
		key := path + "/" + field.name
		switch child := field.value.(type) {
		case object:
			field.value = complexValues(child, version, key)
		case array:
			for j, element := range child {
				if obj, ok := element.(object); ok {
					child[j] = complexValues(obj, version, key)
				}
			}
		case string:
			if child == "" && key != "/sinfo/reason_set_by_user" && key != "/sinfo/reason/user" {
				field.value = nil
			}
		}
		if version >= 45 {
			switch key {
			case "/sinfo/partition/defaults/memory_per_cpu", "/sinfo/partition/maximums/memory_per_cpu", "/sinfo/partition/maximums/shares", "/sinfo/partition/maximums/oversubscribe/jobs", "/sinfo/partition/maximums/oversubscribe/flags":
				field.value = nil
			}
		}
	}
	return value
}
