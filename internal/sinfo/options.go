// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package sinfo implements the Slurm sinfo command contract against Crane data.
// Parsing, planning and rendering do not depend on a running controller.
package sinfo

import (
	"fmt"
	"strconv"
	"strings"
)

const compatibilityVersion = "26.05.4"

type options struct {
	Program                                                      string
	All, Dead, Exact, Future, NoHeader, Local, Long              bool
	Node, Responding, Reasons, Summary, Reservations, Federation bool
	NoConvert, HasSort                                           bool
	Iterate, Verbose                                             int
	Clusters, Nodes, Partition, Sort, States, Format             string
	HasClusters, HasNodes, HasPartition, HasStates, HasFormat    bool
	LongFormat, DefaultFormat, AutoPartitionWidth, AutoNodeWidth bool
	StateFilter                                                  stateExpression
	NodeFilter                                                   *hostList
	Fields                                                       []formatItem
	Serialization, DataParser, Action, ActionValue               string
	HasDataParser                                                bool
	Serializer                                                   serializerOptions
	Diagnostics                                                  []string
}

type optionSpec struct {
	name  string
	short byte
	value byte // 0: absent, 1: required, 2: optional (only with '=').
}

// This is the public option vocabulary, not a general-purpose CLI framework.
var optionSpecs = []optionSpec{
	{"all", 'a', 0}, {"dead", 'd', 0}, {"exact", 'e', 0},
	{"future", 'F', 0}, {"noheader", 'h', 0}, {"iterate", 'i', 1},
	{"long", 'l', 0}, {"cluster", 'M', 1}, {"clusters", 'M', 1},
	{"nodes", 'n', 1}, {"Node", 'N', 0}, {"format", 'o', 1},
	{"Format", 'O', 1}, {"partition", 'p', 1}, {"responding", 'r', 0},
	{"list-reasons", 'R', 0}, {"summarize", 's', 0}, {"sort", 'S', 1},
	{"states", 't', 1}, {"reservation", 'T', 0}, {"verbose", 'v', 0},
	{"version", 'V', 0}, {"autocomplete", 0, 1}, {"federation", 0, 0},
	{"help", 0, 0}, {"helpformat", 0, 0}, {"helpFormat", 0, 0},
	{"helpstate", 0, 0}, {"hide", 0, 0}, {"local", 0, 0},
	{"noconvert", 0, 0}, {"usage", 0, 0}, {"json", 0, 2}, {"yaml", 0, 2},
}

type commandError struct{ message string }

func (e *commandError) Error() string { return e.message }
func optionError(text string) error   { return &commandError{"sinfo: error: " + text + "\n"} }
func getoptError(program, text string) error {
	return &commandError{program + ": " + text + "\nTry \"sinfo --help\" for more information\n"}
}

func parseOptions(argv []string, env map[string]string) (*options, error) {
	o := &options{Program: "sinfo"}
	if len(argv) != 0 {
		o.Program = argv[0]
		argv = argv[1:]
	}
	_, o.All = env["SINFO_ALL"]
	_, o.Federation = env["SINFO_FEDERATION"]
	_, o.Future = env["SINFO_FUTURE"]
	_, o.Local = env["SINFO_LOCAL"]
	o.Partition, o.HasPartition = env["SINFO_PARTITION"]
	if o.All && o.HasPartition {
		return nil, optionError("Conflicting options, SINFO_ALL and SINFO_PARTITION, specified. Please choose one or the other.")
	}
	o.All = o.All || o.HasPartition
	o.Sort, o.HasSort = env["SINFO_SORT"]
	o.Clusters, o.HasClusters = env["SLURM_CLUSTERS"]
	o.Local = o.Local || o.HasClusters
	_, posix := env["POSIXLY_CORRECT"]
	var explicitAll, explicitPartition bool
	apply := func(spec optionSpec, value string) error {
		switch spec.name {
		case "all":
			o.All = true
			explicitAll = true
			o.HasPartition = false
			o.Partition = ""
		case "hide":
			o.All = false
		case "dead":
			o.Dead = true
		case "exact":
			o.Exact = true
		case "future":
			o.Future = true
		case "noheader":
			o.NoHeader = true
		case "iterate":
			o.Iterate = decimalPrefix(value)
			if o.Iterate <= 0 {
				return optionError("Error: invalid entry for --iterate=" + value)
			}
		case "long":
			o.Long = true
		case "cluster", "clusters":
			o.Clusters = value
			o.HasClusters = true
			o.Local = true
		case "nodes":
			list, err := parseHostList(value)
			if err != nil {
				diagnostic := fmt.Sprintf("'%s' invalid entry for --nodes", value)
				if detail, ok := err.(*hostRangeError); ok {
					diagnostic = detail.Error() + "\nsinfo: error: " + diagnostic
				}
				return optionError(diagnostic)
			}
			o.Nodes, o.HasNodes, o.NodeFilter = value, true, list
		case "Node":
			o.Node = true
		case "Format":
			o.LongFormat = true
			o.Format, o.HasFormat = value, true
		case "format":
			o.Format, o.HasFormat = value, true
		case "partition":
			o.Partition, o.HasPartition = value, true
			o.All, explicitPartition = true, true
		case "responding":
			o.Responding = true
		case "list-reasons":
			o.Reasons = true
		case "summarize":
			o.Summary = true
		case "sort":
			o.Sort = value
			o.HasSort = true
		case "states":
			filter, err := parseStates(value, o.StateFilter.AllOf)
			if err != nil {
				return err
			}
			o.States, o.HasStates, o.StateFilter = value, true, filter
		case "reservation":
			o.Reservations = true
		case "verbose":
			o.Verbose++
		case "federation":
			o.Federation = true
		case "local":
			o.Local = true
		case "noconvert":
			o.NoConvert = true
		case "json", "yaml":
			settings, err := parseSerializerEnvironment(env)
			if err != nil {
				return err
			}
			o.Serializer = settings[spec.name]
			o.Serialization, o.DataParser = spec.name, value
		default:
			o.Action, o.ActionValue = spec.name, value
		}
		return nil
	}
	for i := 0; i < len(argv); i++ {
		arg := argv[i]
		if arg == "--" {
			break
		}
		if arg == "-" || !strings.HasPrefix(arg, "-") {
			if posix {
				break
			}
			continue
		}
		if strings.HasPrefix(arg, "--") {
			name, value, attached := strings.Cut(arg[2:], "=")
			spec, err := longOption(o.Program, name, arg)
			if err != nil {
				return nil, err
			}
			if spec.value == 0 && attached {
				return nil, getoptError(o.Program, fmt.Sprintf("option '--%s' doesn't allow an argument", spec.name))
			}
			if spec.value == 1 && !attached {
				if i+1 == len(argv) {
					return nil, getoptError(o.Program, fmt.Sprintf("option '%s' requires an argument", arg))
				}
				i++
				value = argv[i]
			}
			if err := apply(spec, value); err != nil {
				return nil, err
			}
			if spec.name == "json" || spec.name == "yaml" {
				o.HasDataParser = attached
			}
		} else {
			for j := 1; j < len(arg); j++ {
				var found *optionSpec
				for k := range optionSpecs {
					if optionSpecs[k].short == arg[j] {
						found = &optionSpecs[k]
						break
					}
				}
				if found == nil {
					return nil, getoptError(o.Program, fmt.Sprintf("invalid option -- '%c'", arg[j]))
				}
				value := ""
				if found.value != 0 {
					if j+1 < len(arg) {
						value = arg[j+1:]
					} else {
						if i+1 == len(argv) {
							return nil, getoptError(o.Program, fmt.Sprintf("option requires an argument -- '%c'", arg[j]))
						}
						i++
						value = argv[i]
					}
					j = len(arg)
				}
				if err := apply(*found, value); err != nil {
					return nil, err
				}
				if o.Action != "" {
					return o, nil
				}
			}
		}
		if o.Action != "" {
			return o, nil
		}
	}
	if o.Long && o.HasFormat {
		return nil, &commandError{"sinfo: fatal: Options -o(--format) and -l(--long) are mutually exclusive. Please remove one and retry.\n"}
	}
	if explicitAll && explicitPartition {
		return nil, optionError("Conflicting options, -a and -p, specified. Please choose one or the other.")
	}
	if !o.HasFormat {
		o.DefaultFormat = true
		o.LongFormat = true
		switch {
		case o.Summary:
			o.AutoPartitionWidth = true
			o.Format = "partition:9 ,available:.5 ,time:.10 ,nodeaiot:.16 ,nodelist:0"
		case o.Node:
			o.AutoPartitionWidth, o.AutoNodeWidth = true, true
			o.Format = "nodelist:0 ,nodes:.6 ,partition:.9 ,statecompact:6"
			if o.Long {
				o.Format = "nodelist:0 ,nodes:.6 ,partition:.9 ,statelong:.11 ,cpus:4 ,socketcorethread:.8 ,memory:.6 ,disk:.8 ,weight:.6 ,features:.8 ,reason:20"
			}
		case o.Reasons:
			o.Format = "reason:20 ,user:9 ,timestamp:19 ,nodelist:0"
			if o.Long {
				o.Format = "reason:20 ,userlong:12 ,timestamp:19 ,statecompact:6 ,nodelist:0"
			}
		default:
			if value, exists := env["SINFO_FORMAT"]; exists {
				o.Format, o.LongFormat = value, false
			} else {
				o.AutoPartitionWidth = true
				o.Format = "partition:9 ,available:.5 ,time:.10 ,nodes:.6 ,statecompact:.6 ,nodelist:0"
				if o.Long {
					o.Format = "partition:9 ,available:.5 ,time:.10 ,size:.10 ,root:.4 ,oversubscribe:.8 ,groups:.10 ,nodes:.6 ,statelong:.11 ,reservation:.11 ,nodelist:0"
				}
			}
		}
	}
	o.Fields, o.Diagnostics = parseFormat(o.Format, o.LongFormat)
	if o.Reasons && !o.HasStates {
		o.States = "down,fail,drain"
		o.StateFilter, _ = parseStates(o.States, false)
	}
	return o, nil
}

func longOption(program, name, arg string) (optionSpec, error) {
	var matches []optionSpec
	for _, spec := range optionSpecs {
		if spec.name == name {
			return spec, nil
		}
		if name != "" && strings.HasPrefix(spec.name, name) {
			matches = append(matches, spec)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		same := matches[0].short != 0
		for _, spec := range matches[1:] {
			same = same && spec.short == matches[0].short && spec.value == matches[0].value
		}
		if same {
			return matches[0], nil
		}
		// libc diagnostics follow the declaration order of the long-option table.
		var names []string
		for _, candidate := range strings.Fields(optionOrder) {
			for _, spec := range matches {
				if candidate == spec.name {
					names = append(names, "'--"+spec.name+"'")
				}
			}
		}
		return optionSpec{}, getoptError(program, fmt.Sprintf("option '%s' is ambiguous; possibilities: %s", arg, strings.Join(names, " ")))
	}
	return optionSpec{}, getoptError(program, fmt.Sprintf("unrecognized option '%s'", arg))
}

func decimalPrefix(value string) int {
	value = strings.TrimLeft(value, " \t\r\n\v\f")
	end := 0
	if len(value) != 0 && (value[0] == '-' || value[0] == '+') {
		end++
	}
	start := end
	for end < len(value) && value[end] >= '0' && value[end] <= '9' {
		end++
	}
	if start == end {
		return 0
	}
	// The reference's atoi converts through a signed long before narrowing to
	// int. ParseInt returns the saturated long even when it reports overflow.
	n, _ := strconv.ParseInt(value[:end], 10, 64)
	return int(int32(n))
}
