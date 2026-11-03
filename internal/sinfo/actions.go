// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	_ "embed"
	"fmt"
	"io"
	"strings"
)

// These strings reproduce the observable help/usage interface of the target
// command. They contain no scheduler code and do not drive option parsing.
//
//go:embed help.txt
var helpText string

//go:embed usage.txt
var usageText string

const optionOrder = "autocomplete all dead exact federation future help helpformat helpFormat helpstate hide iterate local long cluster clusters nodes noconvert noheader Node format Format partition responding list-reasons summarize sort states reservation usage verbose version json yaml"

func writeAction(w io.Writer, o *options) error {
	var output strings.Builder
	switch o.Action {
	case "help":
		output.WriteString(helpText)
	case "usage":
		output.WriteString(usageText)
	case "version":
		fmt.Fprintf(&output, "slurm %s\n", compatibilityVersion)
	case "helpstate":
		output.WriteString(strings.ReplaceAll(stateNames, ",", "\n"))
		output.WriteByte('\n')
	case "helpformat", "helpFormat":
		count, columns := 0, 4
		if o.Action == "helpformat" {
			columns = 8
		}
		for _, field := range fieldSpecs {
			if field.Hidden || o.Action == "helpformat" && field.Short == 0 {
				continue
			}
			if count > 0 && count%columns == 0 {
				output.WriteByte('\n')
			}
			if o.Action == "helpformat" {
				fmt.Fprintf(&output, "%%%-5c", field.Short)
			} else {
				fmt.Fprintf(&output, "%-20s", field.Name)
			}
			count++
		}
		output.WriteByte('\n')
	case "autocomplete":
		query := o.ActionValue
		if query == "" {
			return nil
		}
		alpha := query[0] >= 'a' && query[0] <= 'z' || query[0] >= 'A' && query[0] <= 'Z'
		for _, name := range strings.Fields(optionOrder) {
			for _, spec := range optionSpecs {
				if spec.name != name {
					continue
				}
				if spec.short != 0 && (query[0] == '-' || alpha) {
					flag := "-" + string(spec.short)
					if strings.Contains(flag, query) {
						output.WriteString(flag + "\n")
					}
				}
				flag := "--" + spec.name
				if (len(query) > 1 || alpha) && strings.Contains(flag, query) {
					if spec.value != 0 {
						output.WriteString(flag + "=\n")
					}
					if spec.value == 2 {
						output.WriteString(flag + " \n")
					}
					if spec.value == 0 {
						output.WriteString(flag + "\n")
					}
				}
			}
		}
		if output.Len() != 0 {
			output.WriteByte('\n')
		}
	}
	_, err := io.WriteString(w, output.String())
	return err
}
