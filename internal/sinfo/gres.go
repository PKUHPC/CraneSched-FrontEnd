// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"CraneFrontEnd/generated/protos"
	"fmt"
	"sort"
	"strings"
)

type gresDevice struct{ kind, path string }

// The resource order comes from the controller's configuration, independently
// of protobuf map iteration. GPU devices use natural file order, as in Slurm's
// file-backed GPU configuration without Links/AutoDetect. Other file-backed
// resources retain declaration order. Crane has no GPU topology/Links policy.
func projectGRES(total, allocated *protos.DedicatedResourceInNode, order []*protos.SinfoGresResource) (string, string, error) {
	names := []string{}
	devices := map[string][]gresDevice{}
	declared := map[string]map[string]string{}
	for _, resource := range order {
		if resource == nil || resource.Name == "" {
			return "", "", fmt.Errorf("controller returned an invalid GRES declaration")
		}
		name := resource.Name
		if declared[name] == nil {
			names = append(names, name)
			declared[name] = map[string]string{}
		}
		for _, path := range resource.Devices {
			if kind, found := declared[name][path]; found {
				if kind != resource.Type {
					return "", "", fmt.Errorf("GRES %s device %q has conflicting types", name, path)
				}
				continue
			}
			declared[name][path] = resource.Type
			devices[name] = append(devices[name], gresDevice{resource.Type, path})
		}
	}
	inventory := map[string]map[string]string{}
	for name, types := range total.GetNameTypeMap() {
		inventory[name] = map[string]string{}
		for kind, slots := range types.GetTypeSlotsMap() {
			for _, slot := range slots.GetSlots() {
				if _, exists := inventory[name][slot]; exists {
					return "", "", fmt.Errorf("GRES %s has duplicate device %q", name, slot)
				}
				if declaredKind, found := declared[name][slot]; !found || declaredKind != kind {
					return "", "", fmt.Errorf("GRES %s:%s device %q is missing its configuration order", name, kind, slot)
				}
				inventory[name][slot] = kind
			}
		}
	}
	used := map[string]map[string]bool{}
	for name, types := range allocated.GetNameTypeMap() {
		used[name] = map[string]bool{}
		for kind, slots := range types.GetTypeSlotsMap() {
			for _, slot := range slots.GetSlots() {
				actualKind, found := inventory[name][slot]
				if !found || actualKind != kind || used[name][slot] {
					return "", "", fmt.Errorf("allocated GRES %s:%s contains an unknown or duplicate device %q", name, kind, slot)
				}
				used[name][slot] = true
			}
		}
	}
	var configured, consumption []string
	for _, name := range names {
		if len(inventory[name]) == 0 {
			continue
		}
		if name == "mps" || name == "shard" {
			return "", "", fmt.Errorf("current Crane version does not support Slurm shared GRES %q accounting", name)
		}
		ordered := devices[name]
		if name == "gpu" {
			sort.SliceStable(ordered, func(i, j int) bool { return naturalLess(ordered[i].path, ordered[j].path) })
		}
		kinds := []string{}
		counts := map[string]uint64{}
		indices := map[string][]int{}
		index := 0
		for _, device := range ordered {
			if _, found := inventory[name][device.path]; !found {
				continue
			}
			if counts[device.kind] == 0 {
				kinds = append(kinds, device.kind)
			}
			counts[device.kind]++
			if used[name][device.path] {
				indices[device.kind] = append(indices[device.kind], index)
			}
			index++
		}
		for _, kind := range kinds {
			prefix := name
			if kind != "" {
				prefix += ":" + kind
			}
			count, unit := counts[kind], ""
			for _, suffix := range "KMGTP" {
				if count == 0 || count%1024 != 0 {
					break
				}
				count /= 1024
				unit = string(suffix)
			}
			configured = append(configured, fmt.Sprintf("%s:%d%s", prefix, count, unit))
			consumption = append(consumption, fmt.Sprintf("%s:%d(IDX:%s)", prefix, len(indices[kind]), gresIndices(indices[kind])))
		}
	}
	return strings.Join(configured, ","), strings.Join(consumption, ","), nil
}

func gresIndices(indices []int) string {
	if len(indices) == 0 {
		return "N/A"
	}
	var ranges []string
	for start := 0; start < len(indices); {
		end := start + 1
		for end < len(indices) && indices[end] == indices[end-1]+1 {
			end++
		}
		value := fmt.Sprint(indices[start])
		if end > start+1 {
			value += fmt.Sprint("-", indices[end-1])
		}
		ranges = append(ranges, value)
		start = end
	}
	value := strings.Join(ranges, ",")
	// Slurm exposes a 64-byte bit-format buffer, including the terminator.
	if len(value) > 63 {
		value = value[:63]
	}
	return value
}
