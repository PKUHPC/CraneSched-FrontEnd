// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import "fmt"

func unsupported(entry, feature string) error {
	return optionError(fmt.Sprintf("%s: current Crane version does not support %s", entry, feature))
}

// Check only effective requests. In particular -T has its own report and does
// not consume fields, states, node filters, sorting or a structured serializer.
func validateCapabilities(o *options) error {
	if o.HasClusters {
		return unsupported("--clusters", "multi-cluster queries")
	}
	if o.Federation && !o.Local {
		return unsupported("--federation", "federated queries")
	}
	if o.Reservations {
		return nil
	}
	if o.Future {
		return unsupported("--future", "future node registration")
	}
	for _, item := range o.Fields {
		if item.Field != nil && item.Field.Capability != "" {
			return unsupported("field "+item.Field.Name, item.Field.Capability)
		}
	}
	// Sorting by an unexposed feature is still a request for that feature.
	for _, key := range o.Sort {
		name := map[rune]string{'b': "features_act", 'd': "Disk", 'f': "Features", 'g': "Groups", 'h': "OverSubscribe", 'O': "CPUsLoad", 'e': "FreeMem", 'p': "PriorityTier", 'r': "Root", 's': "Size", 'w': "Weight"}[key]
		if f := fieldByName(name); f != nil && f.Capability != "" {
			return unsupported(fmt.Sprintf("sort key %c", key), f.Capability)
		}
	}
	if o.HasStates {
		for _, term := range o.StateFilter.Terms {
			switch term.Name {
			case "future", "planned", "blocked", "cloud", "inval", "fail", "maint", "reboot", "reboot^", "external", "dynamic_future", "dynamic_norm":
				return unsupported("state "+term.Name, "the requested node state")
			}
		}
	}
	return nil
}
