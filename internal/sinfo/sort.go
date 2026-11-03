// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"cmp"
	"sort"
	"strings"
)

func sortRows(rows []*row, o *options) {
	// Slurm only populates hostnames and addresses when those output fields
	// are requested. Sorting on an unpopulated column is a stable no-op.
	collected := make(map[byte]bool)
	for _, item := range o.Fields {
		if item.Field != nil {
			collected[item.Field.Short] = true
		}
	}
	keys := o.Sort
	if !o.HasSort {
		keys = "#P,-t"
		if o.Node {
			keys = "N"
		}
	}
	for i := len(keys) - 1; i >= 0; i-- {
		key := keys[i]
		if (key == 'n' || key == 'o') && !collected[key] {
			continue
		}
		if !strings.ContainsRune("aAbcdDEfFghHlmMnoNOepPrRsStTuUVwXYZz", rune(key)) {
			continue
		}
		reverse, partOrder := i > 0 && keys[i-1] == '-', i > 0 && keys[i-1] == '#'
		sort.SliceStable(rows, func(a, b int) bool {
			diff := compareRows(rows[a], rows[b], key, partOrder)
			if reverse {
				return diff > 0
			}
			return diff < 0
		})
	}
}

func compareRows(a, b *row, key byte, partOrder bool) int {
	pa, pb := a.Partition, b.Partition
	na, nb := &node{}, &node{}
	if len(a.Nodes) != 0 {
		na = a.Nodes[0]
	}
	if len(b.Nodes) != 0 {
		nb = b.Nodes[0]
	}
	if group, ok := map[byte]string{'c': "cpus", 'd': "disk", 'm': "memory", 'O': "load", 'e': "freemem", 'w': "weight", 'X': "sockets", 'Y': "cores", 'Z': "threads"}[key]; ok {
		return a.compareNumber(b, group)
	}
	switch key {
	case 'P', 'R':
		if partOrder {
			return cmp.Compare(a.PartitionOrder, b.PartitionOrder)
		}
		return strings.Compare(pa.Name, pb.Name)
	case 'a':
		states := map[string]int{"inactive": 0, "down": 1, "drain": 2, "up": 3}
		return cmp.Compare(states[pa.Available], states[pb.Available])
	case 'A', 'F':
		ca, _ := a.counts()
		cb, _ := b.counts()
		return cmp.Compare(ca.Alloc, cb.Alloc)
	case 'D':
		return cmp.Compare(len(a.Nodes), len(b.Nodes))
	case 'b':
		return strings.Compare(na.ActiveFeatures, nb.ActiveFeatures)
	case 'E':
		return strings.Compare(na.Reason, nb.Reason)
	case 'f':
		return strings.Compare(na.Features, nb.Features)
	case 'g':
		return strings.Compare(pa.Groups, pb.Groups)
	case 'h':
		return cmp.Compare(shareOrder(pa.OverSubscribe), shareOrder(pb.OverSubscribe))
	case 'H':
		return na.ReasonTime.Compare(nb.ReasonTime)
	case 'l':
		value := func(limit durationLimit) uint64 {
			if limit.Infinite {
				return ^uint64(0)
			}
			return limit.Seconds
		}
		return cmp.Compare(value(pa.TimeLimit), value(pb.TimeLimit))
	case 'M':
		order := map[string]uint16{"OFF": 0, "SUSPEND": 1, "REQUEUE": 2, "CANCEL": 8, "GANG": 0x8000}
		return cmp.Compare(order[pa.PreemptMode], order[pb.PreemptMode])
	case 'N', 'n', 'o':
		kind := ""
		if key == 'n' {
			kind = "hostname"
		}
		if key == 'o' {
			kind = "address"
		}
		first := func(r *row) string {
			names := r.names(kind)
			if len(names) == 0 {
				return ""
			}
			n := names[0]
			for _, name := range names[1:] {
				if hostLess(name, n) {
					n = name
				}
			}
			return n
		}
		return compareName(first(a), first(b))
	case 'p':
		return cmp.Compare(pa.PriorityTier, pb.PriorityTier)
	case 'r':
		if pa.RootOnly == pb.RootOnly {
			return 0
		}
		if pa.RootOnly {
			return 1
		}
		return -1
	case 's':
		size := func(p *partition) uint32 {
			if p.MaxNodes == 0xffffffff {
				return 0xffffffff
			}
			return uint32(p.MaxNodes + p.MinNodes)
		}
		return cmp.Compare(size(pa), size(pb))
	case 'S':
		return cmp.Compare(pa.PriorityFactor, pb.PriorityFactor)
	case 't', 'T':
		return cmp.Compare(na.State.order(), nb.State.order())
	case 'u', 'U':
		value := func(uid *uint32) uint32 {
			if uid == nil {
				return 0xfffffffe
			}
			return *uid
		}
		return cmp.Compare(value(na.ReasonUID), value(nb.ReasonUID))
	case 'V':
		return strings.Compare(a.Cluster, b.Cluster)
	case 'z':
		for _, group := range []string{"sockets", "cores", "threads"} {
			if diff := a.compareNumber(b, group); diff != 0 {
				return diff
			}
		}
	}
	return 0
}

func shareOrder(value string) uint16 {
	switch value {
	case "EXCLUSIVE":
		return 0
	case "NO":
		return 1
	}
	name, count, _ := strings.Cut(value, ":")
	n := uint16(decimalPrefix(count))
	if name == "FORCE" {
		n |= 0x8000
	}
	return n
}
