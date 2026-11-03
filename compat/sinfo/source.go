// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"CraneFrontEnd/compat/utils"
	"CraneFrontEnd/generated/protos"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// query contains only facts needed by the controller and snapshot adapter.
type query struct{ ReservationsOnly bool }

type snapshotSource interface {
	Load(context.Context, query) (*snapshot, error)
	Close() error
}

type craneSource struct{ session *utils.Session }

func openCraneSource(ctx context.Context, configPath string) (snapshotSource, error) {
	session, err := utils.OpenSession(ctx, configPath)
	if err != nil {
		return nil, err
	}
	return &craneSource{session: session}, nil
}

func (s *craneSource) Load(ctx context.Context, q query) (*snapshot, error) {
	var reply *protos.QuerySinfoInfoReply
	err := s.session.Query(ctx, func(ctx context.Context, client protos.CraneCtldClient, uid uint32) error {
		var err error
		reply, err = client.QuerySinfoInfo(ctx, &protos.QuerySinfoInfoRequest{Uid: uid, ReservationsOnly: q.ReservationsOnly})
		return err
	})
	if status.Code(err) == codes.Unimplemented {
		return nil, fmt.Errorf("Crane controller lacks the sinfo query API; upgrade the controller")
	}
	if err != nil {
		return nil, err
	}
	return adaptSnapshot(reply, q)
}

func (s *craneSource) Close() error { return s.session.Close() }

func adaptSnapshot(reply *protos.QuerySinfoInfoReply, q query) (*snapshot, error) {
	if reply.GetSchemaVersion() != 1 {
		return nil, fmt.Errorf("Crane controller does not provide sinfo query schema 1")
	}
	s := &snapshot{Cluster: reply.ClusterName, UnsupportedFields: map[string]string{}}
	for _, field := range fieldSpecs {
		if field.Capability != "" {
			s.UnsupportedFields[field.Name] = field.Capability
		}
	}
	for _, raw := range reply.Reservations {
		if raw.StartTime == nil || raw.Duration == nil {
			return nil, fmt.Errorf("reservation %q is missing its time interval", raw.ReservationName)
		}
		start := raw.StartTime.AsTime()
		s.Reservations = append(s.Reservations, reservation{Name: raw.ReservationName, Nodes: raw.CranedRegex, Start: start, End: start.Add(raw.Duration.AsDuration())})
	}
	if q.ReservationsOnly {
		return s, nil
	}
	for _, raw := range reply.Nodes {
		detail := raw.GetNode()
		if detail == nil || detail.ResTotal == nil || detail.ResAlloc == nil {
			return nil, fmt.Errorf("controller returned an incomplete node record")
		}
		cpus, err := wholeCPUs(detail.ResTotal.CpuCount, detail.Hostname)
		if err != nil {
			return nil, err
		}
		allocation := raw.ActiveAllocatedResources
		if allocation == nil {
			return nil, fmt.Errorf("node %q is missing active allocation data", detail.Hostname)
		}
		allocated := allocation.CpuCount
		if math.IsNaN(allocated) || math.IsInf(allocated, 0) || allocated < 0 || allocated > float64(cpus) {
			return nil, fmt.Errorf("node %q has an invalid allocated CPU count", detail.Hostname)
		}
		n := node{
			Name: detail.Hostname, Hostname: detail.NodeHostname, Address: detail.NodeAddr, Version: detail.CranedVersion,
			CPUs: cpus, AllocCPUs: uint64(allocated), FractionalAllocation: allocated != math.Trunc(allocated), HasCPUAllocation: allocated > 0,
			Memory: detail.ResTotal.MemoryBytes / (1024 * 1024), AllocMemory: allocation.MemoryBytes / (1024 * 1024),
			Port: uint64(raw.Port), Reason: raw.Reason, ReasonUID: raw.ReasonUid,
			TelemetryExpected: raw.HasRegistered,
		}
		n.Gres, n.GresUsed, n.GresIssue = projectGRES(detail.ResTotal.Gres, allocation.Gres, raw.GresOrder)
		if raw.CpuLoad != nil {
			value := uint64(*raw.CpuLoad)
			n.CPULoad = &value
		}
		n.FreeMemory = raw.FreeMemoryMb
		if raw.ReasonTime != nil {
			n.ReasonTime = raw.ReasonTime.AsTime()
		}
		if raw.SchedulingTopology != nil {
			n.Sockets = uint64(raw.SchedulingTopology.Sockets)
			n.Cores = uint64(raw.SchedulingTopology.CoresPerSocket)
			n.Threads = uint64(raw.SchedulingTopology.ThreadsPerCore)
		}
		n.State = nodeState{Base: "idle", NoRespond: !raw.Responding, Drain: detail.ControlState == protos.CranedControlState_CRANE_DRAIN}
		switch {
		case !raw.Responding && !raw.HasRegistered:
			n.State.Base = "unknown"
		case !raw.Responding:
			n.State.Base = "down"
		case allocated >= float64(cpus) && cpus != 0:
			n.State.Base = "allocated"
		case allocated > 0:
			n.State.Base = "mixed"
		}
		switch detail.PowerState {
		case protos.CranedPowerState_CRANE_POWER_SLEEPING, protos.CranedPowerState_CRANE_POWER_POWEREDOFF:
			n.State.Power = "powered_down"
		case protos.CranedPowerState_CRANE_POWER_TO_SLEEPING, protos.CranedPowerState_CRANE_POWER_POWERING_OFF:
			n.State.Power = "powering_down"
		case protos.CranedPowerState_CRANE_POWER_WAKING_UP, protos.CranedPowerState_CRANE_POWER_POWERING_ON:
			n.State.Power = "powering_up"
		}
		if n.State.Power != "" {
			// A successful suspend/resume transition is expected to stop
			// heartbeats. Crane has no separate administrative DOWN state.
			n.State.NoRespond = n.State.Power == "powering_up" && !raw.Responding
			n.State.Base = "idle"
			if allocated >= float64(cpus) && cpus != 0 {
				n.State.Base = "allocated"
			} else if allocated > 0 {
				n.State.Base = "mixed"
			}
		}
		n.State.Completing = raw.Completing && raw.Responding && n.State.Power != "powered_down" && n.State.Power != "powering_up"
		if len(raw.ActiveReservations) != 0 {
			// Crane excludes overlapping whole-node reservations. The Slurm
			// node record exposes one name, not a fabricated comma-separated list.
			if len(raw.ActiveReservations) != 1 {
				return nil, fmt.Errorf("node %q has overlapping reservations without an ordered reservation projection", n.Name)
			}
			n.Reservation = raw.ActiveReservations[0]
			n.State.Reserved = true
		}
		s.Nodes = append(s.Nodes, n)
	}
	for _, raw := range reply.Partitions {
		detail := raw.Partition
		if detail == nil {
			return nil, fmt.Errorf("controller returned an incomplete partition record")
		}
		list, err := utils.ParseHostList(detail.Hostlist)
		if err != nil {
			return nil, fmt.Errorf("partition %q has an invalid node list: %w", detail.Name, err)
		}
		p := partition{
			Name: detail.Name, Default: raw.IsDefault, Available: "up", TimeLimit: durationLimit{Infinite: raw.TimeLimitUnlimited},
			DefaultTime: durationLimit{Unset: true}, PriorityFactor: uint64(raw.PriorityJobFactor),
			Groups: "all", AllocNodes: "all", OverSubscribe: "NO", MaxNodes: 0xffffffff, MaxCPUs: 0xffffffff, MinNodes: 1,
			AllowedAccounts: append([]string(nil), detail.AllowedAccounts...), DeniedAccounts: append([]string(nil), detail.DeniedAccounts...),
			DefaultMemCPU: detail.DefaultMemPerCpu / (1024 * 1024), MaxMemCPU: detail.MaxMemPerCpu / (1024 * 1024),
			DefaultMemNode: detail.DefaultMemPerNode / (1024 * 1024), MaxMemNode: detail.MaxMemPerNode / (1024 * 1024),
			ConfiguredNodes: detail.Hostlist,
		}
		sort.Strings(p.AllowedAccounts)
		sort.Strings(p.DeniedAccounts)
		if !raw.AcceptsJobs {
			p.Available = "down"
		}
		if !raw.TimeLimitUnlimited {
			return nil, fmt.Errorf("partition %q is missing its partition time limit", p.Name)
		}
		mode, ok := map[protos.PreemptMode]string{
			protos.PreemptMode_PREEMPT_MODE_OFF: "OFF", protos.PreemptMode_PREEMPT_MODE_CANCEL: "CANCEL",
			protos.PreemptMode_PREEMPT_MODE_REQUEUE: "REQUEUE", protos.PreemptMode_PREEMPT_MODE_SUSPEND: "SUSPEND",
		}[raw.PreemptMode]
		if !ok {
			return nil, fmt.Errorf("partition %q has an unknown preemption mode", p.Name)
		}
		p.PreemptMode = mode
		for _, n := range s.Nodes {
			if list.Match(n.Name) {
				p.Nodes = append(p.Nodes, n.Name)
				p.TotalCPUs += n.CPUs
				p.TotalMemory += n.Memory
			}
		}
		if len(p.Nodes) != int(detail.TotalNodes) {
			return nil, fmt.Errorf("partition %q references nodes missing from the controller snapshot", p.Name)
		}
		s.Partitions = append(s.Partitions, p)
	}
	return s, nil
}

func wholeCPUs(value float64, name string) (uint64, error) {
	if value < 0 || value > math.MaxUint32 || math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value {
		return 0, fmt.Errorf("node %q CPU count cannot be represented by Slurm sinfo: %g", name, value)
	}
	return uint64(value), nil
}

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
			sort.SliceStable(ordered, func(i, j int) bool { return utils.NaturalLess(ordered[i].path, ordered[j].path) })
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
