// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"CraneFrontEnd/generated/protos"
	"fmt"
	"math"
	"sort"
)

func adaptSnapshot(reply *protos.QuerySinfoInfoReply, o *options) (*snapshot, error) {
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
	if o.Reservations {
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
		list, err := parseHostList(detail.Hostlist)
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
