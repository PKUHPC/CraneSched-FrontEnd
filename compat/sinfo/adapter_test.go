// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"CraneFrontEnd/generated/protos"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func adapterFixture() *protos.QuerySinfoInfoReply {
	allocation := &protos.ResourceInNodeV3{CpuCount: 1, MemoryBytes: 1 << 30}
	return &protos.QuerySinfoInfoReply{
		SchemaVersion: 1, ClusterName: "example",
		Nodes: []*protos.SinfoNodeInfo{{
			Node: &protos.CranedInfo{
				Hostname: "n1", NodeHostname: "host1", NodeAddr: "192.0.2.1", CranedVersion: "Crane-test",
				ResourceState: protos.CranedResourceState_CRANE_ALLOC,
				ResTotal:      &protos.ResourceInNodeV3{CpuCount: 4, MemoryBytes: 1 << 30},
				ResAlloc:      allocation,
			}, Responding: true, HasRegistered: true, Port: 1234, ActiveAllocatedResources: allocation,
		}},
		Partitions: []*protos.SinfoPartitionInfo{{
			Partition: &protos.PartitionInfo{Name: "batch", Hostlist: "n1", TotalNodes: 1, State: protos.PartitionState_PARTITION_DOWN},
			IsDefault: true, AcceptsJobs: true, TimeLimitUnlimited: true, PriorityJobFactor: 5,
		}},
	}
}

func TestCompletingAllocationProjection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		active float64
		drain  bool
		want   string
	}{
		{"only_cleanup", 0, false, "comp|0/4/0/4|1/0/0/1|0\n"},
		{"other_partial_allocation", 1, false, "comp|1/3/0/4|1/0/0/1|0\n"},
		{"other_full_allocation", 4, false, "alloc+|4/0/0/4|1/0/0/1|0\n"},
		{"draining_cleanup", 0, true, "drng|0/0/4/4|1/0/0/1|0\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reply := adapterFixture()
			reply.Nodes[0].Completing = true
			reply.Nodes[0].Node.ResAlloc.CpuCount = 4
			reply.Nodes[0].ActiveAllocatedResources = &protos.ResourceInNodeV3{CpuCount: tc.active}
			if tc.drain {
				reply.Nodes[0].Node.ControlState = protos.CranedControlState_CRANE_DRAIN
			}
			o, err := parseCommand([]string{"sinfo", "-h", "-t", "completing", "-O", "StateCompact:0|,CPUsState:0|,NodeAIOT:0|,AllocMem:0"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			s, err := adaptSnapshot(reply, o.Query)
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err = newTextRenderer().table(&out, buildRows(s, o), o); err != nil {
				t.Fatal(err)
			}
			if out.String() != tc.want {
				t.Fatalf("got %q want %q", out.String(), tc.want)
			}
		})
	}
}

func TestMissingAllocationProjectionIsNotIdle(t *testing.T) {
	reply := adapterFixture()
	reply.Nodes[0].ActiveAllocatedResources = nil
	if _, err := adaptSnapshot(reply, query{}); err == nil || !strings.Contains(err.Error(), "active allocation") {
		t.Fatalf("got %v", err)
	}
}

func TestPowerTransitionsDoNotBecomeNodeFailures(t *testing.T) {
	for _, tc := range []struct {
		power protos.CranedPowerState
		want  string
	}{
		{protos.CranedPowerState_CRANE_POWER_SLEEPING, "idle~"},
		{protos.CranedPowerState_CRANE_POWER_POWEREDOFF, "idle~"},
		{protos.CranedPowerState_CRANE_POWER_TO_SLEEPING, "idle%"},
		{protos.CranedPowerState_CRANE_POWER_POWERING_OFF, "idle%"},
		{protos.CranedPowerState_CRANE_POWER_WAKING_UP, "idle#"},
		{protos.CranedPowerState_CRANE_POWER_POWERING_ON, "idle#"},
	} {
		reply := adapterFixture()
		reply.Nodes[0].Responding = false
		reply.Nodes[0].ActiveAllocatedResources.CpuCount = 0
		reply.Nodes[0].Node.PowerState = tc.power
		s, err := adaptSnapshot(reply, query{})
		if err != nil {
			t.Fatal(err)
		}
		noRespond := tc.want == "idle#"
		if s.Nodes[0].State.compact() != tc.want || s.Nodes[0].State.NoRespond != noRespond {
			t.Fatalf("%s: %+v", tc.power, s.Nodes[0].State)
		}
	}
	reply := adapterFixture()
	reply.Nodes[0].Responding = false
	s, err := adaptSnapshot(reply, query{})
	if err != nil || s.Nodes[0].State.compact() != "down*" {
		t.Fatalf("unplanned outage: %+v, %v", s, err)
	}
	reply.Nodes[0].HasRegistered = false
	s, err = adaptSnapshot(reply, query{})
	if err != nil || s.Nodes[0].State.compact() != "unk*" {
		t.Fatalf("initial registration: %+v, %v", s, err)
	}
}

func TestGRESOrderAndIndexBuffer(t *testing.T) {
	order := []*protos.SinfoGresResource{
		{Name: "fpga", Type: "z", Devices: []string{"/dev/f2", "/dev/f0"}},
		{Name: "fpga", Type: "a", Devices: []string{"/dev/f1"}},
		{Name: "gpu", Type: "a", Devices: []string{"/dev/g10"}},
		{Name: "gpu", Type: "z", Devices: []string{"/dev/g2"}},
	}
	total := &protos.DedicatedResourceInNode{NameTypeMap: map[string]*protos.DeviceTypeSlotsMap{}}
	for _, resource := range order {
		if total.NameTypeMap[resource.Name] == nil {
			total.NameTypeMap[resource.Name] = &protos.DeviceTypeSlotsMap{TypeSlotsMap: map[string]*protos.Slots{}}
		}
		total.NameTypeMap[resource.Name].TypeSlotsMap[resource.Type] = &protos.Slots{Slots: resource.Devices}
	}
	configured, used, err := projectGRES(total, total, order)
	if err != nil || configured != "fpga:z:2,fpga:a:1,gpu:z:1,gpu:a:1" || used != "fpga:z:2(IDX:0-1),fpga:a:1(IDX:2),gpu:z:1(IDX:0),gpu:a:1(IDX:1)" {
		t.Fatalf("configured=%q used=%q err=%v", configured, used, err)
	}
	if _, _, err := projectGRES(total, nil, nil); err == nil {
		t.Fatal("lost configuration order was silently invented")
	}
	indices := make([]int, 32)
	parts := make([]string, len(indices))
	for i := range indices {
		indices[i] = i * 2
		parts[i] = fmt.Sprint(indices[i])
	}
	want := strings.Join(parts, ",")[:63]
	if got := gresIndices(indices); got != want {
		t.Fatalf("index buffer got %q want %q", got, want)
	}
}

func TestAdapterKeepsSchedulerFacts(t *testing.T) {
	o, err := parseCommand([]string{"sinfo", "-h", "-o", "%P|%a|%l|%t|%C|%m|%v"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := adaptSnapshot(adapterFixture(), o.Query)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = newTextRenderer().table(&out, buildRows(s, o), o); err != nil {
		t.Fatal(err)
	}
	// Native CRANE_ALLOC may mean memory exhaustion. Slurm state follows CPU
	// allocations; partition administrative availability does not follow liveness.
	want := "batch*|up|infinite|mix|1/3/0/4|1024|Crane-test\n"
	if out.String() != want {
		t.Fatalf("%q != %q", out.String(), want)
	}
}

func TestFractionalCPUProjection(t *testing.T) {
	reply := adapterFixture()
	reply.Nodes[0].Node.ResAlloc.CpuCount = .25
	o, err := parseCommand([]string{"sinfo", "-h", "-t", "allocated", "-o", "%t|%N"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := adaptSnapshot(reply, o.Query)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = newTextRenderer().table(&out, buildRows(s, o), o); err != nil {
		t.Fatal(err)
	}
	if out.String() != "mix|n1\n" {
		t.Fatal(out.String())
	}
	o.Fields, _ = parseFormat("%C", false)
	out.Reset()
	err = newTextRenderer().table(&out, buildRows(s, o), o)
	if err == nil || !strings.Contains(err.Error(), "fractional") || out.Len() != 0 {
		t.Fatalf("out=%q err=%v", out.String(), err)
	}
}

func TestMissingTopologyIsNotInvented(t *testing.T) {
	o, err := parseCommand([]string{"sinfo", "-O", "SocketCoreThread"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := adaptSnapshot(adapterFixture(), o.Query)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := newTextRenderer().table(&out, buildRows(s, o), o); err == nil || out.Len() != 0 {
		t.Fatalf("out=%q err=%v", out.String(), err)
	}
}

func TestAbsentTelemetryIsNotAConfiguredValue(t *testing.T) {
	for _, field := range []string{"CPUsLoad", "FreeMem"} {
		o, err := parseCommand([]string{"sinfo", "-O", field}, nil)
		if err != nil {
			t.Fatal(err)
		}
		s, err := adaptSnapshot(adapterFixture(), o.Query)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := newTextRenderer().table(&out, buildRows(s, o), o); err == nil || out.Len() != 0 || !strings.Contains(err.Error(), "observation") {
			t.Fatalf("%s out=%q err=%v", field, &out, err)
		}
	}
}

func TestStructuredImplicitCapabilitiesDoNotBecomeZeroValues(t *testing.T) {
	for _, kind := range []string{"json", "yaml"} {
		o, err := parseCommand([]string{"sinfo", "--" + kind, "-o", "%N"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		s, err := adaptSnapshot(adapterFixture(), o.Query)
		if err != nil {
			t.Fatal(err)
		}
		var out, diagnostics bytes.Buffer
		err = renderStructured(&out, &diagnostics, []string{"sinfo"}, buildRows(s, o), s, o, newTextRenderer().UserName)
		if err == nil || !strings.Contains(err.Error(), "field Disk") || out.Len() != 0 {
			t.Fatalf("%s out=%q err=%v", kind, &out, err)
		}
		o.DataParser, o.HasDataParser = "v0.0.45+spec_only", true
		if err := renderStructured(&out, &diagnostics, []string{"sinfo"}, nil, s, o, newTextRenderer().UserName); err != nil || out.Len() == 0 {
			t.Fatalf("schema-only output: %v", err)
		}
	}
}

func TestGRESIndicesSpanTypesAndDoNotDependOnMapOrder(t *testing.T) {
	resources := func(types map[string][]string) *protos.DedicatedResourceInNode {
		m := &protos.DeviceTypeSlotsMap{TypeSlotsMap: map[string]*protos.Slots{}}
		for name, slots := range types {
			m.TypeSlotsMap[name] = &protos.Slots{Slots: slots}
		}
		return &protos.DedicatedResourceInNode{NameTypeMap: map[string]*protos.DeviceTypeSlotsMap{"gpu": m}}
	}
	total := resources(map[string][]string{"a": {"/dev/gpu2", "/dev/gpu0"}, "b": {"/dev/gpu11", "/dev/gpu1", "/dev/gpu10"}})
	used := resources(map[string][]string{"a": {"/dev/gpu2"}, "b": {"/dev/gpu11", "/dev/gpu10", "/dev/gpu1"}})
	order := []*protos.SinfoGresResource{
		{Name: "gpu", Type: "a", Devices: []string{"/dev/gpu2", "/dev/gpu0"}},
		{Name: "gpu", Type: "b", Devices: []string{"/dev/gpu11", "/dev/gpu1", "/dev/gpu10"}},
	}
	_, got, err := projectGRES(total, used, order)
	if err != nil || got != "gpu:a:1(IDX:2),gpu:b:3(IDX:1,3-4)" {
		t.Fatalf("got %q err=%v", got, err)
	}
	used.NameTypeMap["gpu"].TypeSlotsMap["a"].Slots = []string{"/dev/missing"}
	if _, _, err = projectGRES(total, used, order); err == nil {
		t.Fatal("unknown allocated device was represented as a valid index")
	}
}

func TestGRESDataErrorOnlyAffectsRequestsForGRES(t *testing.T) {
	reply := adapterFixture()
	reply.Nodes[0].Node.ResAlloc.Gres = &protos.DedicatedResourceInNode{NameTypeMap: map[string]*protos.DeviceTypeSlotsMap{
		"gpu": {TypeSlotsMap: map[string]*protos.Slots{"a": {Slots: []string{"/dev/unknown"}}}},
	}}
	o, err := parseCommand([]string{"sinfo", "-h", "-o", "%N"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := adaptSnapshot(reply, o.Query)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = newTextRenderer().table(&out, buildRows(s, o), o); err != nil || out.String() != "n1\n" {
		t.Fatalf("out=%q err=%v", &out, err)
	}
	o.Fields, _ = parseFormat("GresUsed", true)
	out.Reset()
	if err = newTextRenderer().table(&out, buildRows(s, o), o); err == nil || out.Len() != 0 {
		t.Fatalf("out=%q err=%v", &out, err)
	}
}

type fixtureController struct {
	protos.UnimplementedCraneCtldServer
	request chan *protos.QuerySinfoInfoRequest
}

func (s *fixtureController) QuerySinfoInfo(_ context.Context, req *protos.QuerySinfoInfoRequest) (*protos.QuerySinfoInfoReply, error) {
	s.request <- req
	return adapterFixture(), nil
}

func TestCraneSourceUsesCallerAndDedicatedRPC(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	fixture := &fixtureController{request: make(chan *protos.QuerySinfoInfoRequest, 1)}
	protos.RegisterCraneCtldServer(server, fixture)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte(fmt.Sprintf("ControlMachine: %s\nCraneCtldListenPort: '%s'\n", host, port)), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	source, err := openCraneSource(ctx, configPath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	s, err := source.Load(ctx, query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Nodes) != 1 || s.Nodes[0].Name != "n1" {
		t.Fatalf("%+v", s)
	}
	select {
	case req := <-fixture.request:
		if req.Uid != uint32(os.Getuid()) || req.ReservationsOnly {
			t.Fatalf("%+v", req)
		}
	default:
		t.Fatal("request not observed")
	}
}
