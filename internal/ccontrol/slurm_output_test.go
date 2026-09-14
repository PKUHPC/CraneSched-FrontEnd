package ccontrol

import (
	"CraneFrontEnd/generated/protos"
	"testing"
)

func TestNewSlurmNodeInfoKeepsNodeIdentitiesDistinct(t *testing.T) {
	node := &protos.CranedInfo{
		Hostname:     "crnd1",
		NodeHostname: "host01.example.com",
		NodeAddr:     "10.0.0.1",
	}

	got, err := newSlurmNodeInfo(node)
	if err != nil {
		t.Fatalf("newSlurmNodeInfo returned error: %v", err)
	}
	if got.Hostname != node.Hostname {
		t.Errorf("Hostname = %q, want canonical NodeName %q", got.Hostname, node.Hostname)
	}
	if got.NodeHostname != node.NodeHostname {
		t.Errorf("NodeHostname = %q, want %q", got.NodeHostname, node.NodeHostname)
	}
	if got.NodeAddr != node.NodeAddr {
		t.Errorf("NodeAddr = %q, want %q", got.NodeAddr, node.NodeAddr)
	}
}
