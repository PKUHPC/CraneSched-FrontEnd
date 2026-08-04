package main

import (
	"testing"
	"time"
)

func newTestManager() *PowerManager {
	return &PowerManager{
		nodeVersions:   make(map[string]nodeVersion),
		nodeOperations: make(map[string]*nodeOperationLock),
	}
}

func (c *PowerManager) storedNodeVersion(t *testing.T, nodeID string) nodeVersion {
	t.Helper()
	c.nodesInfoMutex.Lock()
	defer c.nodesInfoMutex.Unlock()
	version, exists := c.nodeVersions[nodeID]
	if !exists {
		t.Fatalf("expected a version entry for node %s", nodeID)
	}
	return version
}

func TestCompareNodeVersion(t *testing.T) {
	current := nodeVersion{generation: 2, revision: 5}

	tests := []struct {
		name       string
		generation uint64
		revision   uint64
		want       int
	}{
		{"lower generation", 1, 9, -1},
		{"same generation lower revision", 2, 4, -1},
		{"equal generation and revision", 2, 5, 0},
		{"same generation higher revision", 2, 6, 1},
		{"higher generation lower revision", 3, 0, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compareNodeVersion(tt.generation, tt.revision, current); got != tt.want {
				t.Errorf("compareNodeVersion(%d, %d, %+v) = %d, want %d",
					tt.generation, tt.revision, current, got, tt.want)
			}
		})
	}
}

func TestAcceptNodeVersionFirstSeenClaims(t *testing.T) {
	manager := newTestManager()

	if !manager.AcceptNodeVersion("node1", 3, 7) {
		t.Fatal("first version for a node must be accepted")
	}

	version := manager.storedNodeVersion(t, "node1")
	if version.generation != 3 || version.revision != 7 {
		t.Errorf("claimed version = gen %d rev %d, want gen 3 rev 7",
			version.generation, version.revision)
	}
	if version.updatedAt.IsZero() {
		t.Error("claimed version must record updatedAt")
	}
}

func TestAcceptNodeVersion(t *testing.T) {
	tests := []struct {
		name       string
		generation uint64
		revision   uint64
		want       bool
	}{
		{"same generation higher revision", 2, 6, true},
		{"same generation equal revision", 2, 5, true},
		{"same generation lower revision", 2, 4, false},
		{"higher generation resets revision", 3, 0, true},
		{"lower generation higher revision", 1, 9, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := newTestManager()
			if !manager.AcceptNodeVersion("node1", 2, 5) {
				t.Fatal("failed to claim the initial version")
			}

			if got := manager.AcceptNodeVersion("node1", tt.generation, tt.revision); got != tt.want {
				t.Fatalf("AcceptNodeVersion(gen %d, rev %d) = %v, want %v",
					tt.generation, tt.revision, got, tt.want)
			}

			version := manager.storedNodeVersion(t, "node1")
			if tt.want {
				if version.generation != tt.generation || version.revision != tt.revision {
					t.Errorf("stored version = gen %d rev %d, want claimed gen %d rev %d",
						version.generation, version.revision, tt.generation, tt.revision)
				}
			} else {
				if version.generation != 2 || version.revision != 5 {
					t.Errorf("rejected call must not modify the stored version, got gen %d rev %d",
						version.generation, version.revision)
				}
			}
		})
	}
}

func TestAcceptNodeVersionIsPerNode(t *testing.T) {
	manager := newTestManager()

	if !manager.AcceptNodeVersion("node1", 5, 5) {
		t.Fatal("failed to claim version for node1")
	}
	if !manager.AcceptNodeVersion("node2", 1, 0) {
		t.Error("node2 must not be constrained by node1's version")
	}
}

func TestApplyNodeDefinitionVersionOrdering(t *testing.T) {
	tests := []struct {
		name       string
		generation uint64
		revision   uint64
		present    bool
		want       bool
	}{
		{"first version is accepted", 0, 0, true, true},
		{"same generation higher revision", 2, 6, true, true},
		{"same generation lower revision", 2, 4, true, false},
		{"higher generation resets revision", 3, 0, true, true},
		{"lower generation is rejected", 1, 9, true, false},
		{"lower generation removal is rejected", 1, 9, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := newTestManager()
			nodeID := "node1"
			if tt.name != "first version is accepted" {
				if !manager.ApplyNodeDefinitionVersion(nodeID, 2, 5, true) {
					t.Fatal("failed to claim the initial version")
				}
			} else {
				tt.generation, tt.revision = 2, 5
			}

			if got := manager.ApplyNodeDefinitionVersion(nodeID, tt.generation, tt.revision, tt.present); got != tt.want {
				t.Fatalf("ApplyNodeDefinitionVersion(gen %d, rev %d, present %v) = %v, want %v",
					tt.generation, tt.revision, tt.present, got, tt.want)
			}

			version := manager.storedNodeVersion(t, nodeID)
			if tt.want {
				if version.generation != tt.generation || version.revision != tt.revision {
					t.Errorf("stored version = gen %d rev %d, want claimed gen %d rev %d",
						version.generation, version.revision, tt.generation, tt.revision)
				}
			} else {
				if version.generation != 2 || version.revision != 5 {
					t.Errorf("rejected call must not modify the stored version, got gen %d rev %d",
						version.generation, version.revision)
				}
			}
		})
	}
}

// Equal versions are decided by whether nodesInfo already reflects the
// definition, so redelivered hooks stay idempotent.
func TestApplyNodeDefinitionVersionEqualVersion(t *testing.T) {
	const nodeID = "node1"

	tests := []struct {
		name     string
		present  bool
		nodeInfo *NodeInfo
		want     bool
	}{
		{
			name:    "removal already effective",
			present: false,
			want:    false,
		},
		{
			name:     "removal still pending",
			present:  false,
			nodeInfo: &NodeInfo{Generation: 2, Revision: 5},
			want:     true,
		},
		{
			name:    "upsert not applied yet",
			present: true,
			want:    true,
		},
		{
			name:     "upsert already applied",
			present:  true,
			nodeInfo: &NodeInfo{Generation: 2, Revision: 5},
			want:     false,
		},
		{
			name:     "upsert applied with older revision",
			present:  true,
			nodeInfo: &NodeInfo{Generation: 2, Revision: 4},
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := newTestManager()
			if !manager.ApplyNodeDefinitionVersion(nodeID, 2, 5, true) {
				t.Fatal("failed to claim the initial version")
			}
			if tt.nodeInfo != nil {
				manager.nodesInfo.Store(nodeID, tt.nodeInfo)
			}

			if got := manager.ApplyNodeDefinitionVersion(nodeID, 2, 5, tt.present); got != tt.want {
				t.Errorf("ApplyNodeDefinitionVersion(gen 2, rev 5, present %v) = %v, want %v",
					tt.present, got, tt.want)
			}
		})
	}
}

func TestPruneStaleNodeVersions(t *testing.T) {
	manager := newTestManager()
	now := time.Now()

	manager.nodeVersions["expired-absent"] = nodeVersion{
		generation: 1, revision: 1, updatedAt: now.Add(-nodeVersionRetention - time.Hour),
	}
	manager.nodeVersions["expired-present"] = nodeVersion{
		generation: 1, revision: 1, updatedAt: now.Add(-nodeVersionRetention - time.Hour),
	}
	manager.nodesInfo.Store("expired-present", &NodeInfo{Generation: 1, Revision: 1})
	manager.nodeVersions["fresh-absent"] = nodeVersion{
		generation: 1, revision: 1, updatedAt: now.Add(-time.Hour),
	}

	manager.pruneStaleNodeVersions()

	manager.nodesInfoMutex.Lock()
	defer manager.nodesInfoMutex.Unlock()
	if _, exists := manager.nodeVersions["expired-absent"]; exists {
		t.Error("expired entry absent from nodesInfo must be pruned")
	}
	if _, exists := manager.nodeVersions["expired-present"]; !exists {
		t.Error("expired entry still present in nodesInfo must be kept")
	}
	if _, exists := manager.nodeVersions["fresh-absent"]; !exists {
		t.Error("entry within the retention window must be kept")
	}
}

func TestPruneStaleNodeVersionsRejectionAfterPrune(t *testing.T) {
	manager := newTestManager()

	// A pruned node loses its version history, so any generation can claim
	// the node again afterwards.
	manager.nodeVersions["node1"] = nodeVersion{
		generation: 9, revision: 9,
		updatedAt: time.Now().Add(-nodeVersionRetention - time.Hour),
	}
	manager.pruneStaleNodeVersions()

	if !manager.AcceptNodeVersion("node1", 1, 0) {
		t.Error("after pruning, an older generation must be accepted as first-seen")
	}
}
