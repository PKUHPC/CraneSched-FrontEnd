/**
 * Copyright (c) 2026 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */

package crun

import (
	"CraneFrontEnd/generated/protos"
	"testing"
)

func TestSetInheritedStepFieldsFromEnv(t *testing.T) {
	t.Setenv("CRANE_NTASKS", "4")
	t.Setenv("CRANE_JOB_NUM_NODES", "1")
	t.Setenv("CRANE_NTASKS_PER_NODE", "4")
	t.Setenv("CRANE_MEM_PER_NODE", "512M")

	step := &protos.StepToCtld{}
	if err := setInheritedStepFieldsFromEnv(step, true); err != nil {
		t.Fatalf("set inherited fields: %v", err)
	}

	if step.Ntasks != 4 || step.NodeNum != 1 || step.NtasksPerNode != 4 {
		t.Fatalf("unexpected topology: nodes=%d ntasks=%d ntasks_per_node=%d", step.NodeNum, step.Ntasks, step.NtasksPerNode)
	}
	if step.MemPerNode == nil || *step.MemPerNode != 512*1024*1024 {
		t.Fatalf("unexpected memory: %v", step.MemPerNode)
	}
}

func TestSetInheritedStepFieldsFromEnvRejectsInvalidMemory(t *testing.T) {
	t.Setenv("CRANE_MEM_PER_NODE", "not-a-memory")

	err := setInheritedStepFieldsFromEnv(&protos.StepToCtld{}, true)
	if err == nil {
		t.Fatal("expected invalid memory error")
	}
}

func TestSetInheritedStepFieldsFromEnvSkipsMemoryWhenExplicitMemoryOptionIsSet(t *testing.T) {
	t.Setenv("CRANE_MEM_PER_NODE", "512M")

	step := &protos.StepToCtld{}
	if err := setInheritedStepFieldsFromEnv(step, false); err != nil {
		t.Fatalf("set inherited fields: %v", err)
	}
	if step.MemPerNode != nil {
		t.Fatalf("unexpected inherited memory: %d", *step.MemPerNode)
	}
}

func TestSetInheritedStepFieldsFromEnvRejectsInvalidTopology(t *testing.T) {
	t.Setenv("CRANE_NTASKS", "not-a-number")

	if err := setInheritedStepFieldsFromEnv(&protos.StepToCtld{}, true); err == nil {
		t.Fatal("expected invalid topology error")
	}
}
