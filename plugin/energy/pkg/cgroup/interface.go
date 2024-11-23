package cgroup

import (
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type CgroupReader interface {
	GetTaskResourceUsage(taskName string, lastUsage *types.TaskResourceUsage) types.TaskResourceUsage
}
