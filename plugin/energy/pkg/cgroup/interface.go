package cgroup

import (
	"fmt"

	"CraneFrontEnd/plugin/energy/pkg/types"
)

type CgroupReader interface {
	GetCgroupStats() types.TaskStats
	GetBoundGPUs() []int
}

type Version int

const (
	V1 Version = iota + 1
	V2
)

func NewCgroupReader(version Version, taskName string) (CgroupReader, error) {
	switch version {
	case V1:
		reader := NewV1Reader(taskName)
		if reader == nil {
			return nil, fmt.Errorf("failed to create v1 reader for task: %s", taskName)
		}
		return reader, nil
	case V2:
		return nil, fmt.Errorf("cgroup v2 not implemented yet")
	default:
		return nil, fmt.Errorf("unsupported cgroup version: %d", version)
	}
}
