package cgroup

import (
	"fmt"
	"math"
	"path/filepath"
	"syscall"
	"time"

	"github.com/containerd/cgroups"
	v1 "github.com/containerd/cgroups/stats/v1"
	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/types"
)

type V1Reader struct {
	control cgroups.Cgroup
}

func NewV1Reader() *V1Reader {
	return &V1Reader{}
}

func (r *V1Reader) loadCgroup(taskName string) error {
	if r.control != nil {
		return nil
	}

	path := filepath.Join("/sys/fs/cgroup", taskName)
	control, err := cgroups.Load(cgroups.V1, cgroups.StaticPath(path))
	if err != nil {
		return fmt.Errorf("load cgroup: %v", err)
	}
	r.control = control
	return nil
}

func (r *V1Reader) getDiskIOStats(metrics *v1.Metrics, lastUsage *types.TaskResourceUsage, duration float64) types.IOStats {
	var stats types.IOStats

	if metrics.Blkio == nil {
		return stats
	}

	for _, entry := range metrics.Blkio.IoServiceBytesRecursive {
		switch entry.Op {
		case "Read":
			stats.CurrentRead = uint64(entry.Value)
		case "Write":
			stats.CurrentWrite = uint64(entry.Value)
		}
	}

	for _, entry := range metrics.Blkio.IoServicedRecursive {
		switch entry.Op {
		case "Read":
			stats.ReadOperations = entry.Value
		case "Write":
			stats.WriteOperations = entry.Value
		}
	}

	if lastUsage != nil && duration > 0 {
		readDiff := stats.CurrentRead - lastUsage.IOStats.CurrentRead
		writeDiff := stats.CurrentWrite - lastUsage.IOStats.CurrentWrite

		stats.ReadSpeed = float64(readDiff) / duration
		stats.WriteSpeed = float64(writeDiff) / duration

		readOpsDiff := stats.ReadOperations - lastUsage.IOStats.ReadOperations
		writeOpsDiff := stats.WriteOperations - lastUsage.IOStats.WriteOperations

		stats.ReadOpsPerSec = float64(readOpsDiff) / duration
		stats.WriteOpsPerSec = float64(writeOpsDiff) / duration

		log.Debugf("IO Operations - Read: %d ops (%.2f IOPS), Write: %d ops (%.2f IOPS)",
			stats.ReadOperations,
			stats.ReadOpsPerSec,
			stats.WriteOperations,
			stats.WriteOpsPerSec)
	}

	return stats
}

func (r *V1Reader) GetTaskResourceUsage(taskName string, lastUsage *types.TaskResourceUsage) types.TaskResourceUsage {
	startTime := time.Now()

	if err := r.loadCgroup(taskName); err != nil {
		log.Errorf("Failed to load cgroup for task %s: %v", taskName, err)
		return types.TaskResourceUsage{LastUpdateTime: startTime}
	}

	metrics, err := r.control.Stat(cgroups.IgnoreNotExist)
	if err != nil {
		log.Errorf("Failed to get cgroup stats: %v", err)
		return types.TaskResourceUsage{LastUpdateTime: startTime}
	}

	duration := time.Since(lastUsage.LastUpdateTime).Seconds()
	ioStats := r.getDiskIOStats(metrics, lastUsage, duration)

	usage := types.TaskResourceUsage{
		LastUpdateTime: startTime,
		CPUStats: types.CPUStats{
			CurrentUsage:       metrics.CPU.Usage.Total,
			AverageUtilization: calculateCPUUtil(metrics, lastUsage, duration),
		},
		MemoryStats: types.MemoryStats{
			CurrentUsage: metrics.Memory.Usage.Usage,
			Utilization:  calculateMemUtil(metrics),
		},
		IOStats: ioStats,
	}

	log.Info("CgroupV1: ", taskName)
	log.Infof("CgroupV1: CPU: %d", usage.CPUStats.CurrentUsage)
	log.Infof("CgroupV1: CPU: %.2f%%", usage.CPUStats.AverageUtilization)
	log.Infof("CgroupV1: Memory: %d", usage.MemoryStats.CurrentUsage)
	log.Infof("CgroupV1: Memory: %.2f%%", usage.MemoryStats.Utilization)
	log.Infof("CgroupV1: IO: %.2f%%", usage.IOStats.ReadSpeed)
	log.Infof("CgroupV1: IO: %.2f%%", usage.IOStats.WriteSpeed)
	log.Infof("CgroupV1: IO: %.2f%%", usage.IOStats.ReadOpsPerSec)
	log.Infof("CgroupV1: IO: %.2f%%", usage.IOStats.WriteOpsPerSec)
	log.Infof("CgroupV1: IO: %d", usage.IOStats.ReadOperations)
	log.Infof("CgroupV1: IO: %d", usage.IOStats.WriteOperations)
	log.Infof("CgroupV1: IO: %d", usage.IOStats.CurrentRead)
	log.Infof("CgroupV1: IO: %d", usage.IOStats.CurrentWrite)

	return usage
}

// calculateCPUUtil 计算CPU利用率
func calculateCPUUtil(metrics *v1.Metrics, lastUsage *types.TaskResourceUsage, duration float64) float64 {
	if lastUsage == nil || duration <= 0 || metrics.CPU == nil {
		return 0
	}

	// 计算CPU使用时间差（纳秒）
	cpuDiff := int64(metrics.CPU.Usage.Total - lastUsage.CPUStats.CurrentUsage)
	if cpuDiff < 0 {
		log.Warnf("Negative CPU usage difference detected, resetting")
		return 0
	}

	// 获取CPU核心数
	numCPUs := float64(len(metrics.CPU.Usage.PerCPU))
	if numCPUs == 0 {
		numCPUs = 1 // 防止除零
	}

	// CPU利用率计算公式：
	// (CPU时间差 / 总时间差) * 100 / CPU核心数
	// CPU时间是纳秒，需要转换为秒
	cpuUtil := (float64(cpuDiff) / (duration * 1e9)) * 100 / numCPUs

	// 合理性检查
	if cpuUtil > 100 {
		log.Warnf("CPU utilization exceeds 100%% (%.2f%%), capping at 100%%", cpuUtil)
		cpuUtil = 100
	}

	return cpuUtil
}

func calculateMemUtil(metrics *v1.Metrics) float64 {
	if metrics.Memory == nil {
		return 0
	}

	usage := metrics.Memory.Usage.Usage
	limit := metrics.Memory.Usage.Limit

	if limit == 0 || limit == math.MaxUint64 {
		var si syscall.Sysinfo_t
		if err := syscall.Sysinfo(&si); err != nil {
			log.Warnf("Failed to get system memory info: %v", err)
			return 0
		}
		limit = si.Totalram
	}

	if limit > 0 {
		memUtil := (float64(usage) / float64(limit)) * 100

		if memUtil > 100 {
			log.Warnf("Memory utilization exceeds 100%% (%.2f%%), capping at 100%%", memUtil)
			memUtil = 100
		}

		log.Debugf("Memory stats: usage=%.2f MB, limit=%.2f MB, utilization=%.2f%%",
			float64(usage)/(1024*1024),
			float64(limit)/(1024*1024),
			memUtil)

		return memUtil
	}

	return 0
}
