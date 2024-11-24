package cgroup

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/cgroups"
	v1 "github.com/containerd/cgroups/stats/v1"
	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/types"
)

const (
	MB        = 1024 * 1024
	NS_TO_SEC = 1e9
)

type V1Reader struct {
	control cgroups.Cgroup

	startCPUTime uint64    // 任务开始时的CPU时间
	startTime    time.Time // 任务开始时间
}

func NewV1Reader(taskName string) *V1Reader {
	if taskName == "" {
		return nil
	}

	control, err := cgroups.Load(cgroups.V1, cgroups.StaticPath(taskName))
	if err != nil {
		return nil
	}

	startCPUTime, err := readTotalCPUTime()
	if err != nil {
		log.Warnf("Failed to read initial CPU time: %v", err)
	}

	reader := V1Reader{
		control:      control,
		startCPUTime: startCPUTime,
		startTime:    time.Now(),
	}

	log.Infof("Successfully loaded cgroup for task: %s", taskName)
	for _, subsystem := range control.Subsystems() {
		log.Infof("Subsystem: %s", subsystem.Name())
	}

	return &reader
}

func (r *V1Reader) Close() error {
	return r.control.Delete()
}

func (r *V1Reader) GetCgroupStats() types.TaskStats {
	currentTime := time.Now()
	usage := types.TaskStats{}

	v1Metrics, err := r.control.Stat(cgroups.IgnoreNotExist)
	if err != nil {
		log.Errorf("Failed to get cgroup stats: %v", err)
		return usage
	}

	if v1Metrics == nil {
		log.Error("Metrics is nil")
		return usage
	}

	duration := currentTime.Sub(r.startTime).Seconds()

	cpuStats := r.calculateCPUStats(v1Metrics)
	memoryStats := calculateMemoryStats(v1Metrics)
	ioStats := calculateIOStats(v1Metrics, duration)

	usage = types.TaskStats{
		CPUStats:    cpuStats,
		MemoryStats: memoryStats,
		IOStats:     ioStats,
	}

	logTaskStats(v1Metrics, usage)
	return usage
}

func (r *V1Reader) calculateCPUStats(v1Metrics *v1.Metrics) types.CPUStats {
	if v1Metrics == nil || v1Metrics.CPU == nil {
		log.Warn("CPU metrics not available")
		return types.CPUStats{}
	}

	stats := types.CPUStats{
		UsageSeconds: float64(v1Metrics.CPU.Usage.Total) / NS_TO_SEC,
	}

	currentCPUTime, err := readTotalCPUTime()
	if err != nil {
		log.Warnf("Failed to read current CPU time: %v", err)
		return stats
	}

	if r.startCPUTime > 0 && currentCPUTime > r.startCPUTime {
		systemCPUDiff := currentCPUTime - r.startCPUTime
		taskCPUJiffies := v1Metrics.CPU.Usage.Total / 10000000 // 转换为jiffies

		numCPUs := float64(len(v1Metrics.CPU.Usage.PerCPU))
		if numCPUs == 0 {
			numCPUs = 1
		}

		cpuUtil := (float64(taskCPUJiffies) / float64(systemCPUDiff)) * 100 / numCPUs
		if cpuUtil > 100 {
			log.Warnf("CPU utilization exceeds 100%% (%.2f%%), capping at 100%%", cpuUtil)
			cpuUtil = 100
		}
		stats.Utilization = cpuUtil
	}

	return stats
}

func calculateMemoryStats(v1Metrics *v1.Metrics) types.MemoryStats {
	if v1Metrics == nil || v1Metrics.Memory == nil {
		log.Warn("Memory metrics not available")
		return types.MemoryStats{}
	}

	stats := types.MemoryStats{
		UsageMB: float64(v1Metrics.Memory.Usage.Usage) / MB,
	}

	limit := v1Metrics.Memory.Usage.Limit
	if limit == 0 || limit == math.MaxUint64 {
		var si syscall.Sysinfo_t
		if err := syscall.Sysinfo(&si); err != nil {
			log.Warnf("Failed to get system memory info: %v", err)
			return stats
		}
		limit = si.Totalram
	}

	if limit > 0 {
		memUtil := (float64(v1Metrics.Memory.Usage.Usage) / float64(limit)) * 100
		if memUtil > 100 {
			log.Warnf("Memory utilization exceeds 100%% (%.2f%%), capping at 100%%", memUtil)
			memUtil = 100
		}
		stats.Utilization = memUtil
	}

	return stats
}

func calculateIOStats(v1Metrics *v1.Metrics, duration float64) types.IOStats {
	var stats types.IOStats

	if v1Metrics == nil || v1Metrics.Blkio == nil {
		log.Warn("IO metrics not available")
		return stats
	}

	var (
		totalReadBytes  uint64
		totalWriteBytes uint64
		totalReadOps    uint64
		totalWriteOps   uint64
	)

	for _, entry := range v1Metrics.Blkio.IoServiceBytesRecursive {
		switch entry.Op {
		case "Read":
			totalReadBytes += entry.Value
		case "Write":
			totalWriteBytes += entry.Value
		}
	}

	for _, entry := range v1Metrics.Blkio.IoServicedRecursive {
		switch entry.Op {
		case "Read":
			totalReadOps += entry.Value
		case "Write":
			totalWriteOps += entry.Value
		}
	}

	stats.ReadMB = float64(totalReadBytes) / MB
	stats.WriteMB = float64(totalWriteBytes) / MB
	stats.ReadOperations = totalReadOps
	stats.WriteOperations = totalWriteOps

	if duration > 0 {
		stats.ReadMBPS = stats.ReadMB / duration
		stats.WriteMBPS = stats.WriteMB / duration
		stats.ReadOpsPerSec = float64(totalReadOps) / duration
		stats.WriteOpsPerSec = float64(totalWriteOps) / duration
	}

	return stats
}

func readTotalCPUTime() (uint64, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, fmt.Errorf("failed to read /proc/stat: %v", err)
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)[1:] // 跳过"cpu"字段
			var total uint64
			for _, field := range fields {
				val, err := strconv.ParseUint(field, 10, 64)
				if err != nil {
					return 0, fmt.Errorf("failed to parse CPU time: %v", err)
				}
				total += val
			}
			return total, nil
		}
	}

	return 0, fmt.Errorf("CPU stats not found in /proc/stat")
}

func logTaskStats(v1Metrics *v1.Metrics, usage types.TaskStats) {
	log.WithFields(log.Fields{
		"cpu_available":    v1Metrics.CPU != nil,
		"memory_available": v1Metrics.Memory != nil,
		"blkio_available":  v1Metrics.Blkio != nil,
		"cpu_usage":        fmt.Sprintf("%.2f s", usage.CPUStats.UsageSeconds),
		"cpu_util":         fmt.Sprintf("%.2f%%", usage.CPUStats.Utilization),
		"memory_usage":     fmt.Sprintf("%.2f MB", usage.MemoryStats.UsageMB),
		"memory_util":      fmt.Sprintf("%.2f%%", usage.MemoryStats.Utilization),
		"io_read_mb":       fmt.Sprintf("%.2f MB", usage.IOStats.ReadMB),
		"io_write_mb":      fmt.Sprintf("%.2f MB", usage.IOStats.WriteMB),
		"io_read_speed":    fmt.Sprintf("%.2f MB/s", usage.IOStats.ReadMBPS),
		"io_write_speed":   fmt.Sprintf("%.2f MB/s", usage.IOStats.WriteMBPS),
		"io_read_ops":      fmt.Sprintf("%.2f IOPS", usage.IOStats.ReadOpsPerSec),
		"io_write_ops":     fmt.Sprintf("%.2f IOPS", usage.IOStats.WriteOpsPerSec),
	}).Info("Resource usage stats")
}
