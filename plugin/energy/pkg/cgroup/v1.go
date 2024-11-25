package cgroup

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
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

	startTime    time.Time // 任务开始时间
	startCPUTime uint64    // 任务开始时的CPU时间
	taskName     string
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
		log.Warnf("\033[31m[CGroup]\033[0m Failed to read initial CPU time: %v", err)
	}

	reader := V1Reader{
		control:      control,
		startCPUTime: startCPUTime,
		startTime:    time.Now(),
		taskName:     taskName,
	}

	log.Infof("\033[33m[CGroup]\033[0m Successfully loaded cgroup for task: %s", taskName)
	for _, subsystem := range control.Subsystems() {
		log.Infof("\033[33m[CGroup]\033[0m Subsystem: %s", subsystem.Name())
	}

	return &reader
}

func (r *V1Reader) GetCgroupStats() types.TaskStats {
	currentTime := time.Now()
	usage := types.TaskStats{}

	v1Metrics, err := r.control.Stat(cgroups.IgnoreNotExist)
	if err != nil {
		log.Errorf("\033[31m[CGroup]\033[0m Failed to get cgroup stats: %v", err)
		return usage
	}

	if v1Metrics == nil {
		log.Error("\033[31m[CGroup]\033[0m Metrics is nil")
		return usage
	}

	duration := currentTime.Sub(r.startTime).Seconds()

	cpuStats := r.calculateCPUStats(v1Metrics)
	memoryStats := calculateMemoryStats(v1Metrics)
	// 目前好像并未支持blkio控制器，所以IO统计数据为空
	ioStats := calculateIOStats(v1Metrics, duration)

	usage = types.TaskStats{
		CPUStats:    cpuStats,
		MemoryStats: memoryStats,
		IOStats:     ioStats,
	}

	r.logTaskStats(usage)
	return usage
}

func (r *V1Reader) calculateCPUStats(v1Metrics *v1.Metrics) types.CPUStats {
	if v1Metrics == nil || v1Metrics.CPU == nil {
		log.Warn("\033[31m[CGroup]\033[0m CPU metrics not available")
		return types.CPUStats{}
	}

	stats := types.CPUStats{
		UsageSeconds: float64(v1Metrics.CPU.Usage.Total) / NS_TO_SEC,
	}

	currentCPUTime, err := readTotalCPUTime()
	if err != nil {
		log.Warnf("\033[31m[CGroup]\033[0m Failed to read current CPU time: %v", err)
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
			log.Warnf("\033[31m[CGroup]\033[0m CPU utilization exceeds 100%% (%.2f%%), capping at 100%%", cpuUtil)
			cpuUtil = 100
		}
		stats.Utilization = cpuUtil
	}

	return stats
}

func calculateMemoryStats(v1Metrics *v1.Metrics) types.MemoryStats {
	if v1Metrics == nil || v1Metrics.Memory == nil {
		log.Warn("\033[31m[CGroup]\033[0m Memory metrics not available")
		return types.MemoryStats{}
	}

	stats := types.MemoryStats{
		UsageMB: float64(v1Metrics.Memory.Usage.Usage) / MB,
	}

	limit := v1Metrics.Memory.Usage.Limit
	if limit == 0 || limit == math.MaxUint64 {
		var si syscall.Sysinfo_t
		if err := syscall.Sysinfo(&si); err != nil {
			log.Warnf("\033[31m[CGroup]\033[0m Failed to get system memory info: %v", err)
			return stats
		}
		limit = si.Totalram
	}

	if limit > 0 {
		memUtil := (float64(v1Metrics.Memory.Usage.Usage) / float64(limit)) * 100
		if memUtil > 100 {
			log.Warnf("\033[31m[CGroup]\033[0m Memory utilization exceeds 100%% (%.2f%%), capping at 100%%", memUtil)
			memUtil = 100
		}
		stats.Utilization = memUtil
	}

	return stats
}

func calculateIOStats(v1Metrics *v1.Metrics, duration float64) types.IOStats {
	var stats types.IOStats

	if v1Metrics == nil || v1Metrics.Blkio == nil {
		log.Warn("\033[31m[CGroup]\033[0m IO metrics not available")
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
		return 0, fmt.Errorf("\033[31m[CGroup]\033[0m failed to read /proc/stat: %v", err)
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)[1:] // 跳过"cpu"字段
			var total uint64
			for _, field := range fields {
				val, err := strconv.ParseUint(field, 10, 64)
				if err != nil {
					return 0, fmt.Errorf("\033[31m[CGroup]\033[0m failed to parse CPU time: %v", err)
				}
				total += val
			}
			return total, nil
		}
	}

	return 0, fmt.Errorf("\033[31m[CGroup]\033[0m CPU stats not found in /proc/stat")
}

func (r *V1Reader) GetBoundGPUs() []int {
	var boundGPUs []int

	deviceListPath := filepath.Join("/sys/fs/cgroup/devices", r.taskName, "devices.list")
	data, err := os.ReadFile(deviceListPath)
	if err != nil {
		log.Warnf("\033[31m[CGroup]\033[0m Failed to read devices.list from %s: %v", deviceListPath, err)
		return boundGPUs
	}

	// 检查是否有访问所有设备的权限
	if strings.Contains(string(data), "a *:* rwm") {
		log.Infof("\033[33m[CGroup]\033[0m Task %s has access to all devices", r.taskName)
		return getAllGPUs()
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}

		// 检查是否是字符设备且主设备号为 195（NVIDIA GPU）
		if fields[0] == "c" && strings.HasPrefix(fields[1], "195:") {
			nums := strings.Split(fields[1], ":")
			if len(nums) == 2 && nums[1] != "*" {
				if minor, err := strconv.Atoi(nums[1]); err == nil {
					// NVIDIA GPU 的次设备号就是 GPU 索引
					if minor < 128 { // 排除控制设备（195:254, 195:255）
						boundGPUs = append(boundGPUs, minor)
					}
				}
			}
		}
	}

	if len(boundGPUs) > 0 {
		sort.Ints(boundGPUs)
		boundGPUs = removeDuplicates(boundGPUs)
		log.Infof("\033[33m[CGroup]\033[0m Task %s is bound to GPUs: %v", r.taskName, boundGPUs)
	}

	return boundGPUs
}

func getAllGPUs() []int {
	var gpus []int
	pattern := "/dev/nvidia[0-9]*"
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return gpus
	}

	for _, match := range matches {
		numStr := strings.TrimPrefix(match, "/dev/nvidia")
		if idx, err := strconv.Atoi(numStr); err == nil {
			gpus = append(gpus, idx)
		}
	}

	sort.Ints(gpus)

	log.Infof("\033[33m[CGroup]\033[0m System has GPUs: %v", gpus)
	return gpus
}

func removeDuplicates(indices []int) []int {
	seen := make(map[int]bool)
	result := []int{}
	for _, idx := range indices {
		if !seen[idx] {
			seen[idx] = true
			result = append(result, idx)
		}
	}
	return result
}

func (r *V1Reader) logTaskStats(usage types.TaskStats) {
	log.Infof("\033[33m[CGroup]\033[0m %s Cgroup Metrics:", r.taskName)
	log.Infof("\033[33m[CGroup]\033[0m CPU Usage: %.2f s, CPU Utilization: %.2f%%", usage.CPUStats.UsageSeconds, usage.CPUStats.Utilization)
	log.Infof("\033[33m[CGroup]\033[0m Memory Usage: %.2f MB, Memory Utilization: %.2f%%", usage.MemoryStats.UsageMB, usage.MemoryStats.Utilization)
	log.Infof("\033[33m[CGroup]\033[0m IO Read: %.2f MB, IO Write: %.2f MB, IO Read Speed: %.2f MB/s, IO Write Speed: %.2f MB/s, IO Read Operations: %.2f IOPS, IO Write Operations: %.2f IOPS", usage.IOStats.ReadMB, usage.IOStats.WriteMB, usage.IOStats.ReadMBPS, usage.IOStats.WriteMBPS, usage.IOStats.ReadOpsPerSec, usage.IOStats.WriteOpsPerSec)
}
