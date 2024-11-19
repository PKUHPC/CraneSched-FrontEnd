package cgroup

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/types"
)

type V1Reader struct {
	config Config
}

func NewV1Reader(config Config) *V1Reader {
	return &V1Reader{
		config: config,
	}
}

func (r *V1Reader) GetTaskResourceUsage(taskName string, lastUsage *types.TaskResourceUsage) types.TaskResourceUsage {
	startTime := time.Now()

	cpuUsage, perCPUUsage, cpuUtil, avgCPUUtil := r.readCPUStats(taskName, lastUsage)
	memUsage, memUtil := r.readMemoryStats(taskName)
	ioRead, ioWrite, readSpeed, writeSpeed := r.readIOStats(taskName, lastUsage)

	usage := types.TaskResourceUsage{
		LastUpdateTime: startTime,
		CPUStats: types.CPUStats{
			CurrentUsage:       cpuUsage,
			PerCPUUsage:        perCPUUsage,
			PerCPUUtilization:  cpuUtil,
			AverageUtilization: avgCPUUtil,
		},
		MemoryStats: types.MemoryStats{
			CurrentUsage: memUsage,
			Utilization:  memUtil,
		},
		IOStats: types.IOStats{
			CurrentRead:  ioRead,
			CurrentWrite: ioWrite,
			ReadSpeed:    readSpeed,
			WriteSpeed:   writeSpeed,
		},
	}

	if elapsed := time.Since(startTime); elapsed > time.Second {
		log.Warnf("\033[33m[CgroupV1] Resource usage collection for task %s took longer than expected: %v\033[0m", taskName, elapsed)
	}

	log.Debugf("\033[32m[CgroupV1] Task %s resource usage: CPU=%.2f%%, Mem=%.2f%%, IO Read=%.2f MB, Write=%.2f MB\033[0m",
		taskName, usage.CPUStats.AverageUtilization, usage.MemoryStats.Utilization, usage.IOStats.ReadSpeed, usage.IOStats.WriteSpeed)

	return usage
}

// 这里计算的 CPU 使用率是基于整机 CPU 的使用率，而不是基于 cgroup 的 CPU 限制
func (r *V1Reader) readCPUStats(taskName string, lastUsage *types.TaskResourceUsage) (uint64, string, map[int]float64, float64) {
	cpuUsagePath := filepath.Join(r.config.CgroupBasePath, "cpu", taskName, "cpuacct.usage")

	cpuUsageData, err := os.ReadFile(cpuUsagePath)
	if err != nil {
		log.Debugf("\033[31m[CgroupV1] failed to read CPU usage time: %v\033[0m", err)
		return 0, "", nil, 0
	}
	currentUsage, err := strconv.ParseUint(strings.TrimSpace(string(cpuUsageData)), 10, 64)
	if err != nil {
		log.Debugf("\033[31m[CgroupV1] failed to parse CPU usage time: %v\033[0m", err)
		return 0, "", nil, 0
	}

	perCPUPath := filepath.Join(r.config.CgroupBasePath, "cpu", "cpuacct.usage_percpu")
	perCPUUsageData, err := os.ReadFile(perCPUPath)
	if err != nil {
		log.Debugf("\033[31m[CgroupV1] failed to read per-CPU usage time: %v\033[0m", err)
		return 0, "", nil, 0
	}
	perCPUUsageStr := string(perCPUUsageData)
	currentPerCPU := strings.Fields(perCPUUsageStr)

	cpuUtil := make(map[int]float64)

	totalCPUUtil := 0.0

	if lastUsage != nil && lastUsage.CPUStats.PerCPUUsage != "" {
		timeDiff := time.Since(lastUsage.LastUpdateTime).Nanoseconds()

		lastPerCPU := strings.Fields(lastUsage.CPUStats.PerCPUUsage)
		if len(currentPerCPU) == len(lastPerCPU) {
			for i := range currentPerCPU {
				currentValue, err1 := strconv.ParseUint(currentPerCPU[i], 10, 64)
				lastValue, err2 := strconv.ParseUint(lastPerCPU[i], 10, 64)

				if err1 == nil && err2 == nil {
					cpuDiff := float64(currentValue - lastValue)
					cpuUtil[i] = (cpuDiff / float64(timeDiff)) * 100
					if cpuUtil[i] > 100 {
						cpuUtil[i] = 100
					}
					totalCPUUtil += cpuUtil[i]
				} else {
					log.Debugf("\033[31m[CgroupV1] failed to parse CPU %d usage time: err1=%v, err2=%v\033[0m", i, err1, err2)
				}
			}
		} else {
			log.Debugf("\033[31m[CgroupV1] CPU core number mismatch: current=%d, last=%d\033[0m", len(currentPerCPU), len(lastPerCPU))
		}
	} else {
		log.Debugf("\033[31m[CgroupV1] first sampling or no history data, initialize utilization to 0\033[0m")
		for i := range currentPerCPU {
			cpuUtil[i] = 0
		}
	}

	return currentUsage, perCPUUsageStr, cpuUtil, totalCPUUtil / float64(len(cpuUtil))
}

// 内存统计是瞬时值
func (r *V1Reader) readMemoryStats(taskName string) (uint64, float64) {
	memUsagePath := filepath.Join(r.config.CgroupBasePath, "memory", taskName, "memory.usage_in_bytes")

	memUsageData, err := os.ReadFile(memUsagePath)
	if err != nil {
		log.Debugf("\033[31m[CgroupV1] failed to read memory usage: %v\033[0m", err)
		return 0, 0
	}

	currentUsage, err := strconv.ParseUint(strings.TrimSpace(string(memUsageData)), 10, 64)
	if err != nil {
		log.Debugf("\033[31m[CgroupV1] failed to parse memory usage: %v, original data: %s\033[0m", err, string(memUsageData))
		return 0, 0
	}

	// 获取系统总内存
	var si syscall.Sysinfo_t
	if err := syscall.Sysinfo(&si); err != nil {
		log.Debugf("\033[31m[CgroupV1] failed to get system memory info: %v\033[0m", err)
		return 0, 0
	}
	totalMem := si.Totalram

	// 计算使用率 - 相对于系统总内存
	var utilization float64
	if totalMem > 0 {
		utilization = (float64(currentUsage) / float64(totalMem)) * 100
		if utilization > 100 {
			utilization = 100
		}
	}

	return currentUsage, utilization
}

// 数据格式：
// 8:0 Read 1234567
// 8:0 Write 7654321
// 8:1 Read 2345678
// 8:1 Write 8765432
// 8:0, 8:1 等是设备号（major:minor）
func (r *V1Reader) readIOStats(taskName string, lastUsage *types.TaskResourceUsage) (uint64, uint64, float64, float64) {
	ioPath := filepath.Join(r.config.CgroupBasePath, "blkio", taskName, "blkio.throttle.io_service_bytes")

	// 检查 blkio 控制器目录是否存在
	blkioDir := filepath.Join(r.config.CgroupBasePath, "blkio", taskName)
	if _, err := os.Stat(blkioDir); os.IsNotExist(err) {
		log.Debugf("\033[31m[CgroupV1] blkio cgroup not available for task %s (this is normal for some containers)\033[0m", taskName)
		return 0, 0, 0, 0
	}

	if _, err := os.Stat(ioPath); os.IsNotExist(err) {
		log.Debugf("\033[31m[CgroupV1] IO stats file not available for task %s: %s\033[0m", taskName, ioPath)
		return 0, 0, 0, 0
	}

	ioData, err := os.ReadFile(ioPath)
	if err != nil {
		log.Debugf("\033[31m[CgroupV1] failed to read IO stats file: %v\033[0m", err)
		return 0, 0, 0, 0
	}

	var currentRead, currentWrite uint64
	var readSpeed, writeSpeed float64

	// 解析IO数据
	lines := strings.Split(strings.TrimSpace(string(ioData)), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) == 3 {
			value, err := strconv.ParseUint(fields[2], 10, 64)
			if err != nil {
				log.Debugf("\033[31m[CgroupV1] Failed to parse IO value: %v\033[0m", err)
				continue
			}

			switch fields[1] {
			case "Read":
				currentRead += value // 累加所有设备的读取量
			case "Write":
				currentWrite += value // 累加所有设备的写入量
			}
		}
	}

	// 如果有历史数据，计算IO速率
	if lastUsage != nil {
		timeDiff := time.Since(lastUsage.LastUpdateTime).Seconds()
		if timeDiff > 0 {
			readDiff := float64(currentRead - lastUsage.IOStats.CurrentRead)
			writeDiff := float64(currentWrite - lastUsage.IOStats.CurrentWrite)

			// 确保速率不为负
			if readDiff >= 0 {
				readSpeed = readDiff / timeDiff
			}
			if writeDiff >= 0 {
				writeSpeed = writeDiff / timeDiff
			}
		}
	}

	return currentRead, currentWrite, readSpeed, writeSpeed
}
