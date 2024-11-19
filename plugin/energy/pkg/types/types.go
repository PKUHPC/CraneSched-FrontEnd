package types

import (
	"time"
)

type NodeData struct {
	NodeID    string
	ClusterID string
	Timestamp time.Time

	// RAPL
	Package     float64 // 累计包能耗(J)
	Core        float64 // 累计核心能耗(J)
	Uncore      float64 // 累计非核心能耗(J)
	DRAM        float64 // 累计内存能耗(J)
	GT          float64 // 累计集成显卡能耗(J)
	Temperature float64 // 多核平均CPU温度(℃)
	Frequencies float64 // 多核平均CPU频率(MHz)

	// NVIDIA GPU
	GPU        float64 // GPU能耗(J)
	GPUPower   float64 // GPU功率(W)
	GPUUtil    float64 // GPU使用率(%)
	GPUMemUtil float64 // GPU显存使用率(%)
	GPUTemp    float64 // GPU温度(℃)

	// System
	CPUUtil    float64 // CPU使用率(%)
	MemoryUtil float64 // 内存使用率(%)
	DiskIO     float64 // 磁盘IO(MB/s)
	NetworkIO  float64 // 网络IO(MB/s)
}

type TaskData struct {
	TaskName  string
	NodeID    string
	ClusterID string

	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration

	NodeDataHistory   []*NodeData
	TaskResourceUsage []TaskResourceUsage

	TotalEnergy  float64 // 执行完任务累计总能耗(J)
	CPUEnergy    float64 // 执行完任务累计CPU能耗(J)
	GPUEnergy    float64 // 执行完任务累计GPU能耗(J)
	DRAMEnergy   float64 // 执行完任务累计内存能耗(J)
	AveragePower float64 // 执行完任务平均功率(W)
	CPUTime      float64 // 执行完任务CPU总使用时间(s)

	CPUUtilization float64 // 平均CPU使用率(%)
	GPUUtilization float64 // 平均GPU使用率(%)
	MemoryUsage    uint64  // 平均内存使用量(B)
	MemoryUtil     float64 // 平均内存使用率(%)
	DiskReadBytes  uint64  // 总读取字节数
	DiskWriteBytes uint64  // 总写入字节数
	DiskReadSpeed  float64 // 平均读取速率(B/s)
	DiskWriteSpeed float64 // 平均写入速率(B/s)
}

type TaskResourceUsage struct {
	LastUpdateTime time.Time
	CPUStats       CPUStats
	MemoryStats    MemoryStats
	IOStats        IOStats
}

type CPUStats struct {
	CurrentUsage       uint64
	PerCPUUsage        string
	PerCPUUtilization  map[int]float64
	AverageUtilization float64
}

type MemoryStats struct {
	CurrentUsage uint64
	Utilization  float64
}

type IOStats struct {
	CurrentRead  uint64
	CurrentWrite uint64
	ReadSpeed    float64
	WriteSpeed   float64
}
