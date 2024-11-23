package types

import (
	"time"
)

type NodeData struct {
	NodeID    string
	ClusterID string
	Timestamp time.Time

	RAPL       RAPLMetrics
	IPMI       IPMIMetrics
	GPU        GPUMetrics
	SystemLoad SystemLoadMetrics
}

type RAPLMetrics struct {
	Package float64 // 间隔包能耗(J)
	Core    float64 // 间隔核心能耗(J)
	Uncore  float64 // 间隔非核心能耗(J)
	DRAM    float64 // 间隔内存能耗(J)
	GT      float64 // 间隔集成显卡能耗(J)
}

type IPMIMetrics struct {
	Power    float64 // 总功率(W)
	CPUPower float64 // CPU功率 (CPU_Power)
	FanPower float64 // 风扇功率 (FAN_Power)
	HDDPower float64 // 硬盘功率 (HDD_Power)

	Energy float64 // 整机间隔能耗(J)
}

type GPUMetrics struct {
	Energy  float64 // GPU能耗(J)
	Power   float64 // GPU功率(W)
	Util    float64 // GPU使用率(%)
	MemUtil float64 // GPU显存使用率(%)
	Temp    float64 // GPU温度(℃)
}

type SystemLoadMetrics struct {
	CPUUtil     float64 // CPU使用率(%)
	CPULoad1    float64 // 1分钟平均负载
	CPULoad5    float64 // 5分钟平均负载
	CPULoad15   float64 // 15分钟平均负载
	Temperature float64 // CPU平均温度(℃)
	Frequencies float64 // CPU平均频率(MHz)

	MemoryUtil  float64 // 内存使用率(%)
	MemoryUsed  float64 // 已用内存(GB)
	MemoryTotal float64 // 总内存(GB)

	DiskUtil float64 // 磁盘使用率(%)
	DiskIO   float64 // 磁盘IO(MB/s)

	NetworkIO float64 // 网络IO总量(MB/s)
	NetworkRx float64 // 网络接收速率(MB/s)
	NetworkTx float64 // 网络发送速率(MB/s)

	Timestamp time.Time // 采集时间戳
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

	CPUUtil        float64 // 平均CPU使用率(%)
	GPUUtil        float64 // 平均GPU使用率(%)
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
	AverageUtilization float64
}

type MemoryStats struct {
	CurrentUsage uint64
	Utilization  float64
}

type IOStats struct {
	CurrentRead  uint64  // 总读取字节数
	CurrentWrite uint64  // 总写入字节数
	ReadSpeed    float64 // 读取速率 (bytes/s)
	WriteSpeed   float64 // 写入速率 (bytes/s)

	ReadOperations  uint64  // 读操作总次数
	WriteOperations uint64  // 写操作总次数
	ReadOpsPerSec   float64 // 读操作速率 (ops/s)
	WriteOpsPerSec  float64 // 写操作速率 (ops/s)
}
