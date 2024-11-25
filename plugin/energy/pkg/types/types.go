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
	Energy  float64
	Power   float64
	Util    float64
	MemUtil float64
	Temp    float64
}

type SystemLoadMetrics struct {
	CPUUtil     float64 // CPU使用率(%)
	CPULoad1    float64 // 1分钟平均负载
	CPULoad5    float64 // 5分钟平均负载
	CPULoad15   float64 // 15分钟平均负载
	Frequencies float64 // CPU平均频率(MHz)

	MemoryUtil  float64 // 内存使用率(%)
	MemoryUsed  float64 // 已用内存(GB)
	MemoryTotal float64 // 总内存(GB)

	DiskUtil float64 // 磁盘使用率(%)
	DiskIO   float64 // 磁盘IO(MB/s)

	NetworkIO float64 // 网络IO总量(MB/s)
	NetworkRx float64 // 网络接收速率(MB/s)
	NetworkTx float64 // 网络发送速率(MB/s)

	Temperature float64 // 系统平均温度(℃)

	Timestamp time.Time // 采集时间戳
}

type TaskData struct {
	TaskName  string
	NodeID    string
	ClusterID string

	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration

	TotalEnergy float64 // 执行完任务累计总能耗(J)
	CPUEnergy   float64 // 执行完任务累计CPU能耗(J)
	GPUEnergy   float64 // 执行完任务累计GPU能耗(J)
	// DRAMEnergy   float64 // 执行完任务累计内存能耗(J)
	AveragePower float64 // 执行完任务平均功率(W)

	TaskStats TaskStats
}

type TaskStats struct {
	CPUStats       CPUStats
	GPUStats       GPUStats
	MemoryStats    MemoryStats
	IOStats        IOStats
	DeviceAccesses []DeviceAccess
}

type CPUStats struct {
	UsageSeconds float64
	Utilization  float64
}

type GPUStats struct {
	Utilization float64
	MemoryUtil  float64
}

type MemoryStats struct {
	UsageMB     float64
	Utilization float64
}

type DeviceAccess struct {
	DeviceType rune // 'c' for char device, 'b' for block device
	Major      int64
	Minor      int64
	Access     string // rwm (read/write/mknod)
	DevPath    string // 设备路径
}

type IOStats struct {
	ReadMB    float64 // 总读取量(MB)
	WriteMB   float64 // 总写入量(MB)
	ReadMBPS  float64 // 读取速率(MB/s)
	WriteMBPS float64 // 写入速率(MB/s)

	ReadOperations  uint64  // 读操作总次数
	WriteOperations uint64  // 写操作总次数
	ReadOpsPerSec   float64 // 读操作速率(IOPS)
	WriteOpsPerSec  float64 // 写操作速率(IOPS)
}
