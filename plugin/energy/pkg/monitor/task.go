package monitor

import (
	"context"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/cgroup"
	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/db"
	"CraneFrontEnd/plugin/energy/pkg/gpu"
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type TaskMonitor struct {
	samplePeriod time.Duration
	config       *config.MonitorConfig

	tasks  map[string]*Task
	taskMu sync.RWMutex
}

func (m *TaskMonitor) Start(taskID uint32, taskName string) {
	log.Infof("\033[32m[TaskMonitor]\033[0m Task monitor switches: %v", m.config.Switches)

	if !m.config.Switches.Task {
		log.Warnf("\033[34m[TaskMonitor]\033[0m Task monitor is not enabled, skipping task: %d, %s", taskID, taskName)
		return
	}

	// 如果既没有开启RAPL也没有开启IPMI，则无法计算任务能耗，跳过该任务
	if !m.config.Switches.RAPL && !m.config.Switches.IPMI {
		log.Warnf("\033[34m[TaskMonitor]\033[0m No energy metrics enabled, skipping task: %d, %s", taskID, taskName)
		return
	}

	log.Infof("\033[34m[TaskMonitor]\033[0m Starting task monitor for task: %d, cgroup path: %s", taskID, taskName)

	task := NewTask(taskID, taskName, m.samplePeriod, m.config)

	m.taskMu.Lock()
	m.tasks[taskName] = task
	m.taskMu.Unlock()

	task.Start()
}

func (m *TaskMonitor) StopTask(taskName string) {
	m.taskMu.Lock()
	if task, exists := m.tasks[taskName]; exists {
		task.Stop()
		delete(m.tasks, taskName)
	}
	m.taskMu.Unlock()
}

func (m *TaskMonitor) Close() {
	m.taskMu.Lock()
	for _, task := range m.tasks {
		task.Stop()
	}
	m.taskMu.Unlock()
}

type Task struct {
	ID           uint32
	Name         string
	boundGPUs    []int
	samplePeriod time.Duration
	config       *config.MonitorConfig

	data        *types.TaskData
	sampleCount int

	cgroupReader *cgroup.V1Reader
	gpuReader    *gpu.GPUReader

	ctx        context.Context
	cancel     context.CancelFunc
	subscriber *NodeDataSubscriber
}

func getNodeID() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

// TODO: 从配置文件中读取集群ID
func getClusterID() string {
	return "cluster-1"
}

func NewTask(id uint32, name string, samplePeriod time.Duration, config *config.MonitorConfig) *Task {
	ctx, cancel := context.WithCancel(context.Background())
	startTime := time.Now()

	cgroupReader := cgroup.NewV1Reader(name)

	return &Task{
		ID:           id,
		Name:         name,
		samplePeriod: samplePeriod,
		config:       config,
		boundGPUs:    cgroupReader.GetBoundGPUs(),
		ctx:          ctx,
		cancel:       cancel,
		subscriber: &NodeDataSubscriber{
			Ch:        make(chan *types.NodeData, 10),
			StartTime: startTime,
		},
		data: &types.TaskData{
			NodeID:    getNodeID(),
			ClusterID: getClusterID(),
			TaskName:  name,
			StartTime: startTime,
		},
		cgroupReader: cgroupReader,
		gpuReader:    gpu.NewGPUReader(),
	}
}

func (t *Task) Start() {
	GetSubscriberManagerInstance().AddSubscriber(t.Name, t.subscriber)

	go t.monitorResourceUsage()
	go t.collectData()
}

func (t *Task) Stop() {
	t.cancel()
	GetSubscriberManagerInstance().DeleteSubscriber(t.Name)

	if err := t.gpuReader.Close(); err != nil {
		log.Errorf("\033[31m[TaskMonitor]\033[0m Failed to close GPU reader: %v", err)
	}
}

func (t *Task) monitorResourceUsage() {
	ticker := time.NewTicker(t.samplePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			taskStats := t.cgroupReader.GetCgroupStats()
			metrics := t.gpuReader.GetGpuStats(t.boundGPUs)
			t.updateTaskStats(&taskStats, &metrics)
		case <-t.ctx.Done():
			return
		}
	}
}

func (t *Task) updateTaskStats(stats *types.TaskStats, metrics *types.GPUMetrics) {
	// Cgroup中这些资源使用量是累积的，所以不需要累加，每次采样直接更新就可以
	t.data.TaskStats.CPUStats.UsageSeconds = stats.CPUStats.UsageSeconds
	t.data.TaskStats.IOStats.ReadMB = stats.IOStats.ReadMB
	t.data.TaskStats.IOStats.WriteMB = stats.IOStats.WriteMB
	t.data.TaskStats.IOStats.ReadOperations = stats.IOStats.ReadOperations
	t.data.TaskStats.IOStats.WriteOperations = stats.IOStats.WriteOperations

	// 这些资源每一次采样都累加一下，最终计算平均值
	t.data.TaskStats.CPUStats.Utilization += stats.CPUStats.Utilization
	t.data.TaskStats.MemoryStats.UsageMB += stats.MemoryStats.UsageMB
	t.data.TaskStats.MemoryStats.Utilization += stats.MemoryStats.Utilization
	t.data.TaskStats.IOStats.ReadMBPS += stats.IOStats.ReadMBPS
	t.data.TaskStats.IOStats.WriteMBPS += stats.IOStats.WriteMBPS
	t.data.TaskStats.IOStats.ReadOpsPerSec += stats.IOStats.ReadOpsPerSec
	t.data.TaskStats.IOStats.WriteOpsPerSec += stats.IOStats.WriteOpsPerSec
	t.data.TaskStats.GPUStats.Utilization += metrics.Util
	t.data.TaskStats.GPUStats.MemoryUtil += metrics.MemUtil

	// GPU能耗是基于时间片的，所以需要累加，但是最终不需要计算平均值
	t.data.GPUEnergy += metrics.Energy

	t.sampleCount++
}

func (t *Task) collectData() {
	for {
		select {
		case data := <-t.subscriber.Ch:
			t.updateEnergy(data)
		case <-t.ctx.Done():
			t.calculateStats()
			if err := db.GetInstance().SaveTaskEnergy(t.data); err != nil {
				log.Errorf("\033[31m[TaskMonitor]\033[0m Error saving task energy data: %v", err)
			}
			return
		}
	}
}

func (t *Task) updateEnergy(data *types.NodeData) {
	// 如果开启了IPMI，则优先使用IPMI的能耗数据
	if t.config.Switches.IPMI {
		// IPMI采样是基于时间片的，所以需要累加，不需要求平均值，最终通过乘以利用率来计算任务能耗
		t.data.CPUEnergy += data.IPMI.CPUEnergy
	} else if t.config.Switches.RAPL {
		// RAPL采样是基于时间片的，所以需要累加，不需要求平均值，最终通过乘以利用率来计算任务能耗
		t.data.CPUEnergy += data.RAPL.Package
	}
}

func (t *Task) calculateStats() {
	if t.sampleCount < 2 {
		log.Errorf("\033[31m[TaskMonitor]\033[0m Insufficient samples for statistics")
		return
	}

	t.data.EndTime = time.Now()
	t.data.Duration = t.data.EndTime.Sub(t.data.StartTime)
	sampleCount := float64(t.sampleCount)

	t.calculateAverages(sampleCount)
	t.calculateEnergy()

	t.logTaskStats()
}

func (t *Task) calculateAverages(sampleCount float64) {
	t.data.TaskStats.CPUStats.Utilization /= sampleCount
	t.data.TaskStats.MemoryStats.UsageMB /= sampleCount
	t.data.TaskStats.MemoryStats.Utilization /= sampleCount
	t.data.TaskStats.IOStats.ReadMBPS /= sampleCount
	t.data.TaskStats.IOStats.WriteMBPS /= sampleCount
	t.data.TaskStats.IOStats.ReadOpsPerSec /= sampleCount
	t.data.TaskStats.IOStats.WriteOpsPerSec /= sampleCount
	t.data.TaskStats.GPUStats.Utilization /= sampleCount
	t.data.TaskStats.GPUStats.MemoryUtil /= sampleCount
}

func (t *Task) calculateEnergy() {
	t.data.CPUEnergy = t.data.CPUEnergy * (t.data.TaskStats.CPUStats.Utilization / 100.0)
	t.data.TotalEnergy = t.data.CPUEnergy + t.data.GPUEnergy
	if duration := t.data.Duration.Seconds(); duration > 0 {
		t.data.AveragePower = t.data.TotalEnergy / duration
	}
}

func (t *Task) logTaskStats() {
	log.Infof("\033[34m[TaskMonitor]\033[0m Task Statistics for %s:", t.Name)
	log.Infof("  Task name: %s", t.data.TaskName)
	log.Infof("  Node ID: %s", t.data.NodeID)
	log.Infof("  Cluster ID: %s", t.data.ClusterID)
	log.Infof("  Duration: %.2f seconds", t.data.Duration.Seconds())

	log.Infof("\033[34m[TaskMonitor]\033[0m Energy Statistics:")
	log.Infof("  Total energy: %.2f J", t.data.TotalEnergy)
	log.Infof("  Average power: %.2f W", t.data.AveragePower)
	log.Infof("  CPU energy: %.2f J (utilization: %.2f%%)",
		t.data.CPUEnergy, t.data.TaskStats.CPUStats.Utilization)
	log.Infof("  GPU energy: %.2f J (utilization: %.2f%%)",
		t.data.GPUEnergy, t.data.TaskStats.GPUStats.Utilization)

	log.Infof("\033[34m[TaskMonitor]\033[0m Resource Usage Statistics:")
	log.Infof("  CPU utilization: %.2f%%", t.data.TaskStats.CPUStats.Utilization)
	log.Infof("  CPU usage: %.2f seconds", t.data.TaskStats.CPUStats.UsageSeconds)
	log.Infof("  GPU utilization: %.2f%%", t.data.TaskStats.GPUStats.Utilization)
	log.Infof("  GPU memory utilization: %.2f%%", t.data.TaskStats.GPUStats.MemoryUtil)
	log.Infof("  Memory utilization: %.2f%%", t.data.TaskStats.MemoryStats.Utilization)
	log.Infof("  Memory usage: %.2f MB", t.data.TaskStats.MemoryStats.UsageMB)
	log.Infof("  Disk read: %.2f MB (%.2f MB/s)",
		t.data.TaskStats.IOStats.ReadMB, t.data.TaskStats.IOStats.ReadMBPS)
	log.Infof("  Disk write: %.2f MB (%.2f MB/s)",
		t.data.TaskStats.IOStats.WriteMB, t.data.TaskStats.IOStats.WriteMBPS)
	log.Infof("  Disk read operations count: %d", t.data.TaskStats.IOStats.ReadOperations)
	log.Infof("  Disk write operations count: %d", t.data.TaskStats.IOStats.WriteOperations)
	log.Infof("  Disk read operations: %.2f IOPS", t.data.TaskStats.IOStats.ReadOpsPerSec)
	log.Infof("  Disk write operations: %.2f IOPS", t.data.TaskStats.IOStats.WriteOpsPerSec)
}
