package monitor

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sync"
	"time"

	"CraneFrontEnd/plugin/energy/pkg/cgroup"
	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/db"
	"CraneFrontEnd/plugin/energy/pkg/gpu"
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type TaskMonitor struct {
	samplePeriod time.Duration
	config       *config.MonitorConfig

	tasks map[uint32]*Task
	mutex sync.RWMutex
}

func (m *TaskMonitor) Start(taskID uint32, cgroupName string, boundGPUs []int) {
	log.Infof("Starting task monitor for task: %d, cgroup name: %s", taskID, cgroupName)

	if !m.config.Enabled.Task {
		log.Warnf("Task monitor is not enabled, skipping task: %d, %s", taskID, cgroupName)
		return
	}

	// If neither RAPL nor IPMI is enabled, the task energy cannot be calculated, skip this task
	if !m.config.Enabled.RAPL && !m.config.Enabled.IPMI {
		log.Warnf("No energy metrics enabled, skipping task: %d, %s", taskID, cgroupName)
		return
	}

	task, err := NewTask(taskID, cgroupName, m.samplePeriod, m.config, boundGPUs)
	if err != nil {
		log.Errorf("Failed to create task monitor: %v", err)
		return
	}

	m.mutex.Lock()
	m.tasks[taskID] = task
	m.mutex.Unlock()

	task.StartMonitor()
}

func (m *TaskMonitor) Stop(taskID uint32) {
	m.mutex.Lock()
	if task, exists := m.tasks[taskID]; exists {
		task.StopMonitor()
		delete(m.tasks, taskID)
	}
	m.mutex.Unlock()
}

func (m *TaskMonitor) Close() {
	m.mutex.Lock()
	for _, task := range m.tasks {
		task.StopMonitor()
	}
	m.mutex.Unlock()
}

type Task struct {
	taskID       uint32
	cgroupName   string
	boundGPUs    []int
	samplePeriod time.Duration
	config       *config.MonitorConfig

	data        *types.TaskData
	sampleCount int

	cgroupReader cgroup.CgroupReader
	gpuReader    *gpu.Reader

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

var gpuPatterns = map[string]*struct {
	pattern *regexp.Regexp
	vendor  string
}{
	"nvidia": {
		pattern: regexp.MustCompile(`/dev/nvidia(\d+)`),
		vendor:  "nvidia",
	},
}

func NewTask(taskID uint32, cgroupName string, samplePeriod time.Duration, config *config.MonitorConfig, boundGPUs []int) (*Task, error) {
	cgroupReader, err := cgroup.NewCgroupReader(cgroup.V1, cgroupName)
	if err != nil {
		return nil, fmt.Errorf("failed to create cgroup reader: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	startTime := time.Now()

	return &Task{
		taskID:       taskID,
		cgroupName:   cgroupName,
		samplePeriod: samplePeriod,
		config:       config,
		boundGPUs:    boundGPUs,
		ctx:          ctx,
		cancel:       cancel,
		subscriber: &NodeDataSubscriber{
			Ch:        make(chan *types.NodeData, 10),
			StartTime: startTime,
		},
		data: &types.TaskData{
			TaskID:    taskID,
			NodeID:    getNodeID(),
			StartTime: startTime,
		},
		cgroupReader: cgroupReader,
		gpuReader:    gpu.NewGPUReader(config.GPUType),
	}, nil
}

func (t *Task) StartMonitor() {
	GetSubscriberManagerInstance().AddSubscriber(t.taskID, t.subscriber)

	go t.monitorResourceUsage()
	go t.collectData()
}

func (t *Task) StopMonitor() {
	log.Infof("Stopping task monitor for task: %d", t.taskID)

	t.cancel()
	GetSubscriberManagerInstance().DeleteSubscriber(t.taskID)

	if err := t.gpuReader.Close(); err != nil {
		log.Errorf("Failed to close GPU reader: %v", err)
	}
}

func (t *Task) monitorResourceUsage() {
	ticker := time.NewTicker(t.samplePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			taskStats, err := t.cgroupReader.GetCgroupStats()
			if err != nil {
				log.Infof("Task %d monitor stopping, error: %v", t.taskID, err)
				go t.StopMonitor()
				return
			}
			metrics := t.gpuReader.GetBoundGpuMetrics(t.boundGPUs)
			t.updateTaskStats(&taskStats, metrics)
		case <-t.ctx.Done():
			return
		}
	}
}

func (t *Task) updateTaskStats(stats *types.CgroupStats, metrics *types.GPUMetrics) {
	// The resource usage in cgroup is cumulative, so no need to accumulate, just update directly
	t.data.CgroupStats.CPUStats.UsageSeconds = stats.CPUStats.UsageSeconds
	t.data.CgroupStats.IOStats.ReadMB = stats.IOStats.ReadMB
	t.data.CgroupStats.IOStats.WriteMB = stats.IOStats.WriteMB
	t.data.CgroupStats.IOStats.ReadOperations = stats.IOStats.ReadOperations
	t.data.CgroupStats.IOStats.WriteOperations = stats.IOStats.WriteOperations

	// These resources are accumulated every time they are sampled, and the average value is calculated at the end
	t.data.CgroupStats.CPUStats.Utilization += stats.CPUStats.Utilization
	t.data.CgroupStats.MemoryStats.UsageMB += stats.MemoryStats.UsageMB
	t.data.CgroupStats.MemoryStats.Utilization += stats.MemoryStats.Utilization
	t.data.CgroupStats.IOStats.ReadMBPS += stats.IOStats.ReadMBPS
	t.data.CgroupStats.IOStats.WriteMBPS += stats.IOStats.WriteMBPS
	t.data.CgroupStats.IOStats.ReadOpsPerSec += stats.IOStats.ReadOpsPerSec
	t.data.CgroupStats.IOStats.WriteOpsPerSec += stats.IOStats.WriteOpsPerSec
	t.data.CgroupStats.GPUStats.Utilization += metrics.Util
	t.data.CgroupStats.GPUStats.MemoryUtil += metrics.MemUtil

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
				log.Errorf("Error saving task energy data: %v", err)
			}
			return
		}
	}
}

func (t *Task) updateEnergy(nodeData *types.NodeData) {
	// If IPMI is enabled, use IPMI energy data preferentially
	if t.config.Enabled.IPMI {
		t.data.CPUEnergy += nodeData.IPMI.CPUEnergy
	} else if t.config.Enabled.RAPL {
		t.data.CPUEnergy += nodeData.RAPL.Package
	}
}

func (t *Task) calculateStats() {
	if t.sampleCount < 2 {
		log.Errorf("Insufficient samples for statistics")
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
	t.data.CgroupStats.CPUStats.Utilization /= sampleCount
	t.data.CgroupStats.MemoryStats.UsageMB /= sampleCount
	t.data.CgroupStats.MemoryStats.Utilization /= sampleCount
	t.data.CgroupStats.IOStats.ReadMBPS /= sampleCount
	t.data.CgroupStats.IOStats.WriteMBPS /= sampleCount
	t.data.CgroupStats.IOStats.ReadOpsPerSec /= sampleCount
	t.data.CgroupStats.IOStats.WriteOpsPerSec /= sampleCount
	t.data.CgroupStats.GPUStats.Utilization /= sampleCount
	t.data.CgroupStats.GPUStats.MemoryUtil /= sampleCount
}

func (t *Task) calculateEnergy() {
	t.data.CPUEnergy = t.data.CPUEnergy * (t.data.CgroupStats.CPUStats.Utilization / 100.0)
	t.data.TotalEnergy = t.data.CPUEnergy + t.data.GPUEnergy
	if duration := t.data.Duration.Seconds(); duration > 0 {
		t.data.AveragePower = t.data.TotalEnergy / duration
	}
}

func (t *Task) logTaskStats() {
	log.Infof("Task Statistics for %d:", t.taskID)
	log.Infof("  Task ID: %d", t.taskID)
	log.Infof("  Node ID: %s", t.data.NodeID)
	log.Infof("  Duration: %.2f seconds", t.data.Duration.Seconds())

	log.Infof("Energy Statistics:")
	log.Infof("  Total energy: %.2f J", t.data.TotalEnergy)
	log.Infof("  Average power: %.2f W", t.data.AveragePower)
	log.Infof("  CPU energy: %.2f J (utilization: %.2f%%)",
		t.data.CPUEnergy, t.data.CgroupStats.CPUStats.Utilization)
	log.Infof("  GPU energy: %.2f J (utilization: %.2f%%)",
		t.data.GPUEnergy, t.data.CgroupStats.GPUStats.Utilization)

	log.Infof("Resource Usage Statistics:")
	log.Infof("  CPU utilization: %.2f%%", t.data.CgroupStats.CPUStats.Utilization)
	log.Infof("  CPU usage: %.2f seconds", t.data.CgroupStats.CPUStats.UsageSeconds)
	log.Infof("  GPU utilization: %.2f%%", t.data.CgroupStats.GPUStats.Utilization)
	log.Infof("  GPU memory utilization: %.2f%%", t.data.CgroupStats.GPUStats.MemoryUtil)
	log.Infof("  Memory utilization: %.2f%%", t.data.CgroupStats.MemoryStats.Utilization)
	log.Infof("  Memory usage: %.2f MB", t.data.CgroupStats.MemoryStats.UsageMB)
	log.Infof("  Disk read: %.2f MB (%.2f MB/s)",
		t.data.CgroupStats.IOStats.ReadMB, t.data.CgroupStats.IOStats.ReadMBPS)
	log.Infof("  Disk write: %.2f MB (%.2f MB/s)",
		t.data.CgroupStats.IOStats.WriteMB, t.data.CgroupStats.IOStats.WriteMBPS)
	log.Infof("  Disk read operations count: %d", t.data.CgroupStats.IOStats.ReadOperations)
	log.Infof("  Disk write operations count: %d", t.data.CgroupStats.IOStats.WriteOperations)
	log.Infof("  Disk read operations: %.2f IOPS", t.data.CgroupStats.IOStats.ReadOpsPerSec)
	log.Infof("  Disk write operations: %.2f IOPS", t.data.CgroupStats.IOStats.WriteOpsPerSec)
}
