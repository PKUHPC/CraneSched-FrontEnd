package monitor

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/cgroup"
	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/db"
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type TaskMonitor struct {
	samplePeriod time.Duration
	config       *config.MonitorConfig
	cgroupReader cgroup.CgroupReader

	tasks  map[string]*Task
	taskMu sync.RWMutex
}

func (m *TaskMonitor) Start(taskID uint32, taskName string) {
	log.Infof("\033[32m[TaskMonitor]\033[0m Task monitor config: %v", m.config.Switches.Task)

	if !m.config.Switches.Task {
		log.Infof("Task monitor is not enabled, skipping task: %d, %s", taskID, taskName)
		return
	}

	if !m.config.Switches.RAPL && !m.config.Switches.GPU {
		log.Infof("No energy metrics enabled, skipping task: %d, %s", taskID, taskName)
		return
	}

	log.Infof("Starting task monitor for task: %d, cgroup path: %s", taskID, taskName)

	task := NewTask(taskID, taskName, m.samplePeriod, m.cgroupReader)

	m.taskMu.Lock()
	m.tasks[taskName] = task
	m.taskMu.Unlock()

	task.Start()
}

func (m *TaskMonitor) Stop(taskName string) {
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
	ID        uint32
	Name      string
	StartTime time.Time

	// 控制相关
	ctx        context.Context
	cancel     context.CancelFunc
	subscriber *NodeDataSubscriber

	// 数据相关
	data         *types.TaskData
	usageChan    chan types.TaskResourceUsage
	samplePeriod time.Duration

	cgroupReader cgroup.CgroupReader
}

func NewTask(id uint32, name string, samplePeriod time.Duration, cgroupReader cgroup.CgroupReader) *Task {
	ctx, cancel := context.WithCancel(context.Background())
	startTime := time.Now()

	return &Task{
		ID:        id,
		Name:      name,
		StartTime: startTime,
		ctx:       ctx,
		cancel:    cancel,
		subscriber: &NodeDataSubscriber{
			Ch:        make(chan *types.NodeData, 10),
			StartTime: startTime,
		},
		data: &types.TaskData{
			TaskName:          name,
			StartTime:         startTime,
			NodeDataHistory:   make([]*types.NodeData, 0, 100),
			TaskResourceUsage: make([]types.TaskResourceUsage, 0, 100),
		},
		usageChan:    make(chan types.TaskResourceUsage, 10),
		samplePeriod: samplePeriod,
		cgroupReader: cgroupReader,
	}
}

func (t *Task) Start() {
	GetSubscriberManagerInstance().AddSubscriber(t.Name, t.subscriber)

	// 启动资源使用率监控
	go t.monitorResourceUsage()
	// 启动数据收集
	go t.collectData()
}

func (t *Task) Stop() {
	t.cancel()
	GetSubscriberManagerInstance().DeleteSubscriber(t.Name)
	close(t.usageChan)
}

func (t *Task) monitorResourceUsage() {
	ticker := time.NewTicker(t.samplePeriod)
	defer ticker.Stop()

	var lastUsage *types.TaskResourceUsage
	for {
		select {
		case <-ticker.C:
			usage := t.cgroupReader.GetTaskResourceUsage(t.Name, lastUsage)
			select {
			case t.usageChan <- usage:
				lastUsage = &usage
			case <-t.ctx.Done():
				return
			}
		case <-t.ctx.Done():
			return
		}
	}
}

func (t *Task) collectData() {
	for {
		select {
		case data := <-t.subscriber.Ch:
			t.data.NodeDataHistory = append(t.data.NodeDataHistory, data)
		case usage := <-t.usageChan:
			t.data.TaskResourceUsage = append(t.data.TaskResourceUsage, usage)
		case <-t.ctx.Done():
			t.calculateStats()
			if err := db.GetInstance().SaveTaskEnergy(t.data); err != nil {
				log.Errorf("Error saving task energy data: %v", err)
			}
			return
		}
	}
}

func (t *Task) calculateResourceStats() {
	if len(t.data.TaskResourceUsage) < 2 {
		log.Errorf("insufficient task resource data points for task: %s", t.Name)
		return
	}

	// 计算CPU时间
	startUsage := t.data.TaskResourceUsage[0]
	endUsage := t.data.TaskResourceUsage[len(t.data.TaskResourceUsage)-1]
	t.data.CPUTime = float64(endUsage.CPUStats.CurrentUsage-startUsage.CPUStats.CurrentUsage) / 1e9

	// 计算平均资源使用率
	var totalMemoryUtil, totalCPUUtil float64
	for _, usage := range t.data.TaskResourceUsage {
		totalMemoryUtil += usage.MemoryStats.Utilization
		totalCPUUtil += usage.CPUStats.AverageUtilization
	}

	t.data.CPUUtil = totalCPUUtil / float64(len(t.data.TaskResourceUsage))
	t.data.MemoryUtil = totalMemoryUtil / float64(len(t.data.TaskResourceUsage))

	t.data.MemoryUsage = endUsage.MemoryStats.CurrentUsage
	t.data.DiskReadBytes = endUsage.IOStats.CurrentRead
	t.data.DiskWriteBytes = endUsage.IOStats.CurrentWrite
	t.data.DiskReadSpeed = float64(t.data.DiskReadBytes) / t.data.Duration.Seconds()
	t.data.DiskWriteSpeed = float64(t.data.DiskWriteBytes) / t.data.Duration.Seconds()
}

// calculateEnergyStats 计算能耗统计
func (t *Task) calculateEnergyStats() {
	if len(t.data.NodeDataHistory) < 2 {
		log.Errorf("insufficient node data points for task: %s", t.Name)
		return
	}

	var totalCPUEnergy, totalDRAMEnergy, totalGPUEnergy float64
	var totalCPUUtil, totalGPUUtil float64

	// 计算总能耗和平均使用率
	for _, data := range t.data.NodeDataHistory {
		totalCPUEnergy += data.RAPL.Core + data.RAPL.Uncore
		totalDRAMEnergy += data.RAPL.DRAM
		totalGPUEnergy += data.GPU.Energy

		totalCPUUtil += data.SystemLoad.CPUUtil
		totalGPUUtil += data.GPU.Util
	}

	// 计算平均使用率
	sampleCount := float64(len(t.data.NodeDataHistory))
	t.data.CPUUtil = totalCPUUtil / sampleCount
	t.data.GPUUtil = totalGPUUtil / sampleCount

	// 根据使用率分配能耗
	t.data.CPUEnergy = totalCPUEnergy * (t.data.CPUUtil / 100.0)
	t.data.DRAMEnergy = totalDRAMEnergy * (t.data.MemoryUtil / 100.0)
	t.data.GPUEnergy = totalGPUEnergy * (t.data.GPUUtil / 100.0)

	// 计算总能耗和平均功率
	t.data.TotalEnergy = t.data.CPUEnergy + t.data.DRAMEnergy + t.data.GPUEnergy
	t.data.AveragePower = t.data.TotalEnergy / t.data.Duration.Seconds()
}

// logStats 记录统计信息
func (t *Task) logStats() {
	log.Infof("Task Statistics for %s:", t.Name)
	// 基本信息
	log.Infof("  Task name: %s", t.data.TaskName)
	log.Infof("  Node ID: %s", t.data.NodeID)
	log.Infof("  Cluster ID: %s", t.data.ClusterID)
	log.Infof("  Duration: %.2f seconds", t.data.Duration.Seconds())

	// 能耗统计
	log.Infof("Energy Statistics:")
	log.Infof("  Total energy: %.2f J", t.data.TotalEnergy)
	log.Infof("  Average power: %.2f W", t.data.AveragePower)
	log.Infof("  CPU energy: %.2f J (utilization: %.2f%%)", t.data.CPUEnergy, t.data.CPUUtil)
	log.Infof("  GPU energy: %.2f J (utilization: %.2f%%)", t.data.GPUEnergy, t.data.GPUUtil)
	log.Infof("  DRAM energy: %.2f J (utilization: %.2f%%)", t.data.DRAMEnergy, t.data.MemoryUtil)

	// 资源使用统计
	log.Infof("Resource Usage Statistics:")
	log.Infof("  CPU time: %.2f seconds", t.data.CPUTime)
	log.Infof("  Memory usage: %d bytes", t.data.MemoryUsage)
	log.Infof("  Memory utilization: %.2f%%", t.data.MemoryUtil)
	log.Infof("  Disk read: %d bytes (%.2f B/s)", t.data.DiskReadBytes, t.data.DiskReadSpeed)
	log.Infof("  Disk write: %d bytes (%.2f B/s)", t.data.DiskWriteBytes, t.data.DiskWriteSpeed)
}

func (t *Task) calculateStats() {
	if len(t.data.NodeDataHistory) < 2 || len(t.data.TaskResourceUsage) < 2 {
		log.Errorf("insufficient data points for task: %s", t.Name)
		return
	}

	// 设置时间信息
	t.data.EndTime = time.Now()
	t.data.Duration = t.data.EndTime.Sub(t.data.StartTime)

	t.calculateResourceStats()
	t.calculateEnergyStats()
	t.logStats()
}
