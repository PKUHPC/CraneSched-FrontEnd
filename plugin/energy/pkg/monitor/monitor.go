package monitor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/cgroup"
	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/db"
	"CraneFrontEnd/plugin/energy/pkg/gpu"
	"CraneFrontEnd/plugin/energy/pkg/rapl"
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type NodeDataSubscriber struct {
	Ch        chan *types.NodeData
	StartTime time.Time
}

type SystemMonitor struct {
	config       config.MonitorConfig
	db           db.DBInterface
	samplePeriod time.Duration

	raplReader   *rapl.RAPLReader
	gpuReader    *gpu.GPUReader
	cgroupReader cgroup.CgroupReader

	stopCh chan struct{}

	taskCancels map[string]context.CancelFunc
	taskMu      sync.RWMutex

	// 节点向每个任务发送数据的广播器
	subscribers sync.Map
}

func NewSystemMonitor(config config.MonitorConfig, db db.DBInterface) (*SystemMonitor, error) {
	duration, err := time.ParseDuration(config.SamplePeriod)
	if err != nil {
		return nil, fmt.Errorf("invalid sample period: %v", err)
	}

	sm := &SystemMonitor{
		config:       config,
		samplePeriod: duration,
		db:           db,
		stopCh:       make(chan struct{}),
		taskCancels:  make(map[string]context.CancelFunc),
	}

	raplReader, err := rapl.NewRAPLReader(config, db)
	if err != nil {
		return nil, err
	}
	sm.raplReader = raplReader

	gpuReader, err := gpu.NewGPUReader()
	if err != nil {
		log.Errorf("GPU reader initialization failed: %v", err)
	}
	sm.gpuReader = gpuReader

	cgroupReader := cgroup.NewV1Reader(cgroup.Config{
		CgroupBasePath: config.CgroupBasePath,
	})
	sm.cgroupReader = cgroupReader

	return sm, nil
}

func (r *SystemMonitor) StartNodeMonitor() error {
	go r.collectNodeEnergy()

	return nil
}

func (r *SystemMonitor) StartTaskMonitor(taskID uint32, taskName string) error {
	log.Infof("Starting task monitor for task: %d, cgroup path: %s", taskID, taskName)

	cpuCgroupPath := filepath.Join(r.config.CgroupBasePath, "cpu", taskName)
	if _, err := os.Stat(cpuCgroupPath); err != nil {
		return fmt.Errorf("cgroup path not found: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	r.taskMu.Lock()
	r.taskCancels[taskName] = cancel
	r.taskMu.Unlock()

	subscriber := &NodeDataSubscriber{
		Ch:        make(chan *types.NodeData, 10),
		StartTime: time.Now(),
	}
	r.subscribers.Store(taskName, subscriber)

	taskData := &types.TaskData{
		TaskName:          taskName,
		StartTime:         subscriber.StartTime,
		NodeDataHistory:   make([]*types.NodeData, 0, 100),
		TaskResourceUsage: make([]types.TaskResourceUsage, 0, 100),
	}

	// go r.watchCgroup(ctx, taskName)

	go r.monitorTask(ctx, taskData, subscriber.Ch)

	return nil
}

func (r *SystemMonitor) StopTaskMonitor(taskName string) {
	r.taskMu.Lock()
	if cancel, exists := r.taskCancels[taskName]; exists {
		cancel()
		delete(r.taskCancels, taskName)
	}
	r.taskMu.Unlock()
}

// func (r *SystemMonitor) watchCgroup(ctx context.Context, taskName string) {
// 	watcher, err := fsnotify.NewWatcher()
// 	if err != nil {
// 		log.Errorf("Failed to create file watcher: %v", err)
// 		return
// 	}
// 	defer watcher.Close()

// 	cpuCgroupPath := filepath.Join(r.config.CgroupBasePath, "cpu", taskName)
// 	if err := watcher.Add(cpuCgroupPath); err != nil {
// 		log.Errorf("Failed to add CPU cgroup directory to watcher: %v", err)
// 		return
// 	}

// 	for {
// 		select {
// 		case event, ok := <-watcher.Events:
// 			if !ok {
// 				return
// 			}
// 			if event.Op&fsnotify.Remove != 0 {
// 				r.StopTaskMonitor(taskName)
// 				return
// 			}
// 		case err, ok := <-watcher.Errors:
// 			if !ok {
// 				return
// 			}
// 			log.Printf("cgroup watcher error: %v", err)
// 		case <-ctx.Done():
// 			return
// 		case <-r.stopCh:
// 			return
// 		}
// 	}
// }

func (r *SystemMonitor) monitorTask(ctx context.Context, taskData *types.TaskData, nodeDataChan chan *types.NodeData) {
	ticker := time.NewTicker(r.samplePeriod)
	defer ticker.Stop()
	defer r.subscribers.Delete(taskData.TaskName)

	usageChan := make(chan types.TaskResourceUsage, 10)
	defer close(usageChan)

	go func() {
		var lastUsage *types.TaskResourceUsage
		for {
			select {
			case <-ticker.C:
				usage := r.cgroupReader.GetTaskResourceUsage(taskData.TaskName, lastUsage)
				select {
				case usageChan <- usage:
					lastUsage = &usage
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case data := <-nodeDataChan:
			taskData.NodeDataHistory = append(taskData.NodeDataHistory, data)
		case usage := <-usageChan:
			taskData.TaskResourceUsage = append(taskData.TaskResourceUsage, usage)
		case <-ctx.Done():
			if err := r.calculateTaskStats(taskData); err != nil {
				log.Errorf("Error calculating task stats: %v", err)
			}
			if err := r.db.SaveTaskEnergy(taskData); err != nil {
				log.Errorf("Error saving task energy data: %v", err)
			}
			return
		}
	}
}

func (r *SystemMonitor) calculateTaskStats(taskData *types.TaskData) error {
	if len(taskData.NodeDataHistory) < 2 || len(taskData.TaskResourceUsage) < 2 {
		return fmt.Errorf("size: %d of node data history or %d of task resource usage is insufficient for task: %s", len(taskData.NodeDataHistory), len(taskData.TaskResourceUsage), taskData.TaskName)
	}

	startEnergy := taskData.NodeDataHistory[0]
	endEnergy := taskData.NodeDataHistory[len(taskData.NodeDataHistory)-1]
	startUsage := taskData.TaskResourceUsage[0]
	endUsage := taskData.TaskResourceUsage[len(taskData.TaskResourceUsage)-1]

	taskData.EndTime = time.Now()
	taskData.Duration = taskData.EndTime.Sub(taskData.StartTime)

	// 计算CPU总时间(纳秒转秒)
	taskData.CPUTime = float64(endUsage.CPUStats.CurrentUsage-startUsage.CPUStats.CurrentUsage) / 1e9

	cpuEnergyDiff := (endEnergy.Core - startEnergy.Core) +
		(endEnergy.Uncore - startEnergy.Uncore)
	dramEnergyDiff := endEnergy.DRAM - startEnergy.DRAM
	gpuEnergyDiff := endEnergy.GPU - startEnergy.GPU

	// 有待考虑：这里计算的 GPU 使用率是基于整机 GPU 的使用率，不能用于计算任务的 GPU 能耗
	var totalGPUUtil, totalCPUUtil, totalMemoryUtil float64
	for _, data := range taskData.NodeDataHistory {
		totalGPUUtil += data.GPUUtil
	}
	taskData.GPUUtilization = totalGPUUtil / float64(len(taskData.NodeDataHistory))

	for _, data := range taskData.TaskResourceUsage {
		totalCPUUtil += data.CPUStats.AverageUtilization
		totalMemoryUtil += data.MemoryStats.Utilization
	}
	taskData.CPUUtilization = totalCPUUtil / float64(len(taskData.NodeDataHistory))
	taskData.MemoryUtil = totalMemoryUtil / float64(len(taskData.NodeDataHistory))

	taskData.CPUEnergy = cpuEnergyDiff * (taskData.CPUUtilization / 100.0)
	taskData.DRAMEnergy = dramEnergyDiff * (taskData.MemoryUtil / 100.0)
	taskData.GPUEnergy = gpuEnergyDiff * (taskData.GPUUtilization / 100.0)

	taskData.TotalEnergy = taskData.CPUEnergy + taskData.DRAMEnergy + taskData.GPUEnergy
	taskData.AveragePower = taskData.TotalEnergy / taskData.Duration.Seconds()

	log.Infof("energy stats:")
	log.Infof("  CPU energy diff: %.2f J", cpuEnergyDiff)
	log.Infof("  DRAM energy diff: %.2f J", dramEnergyDiff)
	log.Infof("  GPU energy diff: %.2f J", gpuEnergyDiff)
	log.Infof("  Task name: %s", taskData.TaskName)
	log.Infof("  Node ID: %s", taskData.NodeID)
	log.Infof("  Cluster ID: %s", taskData.ClusterID)
	log.Infof("  Start time: %v", taskData.StartTime)
	log.Infof("  End time: %v", taskData.EndTime)
	log.Infof("  Duration: %.8f seconds", taskData.Duration.Seconds())
	log.Infof("  CPU time: %.8f seconds", taskData.CPUTime)
	log.Infof("  Total energy: %.2f J", taskData.TotalEnergy)
	log.Infof("  Average power: %.2f W", taskData.AveragePower)
	log.Infof("  CPU energy: %.2f J (utilization: %.2f%%)", taskData.CPUEnergy, taskData.CPUUtilization)
	log.Infof("  GPU energy: %.2f J (utilization: %.2f%%)", taskData.GPUEnergy, taskData.GPUUtilization)
	log.Infof("  DRAM energy: %.2f J (utilization: %.2f%%)", taskData.DRAMEnergy, taskData.MemoryUtil)
	log.Infof("  Memory usage: %d B", taskData.MemoryUsage)
	log.Infof("  Memory utilization: %.2f%%", taskData.MemoryUtil)
	log.Infof("  Disk read bytes: %d B", taskData.DiskReadBytes)
	log.Infof("  Disk write bytes: %d B", taskData.DiskWriteBytes)
	log.Infof("  Disk read speed: %.2f B/s", taskData.DiskReadSpeed)
	log.Infof("  Disk write speed: %.2f B/s", taskData.DiskWriteSpeed)

	return nil
}

func (r *SystemMonitor) collectNodeEnergy() {
	ticker := time.NewTicker(r.samplePeriod)
	defer ticker.Stop()

	metricsCollector := func() (*types.NodeData, error) {
		data, err := r.raplReader.GetMetrics()
		if err != nil {
			return nil, fmt.Errorf("RAPL metrics: %v", err)
		}

		if r.gpuReader != nil {
			if gpuData, err := r.gpuReader.GetMetrics(); err == nil && gpuData != nil {
				data.GPU = gpuData.Energy
				data.GPUPower = gpuData.Power
				data.GPUUtil = gpuData.Util
				data.GPUMemUtil = gpuData.MemUtil
				data.GPUTemp = gpuData.Temp
			}
		}
		return data, nil
	}

	for {
		select {
		case <-ticker.C:
			if data, err := metricsCollector(); err == nil {
				r.logMetrics(data)

				// 这里需要优化，如果数据量很大，会导致数据库写入很慢
				if err := r.db.SaveNodeEnergy(data); err != nil {
					log.Errorf("Error saving node energy data: %v", err)
				}

				r.subscribers.Range(func(key, value interface{}) bool {
					if sub, ok := value.(*NodeDataSubscriber); ok {
						// 只发送每个任务开始后的数据
						if data.Timestamp.After(sub.StartTime) {
							select {
							case sub.Ch <- data:
							default:
								log.Infof("Warning: task %v channel full, skipping data", key)
							}
						}
					}
					return true
				})
			}
		case <-r.stopCh:
			return
		}
	}
}

func (r *SystemMonitor) logMetrics(data *types.NodeData) {
	log.Infof("\033[32m[Node]\033[0m Node metrics collected:")
	log.Infof("  Time: %v", data.Timestamp)
	log.Infof("  RAPL - Package: %.2f J, Core: %.2f J, Uncore: %.2f J, DRAM: %.2f J, GT: %.2f J",
		data.Package, data.Core, data.Uncore, data.DRAM, data.GT)
	log.Infof("  GPU - Energy: %.2f J, Power: %.2f W, Util: %.2f%%, MemUtil: %.2f%%, Temp: %.2f℃",
		data.GPU, data.GPUPower, data.GPUUtil, data.GPUMemUtil, data.GPUTemp)
	log.Infof("  System - CPU: %.2f%%, Memory: %.2f%%", data.CPUUtil, data.MemoryUtil)
	log.Infof("  IO - Disk: %v MB/s, Network: %v MB/s", data.DiskIO, data.NetworkIO)
	log.Infof("  Temperature: %.2f℃, Frequencies: %.2f MHz", data.Temperature, data.Frequencies)
}

func (sm *SystemMonitor) Close() error {
	sm.taskMu.Lock()
	for _, cancel := range sm.taskCancels {
		cancel()
	}
	sm.taskMu.Unlock()

	sm.subscribers.Range(func(key, value interface{}) bool {
		if ch, ok := value.(chan *types.NodeData); ok {
			close(ch)
		}
		sm.subscribers.Delete(key)
		return true
	})

	close(sm.stopCh)

	if err := sm.raplReader.Close(); err != nil {
		log.Infof("error closing RAPL reader: %v", err)
	}

	if sm.gpuReader != nil {
		if err := sm.gpuReader.Close(); err != nil {
			log.Infof("error closing GPU reader: %v", err)
		}
	}

	return nil
}
