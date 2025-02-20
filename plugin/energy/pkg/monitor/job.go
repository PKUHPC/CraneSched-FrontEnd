package monitor

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	"CraneFrontEnd/plugin/energy/pkg/cgroup"
	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/db"
	"CraneFrontEnd/plugin/energy/pkg/gpu"
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type JobMonitor struct {
	samplePeriod time.Duration
	config       *config.MonitorConfig

	jobs  map[uint32]*Job
	mutex sync.RWMutex
}

func (m *JobMonitor) Start(jobID uint32, cgroupName string, resourceRequest ResourceRequest) {
	log.Infof("Starting job monitor for job: %d, cgroup name: %s", jobID, cgroupName)

	if !m.config.Enabled.Job {
		log.Warnf("Job monitor is not enabled, skipping job: %d, %s", jobID, cgroupName)
		return
	}

	// If neither RAPL nor IPMI is enabled, the job energy cannot be calculated, skip this job
	if !m.config.Enabled.RAPL && !m.config.Enabled.IPMI {
		log.Warnf("No energy metrics enabled, skipping job: %d, %s", jobID, cgroupName)
		return
	}

	job, err := NewJob(jobID, cgroupName, m.samplePeriod, m.config, resourceRequest)
	if err != nil {
		log.Errorf("Failed to create job monitor: %v", err)
		return
	}

	m.mutex.Lock()
	m.jobs[jobID] = job
	m.mutex.Unlock()

	job.StartMonitor()
}

func (m *JobMonitor) Stop(jobID uint32) {
	m.mutex.Lock()
	if job, exists := m.jobs[jobID]; exists {
		job.StopMonitor()
		delete(m.jobs, jobID)
	}
	m.mutex.Unlock()
}

func (m *JobMonitor) Close() {
	m.mutex.Lock()
	for _, job := range m.jobs {
		job.StopMonitor()
	}
	m.mutex.Unlock()
}

type Job struct {
	jobID        uint32
	cgroupName   string
	samplePeriod time.Duration
	config       *config.MonitorConfig

	resourceRequest ResourceRequest

	data        *types.JobData
	sampleCount int

	cgroupReader cgroup.CgroupReader
	gpuReader    *gpu.Reader

	ctx        context.Context
	cancel     context.CancelFunc
	subscriber *NodeDataSubscriber
}

type ResourceRequest struct {
	ReqCPU    float64
	ReqMemory uint64
	ReqGPUs   []int
}

func getNodeID() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

func NewJob(jobID uint32, cgroupName string, samplePeriod time.Duration, config *config.MonitorConfig, resourceRequest ResourceRequest) (*Job, error) {
	cgroupReader, err := cgroup.NewCgroupReader(cgroup.V1, cgroupName)
	if err != nil {
		return nil, fmt.Errorf("failed to create cgroup reader: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	startTime := time.Now()

	return &Job{
		jobID:           jobID,
		cgroupName:      cgroupName,
		samplePeriod:    samplePeriod,
		config:          config,
		resourceRequest: resourceRequest,
		ctx:             ctx,
		cancel:          cancel,
		subscriber: &NodeDataSubscriber{
			Ch:        make(chan *types.NodeData, 10),
			StartTime: startTime,
		},
		data: &types.JobData{
			JobID:     jobID,
			NodeID:    getNodeID(),
			StartTime: startTime,
		},
		cgroupReader: cgroupReader,
		gpuReader:    gpu.NewGPUReader(config.GPUType),
	}, nil
}

func (t *Job) StartMonitor() {
	GetSubscriberManagerInstance().AddSubscriber(t.jobID, t.subscriber)

	go t.monitorResourceUsage()
	go t.collectData()
}

func (t *Job) StopMonitor() {
	log.Infof("Stopping job monitor for job: %d", t.jobID)

	t.cancel()
	GetSubscriberManagerInstance().DeleteSubscriber(t.jobID)

	if err := t.gpuReader.Close(); err != nil {
		log.Errorf("Failed to close GPU reader: %v", err)
	}
}

func (t *Job) monitorResourceUsage() {
	ticker := time.NewTicker(t.samplePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			jobStats, err := t.cgroupReader.GetCgroupStats()
			if err != nil {
				log.Infof("Job %d monitor stopping, error: %v", t.jobID, err)
				go t.StopMonitor()
				return
			}
			metrics := t.gpuReader.GetBoundGpuMetrics(t.resourceRequest.ReqGPUs)
			t.updateJobStats(&jobStats, metrics)
		case <-t.ctx.Done():
			return
		}
	}
}

func (t *Job) updateJobStats(stats *types.CgroupStats, metrics *types.GPUMetrics) {
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

func (t *Job) collectData() {
	for {
		select {
		case data := <-t.subscriber.Ch:
			t.updateEnergy(data)
		case <-t.ctx.Done():
			t.calculateStats()
			if err := db.GetInstance().SaveJobEnergy(t.data); err != nil {
				log.Errorf("Error saving job energy data: %v", err)
			}
			return
		}
	}
}

func (t *Job) updateEnergy(nodeData *types.NodeData) {
	// If IPMI is enabled, use IPMI energy data preferentially
	if t.config.Enabled.IPMI {
		t.data.CPUEnergy += nodeData.IPMI.CPUEnergy
	} else if t.config.Enabled.RAPL {
		t.data.CPUEnergy += nodeData.RAPL.Package
	}
}

func (t *Job) calculateStats() {
	if t.sampleCount < 2 {
		log.Errorf("Insufficient samples for statistics")
		return
	}

	t.data.EndTime = time.Now()
	t.data.Duration = t.data.EndTime.Sub(t.data.StartTime)
	sampleCount := float64(t.sampleCount)

	t.calculateAverages(sampleCount)
	t.calculateEnergy()

	t.logJobStats()
}

func (t *Job) calculateAverages(sampleCount float64) {
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

func (t *Job) calculateEnergy() {
	t.data.CPUEnergy = t.data.CPUEnergy * (t.data.CgroupStats.CPUStats.Utilization / 100.0)
	t.data.TotalEnergy = t.data.CPUEnergy + t.data.GPUEnergy
	if duration := t.data.Duration.Seconds(); duration > 0 {
		t.data.AveragePower = t.data.TotalEnergy / duration
	}
}

func (t *Job) logJobStats() {
	log.Infof("Job Statistics for %d:", t.jobID)
	log.Infof("  Job ID: %d", t.jobID)
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

func (m *JobMonitor) GetJobMetrics() *types.JobMetrics {
	metrics := &types.JobMetrics{}
	totalCPURequest := 0.0
	totalMemoryRequest := 0.0
	totalRuntime := 0.0

	cpuCores := float64(runtime.NumCPU())
	var memInfo syscall.Sysinfo_t
	syscall.Sysinfo(&memInfo)
	totalMemoryBytes := float64(memInfo.Totalram)

	m.mutex.RLock()
	metrics.JobCount = len(m.jobs)

	for _, job := range m.jobs {
		totalCPURequest += job.resourceRequest.ReqCPU
		totalMemoryRequest += float64(job.resourceRequest.ReqMemory)
		totalRuntime += time.Since(job.data.StartTime).Minutes()
	}
	m.mutex.RUnlock()

	if metrics.JobCount > 0 {
		metrics.ReqCPURate = totalCPURequest / cpuCores
		metrics.AvgReqCPUPerJob = totalCPURequest / float64(metrics.JobCount)
		metrics.ReqMemoryRate = totalMemoryRequest / totalMemoryBytes
		metrics.AvgReqMemoryGBPerJob = (totalMemoryRequest / float64(metrics.JobCount)) / (1024 * 1024 * 1024) // 转换为GB
		metrics.AvgJobRuntime = totalRuntime / float64(metrics.JobCount)
	}

	log.Infof("Job metrics: %+v", metrics)
	log.Infof("ReqCPURate: %f", metrics.ReqCPURate)
	log.Infof("AvgReqCPUPerJob: %f", metrics.AvgReqCPUPerJob)
	log.Infof("ReqMemoryRate: %f", metrics.ReqMemoryRate)
	log.Infof("AvgReqMemoryGBPerJob: %f", metrics.AvgReqMemoryGBPerJob)
	log.Infof("AvgJobRuntime: %f", metrics.AvgJobRuntime)

	return metrics
}
