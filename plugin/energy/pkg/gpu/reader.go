package gpu

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/types"
)

type GPUReader struct {
	hasGPU      bool
	nvmlInited  bool
	deviceCount int
	devices     []*deviceState
}

type deviceState struct {
	index     int
	lastPower float64
	lastTime  time.Time
}

func NewGPUReader() *GPUReader {
	reader := &GPUReader{}
	if err := reader.init(); err != nil {
		log.WithError(err).Info("\033[31m[GPU]\033[0m Init failed, will run in no GPU mode")
	}
	return reader
}

func (r *GPUReader) init() error {
	if err := nvmlInit(); err != nil {
		return fmt.Errorf("NVML init failed: %v", err)
	}
	r.nvmlInited = true

	count, err := nvmlDeviceGetCount()
	if err != nil {
		r.cleanup()
		return fmt.Errorf("failed to get GPU count: %v", err)
	}

	if count > 0 {
		r.hasGPU = true
		r.deviceCount = count
		r.initDeviceStates(count)
		log.Infof("\033[32m[GPU]\033[0m detected %d NVIDIA GPUs", count)
	}
	return nil
}

func (r *GPUReader) initDeviceStates(count int) {
	r.devices = make([]*deviceState, count)
	for i := 0; i < count; i++ {
		r.devices[i] = &deviceState{index: i}
	}
}

func (r *GPUReader) GetMetrics() (*types.GPUMetrics, error) {
	if !r.hasGPU || !r.nvmlInited {
		return &types.GPUMetrics{}, nil
	}

	metrics := &types.GPUMetrics{}
	activeDevices := 0

	for i := 0; i < r.deviceCount; i++ {
		deviceMetrics, err := r.GetDeviceMetrics(i)
		if err != nil {
			log.WithField("device", i).WithError(err).Error("\033[33m[GPU]\033[0m Failed to collect metrics")
			continue
		}

		metrics.Power += deviceMetrics.Power
		metrics.Energy += deviceMetrics.Energy
		metrics.Util += deviceMetrics.Util
		metrics.MemUtil += deviceMetrics.MemUtil
		metrics.Temp += deviceMetrics.Temp
		activeDevices++
	}

	if activeDevices > 0 {
		metrics.Util /= float64(activeDevices)
		metrics.MemUtil /= float64(activeDevices)
		metrics.Temp /= float64(activeDevices)
	}

	return metrics, nil
}

func (r *GPUReader) GetDeviceMetrics(index int) (*types.GPUMetrics, error) {
	metrics := &types.GPUMetrics{}

	device, err := nvmlDeviceGetHandleByIndex(index)
	if err != nil {
		return nil, fmt.Errorf("failed to get device handle: %v", err)
	}

	power, err := device.GetPowerUsage()
	if err != nil {
		return nil, fmt.Errorf("power metrics: %v", err)
	}
	metrics.Power = float64(power) / 1000.0 // mW -> W

	gpuUtil, memUtil, err := device.GetUtilization()
	if err != nil {
		return nil, fmt.Errorf("utilization metrics: %v", err)
	}
	metrics.Util = float64(gpuUtil)
	metrics.MemUtil = float64(memUtil)

	temp, err := device.GetTemperature()
	if err != nil {
		return nil, fmt.Errorf("temperature metrics: %v", err)
	}
	metrics.Temp = float64(temp)

	currentTime := time.Now()
	if state := r.devices[index]; !state.lastTime.IsZero() {
		duration := currentTime.Sub(state.lastTime).Seconds()
		averagePower := (metrics.Power + state.lastPower) / 2
		metrics.Energy = averagePower * duration
	}

	r.devices[index].lastPower = metrics.Power
	r.devices[index].lastTime = currentTime

	return metrics, nil
}

func (r *GPUReader) LogMetrics(metrics *types.GPUMetrics) {
	log.WithFields(log.Fields{
		"power":    fmt.Sprintf("%.2f W", metrics.Power),
		"energy":   fmt.Sprintf("%.2f J", metrics.Energy),
		"util":     fmt.Sprintf("%.2f%%", metrics.Util),
		"mem_util": fmt.Sprintf("%.2f%%", metrics.MemUtil),
		"temp":     fmt.Sprintf("%.1f°C", metrics.Temp),
	}).Info("\033[33m[GPU]\033[0m GPU Metrics")
}

func (r *GPUReader) Close() error {
	return r.cleanup()
}

func (r *GPUReader) cleanup() error {
	if r.nvmlInited {
		if err := nvmlShutdown(); err != nil {
			return fmt.Errorf("\033[31m[GPU]\033[0m failed to shutdown NVML: %v", err)
		}
		r.nvmlInited = false
	}
	return nil
}

func (r *GPUReader) GetDeviceCount() int {
	return r.deviceCount
}

func (r *GPUReader) HasGPU() bool {
	return r.hasGPU && r.nvmlInited
}

func (r *GPUReader) GetGpuStats(indices []int) types.GPUMetrics {
	if !r.hasGPU || !r.nvmlInited {
		return types.GPUMetrics{}
	}

	metrics := types.GPUMetrics{}
	activeDevices := 0

	for _, idx := range indices {
		if idx < 0 || idx >= r.deviceCount {
			log.Errorf("invalid device index: %d, total devices: %d", idx, r.deviceCount)
			continue
		}

		deviceMetrics, err := r.GetDeviceMetrics(idx)
		if err != nil {
			log.WithField("device", idx).WithError(err).Error("\033[33m[GPU]\033[0m Failed to collect metrics")
			continue
		}

		metrics.Power += deviceMetrics.Power
		metrics.Energy += deviceMetrics.Energy
		metrics.Util += deviceMetrics.Util
		metrics.MemUtil += deviceMetrics.MemUtil
		metrics.Temp += deviceMetrics.Temp
		activeDevices++
	}

	if activeDevices > 0 {
		metrics.Util /= float64(activeDevices)
		metrics.MemUtil /= float64(activeDevices)
		metrics.Temp /= float64(activeDevices)
	}

	return metrics
}
