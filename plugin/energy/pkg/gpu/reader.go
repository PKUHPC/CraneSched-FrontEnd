package gpu

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/types"
)

type GPUReader struct {
	HasGPU        bool
	NvmlInited    bool
	DeviceCount   int
	LastPowerTime time.Time
	LastPower     float64
}

func NewGPUReader() *GPUReader {
	reader := &GPUReader{}

	if err := nvmlInit(); err != nil {
		log.Infof("\033[31m[GPU]\033[0m NVML init failed or no NVIDIA GPU present: %v, will run in no GPU mode", err)
		return reader
	}

	reader.NvmlInited = true

	count, err := nvmlDeviceGetCount()
	if err != nil {
		log.Infof("\033[31m[GPU]\033[0m failed to get GPU count: %v, will run in no GPU mode", err)
		reader.cleanup()
		return reader
	}

	if count > 0 {
		reader.HasGPU = true
		reader.DeviceCount = count
		log.Infof("\033[32m[GPU]\033[0m detected %d NVIDIA GPUs", count)
	} else {
		log.Info("\033[32m[GPU]\033[0m no NVIDIA GPUs detected")
	}

	return reader
}

func (r *GPUReader) GetMetrics() (*types.GPUMetrics, error) {
	metrics := &types.GPUMetrics{}

	if !r.HasGPU || !r.NvmlInited {
		return metrics, nil
	}

	currentTime := time.Now()
	var activeGPUs int

	for i := 0; i < r.DeviceCount; i++ {
		device, err := nvmlDeviceGetHandleByIndex(i)
		if err != nil {
			log.Errorf("\033[33m[GPU]\033[0m skipping GPU %d: %v", i, err)
			continue
		}

		if power, err := device.GetPowerUsage(); err == nil {
			metrics.Power += float64(power) / 1000.0 // mW -> W
		} else {
			log.Errorf("\033[33m[GPU]\033[0m failed to get power usage for GPU %d: %v", i, err)
		}

		if gpuUtil, memUtil, err := device.GetUtilization(); err == nil {
			metrics.Util += float64(gpuUtil)
			metrics.MemUtil += float64(memUtil)
			activeGPUs++
		} else {
			log.Errorf("\033[33m[GPU]\033[0m failed to get utilization for GPU %d: %v", i, err)
		}

		if temp, err := device.GetTemperature(); err == nil {
			metrics.Temp += float64(temp)
		} else {
			log.Errorf("\033[33m[GPU]\033[0m failed to get temperature for GPU %d: %v", i, err)
		}
	}

	if !r.LastPowerTime.IsZero() {
		duration := currentTime.Sub(r.LastPowerTime).Seconds()

		averagePower := (metrics.Power + r.LastPower) / 2
		metrics.Energy = averagePower * duration
	}

	r.LastPowerTime = currentTime
	r.LastPower = metrics.Power

	if activeGPUs > 0 {
		metrics.Util /= float64(activeGPUs)
		metrics.MemUtil /= float64(activeGPUs)

		metrics.Temp /= float64(activeGPUs)
	}

	// r.logMetrics(metrics)

	return metrics, nil
}

func (r *GPUReader) LogMetrics(metrics *types.GPUMetrics) {
	log.Printf("\033[33m[GPU]\033[0m GPU Metrics:")
	log.Printf("\033[33m[GPU]\033[0m Power: %.2f W, Energy: %.2f J", metrics.Power, metrics.Energy)
	log.Printf("\033[33m[GPU]\033[0m Util: %.2f%% (MemUtil: %.2f%%)", metrics.Util, metrics.MemUtil)
	log.Printf("\033[33m[GPU]\033[0m Temp: %.1f°C", metrics.Temp)
}

func (r *GPUReader) Close() error {
	return r.cleanup()
}

func (r *GPUReader) cleanup() error {
	if r.NvmlInited {
		if err := nvmlShutdown(); err != nil {
			return fmt.Errorf("\033[31m[GPU]\033[0m failed to shutdown NVML: %v", err)
		}
		r.NvmlInited = false
	}
	return nil
}
