package gpu

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

type Metrics struct {
	Energy  float64
	Power   float64
	Util    float64
	MemUtil float64
	Temp    float64
}

type GPUReader struct {
	HasGPU        bool
	NvmlInited    bool
	DeviceCount   int
	LastPowerTime time.Time
	LastPower     float64
}

func NewGPUReader() (*GPUReader, error) {
	reader := &GPUReader{}

	if err := nvmlInit(); err != nil {
		log.Errorf("NVML init failed: %v, will run in no GPU mode", err)
		return reader, nil
	}

	reader.NvmlInited = true

	count, err := nvmlDeviceGetCount()
	if err != nil {
		log.Errorf("failed to get GPU count: %v", err)
		reader.cleanup()
		return reader, nil
	}

	if count > 0 {
		reader.HasGPU = true
		reader.DeviceCount = count
		log.Infof("detected %d NVIDIA GPUs", count)
	}

	return reader, nil
}

func (r *GPUReader) GetMetrics() (*Metrics, error) {
	metrics := &Metrics{}

	if !r.HasGPU || !r.NvmlInited {
		return metrics, nil
	}

	currentTime := time.Now()
	var activeGPUs int

	for i := 0; i < r.DeviceCount; i++ {
		device, err := nvmlDeviceGetHandleByIndex(i)
		if err != nil {
			log.Errorf("skipping GPU %d: %v", i, err)
			continue
		}

		if power, err := device.GetPowerUsage(); err == nil {
			metrics.Power += float64(power) / 1000.0 // mW -> W
		} else {
			log.Errorf("failed to get power usage for GPU %d: %v", i, err)
		}

		if gpuUtil, memUtil, err := device.GetUtilization(); err == nil {
			metrics.Util += float64(gpuUtil)
			metrics.MemUtil += float64(memUtil)
			activeGPUs++
		} else {
			log.Errorf("failed to get utilization for GPU %d: %v", i, err)
		}

		if temp, err := device.GetTemperature(); err == nil {
			metrics.Temp += float64(temp)
		} else {
			log.Errorf("failed to get temperature for GPU %d: %v", i, err)
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

	return metrics, nil
}

func (r *GPUReader) Close() error {
	return r.cleanup()
}

func (r *GPUReader) cleanup() error {
	if r.NvmlInited {
		if err := nvmlShutdown(); err != nil {
			return fmt.Errorf("failed to shutdown NVML: %v", err)
		}
		r.NvmlInited = false
	}
	return nil
}
