package gpu

/*
#cgo LDFLAGS: -lnvidia-ml
#include <nvml.h>
*/
import "C"
import (
	"fmt"
)

type NVMLDevice struct {
	handle C.nvmlDevice_t
}

func nvmlInit() error {
	if result := C.nvmlInit(); result != C.NVML_SUCCESS {
		return fmt.Errorf("NVML init failed: %v", result)
	}
	return nil
}

func nvmlShutdown() error {
	if result := C.nvmlShutdown(); result != C.NVML_SUCCESS {
		return fmt.Errorf("NVML shutdown failed: %v", result)
	}
	return nil
}

func nvmlDeviceGetCount() (int, error) {
	var count C.uint
	if result := C.nvmlDeviceGetCount(&count); result != C.NVML_SUCCESS {
		return 0, fmt.Errorf("failed to get device count: %v", result)
	}
	return int(count), nil
}

func nvmlDeviceGetHandleByIndex(index int) (*NVMLDevice, error) {
	var handle C.nvmlDevice_t
	if result := C.nvmlDeviceGetHandleByIndex(C.uint(index), &handle); result != C.NVML_SUCCESS {
		return nil, fmt.Errorf("failed to get device handle: %v", result)
	}
	return &NVMLDevice{handle: handle}, nil
}

func (d *NVMLDevice) GetPowerUsage() (uint, error) {
	var power C.uint
	if result := C.nvmlDeviceGetPowerUsage(d.handle, &power); result != C.NVML_SUCCESS {
		return 0, fmt.Errorf("failed to get power usage: %v", result)
	}
	return uint(power), nil
}

func (d *NVMLDevice) GetTemperature() (uint, error) {
	var temp C.uint
	if result := C.nvmlDeviceGetTemperature(d.handle, C.NVML_TEMPERATURE_GPU, &temp); result != C.NVML_SUCCESS {
		return 0, fmt.Errorf("failed to get temperature: %v", result)
	}
	return uint(temp), nil
}

func (d *NVMLDevice) GetUtilization() (uint, uint, error) {
	var utilization C.nvmlUtilization_t
	if result := C.nvmlDeviceGetUtilizationRates(d.handle, &utilization); result != C.NVML_SUCCESS {
		return 0, 0, fmt.Errorf("failed to get utilization: %v", result)
	}
	return uint(utilization.gpu), uint(utilization.memory), nil
}
