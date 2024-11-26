//go:build !with_nvml

package gpu

// If NVML is not enabled, use this stub
type NVMLDevice struct{}

func nvmlInit() error {
	return nil
}

func nvmlShutdown() error {
	return nil
}

func nvmlDeviceGetCount() (int, error) {
	return 0, nil
}

func nvmlDeviceGetHandleByIndex(index int) (*NVMLDevice, error) {
	return nil, nil
}

func (d *NVMLDevice) GetPowerUsage() (uint, error) {
	return 0, nil
}

func (d *NVMLDevice) GetTemperature() (uint, error) {
	return 0, nil
}

func (d *NVMLDevice) GetUtilization() (uint, uint, error) {
	return 0, 0, nil
}

func (d *NVMLDevice) GetUUID() (string, error) {
	return "", nil
}

func (d *NVMLDevice) GetPciInfo() (string, error) {
	return "", nil
}
