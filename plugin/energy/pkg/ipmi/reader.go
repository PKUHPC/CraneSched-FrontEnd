package ipmi

import (
	"fmt"
	"sync"
	"time"

	logrus "github.com/sirupsen/logrus"
	"github.com/u-root/u-root/pkg/ipmi"

	"CraneFrontEnd/plugin/energy/pkg/types"
)

var log = logrus.WithField("component", "IPMI")

const (
	MetricsCacheDuration   = 500 * time.Millisecond
	CMD_GET_SENSOR_READING = 0x2D
	NETFN_SENSOR           = 0x04
)

type IPMIReader struct {
	ipmi    *ipmi.IPMI
	HasIPMI bool

	LastPowerTime  time.Time
	LastTotalPower float64
	LastCPUPower   float64
	metricsCache   *types.IPMIMetrics
	mutex          sync.Mutex
}

type SensorConfig struct {
	Name      string
	SensorNum uint8
	Scale     float64
	MaxValue  float64
}

var powerSensors = []SensorConfig{
	{
		Name:      "Total_Power",
		SensorNum: 0xdb,
		Scale:     8.0,
		MaxValue:  1000.0,
	},
	{
		Name:      "CPU_Power",
		SensorNum: 0xdc,
		Scale:     2.0,
		MaxValue:  500.0,
	},
	{
		Name:      "FAN_Power",
		SensorNum: 0xde,
		Scale:     1.0,
		MaxValue:  100.0,
	},
	{
		Name:      "HDD_Power",
		SensorNum: 0xdf,
		Scale:     1.0,
		MaxValue:  50.0,
	},
}

func NewIPMIReader() *IPMIReader {
	reader := &IPMIReader{}

	i, err := ipmi.Open(0)
	if err != nil {
		log.Warnf("Failed to open IPMI device: %v", err)
		return nil
	}

	reader.ipmi = i
	reader.HasIPMI = true

	return reader
}

func (r *IPMIReader) getPowerUsage(sensor SensorConfig) (float64, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if !r.HasIPMI {
		return 0, fmt.Errorf("IPMI not available")
	}

	cmd := []byte{NETFN_SENSOR, CMD_GET_SENSOR_READING, sensor.SensorNum}
	resp, err := r.ipmi.RawCmd(cmd)
	if err != nil {
		return 0, fmt.Errorf("failed to get sensor reading: %v", err)
	}

	if len(resp) < 4 {
		return 0, fmt.Errorf("invalid response length")
	}

	if resp[0] != 0x00 {
		return 0, fmt.Errorf("command failed with code: 0x%02x", resp[0])
	}

	if resp[2] != 0xC0 || resp[3] != 0xC0 {
		return 0, fmt.Errorf("invalid reading flags: %02x %02x", resp[2], resp[3])
	}

	rawReading := float64(resp[1])
	power := rawReading * sensor.Scale

	// verify if the reading is within the reasonable range
	if power < 0 || power > sensor.MaxValue {
		log.Warnf("%s reading out of range: %.2f W (raw: 0x%02x)",
			sensor.Name, power, uint8(rawReading))
		return 0, fmt.Errorf("reading out of range")
	}

	return power, nil
}

func (r *IPMIReader) GetMetrics() (*types.IPMIMetrics, error) {
	if !r.HasIPMI {
		return &types.IPMIMetrics{}, fmt.Errorf("IPMI not available")
	}

	if time.Since(r.LastPowerTime) < MetricsCacheDuration && r.metricsCache != nil {
		return r.metricsCache, nil
	}

	currentTime := time.Now()
	metrics := &types.IPMIMetrics{}

	for _, sensor := range powerSensors {
		if reading, err := r.getPowerUsage(sensor); err == nil {
			switch sensor.Name {
			case "Total_Power":
				metrics.Power = reading
			case "CPU_Power":
				metrics.CPUPower = reading
			case "FAN_Power":
				metrics.FanPower = reading
			case "HDD_Power":
				metrics.HDDPower = reading
			}
		} else {
			log.Warnf("Failed to get %s: %v", sensor.Name, err)
		}
	}

	if !r.LastPowerTime.IsZero() {
		duration := currentTime.Sub(r.LastPowerTime).Seconds()
		metrics.Energy = (metrics.Power + r.LastTotalPower) * duration / 2
		metrics.CPUEnergy = (metrics.CPUPower + r.LastCPUPower) * duration / 2
	} else {
		metrics.Energy = 0
	}

	r.LastPowerTime = currentTime
	r.LastTotalPower = metrics.Power
	r.LastCPUPower = metrics.CPUPower
	r.metricsCache = metrics

	return metrics, nil
}

func (r *IPMIReader) LogMetrics(metrics *types.IPMIMetrics) {
	log.Infof("IPMI Metrics:")
	log.Infof("Power: %.2f W, Energy: %.2f J", metrics.Power, metrics.Energy)
	log.Infof("CPU Power: %.2f W, CPU Energy: %.2f J", metrics.CPUPower, metrics.CPUEnergy)
	log.Infof("Fan Power: %.2f W, HDD Power: %.2f W", metrics.FanPower, metrics.HDDPower)
}

func (r *IPMIReader) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.ipmi != nil {
		return r.ipmi.Close()
	}
	return nil
}
