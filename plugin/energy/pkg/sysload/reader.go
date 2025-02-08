package sysload

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	logrus "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/types"
)

var log = logrus.WithField("component", "SystemLoad")

type NetworkStats struct {
	TotalRx   uint64
	TotalTx   uint64
	Timestamp time.Time
}

type SystemLoadReader struct {
	mu             sync.Mutex
	lastDiskIO     *disk.IOCountersStat
	lastNetwork    *NetworkStats
	lastUpdateTime time.Time
}

func NewSystemLoadReader() *SystemLoadReader {
	return &SystemLoadReader{}
}

func (s *SystemLoadReader) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastDiskIO = nil
	s.lastNetwork = nil
	s.lastUpdateTime = time.Time{}
}

func (s *SystemLoadReader) GetCPUUtilization() (float64, error) {
	percents, err := cpu.Percent(0, false)
	if err != nil {
		return 0, fmt.Errorf("get CPU utilization: %v", err)
	}
	if len(percents) == 0 {
		return 0, fmt.Errorf("no CPU utilization data")
	}
	return percents[0], nil
}

func (s *SystemLoadReader) GetDiskIO() (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	diskIO, err := disk.IOCounters()
	if err != nil {
		return 0, fmt.Errorf("get disk IO: %v", err)
	}

	var totalRead, totalWrite uint64
	for _, io := range diskIO {
		totalRead += io.ReadBytes
		totalWrite += io.WriteBytes
	}

	current := &disk.IOCountersStat{
		ReadBytes:  totalRead,
		WriteBytes: totalWrite,
	}

	if s.lastDiskIO == nil || s.lastUpdateTime.IsZero() {
		s.lastDiskIO = current
		s.lastUpdateTime = time.Now()
		return 0, nil
	}

	duration := time.Since(s.lastUpdateTime).Seconds()
	if duration <= 0 {
		return 0, fmt.Errorf("invalid time duration")
	}

	readSpeed := float64(totalRead-s.lastDiskIO.ReadBytes) / (1024 * 1024 * duration)
	writeSpeed := float64(totalWrite-s.lastDiskIO.WriteBytes) / (1024 * 1024 * duration)

	s.lastDiskIO = current
	s.lastUpdateTime = time.Now()

	return readSpeed + writeSpeed, nil
}

func (s *SystemLoadReader) GetCPUTemperature() (float64, error) {
	temps, err := host.SensorsTemperatures()
	if err != nil {
		return 0, fmt.Errorf("get CPU temperature: %v", err)
	}

	var total float64
	count := 0
	for _, temp := range temps {
		if temp.Temperature > 0 && isCPUTemp(temp.SensorKey) {
			total += temp.Temperature
			count++
		}
	}

	if count == 0 {
		return 0, fmt.Errorf("no valid CPU temperature sensors found")
	}

	return total / float64(count), nil
}

func (s *SystemLoadReader) GetCPUFrequency() (float64, error) {
	freqs, err := cpu.Info()
	if err != nil {
		return 0, fmt.Errorf("get CPU frequency: %v", err)
	}

	if len(freqs) == 0 {
		return 0, fmt.Errorf("no CPU frequency data")
	}

	var total float64
	for _, freq := range freqs {
		total += freq.Mhz
	}

	return total / float64(len(freqs)), nil
}

func (s *SystemLoadReader) GetCPULoad() (float64, float64, float64, error) {
	loadInfo, err := load.Avg()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("get CPU load: %v", err)
	}
	return loadInfo.Load1, loadInfo.Load5, loadInfo.Load15, nil
}

func bytesToGB(bytes uint64) float64 {
	return float64(bytes) / (1024 * 1024 * 1024)
}

func (s *SystemLoadReader) GetMemoryDetails() (float64, float64, float64, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("get memory info: %v", err)
	}

	usedGB := bytesToGB(memInfo.Used)
	totalGB := bytesToGB(memInfo.Total)

	return memInfo.UsedPercent, usedGB, totalGB, nil
}

func (s *SystemLoadReader) GetDiskUtilization() (float64, error) {
	parts, err := disk.Partitions(false)
	if err != nil {
		return 0, fmt.Errorf("get disk partitions: %v", err)
	}

	var totalSize, usedSize uint64
	for _, part := range parts {
		usage, err := disk.Usage(part.Mountpoint)
		if err != nil {
			continue
		}
		totalSize += usage.Total
		usedSize += usage.Used
	}

	if totalSize == 0 {
		return 0, fmt.Errorf("no valid disk partitions found")
	}

	return float64(usedSize) / float64(totalSize) * 100, nil
}

func (s *SystemLoadReader) GetNetworkDetails() (float64, float64, float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	netIO, err := net.IOCounters(false)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("get network IO: %v", err)
	}

	var totalRx, totalTx uint64
	for _, io := range netIO {
		if io.Name != "lo" { // exclude local loop
			totalRx += io.BytesRecv
			totalTx += io.BytesSent
		}
	}

	current := &NetworkStats{
		TotalRx:   totalRx,
		TotalTx:   totalTx,
		Timestamp: time.Now(),
	}

	if s.lastNetwork == nil {
		s.lastNetwork = current
		return 0, 0, 0, nil
	}

	duration := current.Timestamp.Sub(s.lastNetwork.Timestamp).Seconds()
	if duration <= 0 {
		return 0, 0, 0, fmt.Errorf("invalid time duration")
	}

	rxSpeed := float64(totalRx-s.lastNetwork.TotalRx) / (1024 * 1024 * duration)
	txSpeed := float64(totalTx-s.lastNetwork.TotalTx) / (1024 * 1024 * duration)
	totalSpeed := rxSpeed + txSpeed

	if rxSpeed < 0 || txSpeed < 0 {
		log.Warnf("Negative network speed detected, resetting counters")
		s.lastNetwork = current
		return 0, 0, 0, nil
	}

	s.lastNetwork = current

	return totalSpeed, rxSpeed, txSpeed, nil
}

func (s *SystemLoadReader) GetMetrics() (*types.SystemLoadMetrics, error) {
	metrics := &types.SystemLoadMetrics{}
	var errs []error

	var wg sync.WaitGroup
	wg.Add(6)

	go func() {
		defer wg.Done()
		if util, err := s.GetCPUUtilization(); err == nil {
			metrics.CPUUtil = util
		} else {
			errs = append(errs, err)
		}

		if load1, load5, load15, err := s.GetCPULoad(); err == nil {
			metrics.CPULoad1 = load1
			metrics.CPULoad5 = load5
			metrics.CPULoad15 = load15
		} else {
			errs = append(errs, err)
		}
	}()

	go func() {
		defer wg.Done()
		if util, used, total, err := s.GetMemoryDetails(); err == nil {
			metrics.MemoryUtil = util
			metrics.MemoryUsed = used
			metrics.MemoryTotal = total
		} else {
			errs = append(errs, err)
		}
	}()

	go func() {
		defer wg.Done()
		if io, err := s.GetDiskIO(); err == nil {
			metrics.DiskIO = io
		} else {
			errs = append(errs, err)
		}
		if util, err := s.GetDiskUtilization(); err == nil {
			metrics.DiskUtil = util
		} else {
			errs = append(errs, err)
		}
	}()

	go func() {
		defer wg.Done()
		if total, rx, tx, err := s.GetNetworkDetails(); err == nil {
			metrics.NetworkIO = total
			metrics.NetworkRx = rx
			metrics.NetworkTx = tx
		} else {
			errs = append(errs, err)
		}
	}()

	go func() {
		defer wg.Done()
		if temp, err := s.GetCPUTemperature(); err == nil {
			metrics.CPUTemperature = temp
		} else {
			errs = append(errs, err)
		}
	}()

	go func() {
		defer wg.Done()
		if freq, err := s.GetCPUFrequency(); err == nil {
			metrics.Frequencies = freq
		} else {
			errs = append(errs, err)
		}
	}()

	wg.Wait()

	if len(errs) > 0 {
		return metrics, fmt.Errorf("errors collecting metrics: %v", errs)
	}

	return metrics, nil
}

func isCPUTemp(sensorKey string) bool {
	cpuSensors := []string{"coretemp", "k10temp", "zenpower", "cpu_thermal"}
	for _, sensor := range cpuSensors {
		if strings.Contains(strings.ToLower(sensorKey), sensor) {
			return true
		}
	}
	return false
}

func (s *SystemLoadReader) LogMetrics(metrics *types.SystemLoadMetrics) {
	log.Printf("System Load Metrics:")
	log.Printf("CPU: %.2f%% (Load: %.2f, %.2f, %.2f)",
		metrics.CPUUtil, metrics.CPULoad1, metrics.CPULoad5, metrics.CPULoad15)
	log.Printf("CPU Temperature: %.1f°C, Frequency: %.1f MHz",
		metrics.CPUTemperature, metrics.Frequencies)
	log.Printf("Memory: %.2f%% (Used: %.2f GB, Total: %.2f GB)",
		metrics.MemoryUtil, metrics.MemoryUsed, metrics.MemoryTotal)
	log.Printf("Disk: %.2f%% (IO: %.2f MB/s)",
		metrics.DiskUtil, metrics.DiskIO)
	log.Printf("Network: %.2f MB/s (Rx: %.2f MB/s, Tx: %.2f MB/s)",
		metrics.NetworkIO, metrics.NetworkRx, metrics.NetworkTx)
}
