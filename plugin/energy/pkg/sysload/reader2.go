package sysload

// import (
// 	"fmt"
// 	"log"
// 	"os"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"syscall"
// 	"time"

// 	"github.com/shirou/gopsutil/cpu"
// 	"github.com/shirou/gopsutil/host"

// 	"CraneFrontEnd/plugin/energy/pkg/types"
// )

// type CPUStats struct {
// 	User    uint64
// 	Nice    uint64
// 	System  uint64
// 	Idle    uint64
// 	Iowait  uint64
// 	Irq     uint64
// 	Softirq uint64
// 	Steal   uint64
// }

// type IOStats struct {
// 	ReadBytes  uint64
// 	WriteBytes uint64
// 	Timestamp  time.Time
// }

// type SystemLoadReader struct {
// 	mu          sync.Mutex
// 	lastCPU     *CPUStats
// 	lastDisk    *IOStats
// 	lastNetwork *IOStats
// }

// func NewSystemLoadReader() *SystemLoadReader {
// 	return &SystemLoadReader{}
// }

// func (s *SystemLoadReader) Close() {}

// func (s *SystemLoadReader) GetMetrics() (*types.SystemLoadMetrics, error) {
// 	result := &types.SystemLoadMetrics{}

// 	var wg sync.WaitGroup
// 	var errCPU, errMem, errDisk, errNet, errTemp, errFreq error

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		if data, err := os.ReadFile("/proc/stat"); err == nil {
// 			result.CPUUtil = s.GetCPUUtil(string(data))
// 		} else {
// 			errCPU = err
// 		}
// 	}()

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		var si syscall.Sysinfo_t
// 		if err := syscall.Sysinfo(&si); err == nil {
// 			usedMem := float64(si.Totalram-si.Freeram) * float64(si.Unit)
// 			totalMem := float64(si.Totalram) * float64(si.Unit)
// 			result.MemoryUtil = (usedMem / totalMem) * 100
// 		} else {
// 			errMem = err
// 		}
// 	}()

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		if data, err := os.ReadFile("/proc/diskstats"); err == nil {
// 			result.DiskIO = s.GetDiskIO(string(data))
// 		} else {
// 			errDisk = err
// 		}
// 	}()

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		if data, err := os.ReadFile("/proc/net/dev"); err == nil {
// 			result.NetworkIO = s.GetNetworkIO(string(data))
// 		} else {
// 			errNet = err
// 		}
// 	}()

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		if temp, err := s.GetCPUTemperature(); err == nil {
// 			result.Temperature = temp
// 		} else {
// 			errTemp = err
// 		}
// 	}()

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		if freq, err := s.GetCPUFrequency(); err == nil {
// 			result.Frequencies = freq
// 		} else {
// 			errFreq = err
// 		}
// 	}()

// 	wg.Wait()

// 	if errCPU != nil || errMem != nil || errDisk != nil || errNet != nil || errTemp != nil || errFreq != nil {
// 		return nil, fmt.Errorf("errors collecting system metrics: CPU=%v, Mem=%v, Disk=%v, Net=%v",
// 			errCPU, errMem, errDisk, errNet)
// 	}

// 	return result, nil
// }

// func (s *SystemLoadReader) LogMetrics(metrics *types.SystemLoadMetrics) {
// 	log.Printf("\033[35m[System Load]\033[0m CPU Util: %.2f%%", metrics.CPUUtil)
// 	log.Printf("\033[35m[System Load]\033[0m Memory Util: %.2f%% (Used: %.2f GB, Total: %.2f GB)",
// 		metrics.MemoryUtil, metrics.MemoryUsed, metrics.MemoryTotal)
// 	log.Printf("\033[35m[System Load]\033[0m Disk IO: %.2f MB/s", metrics.DiskIO)
// 	log.Printf("\033[35m[System Load]\033[0m Network IO: %.2f MB/s", metrics.NetworkIO)
// }

// func (s *SystemLoadReader) GetCPUUtil(statData string) float64 {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	lines := strings.Split(statData, "\n")
// 	for _, line := range lines {
// 		if strings.HasPrefix(line, "cpu ") {
// 			fields := strings.Fields(line)
// 			if len(fields) < 8 {
// 				continue
// 			}

// 			current := &CPUStats{}
// 			// 用户态
// 			current.User, _ = strconv.ParseUint(fields[1], 10, 64)
// 			// 低优先级用户态
// 			current.Nice, _ = strconv.ParseUint(fields[2], 10, 64)
// 			// 内核态
// 			current.System, _ = strconv.ParseUint(fields[3], 10, 64)
// 			// 空闲时间
// 			current.Idle, _ = strconv.ParseUint(fields[4], 10, 64)
// 			// 等待I/O完成的时间
// 			current.Iowait, _ = strconv.ParseUint(fields[5], 10, 64)
// 			// 硬中断时间
// 			current.Irq, _ = strconv.ParseUint(fields[6], 10, 64)
// 			// 软中断时间
// 			current.Softirq, _ = strconv.ParseUint(fields[7], 10, 64)
// 			// 虚拟机偷取的时间
// 			if len(fields) > 8 {
// 				current.Steal, _ = strconv.ParseUint(fields[8], 10, 64)
// 			}

// 			if s.lastCPU == nil {
// 				s.lastCPU = current
// 				return 0
// 			}

// 			// 计算总的CPU时间
// 			prevIdle := s.lastCPU.Idle + s.lastCPU.Iowait
// 			idle := current.Idle + current.Iowait

// 			prevNonIdle := s.lastCPU.User + s.lastCPU.Nice + s.lastCPU.System +
// 				s.lastCPU.Irq + s.lastCPU.Softirq + s.lastCPU.Steal
// 			nonIdle := current.User + current.Nice + current.System +
// 				current.Irq + current.Softirq + current.Steal

// 			prevTotal := prevIdle + prevNonIdle
// 			total := idle + nonIdle

// 			totalDiff := total - prevTotal
// 			idleDiff := idle - prevIdle

// 			s.lastCPU = current

// 			if totalDiff == 0 {
// 				return 0
// 			}

// 			return (float64(totalDiff-idleDiff) / float64(totalDiff)) * 100
// 		}
// 	}
// 	return 0
// }

// // GetDiskIO 计算磁盘IO速率(MB/s)
// func (s *SystemLoadReader) GetDiskIO(diskData string) float64 {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	var currentReadBytes, currentWriteBytes uint64
// 	lines := strings.Split(diskData, "\n")

// 	// 数据格式:
// 	// 1       0 ram0 0 0 0 0 0 0 0 0 0 0 0
// 	// 1       1 ram1 0 0 0 0 0 0 0 0 0 0 0
// 	// 8       0 sda 1234567 2345 12345678 1234 2345678 3456 23456789 2345 0 2345 3456
// 	// 259     0 nvme0n1 31234 0 2434898 7605 5567 0 44536 1798 0 7454 9403
// 	for _, line := range lines {
// 		fields := strings.Fields(line)
// 		if len(fields) < 14 {
// 			continue
// 		}

// 		// 字段编号   含义
// 		// 1         主设备号
// 		// 2         次设备号
// 		// 3         设备名
// 		// 4         读完成次数
// 		// 5         合并读完成次数
// 		// 6         读扇区数           # 这里使用的是字段6 (fields[5])
// 		// 7         读花费的毫秒数
// 		// 8         写完成次数
// 		// 9         合并写完成次数
// 		// 10        写扇区数           # 这里使用的是字段10 (fields[9])
// 		// 11        写花费的毫秒数
// 		// 12        正在处理的输入/输出请求数
// 		// 13        输入/输出花费的毫秒数
// 		// 14        输入/输出花费的加权毫秒数

// 		// 跳过分区，只统计磁盘
// 		if strings.HasPrefix(fields[2], "sd") || strings.HasPrefix(fields[2], "nvme") {
// 			// 读扇区数
// 			readSectors, _ := strconv.ParseUint(fields[5], 10, 64)
// 			// 写扇区数
// 			writeSectors, _ := strconv.ParseUint(fields[9], 10, 64)
// 			// 读字节数
// 			currentReadBytes += readSectors * 512
// 			// 写字节数
// 			currentWriteBytes += writeSectors * 512
// 		}
// 	}

// 	current := &IOStats{
// 		ReadBytes:  currentReadBytes,
// 		WriteBytes: currentWriteBytes,
// 		Timestamp:  time.Now(),
// 	}

// 	if s.lastDisk == nil {
// 		s.lastDisk = current
// 		return 0
// 	}

// 	duration := current.Timestamp.Sub(s.lastDisk.Timestamp).Seconds()
// 	if duration == 0 {
// 		return 0
// 	}

// 	readSpeed := float64(current.ReadBytes-s.lastDisk.ReadBytes) / (1024 * 1024 * duration)
// 	writeSpeed := float64(current.WriteBytes-s.lastDisk.WriteBytes) / (1024 * 1024 * duration)

// 	s.lastDisk = current
// 	return readSpeed + writeSpeed
// }

// // GetNetworkIO 计算网络IO速率(MB/s)
// // 数据格式:
// // Inter-|   Receive                                                |  Transmit
// // face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
// //
// //	lo:    1234      12    0    0    0     0          0         0     1234      12    0    0    0     0       0          0
// //	eth0: 1250000    1000    0    0    0     0          0         0   850000     900    0    0    0     0       0          0
// //
// // wlan0:  500000     500    0    0    0     0          0         0   250000     300    0    0    0     0       0          0
// func (s *SystemLoadReader) GetNetworkIO(netData string) float64 {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	var currentReadBytes, currentWriteBytes uint64
// 	lines := strings.Split(netData, "\n")

// 	for _, line := range lines {
// 		fields := strings.Fields(line)
// 		if len(fields) < 10 {
// 			continue
// 		}

// 		// 跳过回环接口
// 		iface := strings.TrimRight(fields[0], ":")
// 		if iface == "lo" {
// 			continue
// 		}

// 		// 读字节数
// 		rxBytes, _ := strconv.ParseUint(fields[1], 10, 64)
// 		// 写字节数
// 		txBytes, _ := strconv.ParseUint(fields[9], 10, 64)

// 		// 累加所有接口流量
// 		currentReadBytes += rxBytes
// 		currentWriteBytes += txBytes
// 	}

// 	current := &IOStats{
// 		ReadBytes:  currentReadBytes,
// 		WriteBytes: currentWriteBytes,
// 		Timestamp:  time.Now(),
// 	}

// 	if s.lastNetwork == nil {
// 		s.lastNetwork = current
// 		return 0
// 	}

// 	duration := current.Timestamp.Sub(s.lastNetwork.Timestamp).Seconds()
// 	if duration == 0 {
// 		return 0
// 	}

// 	rxSpeed := float64(current.ReadBytes-s.lastNetwork.ReadBytes) / (1024 * 1024 * duration)
// 	txSpeed := float64(current.WriteBytes-s.lastNetwork.WriteBytes) / (1024 * 1024 * duration)

// 	s.lastNetwork = current
// 	return rxSpeed + txSpeed
// }

// // ReadCPUTemperature 读取CPU平均温度（摄氏度）
// func (s *SystemLoadReader) GetCPUTemperature() (float64, error) {
// 	temps, err := host.SensorsTemperatures()
// 	if err != nil {
// 		return 0, err
// 	}

// 	var total float64
// 	count := 0

// 	for _, temp := range temps {
// 		// 只统计CPU相关的温度
// 		if isCPUTemp(temp.SensorKey) {
// 			total += temp.Temperature
// 			count++
// 		}
// 	}

// 	if count == 0 {
// 		return 0, fmt.Errorf("no CPU temperature sensors found")
// 	}

// 	return total / float64(count), nil
// }

// // isCPUTemp 判断是否是CPU温度传感器
// func isCPUTemp(sensorKey string) bool {
// 	cpuSensors := []string{"coretemp", "k10temp", "zenpower", "cpu_thermal"}
// 	for _, sensor := range cpuSensors {
// 		if strings.Contains(sensorKey, sensor) {
// 			return true
// 		}
// 	}
// 	return false
// }

// // ReadCPUFrequency 读取CPU平均频率（MHz）
// func (s *SystemLoadReader) GetCPUFrequency() (float64, error) {
// 	infos, err := cpu.Info()
// 	if err != nil {
// 		return 0, err
// 	}

// 	if len(infos) == 0 {
// 		return 0, fmt.Errorf("no CPU info found")
// 	}

// 	var total float64
// 	count := 0

// 	for _, info := range infos {
// 		total += info.Mhz
// 		count++
// 	}

// 	return total / float64(count), nil
// }
