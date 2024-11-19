package rapl

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/db"
	"CraneFrontEnd/plugin/energy/pkg/sysstat"
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type RAPLDomain struct {
	Name           string
	Path           string
	MaxEnergyRange float64
	TDP            float64
	Enabled        bool
}

type RAPLReader struct {
	domains  map[string]*RAPLDomain
	stopCh   chan struct{}
	db       db.DBInterface
	sysStats *sysstat.SystemStats
	config   config.MonitorConfig
}

func NewRAPLReader(config config.MonitorConfig, db db.DBInterface) (*RAPLReader, error) {

	reader := &RAPLReader{
		domains:  make(map[string]*RAPLDomain),
		sysStats: sysstat.NewSystemStats(),
		db:       db,
		stopCh:   make(chan struct{}),
		config:   config,
	}

	if err := reader.discoverDomains(); err != nil {
		return nil, fmt.Errorf("failed to discover RAPL domains: %v", err)
	}

	return reader, nil
}

func (r *RAPLReader) discoverDomains() error {
	basePath := "/sys/class/powercap"
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "intel-rapl:") {
			domain := &RAPLDomain{
				Name: entry.Name(),
				Path: filepath.Join(basePath, entry.Name()),
			}

			// 读取域的最大能耗范围，目前尚未使用到
			maxEnergyRange, err := r.readMaxEnergyRange(domain.Path)
			if err == nil {
				domain.MaxEnergyRange = maxEnergyRange
			}

			// TDP值，目前尚未使用到
			tdp, err := r.readTDP(domain.Path)
			if err == nil {
				domain.TDP = tdp
			}

			domain.Enabled = true
			r.domains[domain.Name] = domain
		}
	}

	log.Info("\033[32m[RAPL]\033[0m Discovered RAPL domains:")
	for name, domain := range r.domains {
		log.Infof("  %s: %s", name, domain.Path)
	}

	return nil
}

func (r *RAPLReader) GetMetrics() (*types.NodeData, error) {
	result := &types.NodeData{
		Timestamp:   time.Now(),
		Temperature: 0,
		Frequencies: 0,
	}

	// Collect energy data
	for name, domain := range r.domains {
		if !domain.Enabled {
			continue
		}

		energy, err := r.readDomainEnergy(domain)
		if err != nil {
			log.Errorf("Error reading energy for domain %s: %v", name, err)
			continue
		}

		parts := strings.Split(name, ":")
		if len(parts) < 2 {
			log.Errorf("Invalid domain name format: %s", name)
			continue
		}

		log.Infof("\033[32m[RAPL]\033[0m Reading energy for domain %s: %.2f J", name, energy)

		if len(parts) == 2 {
			result.Package += energy
			log.Infof("\033[32m[RAPL]\033[0m Added to Package total: %.2f J", result.Package)
		} else if len(parts) == 3 {
			switch parts[2] {
			case "0":
				result.Core += energy
				log.Infof("\033[32m[RAPL]\033[0m Added to Core total: %.2f J", result.Core)
			case "1":
				result.Uncore += energy
				log.Infof("\033[32m[RAPL]\033[0m Added to Uncore total: %.2f J", result.Uncore)
			case "2":
				result.DRAM += energy
				log.Infof("\033[32m[RAPL]\033[0m Added to DRAM total: %.2f J", result.DRAM)
			case "3":
				result.GT += energy
				log.Infof("\033[32m[RAPL]\033[0m Added to GT total: %.2f J", result.GT)
			default:
				log.Infof("\033[31m[RAPL]\033[0m Unknown subdomain: %s", parts[2])
			}
		}
	}

	// Concurrent collection of system metrics
	var wg sync.WaitGroup
	var errCPU, errMem, errDisk, errNet error

	// CPU utilization
	wg.Add(1)
	go func() {
		defer wg.Done()
		if data, err := os.ReadFile("/proc/stat"); err == nil {
			result.CPUUtil = r.sysStats.GetCPUUtil(string(data))
		} else {
			errCPU = err
		}
	}()

	// Memory utilization
	wg.Add(1)
	go func() {
		defer wg.Done()
		var si syscall.Sysinfo_t
		if err := syscall.Sysinfo(&si); err == nil {
			usedMem := float64(si.Totalram-si.Freeram) * float64(si.Unit)
			totalMem := float64(si.Totalram) * float64(si.Unit)
			result.MemoryUtil = (usedMem / totalMem) * 100
		} else {
			errMem = err
		}
	}()

	// Disk I/O
	wg.Add(1)
	go func() {
		defer wg.Done()
		if data, err := os.ReadFile("/proc/diskstats"); err == nil {
			result.DiskIO = r.sysStats.GetDiskIO(string(data))
		} else {
			errDisk = err
		}
	}()

	// Network I/O
	wg.Add(1)
	go func() {
		defer wg.Done()
		if data, err := os.ReadFile("/proc/net/dev"); err == nil {
			result.NetworkIO = r.sysStats.GetNetworkIO(string(data))
		} else {
			errNet = err
		}
	}()

	// Optional temperature and frequency collection
	if r.config.Switches.Temperature {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if temp, err := r.sysStats.GetCPUTemperature(); err == nil {
				result.Temperature = temp
			}
		}()
	}

	if r.config.Switches.Frequency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if freq, err := r.sysStats.GetCPUFrequency(); err == nil {
				result.Frequencies = freq
			}
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Check for errors
	if errCPU != nil || errMem != nil || errDisk != nil || errNet != nil {
		log.Errorf("Errors collecting system metrics: CPU=%v, Mem=%v, Disk=%v, Net=%v",
			errCPU, errMem, errDisk, errNet)
	}

	return result, nil
}

func (r *RAPLReader) readDomainEnergy(domain *RAPLDomain) (float64, error) {
	data, err := os.ReadFile(filepath.Join(domain.Path, "energy_uj"))
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
	return value / 1e6, err // 转换为焦耳
}

func (r *RAPLReader) readMaxEnergyRange(path string) (float64, error) {
	data, err := os.ReadFile(filepath.Join(path, "max_energy_range_uj"))
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
	return value / 1e6, err // 转换为焦耳
}

// readTDP 读取热设计功率
func (r *RAPLReader) readTDP(path string) (float64, error) {
	data, err := os.ReadFile(filepath.Join(path, "constraint_0_power_limit_uw"))
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
	return value / 1e6, err // 转换为瓦特
}

func (r *RAPLReader) Close() error {
	close(r.stopCh)
	return nil
}
