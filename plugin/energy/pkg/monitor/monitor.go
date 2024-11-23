package monitor

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/cgroup"
	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/gpu"
	"CraneFrontEnd/plugin/energy/pkg/ipmi"
	"CraneFrontEnd/plugin/energy/pkg/rapl"
	"CraneFrontEnd/plugin/energy/pkg/sysload"
)

type Monitor struct {
	NodeMonitor *NodeMonitor
	TaskMonitor *TaskMonitor
}

func NewMonitor(config config.MonitorConfig) *Monitor {
	duration, err := time.ParseDuration(config.SamplePeriod)
	if err != nil {
		log.Errorf("invalid sample period: %v", err)
		return nil
	}

	var raplReader *rapl.RAPLReader
	var ipmiReader *ipmi.IPMIReader
	var gpuReader *gpu.GPUReader
	var sysLoadReader *sysload.SystemLoadReader

	switches := config.Switches
	if switches.RAPL {
		raplReader = rapl.NewRAPLReader()
	}
	if switches.IPMI {
		ipmiReader = ipmi.NewIPMIReader()
	}
	if switches.GPU {
		gpuReader = gpu.NewGPUReader()
	}
	if switches.System {
		sysLoadReader = sysload.NewSystemLoadReader()
	}

	return &Monitor{
		NodeMonitor: &NodeMonitor{
			samplePeriod:  duration,
			config:        &config,
			raplReader:    raplReader,
			ipmiReader:    ipmiReader,
			gpuReader:     gpuReader,
			sysLoadReader: sysLoadReader,
			stopCh:        make(chan struct{}),
		},
		TaskMonitor: &TaskMonitor{
			samplePeriod: duration,
			config:       &config,
			cgroupReader: cgroup.NewV1Reader(),
			tasks:        make(map[string]*Task),
			taskMu:       sync.RWMutex{},
		},
	}
}

func (sm *Monitor) Close() {
	sm.NodeMonitor.Close()
	sm.TaskMonitor.Close()
}
