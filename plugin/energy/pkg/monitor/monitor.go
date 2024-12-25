package monitor

import (
	"sync"
	"time"

	logrus "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/gpu"
	"CraneFrontEnd/plugin/energy/pkg/ipmi"
	"CraneFrontEnd/plugin/energy/pkg/rapl"
	"CraneFrontEnd/plugin/energy/pkg/sysload"
)

var log = logrus.WithField("component", "Monitor")

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
	var gpuReader *gpu.Reader
	var sysLoadReader *sysload.SystemLoadReader

	if config.Enabled.RAPL {
		raplReader = rapl.NewRAPLReader()
	}
	if config.Enabled.IPMI {
		ipmiReader = ipmi.NewIPMIReader()
	}
	if config.Enabled.GPU {
		gpuReader = gpu.NewGPUReader(config.GPUType)
	}
	if config.Enabled.System {
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
			tasks:        make(map[uint32]*Task),
			mutex:        sync.RWMutex{},
		},
	}
}

func (sm *Monitor) Close() {
	sm.NodeMonitor.Close()
	sm.TaskMonitor.Close()
}
