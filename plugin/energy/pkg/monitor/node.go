package monitor

import (
	"time"

	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/db"
	"CraneFrontEnd/plugin/energy/pkg/gpu"
	"CraneFrontEnd/plugin/energy/pkg/ipmi"
	"CraneFrontEnd/plugin/energy/pkg/rapl"
	"CraneFrontEnd/plugin/energy/pkg/sysload"
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type NodeMonitor struct {
	samplePeriod time.Duration
	config       *config.MonitorConfig

	raplReader    *rapl.RAPLReader
	ipmiReader    *ipmi.IPMIReader
	gpuReader     *gpu.GPUReader
	sysLoadReader *sysload.SystemLoadReader

	stopCh chan struct{}
}

func (r *NodeMonitor) Start() {
	go r.collectNodeEnergy()
}

func (r *NodeMonitor) collectNodeEnergy() {
	ticker := time.NewTicker(r.samplePeriod)
	defer ticker.Stop()

	metricsCollector := func() (*types.NodeData, error) {
		data := &types.NodeData{
			Timestamp: time.Now(),
		}

		if r.raplReader != nil {
			if raplData, err := r.raplReader.GetMetrics(); err == nil && raplData != nil {
				data.RAPL = *raplData
			}
		}

		if r.ipmiReader != nil {
			if ipmiData, err := r.ipmiReader.GetMetrics(); err == nil && ipmiData != nil {
				data.IPMI = *ipmiData
			}
		}

		if r.gpuReader != nil {
			if gpuData, err := r.gpuReader.GetMetrics(); err == nil && gpuData != nil {
				data.GPU = *gpuData
			}
		}

		if r.sysLoadReader != nil {
			if sysData, err := r.sysLoadReader.GetMetrics(); err == nil && sysData != nil {
				data.SystemLoad = *sysData
			}
		}

		// r.raplReader.LogMetrics(&data.RAPL)
		// r.ipmiReader.LogMetrics(&data.IPMI)
		// r.gpuReader.LogMetrics(&data.GPU)
		// r.sysLoadReader.LogMetrics(&data.SystemLoad)

		return data, nil
	}

	for {
		select {
		case <-ticker.C:
			if data, err := metricsCollector(); err == nil {

				if err := db.GetInstance().SaveNodeEnergy(data); err != nil {
					log.Errorf("Error saving node energy data: %v", err)
				}

				r.broadcastNodeData(data)
			}
		case <-r.stopCh:
			return
		}
	}
}

func (r *NodeMonitor) broadcastNodeData(data *types.NodeData) {
	subscriberManager := GetSubscriberManagerInstance()
	subscriberManager.subscribers.Range(func(key, value interface{}) bool {
		if sub, ok := value.(*NodeDataSubscriber); ok {
			if data.Timestamp.After(sub.StartTime) {
				select {
				case sub.Ch <- data:
				default:
					log.Infof("Warning: task %v channel full, skipping data", key)
				}
			}
		}
		return true
	})
}

func (r *NodeMonitor) Close() {
	if r.raplReader != nil {
		if err := r.raplReader.Close(); err != nil {
			log.Infof("error closing RAPL reader: %v", err)
		}
	}

	if r.gpuReader != nil {
		if err := r.gpuReader.Close(); err != nil {
			log.Infof("error closing GPU reader: %v", err)
		}
	}

	if r.sysLoadReader != nil {
		r.sysLoadReader.Close()
	}

	close(r.stopCh)
}
