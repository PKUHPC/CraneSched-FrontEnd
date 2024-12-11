package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	nested "github.com/antonfisher/nested-logrus-formatter"
	logrus "github.com/sirupsen/logrus"

	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/db"
	"CraneFrontEnd/plugin/energy/pkg/monitor"
)

var log = logrus.WithField("component", "EnergyPlugin")

func init() {
	logrus.SetFormatter(&nested.Formatter{
		HideKeys:        true,
		TimestampFormat: "2006-01-02 15:04:05",
		ShowFullLevel:   true,
		NoColors:        false,
		NoFieldsColors:  false,
		NoFieldsSpace:   true,
		FieldsOrder:     []string{"caller", "component"},

		CustomCallerFormatter: func(f *runtime.Frame) string {
			filename := path.Base(f.File)
			return fmt.Sprintf(" [%s:%d]", filename, f.Line)
		},
	})

	logrus.SetReportCaller(true)
	logrus.SetLevel(logrus.DebugLevel)
}

var _ api.Plugin = EnergyPlugin{}

var PluginInstance = EnergyPlugin{}

var gpuConfigs = map[string]struct {
	pattern    *regexp.Regexp
	validRange func(int) bool
}{
	"nvidia": {
		pattern:    regexp.MustCompile(`/dev/nvidia(\d+)`),
		validRange: func(num int) bool { return num >= 0 && num < 128 },
	},
}

type GlobalMonitor struct {
	config  *config.Config
	monitor *monitor.Monitor
}

var globalMonitor GlobalMonitor

type EnergyPlugin struct{}

func (p EnergyPlugin) Name() string {
	return "energy"
}

func (p EnergyPlugin) Version() string {
	return "1.0.0"
}

func (p EnergyPlugin) Load(meta api.PluginMeta) error {
	log.Info("Initializing energy plugin")

	cfg, err := config.LoadConfig(meta.Config)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if err := setupLogging(cfg.Monitor.LogPath); err != nil {
		log.Warnf("Failed to setup logging: %v, using default stderr", err)
	}

	globalMonitor.config = cfg
	config.PrintConfig(cfg)

	if err := p.ensureInitialized(); err != nil {
		return fmt.Errorf("failed to initialize resources: %w", err)
	}

	globalMonitor.monitor.NodeMonitor.Start()

	return nil
}

func (p EnergyPlugin) Unload(meta api.PluginMeta) error {
	log.Info("Unloading energy plugin")

	if globalMonitor.monitor != nil {
		globalMonitor.monitor.Close()
		globalMonitor.monitor = nil
	}

	if db.GetInstance() != nil {
		if err := db.GetInstance().Close(); err != nil {
			return fmt.Errorf("error closing database: %v", err)
		}
	}

	globalMonitor = GlobalMonitor{}

	log.Info("energy plugin gracefully unloaded")
	return nil
}

func (p EnergyPlugin) StartHook(ctx *api.PluginContext) {}

func (p EnergyPlugin) EndHook(ctx *api.PluginContext) {}

func (p EnergyPlugin) CreateCgroupHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.CreateCgroupHookRequest)
	if !ok {
		log.Error("Invalid request type, expected CreateCgroupHookRequest")
		return
	}

	log.Infof("CreateCgroupHook received for cgroup: %s", req.Cgroup)

	boundGPUs := getBoundGPUs(req.Resource, globalMonitor.config.Monitor.GPUType)
	globalMonitor.monitor.TaskMonitor.Start(req.TaskId, req.Cgroup, boundGPUs)
}

func (p EnergyPlugin) DestroyCgroupHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.DestroyCgroupHookRequest)
	if !ok {
		log.Error("Invalid request type, expected DestroyCgroupHookRequest")
		return
	}

	log.Infof("DestroyCgroupHook received for cgroup: %s", req.Cgroup)
	globalMonitor.monitor.TaskMonitor.Stop(req.TaskId)
}

func (p EnergyPlugin) ensureInitialized() error {
	if db.GetInstance() == nil {
		err := db.InitDB(globalMonitor.config)
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
	}

	if globalMonitor.monitor == nil {
		globalMonitor.monitor = monitor.NewMonitor(globalMonitor.config.Monitor)
	}

	return nil
}

func setupLogging(logPath string) error {
	logDir := path.Dir(logPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	logFile, err := os.OpenFile(logPath,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	logrus.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.Infof("Successfully set up logging to file %s", logPath)
	return nil
}

func getBoundGPUs(res *protos.DedicatedResourceInNode, gpuType string) []int {
	boundGPUs := make([]int, 0)

	gpuConfig, exists := gpuConfigs[gpuType]
	if !exists {
		log.Errorf("Unsupported GPU type: %s", gpuType)
		return boundGPUs
	}

	for deviceName, typeSlotMap := range res.GetNameTypeMap() {
		if !strings.Contains(strings.ToLower(deviceName), "gpu") {
			continue
		}

		for typeName, slots := range typeSlotMap.TypeSlotsMap {
			log.Infof("Device type: %s", typeName)

			for _, slot := range slots.Slots {
				matches := gpuConfig.pattern.FindStringSubmatch(slot)
				if len(matches) != 2 {
					log.Errorf("Invalid %s GPU device path format: %s", gpuType, slot)
					continue
				}

				deviceNum, err := strconv.Atoi(matches[1])
				if err != nil {
					log.Errorf("Failed to parse GPU number: %v", err)
					continue
				}

				if gpuConfig.validRange(deviceNum) {
					log.Infof("Bound %s GPU device number: %d", gpuType, deviceNum)
					boundGPUs = append(boundGPUs, deviceNum)
				} else {
					log.Warnf("Invalid %s GPU device number: %d", gpuType, deviceNum)
				}
			}
		}
	}

	return boundGPUs
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
