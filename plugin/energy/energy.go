package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"runtime"

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

var globalMonitor *monitor.Monitor

var _ api.Plugin = EnergyPlugin{}

var PluginInstance = EnergyPlugin{}

type EnergyPlugin struct {
	config *config.Config
}

func (p EnergyPlugin) Init(meta api.PluginMeta) error {
	log.Info("Initializing plugin")

	cfg, err := config.LoadConfig(meta.Config)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	if err := setupLogging(cfg.Monitor.LogPath); err != nil {
		log.Warnf("Failed to setup logging: %v, using default stderr", err)
	}

	p.config = cfg
	config.PrintConfig(cfg)

	if err := p.ensureInitialized(); err != nil {
		return fmt.Errorf("failed to initialize resources: %w", err)
	}

	globalMonitor.NodeMonitor.Start()

	return nil
}

func (p EnergyPlugin) Name() string {
	return "energy"
}

func (p EnergyPlugin) Version() string {
	return "1.0.0"
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

	boundDevices := []string{}
	boundDevices = append(boundDevices, req.Devices...)
	globalMonitor.TaskMonitor.Start(req.TaskId, req.Cgroup, boundDevices)
}

func (p EnergyPlugin) DestroyCgroupHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.DestroyCgroupHookRequest)
	if !ok {
		log.Error("Invalid request type, expected DestroyCgroupHookRequest")
		return
	}

	log.Infof("DestroyCgroupHook received for cgroup: %s", req.Cgroup)
	globalMonitor.TaskMonitor.Stop(req.TaskId)
}

func (p EnergyPlugin) ensureInitialized() error {
	if db.GetInstance() == nil {
		err := db.InitDB(p.config)
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
	}

	if globalMonitor == nil {
		globalMonitor = monitor.NewMonitor(p.config.Monitor)
	}

	return nil
}

func (p EnergyPlugin) Close() {
	log.Info("EnergyPlugin closing")

	if globalMonitor != nil {
		globalMonitor.Close()
		globalMonitor = nil
	}

	if db.GetInstance() != nil {
		if err := db.GetInstance().Close(); err != nil {
			log.Errorf("Error closing database: %v", err)
		}
	}

	log.Info("EnergyPlugin closed")
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

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
