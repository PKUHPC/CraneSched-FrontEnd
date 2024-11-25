package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"runtime"

	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/db"
	"CraneFrontEnd/plugin/energy/pkg/monitor"
)

// 初始化日志配置
func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
		ForceColors:     true,
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			filename := path.Base(f.File)
			return "", fmt.Sprintf("%s:%d", filename, f.Line)
		},
	})

	// 启用调用者信息
	log.SetReportCaller(true)

	log.SetLevel(log.DebugLevel)

	logDir := "/var/log/crane"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Warnf("\033[33m[EnergyPlugin]\033[0m Failed to create log directory: %v", err)
		return
	}

	logFile, err := os.OpenFile(path.Join(logDir, "energy.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err == nil {
		log.SetOutput(io.MultiWriter(os.Stdout, logFile))
		log.Info("\033[32m[EnergyPlugin]\033[0m Successfully set up logging to file")
	} else {
		log.Warnf("\033[33m[EnergyPlugin]\033[0m Failed to log to file: %v, using default stderr", err)
	}
}

var (
	globalMonitor *monitor.Monitor
)

var _ api.Plugin = EnergyPlugin{}

var PluginInstance = EnergyPlugin{}

type EnergyPlugin struct {
	config *config.Config
}

func (p EnergyPlugin) Init(meta api.PluginMeta) error {
	log.Info("\033[32m[EnergyPlugin]\033[0m Initializing plugin")

	cfg, err := config.LoadConfig(meta.Config)
	if err != nil {
		return fmt.Errorf("\033[31m[EnergyPlugin]\033[0m failed to load config: %w", err)
	}

	p.config = cfg
	config.PrintConfig(cfg)

	if err := p.ensureInitialized(); err != nil {
		return fmt.Errorf("\033[31m[EnergyPlugin]\033[0m failed to initialize resources: %w", err)
	}

	globalMonitor.NodeMonitor.Start()

	return nil
}

func (p EnergyPlugin) Name() string {
	return "energy"
}

func (p EnergyPlugin) Version() string {
	return "v1.0.0"
}

func (p EnergyPlugin) StartHook(ctx *api.PluginContext) {
	_, ok := ctx.Request().(*protos.StartHookRequest)
	if !ok {
		log.Error("\033[31m[EnergyPlugin]\033[0m Invalid request type, expected StartHookRequest")
		return
	}

	// taskID := req.TaskInfoList[0].TaskId
	// log.Infof("\033[32m[EnergyPlugin]\033[0m Task start hook triggered for task: %d", taskID)
}

func (p EnergyPlugin) EndHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.EndHookRequest)
	if !ok {
		log.Error("\033[31m[EnergyPlugin]\033[0m Invalid request type, expected EndHookRequest")
		return
	}
	for _, task := range req.TaskInfoList {
		taskID := task.TaskId
		taskName := fmt.Sprintf("Crane_Task_%d", taskID)

		log.Infof("\033[32m[EnergyPlugin]\033[0m Stopping task monitor for task: %s", taskName)

		globalMonitor.TaskMonitor.StopTask(taskName)
	}
}

func (p EnergyPlugin) JobMonitorHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.JobMonitorHookRequest)
	if !ok {
		log.Error("\033[31m[EnergyPlugin]\033[0m Invalid request type, expected JobMonitorHookRequest")
		return
	}

	log.Infof("\033[32m[EnergyPlugin]\033[0m Starting task monitor for task: %d, cgroup: %s",
		req.TaskId, req.Cgroup)

	globalMonitor.TaskMonitor.Start(req.TaskId, req.Cgroup)
}

func (p EnergyPlugin) ensureInitialized() error {
	if db.GetInstance() == nil {
		err := db.InitDB(p.config)
		if err != nil {
			return fmt.Errorf("\033[31m[EnergyPlugin]\033[0m failed to create database: %w", err)
		}
	}

	if globalMonitor == nil {
		globalMonitor = monitor.NewMonitor(p.config.Monitor)
	}

	return nil
}

func (p EnergyPlugin) Close() {
	log.Info("\033[32m[EnergyPlugin]\033[0m Closing plugin")

	if globalMonitor != nil {
		globalMonitor.Close()
		globalMonitor = nil
	}

	if db.GetInstance() != nil {
		if err := db.GetInstance().Close(); err != nil {
			log.Errorf("\033[31m[EnergyPlugin]\033[0m Error closing database: %v", err)
		}
	}

	log.Info("\033[32m[EnergyPlugin]\033[0m Plugin closed")
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
