package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sync"

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
		log.Warnf("\033[33m[EnergyPlugin] Failed to create log directory: %v\033[0m", err)
		return
	}

	logFile, err := os.OpenFile(path.Join(logDir, "energy.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err == nil {
		log.SetOutput(io.MultiWriter(os.Stdout, logFile))
		log.Info("\033[32m[EnergyPlugin] Successfully set up logging to file\033[0m")
	} else {
		log.Warnf("\033[33m[EnergyPlugin] Failed to log to file: %v, using default stderr\033[0m", err)
	}
}

var (
	globalMonitor *monitor.SystemMonitor
	globalDB      db.DBInterface
	globalMu      sync.Mutex
)

// 确保实现了插件接口
var _ api.Plugin = EnergyPlugin{}

// 导出插件实例
var PluginInstance = EnergyPlugin{}

type EnergyPlugin struct {
	config *config.Config
}

func (p EnergyPlugin) Init(meta api.PluginMeta) error {
	log.Info("\033[32m[EnergyPlugin] Initializing plugin\033[0m")

	cfg, err := config.LoadConfig(meta.Config)
	if err != nil {
		return fmt.Errorf("\033[31m[EnergyPlugin] failed to load config: %w\033[0m", err)
	}

	p.config = cfg
	config.PrintConfig(cfg)

	if err := p.ensureInitialized(); err != nil {
		return fmt.Errorf("\033[31m[EnergyPlugin] failed to initialize resources: %w\033[0m", err)
	}

	// 启动节点监控
	if err := globalMonitor.StartNodeMonitor(); err != nil {
		return fmt.Errorf("\033[31m[EnergyPlugin] failed to start node monitor: %w\033[0m", err)
	}

	log.Info("\033[32m[EnergyPlugin] Plugin initialized successfully\033[0m")
	return nil
}

func (p EnergyPlugin) Name() string {
	return "energy"
}

func (p EnergyPlugin) Version() string {
	return "v1.0.0"
}

func (p EnergyPlugin) StartHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.StartHookRequest)
	if !ok {
		log.Error("\033[31m[EnergyPlugin] Invalid request type, expected StartHookRequest\033[0m")
		return
	}

	taskID := req.TaskInfoList[0].TaskId
	log.Infof("\033[32m[EnergyPlugin] Task start hook triggered for task: %d\033[0m", taskID)
}

func (p EnergyPlugin) EndHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.EndHookRequest)
	if !ok {
		log.Error("\033[31m[EnergyPlugin] Invalid request type, expected EndHookRequest\033[0m")
		return
	}

	taskID := req.TaskInfoList[0].TaskId
	taskName := fmt.Sprintf("Crane_Task_%d", taskID)

	log.Infof("\033[32m[EnergyPlugin] Stopping task monitor for task: %s\033[0m", taskName)

	globalMonitor.StopTaskMonitor(taskName)
}

func (p EnergyPlugin) JobMonitorHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.JobMonitorHookRequest)
	if !ok {
		log.Error("\033[31m[EnergyPlugin] Invalid request type, expected JobMonitorHookRequest\033[0m")
		return
	}

	log.Infof("\033[32m[EnergyPlugin] Starting task monitor for task: %d, cgroup: %s\033[0m",
		req.TaskId, req.Cgroup)

	// 启动任务监控
	if err := globalMonitor.StartTaskMonitor(req.TaskId, req.Cgroup); err != nil {
		log.Errorf("\033[31m[EnergyPlugin] Failed to start task monitor: %v\033[0m", err)
	}
}

func (p EnergyPlugin) ensureInitialized() error {
	globalMu.Lock()
	defer globalMu.Unlock()

	if globalDB == nil {
		database, err := db.NewDatabase(p.config.DB)
		if err != nil {
			return fmt.Errorf("\033[31m[EnergyPlugin] failed to create database: %w\033[0m", err)
		}
		globalDB = database
	}

	if globalMonitor == nil {
		systemMonitor, err := monitor.NewSystemMonitor(p.config.Monitor, globalDB)
		if err != nil {
			globalDB.Close()
			globalDB = nil
			return fmt.Errorf("\033[31m[EnergyPlugin] failed to create system monitor: %w\033[0m", err)
		}
		globalMonitor = systemMonitor
	}

	return nil
}

func (p EnergyPlugin) Close() {
	log.Info("\033[32m[EnergyPlugin] Closing plugin\033[0m")

	globalMu.Lock()
	defer globalMu.Unlock()

	// 关闭监控器
	if globalMonitor != nil {
		if err := globalMonitor.Close(); err != nil {
			log.Errorf("\033[31m[EnergyPlugin] Error closing monitor: %v\033[0m", err)
		}
		globalMonitor = nil
	}

	// 关闭数据库
	if globalDB != nil {
		if err := globalDB.Close(); err != nil {
			log.Errorf("\033[31m[EnergyPlugin] Error closing database: %v\033[0m", err)
		}
		globalDB = nil
	}

	log.Info("\033[32m[EnergyPlugin] Plugin closed\033[0m")
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
