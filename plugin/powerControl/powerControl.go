package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"

	log "github.com/sirupsen/logrus"
)

var _ api.Plugin = PowerControlPlugin{}

var (
	PluginInstance = PowerControlPlugin{}
	manager        *PowerManager
)

type PowerControlPlugin struct{}

func (p PowerControlPlugin) Name() string {
	return "PowerControl"
}

func (p PowerControlPlugin) Version() string {
	return "v0.0.1"
}

func setupLogging(logFilePath string) error {
	if logFilePath == "" {
		log.Warn("PowerControlLogFile not configured, logging to stdout only")
		return nil
	}

	logDir := filepath.Dir(logFilePath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %v", err)
	}

	file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}

	// Set log output to both file and stdout
	log.SetOutput(io.MultiWriter(os.Stdout, file))
	log.Infof("Log file configured at: %s", logFilePath)
	return nil
}

func (p PowerControlPlugin) Load(meta api.PluginMeta) error {
	log.Info("PowerControl plugin is loading...")

	config, err := LoadConfig(meta.Config)
	if err != nil {
		return fmt.Errorf("failed to load config: %v", err)
	}

	if err := setupLogging(config.PowerControl.PowerControlLogFile); err != nil {
		return fmt.Errorf("failed to setup logging: %v", err)
	}

	if err := StartPredictorService(config, meta.Config); err != nil {
		return fmt.Errorf("failed to start predictor service: %v", err)
	}

	manager = NewPowerManager(config)
	manager.StartAutoPowerManager()

	log.Info("PowerControl plugin loaded successfully")
	return nil
}

func StartPredictorService(config *Config, configPath string) error {
	log.Infof("Starting predictor service with config: %s", configPath)
	cmd := exec.Command("python3", config.PowerControl.PredictorScript, "--config", configPath)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start predictor service: %v", err)
	}

	time.Sleep(3 * time.Second)
	return nil
}

func (p PowerControlPlugin) Unload(meta api.PluginMeta) error {
	log.Info("PowerControl plugin is unloading...")
	manager.StopAutoPowerManager()
	return nil
}

func (p PowerControlPlugin) ExecutePowerActionHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.ExecutePowerActionHookRequest)
	if !ok {
		return
	}

	var err error
	log.Infof("Executing power action %v on node %s", req.Action, req.CranedId)

	switch req.Action {
	case protos.PowerAction_WAKEUP:
		err = manager.wakeUpNode(req.CranedId)
	case protos.PowerAction_POWERON:
		err = manager.powerOnNode(req.CranedId)
	case protos.PowerAction_SLEEP:
		err = manager.sleepNode(req.CranedId)
	case protos.PowerAction_POWEROFF:
		err = manager.powerOffNode(req.CranedId)
	default:
		err = fmt.Errorf("unknown power action: %v", req.Action)
	}

	if err != nil {
		log.Errorf("Failed to execute power action: %v", err)
		return
	}

	log.Infof("Successfully executed power action %v on node %s", req.Action, req.CranedId)
}

func (p PowerControlPlugin) GetCranedByPowerStateHookSync(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.GetCranedByPowerStateHookSyncRequest)
	if !ok {
		log.Errorf("invalid request type, expected GetCranedByPowerStateHookSyncRequest")
		return
	}

	log.Infof("Getting craned list of type %v", req.Type)

	var state NodeState
	switch req.Type {
	case protos.CranedPowerType_ACTIVE:
		state = Active
	case protos.CranedPowerType_IDLE:
		state = Idle
	case protos.CranedPowerType_SLEEPING:
		state = Sleep
	case protos.CranedPowerType_POWEREDOFF:
		state = PoweredOff
	default:
		log.Errorf("unknown node list type: %v", req.Type)
		return
	}

	nodes := manager.GetNodesByState(state)
	log.Infof("Found %d nodes", len(nodes))
	ctx.Set("craned_ids", nodes)
}

func (p PowerControlPlugin) RegisterCranedHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.RegisterCranedHookRequest)
	if !ok {
		return
	}

	var validInterfaces []NetworkInterface

	for _, networkInterface := range req.NetworkInterfaces {
		log.Infof("Checking interface: name=%s, MAC=%s, IPs=%v",
			networkInterface.Name,
			networkInterface.MacAddress,
			networkInterface.Ipv4Addresses)

		if networkInterface.MacAddress == "" || len(networkInterface.Ipv4Addresses) == 0 {
			log.Infof("Skipping interface %s: empty MAC or no IP addresses", networkInterface.Name)
			continue
		}

		ip := networkInterface.Ipv4Addresses[0]
		mac := networkInterface.MacAddress
		name := networkInterface.Name

		// Skip loopback interfaces
		if strings.HasPrefix(name, "lo") || ip == "127.0.0.1" {
			log.Infof("Skipping loopback interface %s", name)
			continue
		}

		// Skip virtual network interfaces
		if strings.HasPrefix(name, "veth") || strings.HasPrefix(name, "virbr") ||
			strings.HasPrefix(name, "docker") || strings.HasPrefix(name, "br-") {
			log.Infof("Skipping virtual interface %s", name)
			continue
		}

		// Skip Docker network
		if strings.HasPrefix(ip, "172.17.") {
			log.Infof("Skipping Docker network interface %s", name)
			continue
		}

		// Skip virtual MAC addresses
		macUpper := strings.ToUpper(mac)
		if strings.HasPrefix(macUpper, "02:42:") || // Docker default
			strings.HasPrefix(macUpper, "00:16:3E:") || // Xen
			strings.HasPrefix(macUpper, "00:50:56:") || // VMware
			strings.HasPrefix(macUpper, "00:0C:29:") { // VMware
			log.Infof("Skipping virtual MAC address %s", mac)
			continue
		}

		validInterfaces = append(validInterfaces, NetworkInterface{
			MAC: mac,
			IP:  ip,
		})

		log.Infof("Added valid interface for node %s: MAC=%s, IP=%s",
			req.CranedId, mac, ip)
	}

	if len(validInterfaces) == 0 {
		log.Errorf("no valid network interface found for node %s", req.CranedId)
		return
	}

	manager.RegisterNode(req.CranedId)

	err := manager.powerTool.RegisterNode(req.CranedId, validInterfaces)
	if err != nil {
		return
	}
}

func (p PowerControlPlugin) StartHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.StartHookRequest)
	if !ok {
		return
	}

	for _, task := range req.TaskInfoList {
		taskID := strconv.FormatUint(uint64(task.TaskId), 10)
		log.Infof("Start hook for task %v", taskID)
		log.Infof("task.GetExecutionNode(): %v", task.GetExecutionNode())
		nodes := task.GetExecutionNode()

		for _, node := range nodes {
			manager.AddJobToNode(node, taskID)
		}
	}
}

func (p PowerControlPlugin) EndHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.EndHookRequest)
	if !ok {
		return
	}

	for _, task := range req.TaskInfoList {
		taskID := strconv.FormatUint(uint64(task.TaskId), 10)
		log.Infof("End hook for task %v", taskID)
		log.Infof("task.GetExecutionNode(): %v", task.GetExecutionNode())
		nodes := task.GetExecutionNode()

		for _, node := range nodes {
			manager.RemoveJobFromNode(node, taskID)
		}
	}
}

func (p PowerControlPlugin) CreateCgroupHook(ctx *api.PluginContext) {}

func (p PowerControlPlugin) DestroyCgroupHook(ctx *api.PluginContext) {}

func (p PowerControlPlugin) NodeEventHook(ctx *api.PluginContext) {}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
