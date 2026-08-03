package main

import (
	"fmt"
	"io"
	"net/http"
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
var _ api.JobLifecycleHooks = PowerControlPlugin{}
var _ api.PowerManagementHooks = PowerControlPlugin{}
var _ api.CranedLifecycleHooks = PowerControlPlugin{}
var _ api.NodeDefinitionHooks = PowerControlPlugin{}

var (
	PluginInstance = PowerControlPlugin{}
	manager        *PowerManager
	predictorCmd   *exec.Cmd // Global variable to track predictor process
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
	predictorCmd = exec.Command("python3", config.PowerControl.PredictorScript, "--config", configPath)

	if err := predictorCmd.Start(); err != nil {
		return fmt.Errorf("failed to start predictor service: %v", err)
	}

	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	predictorURL := config.Predictor.URL + "/health"

	for {
		select {
		case <-ticker.C:
			resp, err := http.Get(predictorURL)
			if err == nil && resp.StatusCode == http.StatusOK {
				log.Info("Predictor service started successfully")
				return nil
			}
		case <-timeout:
			return fmt.Errorf("timed out waiting for predictor service to start")
		}
	}
}

func (p PowerControlPlugin) Unload(meta api.PluginMeta) error {
	log.Info("PowerControl plugin is unloading...")

	// Stop the predictor process
	if predictorCmd != nil && predictorCmd.Process != nil {
		if err := predictorCmd.Process.Kill(); err != nil {
			log.Errorf("Failed to kill predictor process: %v", err)
		}
		// Wait for the process to fully terminate
		if err := predictorCmd.Wait(); err != nil {
			log.Errorf("Error waiting for predictor process to exit: %v", err)
		}
	}

	manager.StopAutoPowerManager()
	return nil
}

func (p PowerControlPlugin) UpdatePowerStateHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.UpdatePowerStateHookRequest)
	if !ok {
		log.Errorf("invalid request type, expected UpdatePowerStateHookRequest")
		return
	}
	if req.Dynamic && req.Provider != "powerControl" {
		return
	}

	var err error
	log.Infof("Updating power state to %v on node %s, enable_auto_power_control=%v", req.State, req.CranedId, req.EnableAutoPowerControl)

	// Handle auto power control setting if this is a CRANE_NONE state with enable_auto_power_control parameter
	if req.State == protos.CranedControlState_CRANE_NONE {
		// This is a request to set the auto power control status
		// Note: enable_auto_power_control=true means enable auto power control (exclude=false)
		// enable_auto_power_control=false means disable auto power control (exclude=true)
		exclude := !req.EnableAutoPowerControl
		err = manager.SetNodeExclude(req.CranedId, exclude)
		if err != nil {
			log.Errorf("Failed to set node auto power control status: %v", err)
			return
		}
		if req.EnableAutoPowerControl {
			log.Infof("Successfully enabled auto power control for node %s", req.CranedId)
		} else {
			log.Infof("Successfully disabled auto power control for node %s", req.CranedId)
		}
		return
	}

	// Handle normal power state changes
	switch req.State {
	case protos.CranedControlState_CRANE_POWERON:
		err = manager.powerOnNode(req.CranedId)
	case protos.CranedControlState_CRANE_POWEROFF:
		err = manager.powerOffNode(req.CranedId)
	case protos.CranedControlState_CRANE_SLEEP:
		err = manager.sleepNode(req.CranedId)
	case protos.CranedControlState_CRANE_WAKE:
		err = manager.wakeUpNode(req.CranedId)
	default:
		log.Errorf("Unsupported power state: %v", req.State)
		return
	}

	if err != nil {
		log.Errorf("Failed to change power state: %v", err)
		if _, exists := manager.nodesInfo.Load(req.CranedId); !exists {
			switch req.State {
			case protos.CranedControlState_CRANE_POWERON:
				manager.reportCtldPowerStateChange(req.CranedId, PoweredOff)
			case protos.CranedControlState_CRANE_WAKE:
				manager.reportCtldPowerStateChange(req.CranedId, Sleep)
			case protos.CranedControlState_CRANE_SLEEP:
				manager.reportCtldPowerStateChange(req.CranedId, Idle)
			case protos.CranedControlState_CRANE_POWEROFF:
				manager.reportCtldPowerStateChange(req.CranedId, Sleep)
			}
		}
	} else {
		log.Infof("Successfully changed power state to %v on node %s", req.State, req.CranedId)
	}
}

func (p PowerControlPlugin) NodeDefinitionHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.NodeDefinitionHookRequest)
	if !ok {
		log.Errorf("invalid request type, expected NodeDefinitionHookRequest")
		return
	}
	if req.Provider != "powerControl" {
		manager.RemoveNode(req.CranedId)
		return
	}

	switch req.Action {
	case protos.NodeDefinitionAction_NODE_DEFINITION_ACTION_UPSERT:
		var initialState NodeState
		switch req.PowerState {
		case protos.DynamicNodePowerState_DYNAMIC_NODE_POWER_STATE_ON:
			initialState = Idle
		case protos.DynamicNodePowerState_DYNAMIC_NODE_POWER_STATE_OFF:
			initialState = PoweredOff
		case protos.DynamicNodePowerState_DYNAMIC_NODE_POWER_STATE_POWERING_ON:
			initialState = PoweringOn
		case protos.DynamicNodePowerState_DYNAMIC_NODE_POWER_STATE_POWERING_OFF:
			initialState = PoweringOff
		case protos.DynamicNodePowerState_DYNAMIC_NODE_POWER_STATE_SLEEPING:
			initialState = Sleep
		case protos.DynamicNodePowerState_DYNAMIC_NODE_POWER_STATE_WAKING_UP:
			initialState = Wakingup
		case protos.DynamicNodePowerState_DYNAMIC_NODE_POWER_STATE_TO_SLEEPING:
			initialState = ToSleeping
		default:
			log.Errorf("invalid power state for node %s: %v", req.CranedId, req.PowerState)
			return
		}
		if err := manager.powerTool.RegisterNode(req.CranedId, nil); err != nil {
			log.Errorf("failed to register node definition %s: %v", req.CranedId, err)
			return
		}
		manager.RegisterNode(req.CranedId, initialState, nil)
	case protos.NodeDefinitionAction_NODE_DEFINITION_ACTION_REMOVE:
		manager.RemoveNode(req.CranedId)
	default:
		log.Errorf("invalid node definition action for node %s: %v", req.CranedId, req.Action)
	}
}

func (p PowerControlPlugin) RegisterCranedHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.RegisterCranedHookRequest)
	if !ok {
		return
	}
	if req.Dynamic && req.Provider != "powerControl" {
		manager.RemoveNode(req.CranedId)
		return
	}

	var validInterfaces []NetworkInterface

	for _, networkInterface := range req.NetworkInterfaces {
		log.Debugf("Checking interface: name=%s, MAC=%s, IPs=%v",
			networkInterface.Name,
			networkInterface.MacAddress,
			networkInterface.Ipv4Addresses)

		if networkInterface.MacAddress == "" || len(networkInterface.Ipv4Addresses) == 0 {
			log.Debugf("Skipping interface %s: empty MAC or no IP addresses", networkInterface.Name)
			continue
		}

		ip := networkInterface.Ipv4Addresses[0]
		mac := networkInterface.MacAddress
		name := networkInterface.Name

		// Skip loopback interfaces
		if strings.HasPrefix(name, "lo") || ip == "127.0.0.1" {
			log.Debugf("Skipping loopback interface %s", name)
			continue
		}

		// Skip virtual network interfaces
		if strings.HasPrefix(name, "veth") || strings.HasPrefix(name, "virbr") ||
			strings.HasPrefix(name, "docker") || strings.HasPrefix(name, "br-") {
			log.Debugf("Skipping virtual interface %s", name)
			continue
		}

		// Skip Docker network
		if strings.HasPrefix(ip, "172.17.") {
			log.Debugf("Skipping Docker network interface %s", name)
			continue
		}

		// Skip virtual MAC addresses
		macUpper := strings.ToUpper(mac)
		if strings.HasPrefix(macUpper, "02:42:") || // Docker default
			strings.HasPrefix(macUpper, "00:16:3E:") || // Xen
			strings.HasPrefix(macUpper, "00:50:56:") || // VMware
			strings.HasPrefix(macUpper, "00:0C:29:") { // VMware
			log.Debugf("Skipping virtual MAC address %s", mac)
			continue
		}

		validInterfaces = append(validInterfaces, NetworkInterface{
			MAC: mac,
			IP:  ip,
		})

		log.Debugf("Added valid interface for node %s: MAC=%s, IP=%s",
			req.CranedId, mac, ip)
	}

	if len(validInterfaces) == 0 {
		log.Errorf("no valid network interface found for node %s", req.CranedId)
		return
	}

	err := manager.powerTool.RegisterNode(req.CranedId, validInterfaces)
	if err != nil {
		log.Errorf("failed to register node %s: %v", req.CranedId, err)
		return
	}
	jobs := make(map[string]struct{}, len(req.RunningJobIds))
	for _, jobID := range req.RunningJobIds {
		jobs[strconv.FormatUint(uint64(jobID), 10)] = struct{}{}
	}

	var state NodeState
	switch req.PowerState {
	case protos.CranedPowerState_CRANE_POWER_ACTIVE:
		state = Active
	case protos.CranedPowerState_CRANE_POWER_IDLE:
		state = Idle
	case protos.CranedPowerState_CRANE_POWER_SLEEPING:
		state = Sleep
	case protos.CranedPowerState_CRANE_POWER_POWEREDOFF:
		state = PoweredOff
	case protos.CranedPowerState_CRANE_POWER_TO_SLEEPING:
		state = ToSleeping
	case protos.CranedPowerState_CRANE_POWER_WAKING_UP:
		state = Wakingup
	case protos.CranedPowerState_CRANE_POWER_POWERING_ON:
		state = PoweringOn
	case protos.CranedPowerState_CRANE_POWER_POWERING_OFF:
		state = PoweringOff
	default:
		log.Errorf("invalid power state for node %s: %v", req.CranedId, req.PowerState)
		return
	}
	manager.RegisterNode(req.CranedId, state, jobs)

	log.Infof("Successfully registered node %s", req.CranedId)
}

func (p PowerControlPlugin) StartHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.StartHookRequest)
	if !ok {
		return
	}

	for _, job := range req.JobInfoList {
		jobID := strconv.FormatUint(uint64(job.JobId), 10)
		log.Debugf("Start hook for job %v", jobID)
		log.Debugf("job.GetExecutionNode(): %v", job.GetExecutionNode())
		nodes := job.GetExecutionNode()

		for _, node := range nodes {
			manager.AddJobToNode(node, jobID)
		}
	}
}

func (p PowerControlPlugin) EndHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.EndHookRequest)
	if !ok {
		return
	}

	for _, job := range req.JobInfoList {
		jobID := strconv.FormatUint(uint64(job.JobId), 10)
		log.Debugf("End hook for job %v", jobID)
		log.Debugf("job.GetExecutionNode(): %v", job.GetExecutionNode())
		nodes := job.GetExecutionNode()

		for _, node := range nodes {
			manager.RemoveJobFromNode(node, jobID)
		}
	}
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
