package main

import (
	"errors"
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

// Must match kPowerControlProvider in CraneCtld (CtldPublicDefs.h).
const powerControlProvider = "powerControl"

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
	generation := uint64(0)
	if req.Dynamic {
		if !strings.EqualFold(req.Provider, powerControlProvider) {
			log.Debugf("Skipping power state update for node %s managed by provider %q", req.CranedId, req.Provider)
			return
		}
		if req.Generation == 0 {
			log.Errorf("dynamic power state update for node %s has no generation", req.CranedId)
			return
		}
		generation = req.Generation
	}

	var err error
	log.Infof("Updating power state to %v on node %s, enable_auto_power_control=%v", req.State, req.CranedId, req.EnableAutoPowerControl)

	// Handle auto power control setting if this is a CRANE_NONE state with enable_auto_power_control parameter
	if req.State == protos.CranedControlState_CRANE_NONE {
		unlock := manager.lockNodeOperation(req.CranedId)
		defer unlock()
		if generation != 0 {
			switch manager.NodeTrackingStatus(req.CranedId, generation) {
			case nodeTrackingStale:
				log.Debugf("Ignoring power state update for stale dynamic node %s generation %d", req.CranedId, generation)
				return
			case nodeTrackingUntracked:
				log.Errorf("Dynamic node %s generation %d is defined but not tracked by the power manager; check its BMC configuration", req.CranedId, generation)
				return
			}
		}

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
		err = manager.powerOnNode(req.CranedId, generation)
	case protos.CranedControlState_CRANE_POWEROFF:
		err = manager.powerOffNode(req.CranedId, generation)
	case protos.CranedControlState_CRANE_SLEEP:
		err = manager.sleepNode(req.CranedId, generation)
	case protos.CranedControlState_CRANE_WAKE:
		err = manager.wakeUpNode(req.CranedId, generation)
	default:
		log.Errorf("Unsupported power state: %v", req.State)
		return
	}

	if err != nil {
		if errors.Is(err, errStaleNodeGeneration) {
			log.Debugf("Ignoring power state update for stale dynamic node %s generation %d", req.CranedId, generation)
			return
		}
		if errors.Is(err, errNodeNotTracked) {
			log.Errorf("Dynamic node %s generation %d is defined but not tracked by the power manager; check its BMC configuration", req.CranedId, generation)
			return
		}
		log.Errorf("Failed to change power state: %v", err)
		if value, exists := manager.nodesInfo.Load(req.CranedId); exists {
			info := value.(*NodeInfo)
			if info.Generation != generation {
				log.Debugf("Skipping power state failure report for stale dynamic node %s generation %d", req.CranedId, generation)
				return
			}
			manager.notifyCtldPowerStateChange(req.CranedId, info.State, generation)
		} else {
			// Only observed states are reported; CraneCtld's
			// PowerActionTimeout converges unknown nodes.
			log.Warnf("Node %s not tracked by power manager; no power state reported for failed %v", req.CranedId, req.State)
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
	if req.Generation == 0 {
		log.Errorf("node definition for %s has no generation", req.CranedId)
		return
	}
	unlock := manager.lockNodeOperation(req.CranedId)
	defer unlock()

	if !strings.EqualFold(req.Provider, powerControlProvider) {
		log.Debugf("Node %s definition belongs to provider %q; not managing it", req.CranedId, req.Provider)
		if manager.ApplyNodeDefinitionVersion(req.CranedId, req.Generation, req.Revision, false) {
			manager.RemoveNode(req.CranedId, req.Generation)
		}
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
		if !manager.ApplyNodeDefinitionVersion(req.CranedId, req.Generation, req.Revision, true) {
			log.Debugf("Ignoring stale node definition for %s generation %d revision %d", req.CranedId, req.Generation, req.Revision)
			return
		}
		manager.ResetNodeForNewGeneration(req.CranedId, req.Generation)
		if err := manager.powerTool.RegisterNode(req.CranedId, nil); err != nil {
			log.Errorf("failed to register node definition %s: %v", req.CranedId, err)
			return
		}
		manager.RegisterNode(req.CranedId, initialState, nil, req.Generation, req.Revision)
	case protos.NodeDefinitionAction_NODE_DEFINITION_ACTION_REMOVE:
		if !manager.ApplyNodeDefinitionVersion(req.CranedId, req.Generation, req.Revision, false) {
			log.Debugf("Ignoring stale node removal for %s generation %d revision %d", req.CranedId, req.Generation, req.Revision)
			return
		}
		manager.RemoveNode(req.CranedId, req.Generation)
	default:
		log.Errorf("invalid node definition action for node %s: %v", req.CranedId, req.Action)
	}
}

func (p PowerControlPlugin) RegisterCranedHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.RegisterCranedHookRequest)
	if !ok {
		return
	}
	unlock := manager.lockNodeOperation(req.CranedId)
	defer unlock()

	generation := uint64(0)
	revision := uint64(0)
	if req.Dynamic {
		if req.Generation == 0 {
			log.Errorf("dynamic craned registration for %s has no generation", req.CranedId)
			return
		}
		generation = req.Generation
		revision = req.Revision
		if !strings.EqualFold(req.Provider, powerControlProvider) {
			log.Debugf("Node %s registered under provider %q; not managing it", req.CranedId, req.Provider)
			if manager.ApplyNodeDefinitionVersion(req.CranedId, generation, revision, false) {
				manager.RemoveNode(req.CranedId, generation)
			}
			return
		}
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
	if generation != 0 && !manager.AcceptNodeVersion(req.CranedId, generation, revision) {
		log.Debugf("Ignoring stale craned registration for %s generation %d revision %d", req.CranedId, generation, revision)
		return
	}

	manager.ResetNodeForNewGeneration(req.CranedId, generation)
	if err := manager.powerTool.RegisterNode(req.CranedId, validInterfaces); err != nil {
		log.Errorf("failed to register node %s: %v", req.CranedId, err)
		return
	}
	manager.RegisterNode(req.CranedId, state, jobs, generation, revision)

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
