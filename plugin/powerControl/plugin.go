package main

import (
	"fmt"
	"os/exec"
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
	controller     *PowerController
)

type PowerControlPlugin struct{}

func (p PowerControlPlugin) Name() string {
	return "PowerControl"
}

func (p PowerControlPlugin) Version() string {
	return "v0.0.1"
}

func (p PowerControlPlugin) Load(meta api.PluginMeta) error {
	log.Info("PowerControl plugin is loading...")

	config, err := LoadConfig(meta.Config)
	if err != nil {
		return fmt.Errorf("failed to load config: %v", err)
	}

	if err := StartPredictorService(config, meta.Config); err != nil {
		return fmt.Errorf("failed to start predictor service: %v", err)
	}

	controller = NewPowerController(config)
	controller.StartAutoPowerControl()

	log.Info("PowerControl plugin loaded successfully")
	return nil
}

func StartPredictorService(config *Config, configPath string) error {
	log.Infof("Starting predictor service with config: %s", configPath)
	cmd := exec.Command("python3", config.Predictor.PredictorScript, "--config", configPath)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start predictor service: %v", err)
	}

	time.Sleep(3 * time.Second)
	return nil
}

func (p PowerControlPlugin) Unload(meta api.PluginMeta) error {
	log.Info("PowerControl plugin is unloading...")
	controller.StopAutoPowerControl()
	return nil
}

func (p PowerControlPlugin) ExecutePowerActionHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.ExecutePowerActionHookRequest)
	if !ok {
		ctx.Set("ok", false)
		ctx.Set("error", "invalid request type, expected ExecutePowerActionHookRequest")
		return
	}

	var err error
	log.Infof("Executing power action %v on node %s", req.Action, req.CranedId)

	switch req.Action {
	case protos.PowerAction_WAKEUP:
		err = controller.wakeUpNode(req.CranedId)
	case protos.PowerAction_POWERON:
		err = controller.powerOnNode(req.CranedId)
	case protos.PowerAction_SLEEP:
		err = controller.sleepNode(req.CranedId)
	case protos.PowerAction_POWEROFF:
		err = controller.powerOffNode(req.CranedId)
	default:
		err = fmt.Errorf("unknown power action: %v", req.Action)
	}

	if err != nil {
		log.Errorf("Failed to execute power action: %v", err)
		ctx.Set("ok", false)
		ctx.Set("error", err.Error())
		return
	}

	log.Infof("Successfully executed power action %v on node %s", req.Action, req.CranedId)
	ctx.Set("ok", true)
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

	nodes := controller.GetNodesByState(state)
	log.Infof("Found %d nodes", len(nodes))
	ctx.Set("craned_ids", nodes)
}

func (p PowerControlPlugin) RegisterCranedHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.RegisterCranedHookRequest)
	if !ok {
		ctx.Set("ok", false)
		ctx.Set("error", "invalid request type, expected RegisterCranedHookRequest")
		return
	}

	var bestMac, bestIP string
	var bestScore int = -1

	for _, networkInterface := range req.NetworkInterfaces {
		if networkInterface.MacAddress == "" || len(networkInterface.Ipv4Addresses) == 0 {
			continue
		}

		score := 0
		ip := networkInterface.Ipv4Addresses[0]
		log.Infof("Checking interface %s with IP %s", networkInterface.Name, ip)

		if strings.HasPrefix(ip, "10.") {
			score += 1
		}

		// 172.16.x.x - 172.31.x.x
		if strings.HasPrefix(ip, "172.") {
			secondOctet, _ := strconv.Atoi(strings.Split(ip, ".")[1])
			if secondOctet >= 16 && secondOctet <= 31 {
				score += 1
			}
		}

		// 192.168.x.x
		if strings.HasPrefix(ip, "192.168.") {
			score += 8
		}

		if strings.HasPrefix(ip, "192.168.11.") {
			score += 8
		}

		// Skip docker/container network interfaces (usually 172.17.x.x)
		if strings.HasPrefix(ip, "172.17.") {
			continue
		}

		// Skip interfaces with special MAC prefixes
		macUpper := strings.ToUpper(networkInterface.MacAddress)
		if strings.HasPrefix(macUpper, "02:42:") { // Docker default MAC prefix
			continue
		}

		if score > bestScore {
			bestScore = score
			bestMac = networkInterface.MacAddress
			bestIP = ip
			log.Infof("Found better interface for node %s: MAC=%s, IP=%s, score=%d",
				req.CranedId, bestMac, bestIP, bestScore)
		}
	}

	if bestMac == "" || bestIP == "" {
		ctx.Set("ok", false)
		ctx.Set("error", "no valid network interface found (requires both MAC and IPv4)")
		return
	}

	controller.RegisterNode(req.CranedId)

	err := controller.powerTool.RegisterNode(req.CranedId, bestMac, bestIP)
	if err != nil {
		ctx.Set("ok", false)
		ctx.Set("error", fmt.Sprintf("failed to register node: %v", err))
		return
	}

	ctx.Set("ok", true)
}

func (p PowerControlPlugin) StartHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.StartHookRequest)
	if !ok {
		ctx.Set("ok", false)
		ctx.Set("error", "invalid request type, expected StartHookRequest")
		return
	}

	for _, task := range req.TaskInfoList {
		taskID := strconv.FormatUint(uint64(task.TaskId), 10)
		log.Infof("Start hook for task %v", taskID)
		log.Infof("task.GetExecutionNode(): %v", task.GetExecutionNode())
		nodes := task.GetExecutionNode()
		if len(nodes) == 0 {
			log.Errorf("Failed to parse craned list: %v", task.GetCranedList())
			continue
		}

		controller.AddRunningjob(taskID, len(nodes))

		for _, node := range nodes {
			controller.AddJobToNode(node, taskID)
		}
	}

	avgNodes := controller.GetAverageNodesPerjob()
	log.Infof("Current running jobs stats - Average nodes per job: %.2f", avgNodes)
}

func (p PowerControlPlugin) EndHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.EndHookRequest)
	if !ok {
		ctx.Set("ok", false)
		ctx.Set("error", "invalid request type, expected EndHookRequest")
		return
	}

	for _, task := range req.TaskInfoList {
		taskID := strconv.FormatUint(uint64(task.TaskId), 10)
		log.Infof("End hook for task %v", taskID)
		log.Infof("task.GetExecutionNode(): %v", task.GetExecutionNode())
		nodes := task.GetExecutionNode()
		if len(nodes) == 0 {
			log.Errorf("Failed to parse craned list: %v", task.GetCranedList())
			continue
		}

		controller.RemoveRunningjob(taskID)

		for _, node := range nodes {
			controller.RemoveJobFromNode(node, taskID)
		}
	}

	avgNodes := controller.GetAverageNodesPerjob()
	log.Infof("Current running jobs stats - Average nodes per job: %.2f", avgNodes)
}

func (p PowerControlPlugin) CreateCgroupHook(ctx *api.PluginContext) {}

func (p PowerControlPlugin) DestroyCgroupHook(ctx *api.PluginContext) {}

func (p PowerControlPlugin) NodeEventHook(ctx *api.PluginContext) {}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
