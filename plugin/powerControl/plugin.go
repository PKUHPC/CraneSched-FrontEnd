package main

import (
	"bufio"
	"fmt"
	"os/exec"
	"strconv"
	"time"

	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"

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
	cmd := exec.Command("python3", config.Predictor.PredictorScript, configPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start predictor service: %v", err)
	}

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			log.Info("Predictor: ", scanner.Text())
		}
	}()

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Info("Predictor: ", scanner.Text())
		}
	}()

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

func (p PowerControlPlugin) GetCranedListHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.GetCranedListHookRequest)
	if !ok {
		log.Errorf("invalid request type, expected GetCranedListHookRequest")
		return
	}

	log.Infof("Getting craned list of type %v", req.Type)

	var state NodeState
	switch req.Type {
	case protos.CranedListType_ACTIVE:
		state = Active
	case protos.CranedListType_IDLE:
		state = Idle
	case protos.CranedListType_SLEEPING:
		state = Sleep
	case protos.CranedListType_POWEREDOFF:
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

	var mac, ip string
	for _, networkInterface := range req.NetworkInterfaces {
		if networkInterface.MacAddress == "" {
			continue
		}
		if len(networkInterface.Ipv4Addresses) == 0 {
			continue
		}

		mac = networkInterface.MacAddress
		ip = networkInterface.Ipv4Addresses[0]
		log.Infof("Found valid interface for node %s: MAC=%s, IP=%s",
			req.CranedId, mac, ip)
		break
	}

	if mac == "" || ip == "" {
		ctx.Set("ok", false)
		ctx.Set("error", "no valid network interface found (requires both MAC and IPv4)")
		return
	}

	controller.RegisterNode(req.CranedId)

	err := controller.powerTool.RegisterNode(req.CranedId, mac, ip)
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
		log.Infof("task.GetCranedList(): %v", task.GetCranedList())
		nodes, ok := util.ParseHostList(task.GetCranedList())
		if !ok {
			log.Errorf("Failed to parse craned list: %v", task.GetCranedList())
			continue
		}
		for _, node := range nodes {
			controller.AddJobToNode(node, taskID)
		}
	}
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
		controller.RemoveJobFromNode(task.GetCranedList(), taskID)
	}
}

func (p PowerControlPlugin) CreateCgroupHook(ctx *api.PluginContext) {}

func (p PowerControlPlugin) DestroyCgroupHook(ctx *api.PluginContext) {}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
