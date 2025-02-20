package main

import (
	"CraneFrontEnd/api"
	"fmt"

	"CraneFrontEnd/generated/protos"

	log "github.com/sirupsen/logrus"
)

var _ api.Plugin = PowerControlPlugin{}

var (
	PluginInstance = PowerControlPlugin{}
	controller     *PowerController
)

type PowerControlPlugin struct {
	stopChan chan struct{}
}

func (p PowerControlPlugin) Name() string {
	return "PowerControl"
}

func (p PowerControlPlugin) Version() string {
	return "v0.0.1"
}

func (p PowerControlPlugin) startAutoPowerControl() {
	// p.stopChan = make(chan struct{})
	// go func() {
	// 	ticker := time.NewTicker(5 * time.Minute)
	// 	defer ticker.Stop()

	// 	for {
	// 		select {
	// 		case <-ticker.C:
	// 			wake, on, sleep, off := p.controller.ExecutePowerActions()
	// 			log.Infof("Auto power control executed: wake=%d, on=%d, sleep=%d, off=%d",
	// 				wake, on, sleep, off)
	// 		case <-p.stopChan:
	// 			return
	// 		}
	// 	}
	// }()
}

func (p PowerControlPlugin) Load(meta api.PluginMeta) error {
	log.Info("PowerControl plugin is loading...")

	// 使用默认配置值
	config := struct {
		PredictorURL      string
		MinUptime         int
		MinDowntime       int
		SleepThreshold    int
		BufferRatio       float64
		IdleReserveRatio  float64
		RedundancyNodeNum int
		BatchSize         int
		HTTPPort          string
		CheckInterval     int
	}{
		PredictorURL:      "http://localhost:5000", // 更新预测服务URL
		MinUptime:         600,                     // 10分钟
		MinDowntime:       300,                     // 5分钟
		SleepThreshold:    2160000,                 // 6小时 (6 * 60 * 60)
		BufferRatio:       0.1,                     // 10%
		IdleReserveRatio:  0.05,                    // 5%
		RedundancyNodeNum: 10,                      // 10个冗余节点
		BatchSize:         30,                      // 每批次30个节点
		HTTPPort:          "8081",
		CheckInterval:     60 * 30,
	}

	controller = NewPowerController(
		config.PredictorURL,
		config.MinUptime,
		config.MinDowntime,
		config.SleepThreshold,
		config.BufferRatio,
		config.IdleReserveRatio,
		config.RedundancyNodeNum,
		config.BatchSize,
		config.CheckInterval,
	)

	httpServer := NewHTTPServer(controller, config.HTTPPort)
	go func() {
		if err := httpServer.Start(); err != nil {
			log.Errorf("HTTP server error: %v", err)
		}
	}()

	p.stopChan = make(chan struct{})
	p.startAutoPowerControl()

	log.Info("PowerControl plugin loaded successfully")
	return nil
}

func (p PowerControlPlugin) Unload(meta api.PluginMeta) error {
	log.Info("PowerControl plugin is unloading...")
	if p.stopChan != nil {
		close(p.stopChan)
	}
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

func (p PowerControlPlugin) GetCranedInfoHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.GetCranedInfoHookRequest)
	if !ok {
		log.Errorf("invalid request type, expected GetCranedInfoHookRequest")
		return
	}

	ctx.Set("craned_info", &protos.GetCranedInfoHookReply{
		State:    string(controller.GetNodeState(req.CranedId)),
		JobCount: int32(controller.GetNodeJobCount(req.CranedId)),
	})
}

func (p PowerControlPlugin) GetCranedListHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.GetCranedListHookRequest)
	if !ok {
		log.Errorf("invalid request type, expected GetCranedListHookRequest")
	}

	var nodes []string
	switch req.Type {
	case protos.CranedListType_ACTIVE:
		nodes = controller.GetActiveNodes()
	case protos.CranedListType_IDLE:
		nodes = controller.GetIdleNodes()
	case protos.CranedListType_SLEEPING:
		nodes = controller.GetSleepingNodes()
	case protos.CranedListType_POWEREDOFF:
		nodes = controller.GetPoweredOffNodes()
	default:
		log.Errorf("unknown node list type: %v", req.Type)
	}

	ctx.Set("nodes", nodes)
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

	controller.InitializeNode(req.CranedId)

	err := controller.powerTool.RegisterNode(req.CranedId, mac, ip)
	if err != nil {
		ctx.Set("ok", false)
		ctx.Set("error", fmt.Sprintf("failed to register node: %v", err))
		return
	}

	ctx.Set("ok", true)
}

func (p PowerControlPlugin) CreateCgroupHook(ctx *api.PluginContext)  {}
func (p PowerControlPlugin) DestroyCgroupHook(ctx *api.PluginContext) {}
func (p PowerControlPlugin) StartHook(ctx *api.PluginContext)         {}
func (p PowerControlPlugin) EndHook(ctx *api.PluginContext)           {}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
