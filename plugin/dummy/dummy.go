// This is a dummy plugin for testing and demonstration purposes.
package main

import (
	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"

	log "github.com/sirupsen/logrus"
)

// Compile-time check to ensure DummyPlugin implements api.Plugin
var _ api.Plugin = DummyPlugin{}

// PluginD will call plugin's method thru this variable
var PluginInstance = DummyPlugin{}

type DummyPlugin struct{}

func (dp DummyPlugin) Init(meta api.PluginMeta) error {
	log.Infof("Dummy plugin is loaded.")
	log.Tracef("Metadata: %v", meta)
	return nil
}

func (dp DummyPlugin) Name() string {
	return "Dummy"
}

func (dp DummyPlugin) Version() string {
	return "v0.0.1"
}

func (dp DummyPlugin) StartHook(ctx *api.PluginContext) {
	log.Infoln("StartHook is called!")

	req, ok := ctx.Request().(*protos.StartHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected StartHookRequest.")
		return
	}

	log.Tracef("StartHookReq: \n%v", req.String())
}

func (dp DummyPlugin) EndHook(ctx *api.PluginContext) {
	log.Infoln("EndHook is called!")

	req, ok := ctx.Request().(*protos.EndHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected EndHookRequest.")
		return
	}

	log.Tracef("EndHookReq: \n%v", req.String())
}

func (dp DummyPlugin) JobMonitorHook(ctx *api.PluginContext) {
	log.Infoln("JobMonitorHook is called!")

	req, ok := ctx.Request().(*protos.JobMonitorHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected JobMonitorHookRequest.")
		return
	}

	log.Tracef("JobMonitorHookReq: \n%v", req.String())
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
