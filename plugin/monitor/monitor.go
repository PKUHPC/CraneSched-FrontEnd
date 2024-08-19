// This is a MonitorPlugin for XXXXX.
package main

import (
	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"

	log "github.com/sirupsen/logrus"
)

// Compile-time check to ensure MonitorPlugin implements api.Plugin
var _ api.Plugin = &MonitorPlugin{}

// PluginD will call plugin's method thru this variable
var PluginInstance = MonitorPlugin{}

type MonitorPlugin struct{}

func (dp *MonitorPlugin) Init(meta api.PluginMeta) error {
	log.Infof("Monitor plugin is loaded.")
	log.Tracef("Metadata: %v", meta)
	return nil
}

func (dp *MonitorPlugin) Name() string {
	return "Monitor"
}

func (dp *MonitorPlugin) Version() string {
	return "v0.0.1"
}

func (dp *MonitorPlugin) StartHook(ctx *api.PluginContext) {
	log.Infoln("StartHook is called!")

	req, ok := ctx.Request().(*protos.StartHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected StartHookRequest.")
		return
	}

	log.Tracef("StartHookReq: \n%v", req.String())
}

func (dp *MonitorPlugin) EndHook(ctx *api.PluginContext) {
	log.Infoln("EndHook is called!")

	req, ok := ctx.Request().(*protos.EndHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected EndHookRequest.")
		return
	}

	log.Tracef("EndHookReq: \n%v", req.String())
}

func (dp *MonitorPlugin) JobCheckHook(ctx *api.PluginContext) {
	log.Infoln("JobCheckHook is called!")

	req, ok := ctx.Request().(*protos.JobCheckHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected JobCheckHookRequest.")
		return
	}

	log.Tracef("JobCheckHookReq: \n%v", req.String())
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
