// This is a dummy plugin for testing and demonstration purposes.
package main

import (
	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"
	"fmt"

	log "github.com/sirupsen/logrus"
)

// Compile-time check to ensure DummyPlugin implements api.Plugin
var _ api.Plugin = DummyPlugin{}

// PluginD will call plugin's method thru this variable
var PluginInstance = DummyPlugin{}

type DummyPlugin struct{}

// Init() is used to display plugin info when loading.
func (dp DummyPlugin) Init() error {
	log.Infoln("This is a dummy plugin.")
	return nil
}

func (dp DummyPlugin) Name() string {
	return "DummyPlugin"
}

func (dp DummyPlugin) Version() string {
	return "v0.0.1"
}

func (dp DummyPlugin) PreStartHook(ctx *api.PluginContext) {
	log.Infoln("PreStartHook is called!")

	req, ok := ctx.Request().(*protos.PreStartHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected PreStartHookRequest.")
		return
	}

	fmt.Printf("PreStartHookReq: \n%v\n", req.String())
}

func (dp DummyPlugin) PostStartHook(ctx *api.PluginContext) {
	log.Infoln("PostStartHook is called!")

	req, ok := ctx.Request().(*protos.PostStartHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected PostStartHookRequest.")
		return
	}

	fmt.Printf("PostStartHookReq: \n%v\n", req.String())
}

func (dp DummyPlugin) PreCompletionHook(ctx *api.PluginContext) {
	log.Infoln("PreCompletionHook is called!")

	req, ok := ctx.Request().(*protos.PreCompletionHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected PreCompletionHookRequest.")
		return
	}

	fmt.Printf("PreCompletionHookReq: \n%v\n", req.String())
}

func (dp DummyPlugin) PostCompletionHook(ctx *api.PluginContext) {
	log.Infoln("PostCompletionHook is called!")

	req, ok := ctx.Request().(*protos.PostCompletionHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected PostCompletionHookRequest.")
		return
	}

	fmt.Printf("PostCompletionHookReq: \n%v\n", req.String())
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
