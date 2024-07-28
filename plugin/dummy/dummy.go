// This is a dummy plugin for testing and demonstration purposes.
package main

import (
	"CraneFrontEnd/api"

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

func (dp DummyPlugin) PreRunHook() {
	log.Infoln("PreRunHook is called!")
}

func (dp DummyPlugin) PostRunHook() {
	log.Infoln("PostRunHook is called!")
}

func (dp DummyPlugin) PreCompletionHook() {
	log.Infoln("PreCompletionHook is called!")
}

func (dp DummyPlugin) PostCompletionHook() {
	log.Infoln("PostCompletionHook is called!")
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.")
}
