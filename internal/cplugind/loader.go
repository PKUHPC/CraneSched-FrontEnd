package cplugind

import (
	"CraneFrontEnd/api"
	"fmt"
	"os"
	"plugin"

	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v3"
)

type PluginInfo struct {
	Name string `yaml:"Name"`
	Path string `yaml:"Path"`
}

type PluginConfig struct {
	SockPath string       `yaml:"PlugindSockPath"`
	LogLevel string       `yaml:"PlugindDebugLevel"`
	Plugins  []PluginInfo `yaml:"Plugins"`
}

var (
	gPluginConfig PluginConfig
	gPluginList   []*api.Plugin
)

func ParsePluginConfig(path string) error {
	config, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	gPluginConfig = PluginConfig{}
	if err = yaml.Unmarshal(config, &gPluginConfig); err != nil {
		return err
	}

	return nil
}

func LoadPluginsByConfig(pl []PluginInfo) error {
	gPluginList = make([]*api.Plugin, 0, len(pl))

	for _, p := range pl {
		log.Tracef("Loading plugin %s from %s", p.Name, p.Path)

		// Load by path
		plg, err := plugin.Open(p.Path)
		if err != nil {
			log.Warn(err)
			continue
		}

		// Search for variable
		v, err := plg.Lookup("PluginInstance")
		if err != nil {
			return err
		}

		castV, ok := v.(api.Plugin)
		if !ok {
			return fmt.Errorf("failed to cast plugin %s", p.Name)
		}
		gPluginList = append(gPluginList, &castV)
	}

	return nil
}
