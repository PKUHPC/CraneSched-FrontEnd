/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package cplugind

import (
	"CraneFrontEnd/api"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"path/filepath"
	"plugin"

	log "github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v3"
)

type PluginConfig struct {
	Enabled  bool             `yaml:"Enabled"`
	SockPath string           `yaml:"PlugindSockPath"`
	LogLevel string           `yaml:"PlugindDebugLevel"`
	Plugins  []api.PluginMeta `yaml:"Plugins"`
}

type PluginLoaded struct {
	api.Plugin

	Meta api.PluginMeta
}

var (
	gPluginConfig PluginConfig
	gPluginMap    map[string]*PluginLoaded
)

func ParsePluginConfig(basedir string, path string) error {
	config, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	temp := struct {
		Plugin PluginConfig `yaml:"Plugin"`
	}{}
	if err = yaml.Unmarshal(config, &temp); err != nil {
		return err
	}

	gPluginConfig = temp.Plugin
	if gPluginConfig.LogLevel == "" {
		gPluginConfig.LogLevel = "info"
	}

	if gPluginConfig.SockPath == "" {
		gPluginConfig.SockPath = filepath.Join(basedir, util.DefaultPlugindSocketPath)
	} else {
		gPluginConfig.SockPath = filepath.Join(basedir, gPluginConfig.SockPath)
	}

	return nil
}

func LoadPluginsByConfig(pl []api.PluginMeta) error {
	gPluginMap = make(map[string]*PluginLoaded)

	for _, p := range pl {
		log.Infof("Loading plugin %s from %s", p.Name, p.Path)

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
			return fmt.Errorf("failed to cast plugin instance %v", p.Name)
		}

		gPluginMap[p.Name] = &PluginLoaded{
			Plugin: castV,
			Meta:   p,
		}
	}

	return nil
}
