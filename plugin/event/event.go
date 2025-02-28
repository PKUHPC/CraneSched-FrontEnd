/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package main

import (
	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"
	"os"

	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

var _ api.Plugin = &EventPlugin{}

var PluginInstance = EventPlugin{}

// Plugin internal config
type config struct {
	Database struct {
		Username    string `yaml:"Username"`
		Bucket      string `yaml:"Bucket"`
		Org         string `yaml:"Org"`
		Measurement string `yaml:"Measurement"`
		Token       string `yaml:"Token"`
		Url         string `yaml:"Url"`
	} `yaml:"Database"`
}

type EventPlugin struct {
	config
	client   influxdb2.Client
}

type EventInfo struct {
	StartTime    time.Time
	EndTime      time.Time
	NodeName     string
	ClusterName  string
	Reason       string
	State        protos.CranedControlState
	Uid          uint32
}

func (p *EventPlugin) Name() string {
	return "Event"
}

func (p *EventPlugin) Version() string {
	return "v0.0.1"
}

func (p *EventPlugin) Load(meta api.PluginMeta) error {
	if meta.Config == "" {
		return fmt.Errorf("no config file specified")
	}

	content, err := os.ReadFile(meta.Config)
	if err != nil {
		return err
	}

	if err := yaml.Unmarshal(content, &p.config); err != nil {
		return err
	}

	log.Infoln("Event plugin is initialized.")
	log.Tracef("Event plugin config: %v", p.config)

	return nil
}

func (p *EventPlugin) Unload(meta api.PluginMeta) error {
	log.Infoln("Event plugin is unloaded.")
	return nil
}

func (p *EventPlugin) StartHook(ctx *api.PluginContext) {
	log.Infoln("StartHook is called!")
}

func (p *EventPlugin) EndHook(ctx *api.PluginContext) {
	log.Infoln("EndHook is called!")

}

func (p *EventPlugin) CreateCgroupHook(ctx *api.PluginContext) {
	log.Infoln("CreateCgroupHook is called!")
}

func (p *EventPlugin) DestroyCgroupHook(ctx *api.PluginContext) {
	log.Infoln("DestroyCgroupHook is called!")
}

func (p EventPlugin) InsertEventHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.InsertEventHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected InsertEventHook.")
		return
	}

	dbConfig := p.Database
	p.client = influxdb2.NewClientWithOptions(dbConfig.Url, dbConfig.Token,
		 influxdb2.DefaultOptions().SetPrecision(time.Nanosecond))
	defer p.client.Close()

	influxdbCtx := context.Background()
	if pong, err := p.client.Ping(influxdbCtx); err != nil {
		log.Errorf("Failed to ping InfluxDB: %v", err)
		return
	} else if !pong {
		log.Error("Failed to ping InfluxDB: not pong")
		return
	}
	log.Infof("InfluxDB client is created: %v", p.client.ServerURL())

	writer := p.client.WriteAPIBlocking(dbConfig.Org, dbConfig.Bucket)
	for _, event := range req.GetEventInfoList()  {
		tags := map[string]string{
			"cluster_name":   event.ClusterName,
			"node_name":    event.NodeName,
		}
		fields := map[string]interface{}{
			"uid":          event.Uid,
			"start_time":   event.StartTime.AsTime().UnixNano(),
			"state":        int32(event.State),
			"reason":       event.Reason,
		}
		
		point := influxdb2.NewPoint(dbConfig.Measurement, tags, fields, time.Now())

		if err := writer.WritePoint(influxdbCtx, point); err != nil {
			log.Errorf("Failed to write point to InfluxDB: %v", err)
			break
		}

		log.Tracef("Recorded cluster_name: %v, uid: %v, node_name: %s, state: %d, start_time: %s, Reason: %s",
		event.ClusterName, event.Uid, event.NodeName, event.State, event.StartTime.AsTime().Format(time.RFC3339), event.Reason) 
	}
}