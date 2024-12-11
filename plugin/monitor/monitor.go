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
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"

	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"
	"os"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"gopkg.in/yaml.v3"

	log "github.com/sirupsen/logrus"
)

// Compile-time check to ensure MonitorPlugin implements api.Plugin
var _ api.Plugin = &MonitorPlugin{}

// PluginD will call plugin's method thru this variable
var PluginInstance = MonitorPlugin{}

type config struct {
	// Cgroup pattern
	Cgroup struct {
		CPU      string `yaml:"CPU"`
		Memory   string `yaml:"Memory"`
		ProcList string `yaml:"ProcList"`
	} `yaml:"Cgroup"`

	// InfluxDB configuration
	Database struct {
		Username    string `yaml:"Username"`
		Bucket      string `yaml:"Bucket"`
		Org         string `yaml:"Org"`
		Measurement string `yaml:"Measurement"`
		Token       string `yaml:"Token"`
		Url         string `yaml:"Url"`
	} `yaml:"Database"`

	// Interval for sampling resource usage, in ms
	Interval uint32 `yaml:"Interval"`
	// Buffer size for storing resource usage data
	BufferSize uint32 `yaml:"BufferSize"`

	// Hostname, set at Init time, not configurable
	hostname string
}

type ResourceUsage struct {
	TaskID      int64
	CPUUsage    uint64
	MemoryUsage uint64
	ProcCount   uint64
	Hostname    string
	Timestamp   time.Time
}

type MonitorPlugin struct {
	config
	client   influxdb2.Client
	cond     *sync.Cond         // Sync consumer
	once     sync.Once          // Ensure the Singleton of InfluxDB client
	buffer   chan ResourceUsage // Buffer channel for processing usage data
	jobCtx   map[int64]context.CancelFunc
	jobMutex sync.RWMutex
}

// Dummy implementations
func (dp *MonitorPlugin) StartHook(ctx *api.PluginContext) {}
func (dp *MonitorPlugin) EndHook(ctx *api.PluginContext)   {}

func getProcCount(cgroupPath string) (uint64, error) {
	content, err := os.ReadFile(cgroupPath)
	if err != nil {
		return 0, err
	}

	// return the line count
	return uint64(strings.Count(string(content), "\n")), nil
}

func getCpuUsage(cgroupPath string) (uint64, error) {
	content, err := os.ReadFile(cgroupPath)
	if err != nil {
		return 0, err
	}

	cpuUsage, err := strconv.ParseUint(strings.TrimSpace(string(content)), 10, 64)
	if err != nil {
		return 0, err
	}

	return cpuUsage, nil
}

func getMemoryUsage(cgroupPath string) (uint64, error) {
	content, err := os.ReadFile(cgroupPath)
	if err != nil {
		return 0, err
	}

	memoryUsage, err := strconv.ParseUint(strings.TrimSpace(string(content)), 10, 64)
	if err != nil {
		return 0, err
	}

	return memoryUsage, nil
}

func getHostname() (string, error) {
	content, err := os.ReadFile("/etc/hostname")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(content)), nil
}

func getRealCgroupPath(pattern string, task_name string) string {
	return strings.ReplaceAll(pattern, "%j", task_name)
}

func (p *MonitorPlugin) producer(ctx context.Context, id int64, task_name string) {
	log.Infof("Monitoring goroutine for job #%v started.", id)

	if (p.buffer == nil) || (p.client == nil) {
		log.Errorf("Buffer channel or InfluxDB client not initialized.")
		return
	}

	// As Cgroup may be created without any process inside, we need to mark
	// if the first process is launched.
	migrated := false
	cgroupProcListPath := getRealCgroupPath(p.config.Cgroup.ProcList, task_name)
	cgroupCpuPath := getRealCgroupPath(p.config.Cgroup.CPU, task_name)
	cgroupMemPath := getRealCgroupPath(p.config.Cgroup.Memory, task_name)

	for {
		select {
		case <-ctx.Done():
			log.Infof("Monitoring goroutine for job #%v cancelled.", id)
			return
		default:
			// If none pid found, the cgroup is empty, continue
			procCount, err := getProcCount(cgroupProcListPath)
			if err != nil {
				log.Errorf("Failed to get process count in %s: %v", cgroupProcListPath, err)
				continue
			}
			if !migrated && procCount == 0 {
				time.Sleep(time.Duration(p.Interval) * time.Millisecond)
				continue
			}
			migrated = true

			cpuUsage, err := getCpuUsage(cgroupCpuPath)
			if err != nil {
				log.Errorf("Failed to get CPU usage in %s: %v", cgroupCpuPath, err)
				continue
			}

			memoryUsage, err := getMemoryUsage(cgroupMemPath)
			if err != nil {
				log.Errorf("Failed to get memory usage in %s: %v", cgroupMemPath, err)
				continue
			}

			p.buffer <- ResourceUsage{
				TaskID:      id,
				ProcCount:   procCount,
				CPUUsage:    cpuUsage,
				MemoryUsage: memoryUsage,
				Hostname:    p.hostname,
				Timestamp:   time.Now(),
			}
		}

		// Sleep for the interval
		time.Sleep(time.Duration(p.Interval) * time.Millisecond)
	}
}

func (p *MonitorPlugin) consumer() {
	dbConfig := p.Database
	p.client = influxdb2.NewClientWithOptions(dbConfig.Url, dbConfig.Token, influxdb2.DefaultOptions().SetPrecision(time.Nanosecond))
	defer p.client.Close()

	ctx := context.Background()
	if pong, err := p.client.Ping(ctx); err != nil {
		log.Errorf("Failed to ping InfluxDB: %v", err)
		return
	} else if !pong {
		log.Error("Failed to ping InfluxDB: not pong")
		return
	}
	log.Infof("InfluxDB client is created: %v", p.client.ServerURL())

	writer := p.client.WriteAPIBlocking(dbConfig.Org, dbConfig.Bucket)
	p.cond = sync.NewCond(&sync.Mutex{})
	for stat := range p.buffer {
		tags := map[string]string{
			"job_id":   strconv.FormatInt(stat.TaskID, 10),
			"hostname": stat.Hostname,
		}
		fields := map[string]interface{}{
			"proc_count":   stat.ProcCount,
			"cpu_usage":    stat.CPUUsage,
			"memory_usage": stat.MemoryUsage,
		}
		point := influxdb2.NewPoint(dbConfig.Measurement, tags, fields, stat.Timestamp)

		if err := writer.WritePoint(ctx, point); err != nil {
			log.Errorf("Failed to write point to InfluxDB: %v", err)
			break
		}

		log.Tracef("Recorded Job ID: %v, Hostname: %s, Proc Count: %d, CPU Usage: %d, Memory Usage: %.2f KB at %v",
			stat.TaskID, stat.Hostname, stat.ProcCount, stat.CPUUsage, float64(stat.MemoryUsage)/1024, stat.Timestamp)
	}

	// consumer is done, signal to exit
	p.cond.L.Lock()
	p.cond.Broadcast()
	p.cond.L.Unlock()
}

func (p *MonitorPlugin) Name() string {
	return "Monitor"
}

func (p *MonitorPlugin) Version() string {
	return "v0.0.1"
}

func (p *MonitorPlugin) Load(meta api.PluginMeta) error {
	if meta.Config == "" {
		return errors.New("config file is not specified")
	}

	content, err := os.ReadFile(meta.Config)
	if err != nil {
		return err
	}

	if err := yaml.Unmarshal(content, &p.config); err != nil {
		return err
	}

	p.hostname, err = getHostname()
	if err != nil {
		return err
	}

	if p.BufferSize <= 0 {
		p.BufferSize = 32
		log.Warnf("Buffer size is not specified or invalid, using default: %v", p.BufferSize)
	}
	p.buffer = make(chan ResourceUsage, p.BufferSize)
	p.jobCtx = make(map[int64]context.CancelFunc)

	// Apply default values, use cgroup v1 path
	if p.config.Cgroup.CPU == "" {
		p.config.Cgroup.CPU = "/sys/fs/cgroup/cpuacct/%j/cpuacct.usage"
		log.Warnf("CPU cgroup path is not specified, using default: %s", p.config.Cgroup.CPU)
	}
	if p.config.Cgroup.Memory == "" {
		p.config.Cgroup.Memory = "/sys/fs/cgroup/memory/%j/memory.usage_in_bytes"
		log.Warnf("Memory cgroup path is not specified, using default: %s", p.config.Cgroup.Memory)
	}
	if p.config.Cgroup.ProcList == "" {
		p.config.Cgroup.ProcList = "/sys/fs/cgroup/memory/%j/cgroup.procs"
		log.Warnf("ProcList cgroup path is not specified, using default: %s", p.config.Cgroup.ProcList)
	}

	log.Infoln("Monitor plugin is initialized.")
	log.Tracef("Monitor plugin config: %v", p.config)

	return nil
}

func (p *MonitorPlugin) Unload(meta api.PluginMeta) error {
	// Cancel all monitoring goroutines
	p.jobMutex.Lock()
	for _, cancel := range p.jobCtx {
		cancel()
	}
	p.jobMutex.Unlock()

	// Close channel, then consumer closes the InfluxDB client
	close(p.buffer)

	// Wait for consumer to finish
	if p.cond != nil {
		p.cond.L.Lock()
		p.cond.Wait()
		p.cond.L.Unlock()
	}

	log.Infoln("Monitor plugin gracefully unloaded.")
	return nil
}

func (p *MonitorPlugin) CreateCgroupHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.CreateCgroupHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected CreateCgroupHookRequest.")
		return
	}

	p.once.Do(func() {
		go p.consumer()
	})

	monitorCtx, cancel := context.WithCancel(context.Background())

	p.jobMutex.Lock()
	p.jobCtx[int64(req.TaskId)] = cancel
	p.jobMutex.Unlock()

	go p.producer(monitorCtx, int64(req.TaskId), req.Cgroup)
}

func (p *MonitorPlugin) DestroyCgroupHook(ctx *api.PluginContext) {
	req, ok := ctx.Request().(*protos.DestroyCgroupHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected DestroyCgroupHookRequest.")
		return
	}

	p.jobMutex.Lock()
	if cancel, exists := p.jobCtx[int64(req.TaskId)]; exists {
		cancel()
		delete(p.jobCtx, int64(req.TaskId))
	}
	p.jobMutex.Unlock()

	log.Infof("Monitoring stopped for job #%v", req.TaskId)
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
