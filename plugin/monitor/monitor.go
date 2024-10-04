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
	client influxdb2.Client
	once   sync.Once          // Ensure the Singleton of InfluxDB client
	buffer chan ResourceUsage // Buffer channel for processing usage data
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

func getRealCgroupPath(pattern string, cgroupName string) string {
	return strings.ReplaceAll(pattern, "%j", cgroupName)
}

func validateCgroup(cgroupPath string) bool {
	_, err := os.Stat(cgroupPath)
	return !os.IsNotExist(err)
}

func (p *MonitorPlugin) producer(id int64, cgroup string) {
	log.Infof("Monitoring goroutine for job #%v started.", id)

	if (p.buffer == nil) || (p.client == nil) {
		log.Errorf("Buffer channel or InfluxDB client not initialized.")
		return
	}

	cgroupProcListPath := getRealCgroupPath(p.config.Cgroup.ProcList, cgroup)
	cgroupCpuPath := getRealCgroupPath(p.config.Cgroup.CPU, cgroup)
	cgroupMemPath := getRealCgroupPath(p.config.Cgroup.Memory, cgroup)

	for {
		// If the cgroup is not found, the job is finished, break
		if !validateCgroup(cgroupCpuPath) {
			break
		}

		// If none pid found, the cgroup is empty, break
		procCount, err := getProcCount(cgroupProcListPath)
		if err != nil {
			log.Errorf("Failed to get process count in %s: %v", cgroupProcListPath, err)
			break
		}
		if procCount == 0 {
			break
		}

		cpuUsage, err := getCpuUsage(cgroupCpuPath)
		if err != nil {
			log.Errorf("Failed to get CPU usage in %s: %v", cgroupCpuPath, err)
			break
		}

		memoryUsage, err := getMemoryUsage(cgroupMemPath)
		if err != nil {
			log.Errorf("Failed to get memory usage in %s: %v", cgroupMemPath, err)
			break
		}

		p.buffer <- ResourceUsage{
			TaskID:      id,
			ProcCount:   procCount,
			CPUUsage:    cpuUsage,
			MemoryUsage: memoryUsage,
			Hostname:    p.hostname,
			Timestamp:   time.Now(),
		}

		// Sleep for the interval
		time.Sleep(time.Duration(p.Interval) * time.Millisecond)
	}

	log.Infof("Monitoring goroutine for job #%v exited.", id)
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
}

func (p *MonitorPlugin) Init(meta api.PluginMeta) error {
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

	// Apply default values, use cgroup v1 path
	if p.config.Cgroup.CPU == "" {
		p.config.Cgroup.CPU = "/sys/fs/cgroup/cpuacct/%j/cpuacct.usage"
		log.Warnf("CPU cgroup path is not specified, using default: %s", p.config.Cgroup.CPU)
	}
	if p.config.Cgroup.Memory == "" {
		p.config.Cgroup.Memory = "/sys/fs/cgroup/memory/%j/memory.usage_in_bytes"
		log.Warnf("Memory cgroup path is not specified, using default: %s", p.config.Cgroup.Memory)
	}

	log.Infoln("Monitor plugin is initialized.")
	log.Tracef("Monitor plugin config: %v", p.config)

	return nil
}

func (p *MonitorPlugin) Name() string {
	return "Monitor"
}

func (p *MonitorPlugin) Version() string {
	return "v0.0.1"
}

func (p *MonitorPlugin) JobMonitorHook(ctx *api.PluginContext) {
	log.Traceln("JobMonitorHook is called!")
	req, ok := ctx.Request().(*protos.JobMonitorHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected JobMonitorHookRequest.")
		return
	}
	log.Tracef("JobMonitorHookReq: \n%v", req.String())

	// Start consumer (only once)
	p.once.Do(func() {
		go p.consumer()
	})

	// Start producer goroutine to monitor the resource usage
	go p.producer(int64(req.TaskId), req.Cgroup)
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
