package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"
	"os"
	"time"

	"github.com/google/uuid"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"gopkg.in/yaml.v3"

	log "github.com/sirupsen/logrus"
)

// Compile-time check to ensure MonitorPlugin implements api.Plugin
var _ api.Plugin = &MonitorPlugin{}

// PluginD will call plugin's method thru this variable
var PluginInstance = MonitorPlugin{}

var (
	// Cgroup path prefix
	cgroupCPUPathPrefix    = "/sys/fs/cgroup/cpu/"
	cgroupMemoryPathPrefix = "/sys/fs/cgroup/memory/"

	// Hostname
	hostname = ""
)

type config struct {
	// Cgroup root directory path
	Cgroup struct {
		CPU    string `yaml:"CPU"`
		Memory string `yaml:"Memory"`
	} `yaml:"Cgroup"`

	// InfluxDB configuration
	Database struct {
		Username string `yaml:"Username"`
		Bucket   string `yaml:"Bucket"`
		Org      string `yaml:"Org"`
		Token    string `yaml:"Token"`
		Url      string `yaml:"Url"`
	} `yaml:"Database"`

	// Interval for sampling resource usage, in ms
	Interval uint32 `yaml:"Interval"`
}

type ResourceUsage struct {
	TaskID      int64
	CPUUsage    float64
	MemoryUsage uint64
	Hostname    string
	Timestamp   time.Time
	UniqueTag   string
}

// Channel for sending resource usage data
var statChan = make(chan ResourceUsage)

type MonitorPlugin struct {
	config
	client influxdb2.Client
	once   sync.Once // Ensure the Singleton of InfluxDB client
}

// Dummy implementations
func (dp *MonitorPlugin) StartHook(ctx *api.PluginContext) {}
func (dp *MonitorPlugin) EndHook(ctx *api.PluginContext)   {}

func getCpuUsage(cgroupPath string) (float64, error) {
	cpuUsageFile := fmt.Sprintf("%s/cpuacct.usage", cgroupPath)
	content, err := os.ReadFile(cpuUsageFile)
	if err != nil {
		return 0, err
	}

	startUsage, err := strconv.ParseUint(strings.TrimSpace(string(content)), 10, 64)
	if err != nil {
		return 0, err
	}

	time.Sleep(1 * time.Second)

	content, err = os.ReadFile(cpuUsageFile)
	if err != nil {
		return 0, err
	}

	endUsage, err := strconv.ParseUint(strings.TrimSpace(string(content)), 10, 64)
	if err != nil {
		return 0, err
	}

	cpuUsage := float64(endUsage-startUsage) / 1e9 * 100
	return cpuUsage, nil
}

func getMemoryUsage(cgroupPath string) (uint64, error) {
	memoryUsageFile := fmt.Sprintf("%s/memory.usage_in_bytes", cgroupPath)
	content, err := os.ReadFile(memoryUsageFile)
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

	log.Tracef("InfluxDB client is created: %v", p.client.ServerURL())

	writer := p.client.WriteAPIBlocking(dbConfig.Org, dbConfig.Bucket)
	for stat := range statChan {
		point := influxdb2.NewPoint(
			strconv.FormatInt(stat.TaskID, 10),
			map[string]string{
				"username":   dbConfig.Username,
				"hostname":   stat.Hostname,
				"unique_tag": stat.UniqueTag,
			},
			map[string]interface{}{
				"cpu_usage":    stat.CPUUsage,
				"memory_usage": stat.MemoryUsage,
			},
			stat.Timestamp,
		)

		if err := writer.WritePoint(ctx, point); err != nil {
			log.Errorf("Failed to write point to InfluxDB: %v", err)
			break
		}

		log.Infof("Recorded TaskID: %d, UserName: %s, Hostname: %s, CPU Usage: %.2f%%, Memory Usage: %.2fMB at %v",
			stat.TaskID, dbConfig.Username, stat.Hostname, stat.CPUUsage, float64(stat.MemoryUsage)/(1024*1024), stat.Timestamp)
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

	hostname, err = getHostname()
	if err != nil {
		return err
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

	// Start consumer
	p.once.Do(func() {
		go p.consumer()
	})

	// Producer goroutine to monitor resource usage
	go func(req *protos.JobMonitorHookRequest) {
		log.Tracef("Monitoring goroutine for job #%v started.", req.TaskId)

		cgroupPathCpu := cgroupCPUPathPrefix + req.Cgroup
		cgroupPathMem := cgroupMemoryPathPrefix + req.Cgroup

		for {
			cpuUsage, err := getCpuUsage(cgroupPathCpu)
			if err != nil {
				log.Errorf("Failed to get CPU usage for cgroup %s: %v", cgroupPathCpu, err)
				break
			}

			memoryUsage, err := getMemoryUsage(cgroupPathMem)
			if err != nil {
				log.Errorf("Failed to get memory usage for cgroup %s: %v", cgroupPathMem, err)
				break
			}

			currentTime := time.Now()
			uniqueTag := uuid.New().String()

			statChan <- ResourceUsage{
				TaskID:      int64(req.TaskId),
				CPUUsage:    cpuUsage,
				MemoryUsage: memoryUsage,
				Hostname:    hostname,
				Timestamp:   currentTime,
				UniqueTag:   uniqueTag,
			}

			time.Sleep(time.Duration(p.Interval) * time.Millisecond)
		}
	}(req)
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
