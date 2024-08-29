// This is a MonitorPlugin for XXXXX.
package main

import (
	"context"
	"fmt"

	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"
	"os"
	"os/exec"
	"strconv"
	"strings"
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

// cgroup cpu path & memory path
var (
	cgroupCpuPathPrefix    = "/sys/fs/cgroup/cpu/"
	cgroupMemoryPathPrefix = "/sys/fs/cgroup/memory/"
)

type config struct {
	UserName string `yaml:"UserName"`
	Bucket   string `yaml:"Bucket"`
	Org      string `yaml:"Org"`
	Token    string `yaml:"Token"`
	Url      string `yaml:"Url"`
	Interval uint32 `yaml:"Interval"`
}

type MonitorPlugin struct {
	config
}

func (dp *MonitorPlugin) Init(meta api.PluginMeta) error {
	if meta.Config == "" {
		return fmt.Errorf("no config file specified")
	}

	content, err := os.ReadFile(meta.Config)
	if err != nil {
		return err
	}

	if err := yaml.Unmarshal(content, &dp.config); err != nil {
		return err
	}

	log.Infoln("Monitor plugin is initialized.")
	log.Tracef("Monitor plugin config: %v", dp.config)

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

// func (dp *MonitorPlugin) JobCheckHook(ctx *api.PluginContext) {
// 	log.Infoln("JobCheckHook is called!")

// 	req, ok := ctx.Request().(*protos.JobCheckHookRequest)
// 	if !ok {
// 		log.Errorln("Invalid request type, expected JobCheckHookRequest.")
// 		return
// 	}

// 	log.Tracef("JobCheckHookReq: \n%v", req.String())
// }

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
	cmd := exec.Command("hostname")
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func (dp *MonitorPlugin) JobCheckHook(ctx *api.PluginContext) {
	log.Infoln("JobCheckHook is called!")
	req, ok := ctx.Request().(*protos.JobCheckHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected JobCheckHookRequest.")
		return
	}
	log.Tracef("JobCheckHookReq: \n%v", req.String())

	jobCheckInfo := req.JobcheckInfoList[0]
	cgroupPathCpu := fmt.Sprintf("%s%s", cgroupCpuPathPrefix, jobCheckInfo.Cgroup)
	cgroupPathMem := fmt.Sprintf("%s%s", cgroupMemoryPathPrefix, jobCheckInfo.Cgroup)

	// create influxdb client
	client := influxdb2.NewClient(dp.Url, dp.Token)
	// client := influxdb2.NewClientWithOptions(dp.Url, dp.Token, influxdb2.DefaultOptions().SetPrecision(time.Duration(influxdb2.DefaultOptions().Precision().Microseconds())))
	fmt.Printf("InfluxDB URL: %s\n", dp.Url)
	defer client.Close()
	writeAPI := client.WriteAPIBlocking(dp.Org, dp.Bucket)
	hostname, err := getHostname()
	if err != nil {
		log.Errorf("Failed to get hostname: %v", err)
		return
	}

	// start to write data into influxdb
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

		p := influxdb2.NewPointWithMeasurement(fmt.Sprintf("%d", jobCheckInfo.Taskid)).
			AddTag("username", dp.UserName).
			AddTag("hostname", hostname).
			AddTag("unique_tag", uniqueTag).
			AddField("cpu_usage", cpuUsage).
			AddField("memory_usage", float64(memoryUsage)/(1024*1024)).
			SetTime(currentTime)
		err = writeAPI.WritePoint(context.Background(), p)
		if err != nil {
			log.Errorf("Failed to write point to InfluxDB: %v", err)
			break
		}
		log.Infof("Recorded TaskID: %d, UserName: %s, Hostname: %s, CPU Usage: %.2f%%, Memory Usage: %.2fMB at %v",
			jobCheckInfo.Taskid, dp.UserName, hostname, cpuUsage, float64(memoryUsage)/(1024*1024), currentTime)

		time.Sleep(time.Duration(dp.Interval) * time.Second)
	}

	log.Infoln("Exiting JobCheckHook loop due to an error or end")
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
