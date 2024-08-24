// This is a MonitorPlugin for XXXXX.
package main

import (
	"fmt"

	"CraneFrontEnd/api"
	"CraneFrontEnd/generated/protos"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// Compile-time check to ensure MonitorPlugin implements api.Plugin
var _ api.Plugin = &MonitorPlugin{}

// PluginD will call plugin's method thru this variable
var PluginInstance = MonitorPlugin{}

type MonitorPlugin struct {
	UserName     string `yaml:"UserName"`
	UserPassword string `yaml:"UserPassword"`
}

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
	content, err := os.ReadFile(cpuUsageFile) // Use os.ReadFile instead of ioutil.ReadFile
	if err != nil {
		return 0, err
	}

	startUsage, err := strconv.ParseUint(strings.TrimSpace(string(content)), 10, 64)
	if err != nil {
		return 0, err
	}

	time.Sleep(1 * time.Second)

	content, err = os.ReadFile(cpuUsageFile) // Use os.ReadFile instead of ioutil.ReadFile
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
	content, err := os.ReadFile(memoryUsageFile) // Use os.ReadFile instead of ioutil.ReadFile
	if err != nil {
		return 0, err
	}

	memoryUsage, err := strconv.ParseUint(strings.TrimSpace(string(content)), 10, 64)
	if err != nil {
		return 0, err
	}

	return memoryUsage, nil
}

func (dp *MonitorPlugin) JobCheckHook(ctx *api.PluginContext) {
	log.Infoln("JobCheckHook is called!")

	req, ok := ctx.Request().(*protos.JobCheckHookRequest)
	if !ok {
		log.Errorln("Invalid request type, expected JobCheckHookRequest.")
		return
	}

	log.Tracef("JobCheckHookReq: \n%v", req.String())

	for _, jobCheckInfo := range req.JobcheckInfoList {
		cgroupPath_cpu := fmt.Sprintf("/sys/fs/cgroup/cpu/%s", jobCheckInfo.Cgroup)
		cpuUsage, err := getCpuUsage(cgroupPath_cpu)
		if err != nil {
			log.Errorf("Failed to get CPU usage for cgroup %s: %v", cgroupPath_cpu, err)
			continue
		}
		cgroupPath_mem := fmt.Sprintf("/sys/fs/cgroup/memory/%s", jobCheckInfo.Cgroup)
		memoryUsage, err := getMemoryUsage(cgroupPath_mem)
		if err != nil {
			log.Errorf("Failed to get memory usage for cgroup %s: %v", cgroupPath_mem, err)
			continue
		}

		log.Infof("TaskID: %d, Cgroup: %s, CPU Usage: %.2f%%, Memory Usage: %.2fMB",
			jobCheckInfo.Taskid, jobCheckInfo.Cgroup, cpuUsage, float64(memoryUsage)/(1024*1024))
	}
}

func main() {
	log.Fatal("This is a plugin, should not be executed directly.\n" +
		"Please build it as a shared object (.so) and load it with the plugin daemon.")
}
