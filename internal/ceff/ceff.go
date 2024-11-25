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

package ceff

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"

	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

var (
	stub       protos.CraneCtldClient
	dataConfig *DatabaseConfig
)

// PluginConfig represents the structure of the plugin configuration in config.yaml
type PluginConfig struct {
	Name   string `yaml:"Name"`
	Path   string `yaml:"Path"`
	Config string `yaml:"Config"`
}

type CraneConfig struct {
	Plugin struct {
		Enabled           bool           `yaml:"Enabled"`
		PlugindSockPath   string         `yaml:"PlugindSockPath"`
		PlugindDebugLevel string         `yaml:"PlugindDebugLevel"`
		Plugins           []PluginConfig `yaml:"Plugins"`
	} `yaml:"Plugin"`
}

// DatabaseConfig represents the structure of the database configuration in monitor.yaml
type DatabaseConfig struct {
	Username    string `yaml:"Username"`
	Bucket      string `yaml:"Bucket"`
	Org         string `yaml:"Org"`
	Token       string `yaml:"Token"`
	Measurement string `yaml:"Measurement"`
	Url         string `yaml:"Url"`
}

type ResourceUsageRecord struct {
	TaskID      int64
	CPUUsage    uint64
	MemoryUsage uint64
	ProcCount   uint64
	Hostname    string
	Timestamp   time.Time
}

var isFirstCall = true //use for multi-job print

// GetInfluxdbParameter extracts the InfluxDB configuration from the specified YAML configuration files
func GetInfluxdbParameter(configFilePath string) *DatabaseConfig {
	confFile, err := os.ReadFile(configFilePath)
	if err != nil {
		log.Errorf("Failed to read config file %s: %v", configFilePath, err)
		os.Exit(util.ErrorCmdArg)
	}

	craneConfig := &CraneConfig{}
	err = yaml.Unmarshal(confFile, craneConfig)
	if err != nil {
		log.Errorf("Failed to read config file %s: %v", configFilePath, err)
		os.Exit(util.ErrorCmdArg)
	}

	var monitorConfigPath string
	for _, plugin := range craneConfig.Plugin.Plugins {
		if plugin.Name == "monitor" {
			monitorConfigPath = plugin.Config
			break
		}
	}

	if monitorConfigPath == "" {
		log.Errorf("no monitor plugin found in config.yaml")
		return nil
	}

	confFile, err = os.ReadFile(monitorConfigPath)
	if err != nil {
		log.Errorf("Failed to read config file %s: %v", configFilePath, err)
		os.Exit(util.ErrorCmdArg)
	}

	dbConfig := &struct {
		Database *DatabaseConfig `yaml:"Database"`
	}{}
	if err := yaml.Unmarshal(confFile, dbConfig); err != nil {
		log.Errorf("Monitor readYAMLFile config.yaml")
		return nil
	}
	if dbConfig.Database == nil {
		log.Errorf("Database section not found in YAML")
		return nil
	}

	return dbConfig.Database
}

func QueryDataByTags(moniterConfig *DatabaseConfig, jobIDs []uint32, hostNames []string) ([]*ResourceUsageRecord, error) {
	if len(hostNames) == 0 {
		return nil, fmt.Errorf("HostNames is empty, please check submit parameter")
	}
	client := influxdb2.NewClient(moniterConfig.Url, moniterConfig.Token)
	defer client.Close()

	ctx := context.Background()
	if pong, err := client.Ping(ctx); err != nil {
		log.Errorf("Failed to ping InfluxDB: %v", err)
		return nil, fmt.Errorf("Failed to ping InfluxDB: %v", err)
	} else if !pong {
		return nil, fmt.Errorf("Failed to ping InfluxDB: not pong")
	}

	jobIDFilters := make([]string, len(jobIDs))
	for i, id := range jobIDs {
		jobIDFilters[i] = fmt.Sprintf(`r["job_id"] == "%d"`, id) //Convert to Flux query string
	}
	jobIDCondition := strings.Join(jobIDFilters, " or ")

	// Build hostname filter conditions
	hostnameFilters := make([]string, len(hostNames))
	for i, hostname := range hostNames {
		hostnameFilters[i] = fmt.Sprintf(`r["hostname"] == "%s"`, hostname)
	}
	hostnameCondition := strings.Join(hostnameFilters, " or ")

	fluxQuery := fmt.Sprintf(`
	  from(bucket: "%s")
  	  |> range(start: 0)
      |> filter(fn: (r) => r["_measurement"] == "ResourceUsage" and (%s) and (%s))
      |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> group(columns: ["job_id", "hostname"])
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 1)`, moniterConfig.Bucket, jobIDCondition, hostnameCondition)

	// Execute the query
	queryAPI := client.QueryAPI(moniterConfig.Org)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		log.Printf("Failed to execute query: %v", err)
		return nil, fmt.Errorf("query failed: %w", err)
	}

	var records []*ResourceUsageRecord
	for result.Next() {
		record := result.Record()

		//Extract job_id
		jobIDStr, ok := record.ValueByKey("job_id").(string)
		if !ok {
			log.Printf("Invalid type for job_id")
			continue
		}
		jobID, err := strconv.ParseInt(jobIDStr, 10, 64)
		if err != nil {
			log.Printf("Failed to parse job_id: %v", err)
			continue
		}

		// Extract hostname
		hostname, ok := record.ValueByKey("hostname").(string)
		if !ok {
			log.Printf("Invalid type for hostname")
			continue
		}

		// Extract proc_count
		procCount, ok := record.ValueByKey("proc_count").(uint64)
		if !ok {
			log.Printf("Invalid type for proc_count")
			continue
		}

		// Extract cpu_usage
		cpuUsage, ok := record.ValueByKey("cpu_usage").(uint64)
		if !ok {
			log.Printf("Invalid type for cpu_usage")
			continue
		}

		//Extract memory_usage
		memoryUsage, ok := record.ValueByKey("memory_usage").(uint64)
		if !ok {
			log.Printf("Invalid type for memory_usage")
			continue
		}

		//Assembly results
		records = append(records, &ResourceUsageRecord{
			TaskID:      jobID,
			Hostname:    hostname,
			ProcCount:   procCount,
			CPUUsage:    cpuUsage,
			MemoryUsage: memoryUsage,
			Timestamp:   record.Time(),
		})
	}

	if result.Err() != nil {
		log.Printf("Query parsing error: %v", result.Err())
		return nil, fmt.Errorf("query parsing error: %w", result.Err())
	}

	return records, nil
}

// FilterRecordsByJobID filters records by a specific JobID
func CalculateTotalUsagePtr(records []*ResourceUsageRecord, targetJobID int64) (float64, float64) {
	var filteredRecords []*ResourceUsageRecord
	for _, record := range records {
		if record != nil && record.TaskID == targetJobID {
			filteredRecords = append(filteredRecords, record)
		}
	}

	var totalCPUUseNs, totalMemoryByte uint64
	var totalCPUUseS, totalMemoryMb float64
	for _, record := range filteredRecords {
		if record != nil {
			totalCPUUseNs += record.CPUUsage
			totalMemoryByte += record.MemoryUsage
		}
	}
	totalCPUUseS = float64(totalCPUUseNs) / 1e9
	totalMemoryMb = float64(totalMemoryByte) / (1024 * 1024)

	return totalCPUUseS, totalMemoryMb
}

func calculateRunTime(taskInfo *protos.TaskInfo) (uint64, string) {
	if taskInfo.Status == protos.TaskStatus_Running {
		duration := taskInfo.ElapsedTime.AsDuration()
		return uint64(duration.Seconds()), formatDuration(duration)
	}
	start := taskInfo.StartTime.AsTime()
	end := taskInfo.EndTime.AsTime()
	if end.Before(start) {
		log.Warn("Invalid time range: end is before start")
		return 0, "0-0:0:0"
	}
	duration := end.Sub(start)
	return uint64(duration.Seconds()), formatDuration(duration)
}

func formatDuration(duration time.Duration) string {
	totalSeconds := int(duration.Seconds())
	days := totalSeconds / (24 * 3600)
	hours := (totalSeconds % (24 * 3600)) / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60

	if days > 0 {
		return fmt.Sprintf("%d-%02d:%02d:%02d", days, hours, minutes, seconds)
	}
	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
}

func extractNodeNames(taskInfos []*protos.TaskInfo) ([]string, error) {
	var nodeNames []string
	for _, taskInfo := range taskInfos {
		nodes, ok := util.ParseHostList(taskInfo.GetCranedList())
		if !ok {
			return nil, fmt.Errorf("invalid node pattern: %s", taskInfo.GetCranedList())
		}
		nodeNames = append(nodeNames, nodes...)
	}
	return nodeNames, nil
}

// Print task information
func printTaskInfo(taskInfo *protos.TaskInfo, records []*ResourceUsageRecord) error {
	// Obtain user and group information
	craneUser, err := user.LookupId(strconv.Itoa(int(taskInfo.Uid)))
	if err != nil {
		return fmt.Errorf("failed to get username for UID %d: %w", taskInfo.Uid, err)
	}
	group, err := user.LookupGroupId(strconv.Itoa(int(taskInfo.Gid)))
	if err != nil {
		return fmt.Errorf("failed to get groupname for GID %d: %w", taskInfo.Gid, err)
	}
	if !isFirstCall {
		fmt.Printf("\n")
	} else {
		isFirstCall = false
	}

	fmt.Printf(
		"JobId: %v\n"+
			"Qos: %v\n"+
			"User/Group: %s(%d)/%s(%d)\n"+
			"Account: %v\n",
		taskInfo.TaskId, taskInfo.Qos, craneUser.Username, taskInfo.Uid, group.Name, taskInfo.Gid,
		taskInfo.Account)

	if taskInfo.Status == protos.TaskStatus_Pending || taskInfo.Status == protos.TaskStatus_Running {
		fmt.Printf("JobState: %v\n", taskInfo.Status.String())
	} else {
		fmt.Printf("JobState: %v (exit code %d)\n", taskInfo.Status.String(), taskInfo.ExitCode)
	}

	fmt.Printf(
		"Cores: %.2f\n"+
			"Nodes: %v\n"+
			"Cores per node: %.2f\n",
		taskInfo.ResView.AllocatableRes.CpuCoreLimit*float64(taskInfo.NodeNum),
		taskInfo.NodeNum, taskInfo.ResView.AllocatableRes.CpuCoreLimit)

	if taskInfo.Status == protos.TaskStatus_Pending {
		fmt.Printf("Efficiency not available for jobs in the PENDING state.\n")
		return nil
	}

	// Calculate running time
	runTime, runTimeStr := calculateRunTime(taskInfo)

	// Filter task records
	totalCPUUseS, totalMemoryMb := CalculateTotalUsagePtr(records, int64(taskInfo.TaskId))

	cPUUtilizedStr := "0-0:0:0"
	if taskInfo.Status == protos.TaskStatus_Running || taskInfo.Status == protos.TaskStatus_Completed {
		cPUUtilizedStr = formatDuration(time.Duration(totalCPUUseS) * time.Second)
	}

	cPUEfficiency := 0.0
	if runTime != 0 {
		cPUEfficiency = totalCPUUseS / float64(runTime) * 100
	}

	// Calculate mem efficiency
	memEfficiency := 0.0
	mallocMemoryMbNode := taskInfo.ResView.AllocatableRes.MemoryLimitBytes / (1024 * 1024)
	totalMallocMemoryMb := mallocMemoryMbNode * uint64(taskInfo.NodeNum)
	if totalMallocMemoryMb != 0 {
		memEfficiency = float64(totalMemoryMb) / float64(totalMallocMemoryMb) * 100
	}

	// Print job information
	fmt.Printf(
		"CPU Utilized: %s\n"+
			"CPU Efficiency: %.2f%% of %s core-walltime\n"+
			"Job Wall-clock time: %s\n"+
			"Memory Utilized: %v MB (estimated maximum)\n"+
			"Memory Efficiency: %.2f%% of %.2f GB (%.2f GB/node)\n",
		cPUUtilizedStr, cPUEfficiency, runTimeStr, runTimeStr, totalMemoryMb,
		memEfficiency, float64(totalMallocMemoryMb)/1024, float64(mallocMemoryMbNode)/1024)

	if taskInfo.Status == protos.TaskStatus_Running {
		fmt.Printf("WARNING: Efficiency statistics may be misleading for RUNNING jobs.\n")
	}

	return nil
}

func findNotFoundJobs(jobIdList []uint32, printed map[uint32]bool) []uint32 {
	var notFoundJobs []uint32
	for _, jobID := range jobIdList {
		if !printed[jobID] {
			notFoundJobs = append(notFoundJobs, jobID)
		}
	}
	return notFoundJobs
}

func QueryTasksInfoByIds(jobIds string) util.CraneCmdError {
	jobIdList, err := util.ParseJobIdList(jobIds, ",")
	if err != nil {
		log.Errorf("Invalid job list specified: %v", err)
		return util.ErrorCmdArg
	}

	req := &protos.QueryTasksInfoRequest{FilterTaskIds: jobIdList}
	if stub == nil {
		log.Fatal("gRPC client stub is not initialized")
	}
	reply, err := stub.QueryTasksInfoByIds(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query job info")
		return util.ErrorNetwork
	}

	nodeNames, err := extractNodeNames(reply.TaskInfoList)
	if err != nil {
		log.Errorf("Failed to extract node names: %v", err)
		return util.ErrorCmdArg
	}

	// Query InfluxDB data
	result, err := QueryDataByTags(dataConfig, jobIdList, nodeNames)
	if err != nil {
		log.Errorf("Failed to query job info from InfluxDB: %v", err)
		return util.ErrorNetwork
	}

	// Print task information
	printed := map[uint32]bool{}
	for _, taskInfo := range reply.TaskInfoList {
		if err := printTaskInfo(taskInfo, result); err != nil {
			log.Warnf("Error processing task %v: %v", taskInfo.TaskId, err)
		} else {
			printed[taskInfo.TaskId] = true
		}
	}

	// Prompt for tasks not found
	notFoundJobs := findNotFoundJobs(jobIdList, printed)
	if len(notFoundJobs) > 0 {
		fmt.Printf("Job %v does not exist.\n", notFoundJobs)
	}

	return util.ErrorSuccess
}
