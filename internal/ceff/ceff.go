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
	"encoding/json"
	"fmt"
	"math"
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
	stub     protos.CraneCtldClient
	dbConfig *InfluxDbConfig
)

// InfluxDB Config represents the structure of the database configuration in monitor.yaml
type InfluxDbConfig struct {
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

type CeffTaskInfo struct {
	JobID              uint32            `json:"job_id"`
	QoS                string            `json:"qos"`
	UID                uint32            `json:"uid"`
	UserName           string            `json:"user_name"`
	GID                uint32            `json:"gid"`
	GroupName          string            `json:"group_name"`
	Account            string            `json:"account"`
	JobState           protos.TaskStatus `json:"job_state"`
	Nodes              uint32            `json:"nodes"`
	CoresPerNode       float64           `json:"cores_per_node"`
	CPUUtilizedStr     string            `json:"cpu_utilized_str"`
	CPUEfficiency      float64           `json:"cpu_efficiency"`
	RunTimeStr         string            `json:"run_time_str"`
	TotalMemMB         float64           `json:"total_mem_mb"`
	MemEfficiency      float64           `json:"mem_efficiency"`
	TotalMallocMemMB   float64           `json:"total_malloc_mem_mb"`
	MallocMemMBPerNode float64           `json:"malloc_mem_mb_per_node"`
}

var isFirstCall = true //Used for multi-job print

// Extracts the InfluxDB configuration from the specified YAML configuration files
func GetInfluxDbConfig(config *util.Config) (*InfluxDbConfig, util.CraneCmdError) {
	if !config.Plugin.Enabled {
		log.Errorf("Plugin is not enabled")
		return nil, util.ErrorCmdArg
	}

	var monitorConfigPath string
	for _, plugin := range config.Plugin.Plugins {
		if plugin.Name == "monitor" {
			monitorConfigPath = plugin.Config
			break
		}
	}

	if monitorConfigPath == "" {
		log.Errorf("Monitor plugin not found")
		return nil, util.ErrorCmdArg
	}

	confFile, err := os.ReadFile(monitorConfigPath)
	if err != nil {
		log.Errorf("Failed to read config file %s: %v.", monitorConfigPath, err)
		return nil, util.ErrorCmdArg
	}

	dbConf := &struct {
		Database *InfluxDbConfig `yaml:"Database"`
	}{}
	if err := yaml.Unmarshal(confFile, dbConf); err != nil {
		log.Errorf("Failed to parse YAML config file: %v", err)
		return nil, util.ErrorCmdArg
	}
	if dbConf.Database == nil {
		log.Errorf("Database section not found in YAML")
		return nil, util.ErrorCmdArg
	}

	return dbConf.Database, util.ErrorSuccess
}

func QueryInfluxDbDataByTags(moniterConfig *InfluxDbConfig, jobIDs []uint32, hostNames []string) ([]*ResourceUsageRecord, error) {
	if len(hostNames) == 0 {
		return nil, fmt.Errorf("job not found")
	}
	client := influxdb2.NewClient(moniterConfig.Url, moniterConfig.Token)
	defer client.Close()

	ctx := context.Background()
	if pong, err := client.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping InfluxDB: %v", err)
	} else if !pong {
		return nil, fmt.Errorf("failed to ping InfluxDB: not pong")
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

	// Construct the Flux query
	fluxQuery := fmt.Sprintf(`
	from(bucket: "%s")
	|> range(start: 0)
	|> filter(fn: (r) => 
	    r["_measurement"] == "%s" and 
		(r["_field"] == "cpu_usage" or r["_field"] == "memory_usage") and
		(%s) and (%s))
	|> group(columns: ["job_id", "hostname", "_field"])
	|> max(column: "_value")`, moniterConfig.Bucket,
		moniterConfig.Measurement, jobIDCondition, hostnameCondition)

	// Execute the query
	queryAPI := client.QueryAPI(moniterConfig.Org)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("execute query failed: %w", err)
	}

	// Parse and aggregate the query results
	dataMap := make(map[string]*ResourceUsageRecord)
	for result.Next() {
		record := result.Record()

		// Extract job_id and hostname
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
		hostname, ok := record.ValueByKey("hostname").(string)
		if !ok {
			log.Printf("Invalid type for hostname")
			continue
		}

		// Construct a unique key for aggregation
		key := fmt.Sprintf("%d:%s", jobID, hostname)
		if _, exists := dataMap[key]; !exists {
			dataMap[key] = &ResourceUsageRecord{
				TaskID:   jobID,
				Hostname: hostname,
			}
		}

		// Extract _field and _value
		field := record.ValueByKey("_field").(string)
		value := uint64(record.Value().(uint64))

		// Update the corresponding field in the record
		if field == "cpu_usage" {
			dataMap[key].CPUUsage = value
		} else if field == "memory_usage" {
			dataMap[key].MemoryUsage = value
		}
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query parsing error: %w", result.Err())
	}

	// Convert the aggregated data into a slice
	var records []*ResourceUsageRecord
	for _, record := range dataMap {
		records = append(records, record)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("no matching data available")
	}

	return records, nil
}

// Filters records by a specific JobID
func CalculateTotalUsagePtr(records []*ResourceUsageRecord, targetJobID int64) (float64, float64, error) {
	var filteredRecords []*ResourceUsageRecord
	for _, record := range records {
		if record != nil && record.TaskID == targetJobID {
			filteredRecords = append(filteredRecords, record)
		}
	}
	if len(filteredRecords) == 0 {
		return 0, 0, fmt.Errorf("not find job_id %v data in fluxdb", targetJobID)
	}
	var totalCPUUseNs, totalMemoryByte uint64
	var totalCPUUseS, totalMemMb float64
	for _, record := range filteredRecords {
		if record != nil {
			totalCPUUseNs += record.CPUUsage
			totalMemoryByte += record.MemoryUsage
		}
	}
	totalCPUUseS = float64(totalCPUUseNs) / 1e9
	totalMemMb = float64(totalMemoryByte) / (1024 * 1024)

	return totalCPUUseS, totalMemMb, nil
}

func CalculateRunTime(taskInfo *protos.TaskInfo, cPUTotal float64) (uint64, string) {
	if taskInfo.Status == protos.TaskStatus_Running {
		duration := taskInfo.ElapsedTime.AsDuration()
		return uint64(duration.Seconds() * cPUTotal), FormatDuration(duration * time.Duration(cPUTotal))
	}
	start := taskInfo.StartTime.AsTime()
	end := taskInfo.EndTime.AsTime()
	if end.Before(start) {
		log.Warnf("Invalid time range: end (%v) is before start (%v)", end, start)
		return 0, "0-0:0:0"
	}
	duration := end.Sub(start)
	return uint64(duration.Seconds() * cPUTotal), FormatDuration(duration * time.Duration(cPUTotal))
}

func FormatDuration(duration time.Duration) string {
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

func ExtractNodeNames(taskInfos []*protos.TaskInfo) ([]string, error) {
	var nodeNames []string
	for _, taskInfo := range taskInfos {
		nodes, ok := util.ParseHostList(taskInfo.GetCranedList())
		if !ok {
			return nil, fmt.Errorf("%s", taskInfo.GetCranedList())
		}
		nodeNames = append(nodeNames, nodes...)
	}
	return nodeNames, nil
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

func PrintTaskInfo(taskInfo *protos.TaskInfo, records []*ResourceUsageRecord) error {
	// Filter task records
	totalCPUUseS, totalMemMb, err := CalculateTotalUsagePtr(records, int64(taskInfo.TaskId))
	if err != nil {
		return fmt.Errorf("print task %v info error: %w", taskInfo.TaskId, err)
	}

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

	cpuTotal := taskInfo.ResView.AllocatableRes.CpuCoreLimit * float64(taskInfo.NodeNum)
	if math.Abs(cpuTotal-1) < 1e-9 {
		fmt.Printf("Cores: %.2f\n", cpuTotal)
	} else {
		fmt.Printf(
			"Nodes: %v\n"+
				"Cores per node: %.2f\n",
			taskInfo.NodeNum, taskInfo.ResView.AllocatableRes.CpuCoreLimit)
	}

	if taskInfo.Status == protos.TaskStatus_Pending {
		fmt.Printf("Efficiency not available for jobs in the PENDING state.\n")
		return nil
	}

	// Calculate running time
	runTime, runTimeStr := CalculateRunTime(taskInfo, cpuTotal)

	cPUUtilizedStr := "0-0:0:0"
	cPUUtilizedStr = FormatDuration(time.Duration(totalCPUUseS) * time.Second)

	cPUEfficiency := 0.0
	if runTime != 0 {
		cPUEfficiency = totalCPUUseS / float64(runTime) * 100
	}

	// Calculate mem efficiency
	memEfficiency := 0.0
	mallocMemMbPerNode := float64(taskInfo.ResView.AllocatableRes.MemoryLimitBytes) / (1024 * 1024)
	totalMallocMemMb := mallocMemMbPerNode * float64(taskInfo.NodeNum)
	if totalMallocMemMb != 0 {
		memEfficiency = totalMemMb / totalMallocMemMb * 100
	}

	fmt.Printf(
		"CPU Utilized: %s\n"+
			"CPU Efficiency: %.2f%% of %s core-walltime\n"+
			"Job Wall-clock time: %s\n"+
			"Memory Utilized: %.2f MB (estimated maximum)\n"+
			"Memory Efficiency: %.2f%% of %.2f MB (%.2f MB/node)\n",
		cPUUtilizedStr, cPUEfficiency, runTimeStr, runTimeStr, totalMemMb,
		memEfficiency, totalMallocMemMb, mallocMemMbPerNode)

	if taskInfo.Status == protos.TaskStatus_Running {
		fmt.Printf("WARNING: Efficiency statistics may be misleading for RUNNING jobs.\n")
	}

	return nil
}

func PrintTaskInfoInJson(taskInfo *protos.TaskInfo, records []*ResourceUsageRecord) (*CeffTaskInfo, error) {
	totalCPUUseS, totalMemMb, err := CalculateTotalUsagePtr(records, int64(taskInfo.TaskId))
	if err != nil {
		return nil, fmt.Errorf("print task %v json info error: %w", taskInfo.TaskId, err)
	}

	craneUser, err := user.LookupId(strconv.Itoa(int(taskInfo.Uid)))
	if err != nil {
		return nil, fmt.Errorf("failed to get username for UID %d: %w", taskInfo.Uid, err)
	}
	group, err := user.LookupGroupId(strconv.Itoa(int(taskInfo.Gid)))
	if err != nil {
		return nil, fmt.Errorf("failed to get groupname for GID %d: %w", taskInfo.Gid, err)
	}

	cpuTotal := taskInfo.ResView.AllocatableRes.CpuCoreLimit * float64(taskInfo.NodeNum)
	taskJsonInfo := &CeffTaskInfo{
		JobID:        taskInfo.TaskId,
		QoS:          taskInfo.Qos,
		UserName:     craneUser.Username,
		UID:          taskInfo.Uid,
		GroupName:    group.Name,
		GID:          taskInfo.Gid,
		Account:      taskInfo.Account,
		JobState:     taskInfo.Status,
		Nodes:        taskInfo.NodeNum,
		CoresPerNode: taskInfo.ResView.AllocatableRes.CpuCoreLimit,
	}

	if taskInfo.Status == protos.TaskStatus_Pending {
		return taskJsonInfo, nil
	}

	// Calculate running time
	runTime, runTimeStr := CalculateRunTime(taskInfo, cpuTotal)

	// Filter task records
	cPUUtilizedStr := "0-0:0:0"
	cPUUtilizedStr = FormatDuration(time.Duration(totalCPUUseS) * time.Second)

	cPUEfficiency := 0.0
	if runTime != 0 {
		cPUEfficiency = totalCPUUseS / float64(runTime) * 100
	}

	// Calculate mem efficiency
	memEfficiency := 0.0
	mallocMemMbPerNode := float64(taskInfo.ResView.AllocatableRes.MemoryLimitBytes) / (1024 * 1024)
	totalMallocMemMb := mallocMemMbPerNode * float64(taskInfo.NodeNum)
	if totalMallocMemMb != 0 {
		memEfficiency = totalMemMb / totalMallocMemMb * 100
	}
	taskJsonInfo.CPUUtilizedStr = cPUUtilizedStr
	taskJsonInfo.CPUEfficiency = cPUEfficiency
	taskJsonInfo.RunTimeStr = runTimeStr
	taskJsonInfo.TotalMemMB = totalMemMb
	taskJsonInfo.MemEfficiency = memEfficiency
	taskJsonInfo.TotalMallocMemMB = totalMallocMemMb
	taskJsonInfo.MallocMemMBPerNode = mallocMemMbPerNode

	return taskJsonInfo, nil
}

func QueryTasksInfoByIds(jobIds string) util.CraneCmdError {
	jobIdList, err := util.ParseJobIdList(jobIds, ",")
	if err != nil {
		log.Errorf("Invalid job list specified: %v", err)
		return util.ErrorCmdArg
	}

	req := &protos.QueryTasksInfoRequest{
		FilterTaskIds:               jobIdList,
		OptionIncludeCompletedTasks: true,
	}

	reply, err := stub.QueryTasksInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query job info")
		return util.ErrorNetwork
	}

	nodeNames, err := ExtractNodeNames(reply.TaskInfoList)
	if err != nil {
		log.Errorf("Failed to extract node names: %v", err)
		return util.ErrorBackend
	}

	// Query Resource Usage Records in InfluxDB
	result, err := QueryInfluxDbDataByTags(dbConfig, jobIdList, nodeNames)
	if err != nil {
		log.Errorf("Failed to query job info from InfluxDB: %v", err)
		return util.ErrorBackend
	}

	printed := map[uint32]bool{}
	taskInfoList := []*CeffTaskInfo{}
	if FlagJson {
		for _, taskInfo := range reply.TaskInfoList {
			if taskData, err := PrintTaskInfoInJson(taskInfo, result); err != nil {
				log.Warnf("%v", err)
			} else {
				taskInfoList = append(taskInfoList, taskData)
				printed[taskInfo.TaskId] = true
			}
		}

		jsonData, err := json.MarshalIndent(taskInfoList, "", "  ")
		if err != nil {
			log.Errorf("Error marshalling to JSON: %v", err)
			return util.ErrorBackend
		}

		fmt.Println(string(jsonData))
		notFoundJobs := findNotFoundJobs(jobIdList, printed)
		if len(notFoundJobs) > 0 {
			fmt.Printf("Job %v does not exist.\n", notFoundJobs)
		}

		return util.ErrorSuccess
	}

	for _, taskInfo := range reply.TaskInfoList {
		if err := PrintTaskInfo(taskInfo, result); err != nil {
			log.Warnf("%v", err)
		} else {
			printed[taskInfo.TaskId] = true
		}
	}

	notFoundJobs := findNotFoundJobs(jobIdList, printed)
	if len(notFoundJobs) > 0 {
		fmt.Printf("Job %v does not exist.\n", notFoundJobs)
	}

	return util.ErrorSuccess
}
