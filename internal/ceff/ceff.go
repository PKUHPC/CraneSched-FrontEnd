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
	"net"
	"os/user"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	stub         protos.CraneCtldClient
	pluginClient protos.PluginQueryServiceClient
	pluginConn   *grpc.ClientConn
)

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

// Connect to cplugind for querying efficiency data
func GetPlugindClient(config *util.Config) (protos.PluginQueryServiceClient, *grpc.ClientConn, error) {
	if !config.Plugin.Enabled {
		return nil, nil, util.NewCraneErr(util.ErrorCmdArg, "Plugin is not enabled")
	}

	addr := config.Plugin.ListenAddress
	port := config.Plugin.ListenPort
	if addr == "" || port == "" {
		return nil, nil, util.NewCraneErr(util.ErrorCmdArg,
			"PlugindListenAddress and PlugindListenPort must be configured for ceff")
	}

	endpoint := net.JoinHostPort(addr, port)
	var creds credentials.TransportCredentials
	if config.TlsConfig.Enabled {
		certPath := config.TlsConfig.CaFilePath
		if certPath == "" {
			return nil, nil, util.NewCraneErr(util.ErrorCmdArg,
				"TLS is enabled for plugin client but no certificate file is configured")
		}
		var err error
		creds, err = credentials.NewClientTLSFromFile(certPath, "")
		if err != nil {
			return nil, nil, util.NewCraneErr(util.ErrorCmdArg,
				fmt.Sprintf("Failed to load TLS credentials: %v", err))
		}
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(util.ClientKeepAliveParams),
		grpc.WithConnectParams(util.ClientConnectParams),
		grpc.WithIdleTimeout(time.Duration(math.MaxInt64)),
	)
	if err != nil {
		return nil, nil, util.NewCraneErr(util.ErrorNetwork,
			fmt.Sprintf("Failed to connect to cplugind at %s: %v", endpoint, err))
	}

	return protos.NewPluginQueryServiceClient(conn), conn, nil
}

func CleanupPlugindClient() {
	if pluginConn != nil {
		if err := pluginConn.Close(); err != nil {
			log.WithError(err).Warn("Failed to close plugind connection")
		}
		pluginConn = nil
	}
	pluginClient = nil
}

// Query efficiency data through cplugind
func QueryEfficiencyDataViaPlugind(taskIds []uint32) ([]*ResourceUsageRecord, error) {
	if pluginClient == nil {
		return nil, fmt.Errorf("plugind client not initialized")
	}

	// Get current user for authorization
	currentUser, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to get current user: %v", err)
	}

	uid, err := strconv.ParseUint(currentUser.Uid, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse user ID: %v", err)
	}

	// Request efficiency data from cplugind
	req := &protos.QueryTaskEfficiencyRequest{
		TaskIds: taskIds,
		Uid:     uint32(uid),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	reply, err := pluginClient.QueryTaskEfficiency(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to query efficiency data: %v", err)
	}

	if !reply.Ok {
		return nil, fmt.Errorf("efficiency query failed: %s", reply.ErrorMessage)
	}

	// Convert protobuf data to ResourceUsageRecord format
	var records []*ResourceUsageRecord
	for _, effData := range reply.EfficiencyData {
		record := &ResourceUsageRecord{
			TaskID:      int64(effData.TaskId),
			CPUUsage:    effData.CpuUsage,
			MemoryUsage: effData.MemoryUsage,
			ProcCount:   effData.ProcCount,
			Hostname:    effData.Hostname,
			Timestamp:   effData.Timestamp.AsTime(),
		}
		records = append(records, record)
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

	if taskInfo.NodeNum == 0 {
		return fmt.Errorf("the number of nodes is empty")
	}
	cpuTotal := taskInfo.AllocatedResView.AllocatableRes.CpuCoreLimit
	if math.Abs(cpuTotal-1) < 1e-9 {
		fmt.Printf("Cores: %.2f\n", cpuTotal)
	} else {
		fmt.Printf(
			"Nodes: %v\n"+
				"Cores per node: %.2f\n",
			taskInfo.NodeNum, cpuTotal/float64(taskInfo.NodeNum))
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
	mallocMemMbPerNode := float64(taskInfo.AllocatedResView.AllocatableRes.MemoryLimitBytes) /
		float64(taskInfo.NodeNum) / (1024 * 1024)
	totalMallocMemMb := float64(taskInfo.ReqResView.AllocatableRes.MemoryLimitBytes)
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

	if taskInfo.NodeNum == 0 {
		return nil, fmt.Errorf("the number of nodes is empty")
	}
	cpuTotal := taskInfo.AllocatedResView.AllocatableRes.CpuCoreLimit
	coresPerNode := cpuTotal / float64(taskInfo.NodeNum)
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
		CoresPerNode: coresPerNode,
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
	mallocMemMbPerNode := float64(taskInfo.ReqResView.AllocatableRes.MemoryLimitBytes) /
		float64(taskInfo.NodeNum) / (1024 * 1024)
	totalMallocMemMb := float64(taskInfo.ReqResView.AllocatableRes.MemoryLimitBytes)
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

func QueryTasksInfoByIds(jobIds string) error {
	stepIdList, err := util.ParseStepIdList(jobIds, ",")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid job list specified: %s", err))
	}
	jobIdList := make([]uint32, len(stepIdList))
	for jobId, _ := range stepIdList {
		jobIdList = append(jobIdList, jobId)
	}
	for _, jobId := range jobIdList {
		stepIdList[jobId] = &protos.JobStepIds{Steps: make([]uint32, 0)}
	}

	req := &protos.QueryTasksInfoRequest{
		FilterIds:                   stepIdList,
		OptionIncludeCompletedTasks: true,
	}

	reply, err := stub.QueryTasksInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query job info")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	// Query Resource Usage Records via cplugind (secure approach)
	result, err := QueryEfficiencyDataViaPlugind(jobIdList)
	if err != nil {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to query efficiency data via plugin: %s", err))
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
			return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Error marshalling to JSON: %s", err))
		}

		fmt.Println(string(jsonData))
		notFoundJobs := findNotFoundJobs(jobIdList, printed)
		if len(notFoundJobs) > 0 {
			fmt.Printf("Job %v does not exist.\n", notFoundJobs)
		}

		return nil
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

	return nil
}
