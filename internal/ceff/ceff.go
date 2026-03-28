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
	JobID       int64
	CPUUsage    uint64
	MemoryUsage uint64
	ProcCount   uint64
	Hostname    string
	Timestamp   time.Time
}

type CeffJobInfo struct {
	JobID              uint32            `json:"job_id"`
	QoS                string            `json:"qos"`
	UID                uint32            `json:"uid"`
	UserName           string            `json:"user_name"`
	GID                uint32            `json:"gid"`
	GroupName          string            `json:"group_name"`
	Account            string            `json:"account"`
	JobState           protos.JobStatus `json:"job_state"`
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
func QueryEfficiencyDataViaPlugind(jobIds []uint32) ([]*ResourceUsageRecord, error) {
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
	req := &protos.QueryJobEfficiencyRequest{
		JobIds:  jobIds,
		Uid:     uint32(uid),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	reply, err := pluginClient.QueryJobEfficiency(ctx, req)
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
			JobID:      int64(effData.JobId),
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
		if record != nil && record.JobID == targetJobID {
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

func CalculateRunTime(jobInfo *protos.JobInfo, cPUTotal float64) (uint64, string) {
	if jobInfo.Status == protos.JobStatus_Running {
		duration := jobInfo.ElapsedTime.AsDuration()
		return uint64(duration.Seconds() * cPUTotal), FormatDuration(duration * time.Duration(cPUTotal))
	}
	start := jobInfo.StartTime.AsTime()
	end := jobInfo.EndTime.AsTime()
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

func PrintJobInfo(jobInfo *protos.JobInfo, records []*ResourceUsageRecord) error {
	// Filter job records
	totalCPUUseS, totalMemMb, err := CalculateTotalUsagePtr(records, int64(jobInfo.JobId))
	if err != nil {
		return fmt.Errorf("print job %v info error: %w", jobInfo.JobId, err)
	}

	// Obtain user and group information
	craneUser, err := user.LookupId(strconv.Itoa(int(jobInfo.Uid)))
	if err != nil {
		return fmt.Errorf("failed to get username for UID %d: %w", jobInfo.Uid, err)
	}
	group, err := user.LookupGroupId(strconv.Itoa(int(jobInfo.Gid)))
	if err != nil {
		return fmt.Errorf("failed to get groupname for GID %d: %w", jobInfo.Gid, err)
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
		jobInfo.JobId, jobInfo.Qos, craneUser.Username, jobInfo.Uid, group.Name, jobInfo.Gid,
		jobInfo.Account)

	if jobInfo.Status == protos.JobStatus_Pending || jobInfo.Status == protos.JobStatus_Running {
		fmt.Printf("JobState: %v\n", jobInfo.Status.String())
	} else {
		fmt.Printf("JobState: %v (exit code %d)\n", jobInfo.Status.String(), jobInfo.ExitCode)
	}

	if jobInfo.NodeNum == 0 {
		return fmt.Errorf("the number of nodes is empty")
	}
	cpuTotal := jobInfo.AllocatedResView.AllocatableRes.CpuCoreLimit
	if math.Abs(cpuTotal-1) < 1e-9 {
		fmt.Printf("Cores: %.2f\n", cpuTotal)
	} else {
		fmt.Printf(
			"Nodes: %v\n"+
				"Cores per node: %.2f\n",
			jobInfo.NodeNum, cpuTotal/float64(jobInfo.NodeNum))
	}

	if jobInfo.Status == protos.JobStatus_Pending {
		fmt.Printf("Efficiency not available for jobs in the PENDING state.\n")
		return nil
	}

	// Calculate running time
	runTime, runTimeStr := CalculateRunTime(jobInfo, cpuTotal)

	cPUUtilizedStr := "0-0:0:0"
	cPUUtilizedStr = FormatDuration(time.Duration(totalCPUUseS) * time.Second)

	cPUEfficiency := 0.0
	if runTime != 0 {
		cPUEfficiency = totalCPUUseS / float64(runTime) * 100
	}

	// Calculate mem efficiency
	memEfficiency := 0.0
	mallocMemMbPerNode := float64(jobInfo.AllocatedResView.AllocatableRes.MemoryLimitBytes) /
		float64(jobInfo.NodeNum) / (1024 * 1024)
	totalMallocMemMb := float64(jobInfo.ReqTotalResView.AllocatableRes.MemoryLimitBytes)
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

	if jobInfo.Status == protos.JobStatus_Running {
		fmt.Printf("WARNING: Efficiency statistics may be misleading for RUNNING jobs.\n")
	}

	return nil
}

func PrintJobInfoInJson(jobInfo *protos.JobInfo, records []*ResourceUsageRecord) (*CeffJobInfo, error) {
	totalCPUUseS, totalMemMb, err := CalculateTotalUsagePtr(records, int64(jobInfo.JobId))
	if err != nil {
		return nil, fmt.Errorf("print job %v json info error: %w", jobInfo.JobId, err)
	}

	craneUser, err := user.LookupId(strconv.Itoa(int(jobInfo.Uid)))
	if err != nil {
		return nil, fmt.Errorf("failed to get username for UID %d: %w", jobInfo.Uid, err)
	}
	group, err := user.LookupGroupId(strconv.Itoa(int(jobInfo.Gid)))
	if err != nil {
		return nil, fmt.Errorf("failed to get groupname for GID %d: %w", jobInfo.Gid, err)
	}

	if jobInfo.NodeNum == 0 {
		return nil, fmt.Errorf("the number of nodes is empty")
	}
	cpuTotal := jobInfo.AllocatedResView.AllocatableRes.CpuCoreLimit
	coresPerNode := cpuTotal / float64(jobInfo.NodeNum)
	jobJsonInfo := &CeffJobInfo{
		JobID:        jobInfo.JobId,
		QoS:          jobInfo.Qos,
		UserName:     craneUser.Username,
		UID:          jobInfo.Uid,
		GroupName:    group.Name,
		GID:          jobInfo.Gid,
		Account:      jobInfo.Account,
		JobState:     jobInfo.Status,
		Nodes:        jobInfo.NodeNum,
		CoresPerNode: coresPerNode,
	}

	if jobInfo.Status == protos.JobStatus_Pending {
		return jobJsonInfo, nil
	}

	// Calculate running time
	runTime, runTimeStr := CalculateRunTime(jobInfo, cpuTotal)

	// Filter job records
	cPUUtilizedStr := "0-0:0:0"
	cPUUtilizedStr = FormatDuration(time.Duration(totalCPUUseS) * time.Second)

	cPUEfficiency := 0.0
	if runTime != 0 {
		cPUEfficiency = totalCPUUseS / float64(runTime) * 100
	}

	// Calculate mem efficiency
	memEfficiency := 0.0
	mallocMemMbPerNode := float64(jobInfo.ReqTotalResView.AllocatableRes.MemoryLimitBytes) /
		float64(jobInfo.NodeNum) / (1024 * 1024)
	totalMallocMemMb := float64(jobInfo.ReqTotalResView.AllocatableRes.MemoryLimitBytes)
	if totalMallocMemMb != 0 {
		memEfficiency = totalMemMb / totalMallocMemMb * 100
	}
	jobJsonInfo.CPUUtilizedStr = cPUUtilizedStr
	jobJsonInfo.CPUEfficiency = cPUEfficiency
	jobJsonInfo.RunTimeStr = runTimeStr
	jobJsonInfo.TotalMemMB = totalMemMb
	jobJsonInfo.MemEfficiency = memEfficiency
	jobJsonInfo.TotalMallocMemMB = totalMallocMemMb
	jobJsonInfo.MallocMemMBPerNode = mallocMemMbPerNode

	return jobJsonInfo, nil
}

func QueryJobsInfoByIds(jobIds string) error {
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

	req := &protos.QueryJobsInfoRequest{
		FilterIds:                   stepIdList,
		OptionIncludeCompletedJobs: true,
	}

	reply, err := stub.QueryJobsInfo(context.Background(), req)
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
	jobInfoList := []*CeffJobInfo{}
	if FlagJson {
		for _, jobInfo := range reply.JobInfoList {
			if jobData, err := PrintJobInfoInJson(jobInfo, result); err != nil {
				log.Warnf("%v", err)
			} else {
				jobInfoList = append(jobInfoList, jobData)
				printed[jobInfo.JobId] = true
			}
		}

		jsonData, err := json.MarshalIndent(jobInfoList, "", "  ")
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

	for _, jobInfo := range reply.JobInfoList {
		if err := PrintJobInfo(jobInfo, result); err != nil {
			log.Warnf("%v", err)
		} else {
			printed[jobInfo.JobId] = true
		}
	}

	notFoundJobs := findNotFoundJobs(jobIdList, printed)
	if len(notFoundJobs) > 0 {
		fmt.Printf("Job %v does not exist.\n", notFoundJobs)
	}

	return nil
}
