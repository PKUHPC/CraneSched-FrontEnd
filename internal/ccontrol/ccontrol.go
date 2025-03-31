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

package ccontrol

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

	"gopkg.in/yaml.v3"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

var (
	userUid uint32
	stub    protos.CraneCtldClient
)

func formatDeviceMap(data *protos.DeviceMap) string {
	if data == nil {
		return "None"
	}

	var kvStrings []string
	for deviceName, typeCountMap := range data.NameTypeMap {
		var typeCountPairs []string
		for deviceType, count := range typeCountMap.TypeCountMap {
			if count != 0 {
				typeCountPairs = append(typeCountPairs, fmt.Sprintf("%s:%d", deviceType, count))
			}
		}
		if typeCountMap.Total != 0 {
			typeCountPairs = append(typeCountPairs, strconv.FormatUint(typeCountMap.Total, 10))
		}
		for _, typeCountPair := range typeCountPairs {
			kvStrings = append(kvStrings, fmt.Sprintf("%s:%s", deviceName, typeCountPair))
		}
	}
	if len(kvStrings) == 0 {
		return "None"
	}
	kvString := strings.Join(kvStrings, ", ")

	return kvString
}

func formatDedicatedResource(data *protos.DedicatedResourceInNode) string {
	if data == nil {
		return "None"
	}

	var kvStrings []string
	for deviceName, typeCountMap := range data.NameTypeMap {
		var typeCountPairs []string
		for deviceType, slots := range typeCountMap.TypeSlotsMap {
			slotsSize := len(slots.Slots)
			if slotsSize != 0 {
				typeCountPairs = append(typeCountPairs, fmt.Sprintf("%s:%d", deviceType, slotsSize))
			}
		}
		for _, typeCountPair := range typeCountPairs {
			kvStrings = append(kvStrings, fmt.Sprintf("%s:%s", deviceName, typeCountPair))
		}
	}
	if len(kvStrings) == 0 {
		return "None"
	}
	kvString := strings.Join(kvStrings, ", ")

	return kvString
}

func formatMemToMB(data uint64) string {
	var B2MBRatio uint64 = 1024 * 1024
	if data == 0 {
		return "0"
	} else {
		return fmt.Sprintf("%vM", data/B2MBRatio)
	}
}

func formatAllowedAccounts(allowedAccounts []string) string {
	if len(allowedAccounts) == 0 {
		return "ALL"
	}
	return strings.Join(allowedAccounts, ",")
}

func formatDeniedAccounts(deniedAccounts []string) string {
	if len(deniedAccounts) == 0 {
		return "None"
	}

	return strings.Join(deniedAccounts, ",")
}

func ShowNodes(nodeName string, queryAll bool) util.CraneCmdError {
	req := &protos.QueryCranedInfoRequest{CranedName: nodeName}
	reply, err := stub.QueryCranedInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show nodes")
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return util.ErrorSuccess
	}
	if len(reply.CranedInfoList) == 0 {
		if queryAll {
			fmt.Println("No node is available.")
		} else {
			fmt.Printf("Node %s not found.\n", nodeName)
		}
	} else {
		for _, nodeInfo := range reply.CranedInfoList {
			stateStr := strings.ToLower(nodeInfo.ResourceState.String()[6:])
			if nodeInfo.ControlState != protos.CranedControlState_CRANE_NONE {
				stateStr += "(" + strings.ToLower(nodeInfo.ControlState.String()[6:]) + ")"
			}

			CranedVersion := "unknown"
			if len(nodeInfo.CranedVersion) != 0 {
				CranedVersion = nodeInfo.CranedVersion
			}
			CranedOs := "unknown"
			//format "{os.name} {os.release} {os.version}"
			if len(nodeInfo.SystemDesc) > 2 {
				CranedOs = nodeInfo.SystemDesc
			}
			SystemBootTimeStr := "unknown"
			CranedStartTimeStr := "unknown"
			LastBusyTimeStr := "unknown"

			SystemBootTime := nodeInfo.SystemBootTime.AsTime()
			if !SystemBootTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
				SystemBootTimeStr = SystemBootTime.In(time.Local).Format("2006-01-02 15:04:05")
			}
			CranedStartTime := nodeInfo.CranedStartTime.AsTime()
			if !CranedStartTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
				CranedStartTimeStr = CranedStartTime.In(time.Local).Format("2006-01-02 15:04:05")
			}
			LastBusyTime := nodeInfo.LastBusyTime.AsTime()
			if !LastBusyTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
				LastBusyTimeStr = LastBusyTime.In(time.Local).Format("2006-01-02 15:04:05")
			}

			fmt.Printf("NodeName=%v State=%v CPU=%.2f AllocCPU=%.2f FreeCPU=%.2f\n"+
				"\tRealMemory=%s AllocMem=%s FreeMem=%s\n"+
				"\tGres=%s AllocGres=%s FreeGres=%s\n"+
				"\tPatition=%s RunningJob=%d Version=%s\n"+
				"\tOs=%s\n"+
				"\tBootTime=%s CranedStartTime=%s\n"+
				"\tLastBusyTime=%s\n", nodeInfo.Hostname, stateStr, nodeInfo.ResTotal.AllocatableResInNode.CpuCoreLimit, math.Abs(nodeInfo.ResAlloc.AllocatableResInNode.CpuCoreLimit), math.Abs(nodeInfo.ResAvail.AllocatableResInNode.CpuCoreLimit),

				formatMemToMB(nodeInfo.ResTotal.AllocatableResInNode.MemoryLimitBytes), formatMemToMB(nodeInfo.ResAlloc.AllocatableResInNode.MemoryLimitBytes), formatMemToMB(nodeInfo.ResAvail.AllocatableResInNode.MemoryLimitBytes),

				formatDedicatedResource(nodeInfo.ResTotal.GetDedicatedResInNode()), formatDedicatedResource(nodeInfo.ResAlloc.GetDedicatedResInNode()), formatDedicatedResource(nodeInfo.ResAvail.GetDedicatedResInNode()),

				strings.Join(nodeInfo.PartitionNames, ","), nodeInfo.RunningTaskNum, CranedVersion,

				CranedOs, SystemBootTimeStr, CranedStartTimeStr, LastBusyTimeStr)
		}
	}
	return util.ErrorSuccess
}

func ShowPartitions(partitionName string, queryAll bool) util.CraneCmdError {
	req := &protos.QueryPartitionInfoRequest{PartitionName: partitionName}
	reply, err := stub.QueryPartitionInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show partition")
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return util.ErrorSuccess
	}

	if len(reply.PartitionInfo) == 0 {
		if queryAll {
			fmt.Println("No partition is available.")
		} else {
			fmt.Printf("Partition %s not found.\n", partitionName)
		}
	} else {
		for _, partitionInfo := range reply.PartitionInfo {
			fmt.Printf("PartitionName=%v State=%v\n"+
				"\tAllowedAccounts=%s DeniedAccounts=%s\n"+
				"\tTotalNodes=%d AliveNodes=%d\n"+
				"\tTotalCPU=%.2f AvailCPU=%.2f AllocCPU=%.2f\n"+
				"\tTotalMem=%s AvailMem=%s AllocMem=%s\n"+
				"\tTotalGres=%s AvailGres=%s AllocGres=%s\n"+
				"\tHostList=%v\n\n", partitionInfo.Name, partitionInfo.State.String()[10:], formatAllowedAccounts(partitionInfo.AllowedAccounts), formatDeniedAccounts(partitionInfo.DeniedAccounts), partitionInfo.TotalNodes, partitionInfo.AliveNodes, math.Abs(partitionInfo.ResTotal.AllocatableRes.CpuCoreLimit), math.Abs(partitionInfo.ResAvail.AllocatableRes.CpuCoreLimit), math.Abs(partitionInfo.ResAlloc.AllocatableRes.CpuCoreLimit), formatMemToMB(partitionInfo.ResTotal.AllocatableRes.MemoryLimitBytes), formatMemToMB(partitionInfo.ResAvail.AllocatableRes.MemoryLimitBytes), formatMemToMB(partitionInfo.ResAlloc.AllocatableRes.MemoryLimitBytes), formatDeviceMap(partitionInfo.ResTotal.GetDeviceMap()), formatDeviceMap(partitionInfo.ResAvail.GetDeviceMap()), formatDeviceMap(partitionInfo.ResAlloc.GetDeviceMap()), partitionInfo.Hostlist)
		}
	}
	return util.ErrorSuccess
}

func ShowJobs(jobIds string, queryAll bool) util.CraneCmdError {
	var req *protos.QueryTasksInfoRequest
	var jobIdList []uint32
	var err error

	if !queryAll {
		jobIdList, err = util.ParseJobIdList(jobIds, ",")
		if err != nil {
			log.Errorf("Invalid job list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
	}

	req = &protos.QueryTasksInfoRequest{FilterTaskIds: jobIdList}
	reply, err := stub.QueryTasksInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show jobs")
		return util.ErrorNetwork
	}

	if !reply.GetOk() {
		log.Errorf("Failed to retrive the information of job %v", jobIds)
		return util.ErrorBackend
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return util.ErrorSuccess
	}

	if len(reply.TaskInfoList) == 0 {
		if queryAll {
			fmt.Println("No job is running.")
		} else {
			fmt.Printf("Job %v is not running.\n", jobIdList)
		}
		return util.ErrorSuccess
	}

	formatHostNameStr := func(s string) string {
		if len(s) == 0 {
			return "None"
		} else {
			return s
		}
	}

	formatJobComment := func(s string) string {
		if gjson.Valid(s) {
			return gjson.Get(s, "comment").String()
		}
		return ""
	}

	// Track if any job requested is not returned
	printed := map[uint32]bool{}

	for _, taskInfo := range reply.TaskInfoList {
		timeSubmitStr := "unknown"
		timeStartStr := "unknown"
		timeEndStr := "unknown"
		runTimeStr := "unknown"
		resourcesType := "ReqRes"

		var timeLimitStr string

		// submit_time
		timeSubmit := taskInfo.SubmitTime.AsTime()
		if !timeSubmit.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
			timeSubmitStr = timeSubmit.In(time.Local).Format("2006-01-02 15:04:05")
		}

		// start_time
		timeStart := taskInfo.StartTime.AsTime()
		if !timeStart.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
			timeStartStr = timeStart.In(time.Local).Format("2006-01-02 15:04:05")
		}

		// end_time
		timeEnd := taskInfo.EndTime.AsTime()
		if !timeEnd.Before(timeStart) && timeEnd.Second() < util.MaxJobTimeStamp {
			timeEndStr = timeEnd.In(time.Local).Format("2006-01-02 15:04:05")
		}

		// time_limit
		if taskInfo.TimeLimit.Seconds >= util.MaxJobTimeLimit {
			timeLimitStr = "unlimited"
		} else {
			timeLimitStr = util.SecondTimeFormat(taskInfo.TimeLimit.Seconds)
		}

		// elapsed_time and resources
		if taskInfo.Status == protos.TaskStatus_Running {
			runTimeStr = util.SecondTimeFormat(taskInfo.ElapsedTime.Seconds)
			resourcesType = "AllocRes"
		}

		// uid and gid (egid)
		craneUser, err := user.LookupId(strconv.Itoa(int(taskInfo.Uid)))
		if err != nil {
			log.Errorf("Failed to get username for UID %d: %s\n", taskInfo.Uid, err)
			return util.ErrorGeneric
		}
		group, err := user.LookupGroupId(strconv.Itoa(int(taskInfo.Gid)))
		if err != nil {
			log.Errorf("Failed to get groupname for GID %d: %s\n", taskInfo.Gid, err)
			return util.ErrorGeneric
		}

		printed[taskInfo.TaskId] = true

		fmt.Printf("JobId=%v JobName=%v\n"+
			"\tUser=%s(%d) GroupId=%s(%d) Account=%v\n"+
			"\tJobState=%v RunTime=%v TimeLimit=%s SubmitTime=%v\n"+
			"\tStartTime=%v EndTime=%v Partition=%v NodeList=%v ExecutionHost=%v\n"+
			"\tCmdLine=\"%v\" Workdir=%v\n"+
			"\tPriority=%v Qos=%v CpusPerTask=%v MemPerNode=%v\n"+
			"\t%s=node=%d cpu=%.2f gres=%s\n"+
			"\tReqNodeList=%v ExecludeNodeList=%v\n"+
			"\tComment=%v\n",
			taskInfo.TaskId, taskInfo.Name, craneUser.Username, taskInfo.Uid, group.Name, taskInfo.Gid,
			taskInfo.Account, taskInfo.Status.String(), runTimeStr, timeLimitStr, timeSubmitStr,
			timeStartStr, timeEndStr, taskInfo.Partition, formatHostNameStr(taskInfo.GetCranedList()),
			formatHostNameStr(util.HostNameListToStr(taskInfo.GetExecutionNode())),
			taskInfo.CmdLine, taskInfo.Cwd,
			taskInfo.Priority, taskInfo.Qos, taskInfo.ResView.AllocatableRes.CpuCoreLimit, formatMemToMB(taskInfo.ResView.AllocatableRes.MemoryLimitBytes),
			resourcesType, taskInfo.NodeNum, taskInfo.ResView.AllocatableRes.CpuCoreLimit*float64(taskInfo.NodeNum), formatDeviceMap(taskInfo.ResView.DeviceMap),
			formatHostNameStr(util.HostNameListToStr(taskInfo.GetReqNodes())), formatHostNameStr(util.HostNameListToStr(taskInfo.GetExcludeNodes())), formatJobComment(taskInfo.ExtraAttr),
		)
	}

	// If any job is requested but not returned, remind the user
	if !queryAll {
		notRunningJobs := []uint32{}
		for _, jobId := range jobIdList {
			if !printed[jobId] {
				notRunningJobs = append(notRunningJobs, jobId)
			}
		}
		if len(notRunningJobs) > 0 {
			fmt.Printf("Job %v is not running.\n", notRunningJobs)
		}
	}

	return util.ErrorSuccess
}

func PrintFlattenYAML(prefix string, m interface{}) {
	switch v := m.(type) {
	case map[string]interface{}:
		for key, value := range v {
			newPrefix := key
			if prefix != "" {
				newPrefix = prefix + "." + key
			}
			PrintFlattenYAML(newPrefix, value)
		}
	case []interface{}:
		for i, value := range v {
			PrintFlattenYAML(fmt.Sprintf("%s.%d", prefix, i), value)
		}
	default:
		fmt.Printf("%s = %v\n", prefix, v)
	}
}

func ShowConfig(path string) util.CraneCmdError {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Errorf("Failed to read configuration file: %v\n", err)
		return util.ErrorCmdArg
	}

	var config map[string]interface{}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Errorf("error unmarshalling yaml: %v\n", err)
		return util.ErrorCmdArg
	}
	if FlagJson {
		output, _ := json.Marshal(config)
		fmt.Println(string(output))
	} else {
		PrintFlattenYAML("", config)
	}

	return util.ErrorSuccess
}

func ShowServerCluster() util.CraneCmdError {
	req := &protos.QueryRaftServerListRequest{}
	reply, err := stub.QueryRaftServerList(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query raft server list")
		return util.ErrorNetwork
	}

	if len(reply.ServerList) < 0 {
		util.GrpcErrorPrintf(err, "Empty raft server list")
		return util.ErrorBackend
	}

	for _, server := range reply.ServerList {
		fmt.Printf("server id: %d: %s", server.Id, server.EndPoint)
		if server.Role == protos.QueryRaftServerListReply_Leader {
			fmt.Printf(" (Leader)")
		}
		fmt.Println()
	}

	return util.ErrorSuccess
}

func ShowServerNode() util.CraneCmdError {
	req := &protos.QueryRaftNodeInfoRequest{}

	reply, err := stub.QueryRaftNodeInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query raft server")
		return util.ErrorNetwork
	}

	fmt.Printf("Server id: %d\nLeader id: %d\nRaft log range: ", reply.ServerId, reply.LeaderId)
	if reply.StartIndex >= reply.NextSlot {
		// Start index can be the same as next slot when the log store is empty.
		fmt.Println("(empty)")
	} else {
		fmt.Printf("%d - %d\n", reply.StartIndex, reply.NextSlot)
	}
	fmt.Printf("Last committed index: %d\nCurrent term: %d\nLast snapshot log index: %d\nLast snapshot log term: %d\n", reply.CommittedLogIdx, reply.CurTerm, reply.LastSnapshotLogIdx, reply.LastSnapshotLogTerm)

	return util.ErrorSuccess
}

func SummarizeReply(proto interface{}) util.CraneCmdError {
	switch reply := proto.(type) {
	case *protos.ModifyTaskReply:
		if len(reply.ModifiedTasks) > 0 {
			fmt.Printf("Jobs %v modified successfully.\n", reply.ModifiedTasks)
		}
		if len(reply.NotModifiedTasks) > 0 {
			for i := 0; i < len(reply.NotModifiedTasks); i++ {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to modify job: %d. Reason: %s.\n", reply.NotModifiedTasks[i], reply.NotModifiedReasons[i])
			}
			return util.ErrorBackend
		}
		return util.ErrorSuccess
	case *protos.ModifyCranedStateReply:
		if len(reply.ModifiedNodes) > 0 {
			fmt.Printf("Nodes %v modified successfully.\n", reply.ModifiedNodes)
		}
		if len(reply.NotModifiedNodes) > 0 {
			for i := 0; i < len(reply.NotModifiedNodes); i++ {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to modify node: %s. Reason: %s.\n", reply.NotModifiedNodes[i], reply.NotModifiedReasons[i])
			}
			return util.ErrorBackend
		}
		return util.ErrorSuccess
	default:
		return util.ErrorGeneric
	}
}

func ChangeTaskTimeLimit(taskStr string, timeLimit string) util.CraneCmdError {
	seconds, err := util.ParseDurationStrToSeconds(timeLimit)
	if err != nil {
		log.Errorln(err)
		return util.ErrorCmdArg
	}

	taskIds, err := util.ParseJobIdList(taskStr, ",")
	if err != nil {
		log.Errorf("Invalid job list specified: %v.\n", err)
		return util.ErrorCmdArg
	}

	req := &protos.ModifyTaskRequest{
		Uid:       uint32(os.Getuid()),
		TaskIds:   taskIds,
		Attribute: protos.ModifyTaskRequest_TimeLimit,
		Value: &protos.ModifyTaskRequest_TimeLimitSeconds{
			TimeLimitSeconds: seconds,
		},
	}
	reply, err := stub.ModifyTask(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to change task time limit")
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedTasks) == 0 {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}

	return SummarizeReply(reply)
}

func HoldReleaseJobs(jobs string, hold bool) util.CraneCmdError {
	jobList, err := util.ParseJobIdList(jobs, ",")
	if err != nil {
		log.Errorf("Invalid job list specified: %v.\n", err)
		return util.ErrorCmdArg
	}

	req := &protos.ModifyTaskRequest{
		Uid:       uint32(os.Getuid()),
		TaskIds:   jobList,
		Attribute: protos.ModifyTaskRequest_Hold,
	}
	if hold {
		// The default timer value for hold is unlimited.
		req.Value = &protos.ModifyTaskRequest_HoldSeconds{HoldSeconds: math.MaxInt64}

		// If a time limit for hold constraint is specified, parse it.
		if FlagHoldTime != "" {
			seconds, err := util.ParseDurationStrToSeconds(FlagHoldTime)
			if err != nil {
				log.Errorln(err)
				return util.ErrorCmdArg
			}

			if seconds == 0 {
				log.Errorln("Hold time must be greater than 0.")
				return util.ErrorCmdArg
			}

			req.Value = &protos.ModifyTaskRequest_HoldSeconds{HoldSeconds: seconds}
		}
	} else {
		req.Value = &protos.ModifyTaskRequest_HoldSeconds{HoldSeconds: 0}
	}

	reply, err := stub.ModifyTask(context.Background(), req)
	if err != nil {
		log.Errorf("Failed to modify the job: %v", err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedTasks) == 0 {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}

	return SummarizeReply(reply)
}

func AddServerNode(hostName string, raftPort uint32) util.CraneCmdError {
	req := &protos.AddRaftNodeRequest{
		HostName: hostName,
		RaftPort: raftPort,
	}

	reply, err := stub.AddRaftNode(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to add server node")
		return util.ErrorNetwork
	}

	if !reply.GetOk() {
		fmt.Printf("Failed to add server node: %s\n", reply.GetReason())
		return util.ErrorBackend
	} else {
		fmt.Printf("Added server node successfully, assigned server Id: %d\n", reply.ServerId)
	}

	return util.ErrorSuccess
}

func RemoveServerNode(serverId int) util.CraneCmdError {
	req := &protos.RemoveRaftNodeRequest{
		ServerId: int32(serverId),
	}

	reply, err := stub.RemoveRaftNode(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to remove server node")
		return util.ErrorNetwork
	}

	if !reply.GetOk() {
		fmt.Printf("Failed to remove server node: %s\n", reply.GetReason())
		return util.ErrorBackend
	} else {
		fmt.Println("Remove server node successfully")
	}

	return util.ErrorSuccess
}

func YieldLeadership(id int) util.CraneCmdError {
	if id == util.CurrentLeaderId() {
		fmt.Println("This node is already the current leader, will do nothing")
		return util.ErrorCmdArg
	}
	req := &protos.YieldLeadershipRequest{
		NextServerId: int32(id),
	}

	reply, err := stub.YieldLeadership(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to yield leadership")
		return util.ErrorNetwork
	}

	if !reply.GetOk() {
		fmt.Println("Failed to yield leadership")
		return util.ErrorBackend
	} else {
		fmt.Println("Yield request submitted successfully")
		if id != -1 {
			util.UpdateLeaderIdToFile(id)
		} else {
			req := &protos.QueryLeaderIdRequest{}

			for {
				time.Sleep(400 * time.Millisecond)
				log.Println("Attempting to query current leader ID...")
				reply, err := stub.QueryLeaderId(context.Background(), req)
				if err == nil && reply.LeaderId > 0 {
					log.Printf("Leader id was changed to %d, caching the value.\n", reply.GetLeaderId())
					util.UpdateLeaderIdToFile(int(reply.GetLeaderId()))
					break
				}
			}
		}
	}

	return util.ErrorSuccess
}

func ChangeTaskPriority(taskStr string, priority float64) util.CraneCmdError {
	if priority < 0 {
		log.Errorln("Priority must be greater than or equal to 0.")
		return util.ErrorCmdArg
	}

	taskIds, err := util.ParseJobIdList(taskStr, ",")
	if err != nil {
		log.Errorln(err)
		return util.ErrorCmdArg
	}

	rounded, _ := util.ParseFloatWithPrecision(strconv.FormatFloat(priority, 'f', 1, 64), 1)
	if rounded != priority {
		log.Warnf("Priority will be rounded to %.1f\n", rounded)
	}
	if rounded == 0 {
		log.Warnf("Mandated priority equals 0 means the scheduling priority will be calculated.")
	}

	req := &protos.ModifyTaskRequest{
		Uid:       uint32(os.Getuid()),
		TaskIds:   taskIds,
		Attribute: protos.ModifyTaskRequest_Priority,
		Value: &protos.ModifyTaskRequest_MandatedPriority{
			MandatedPriority: rounded,
		},
	}

	reply, err := stub.ModifyTask(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to change task priority")
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedTasks) == 0 {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}

	return SummarizeReply(reply)
}

func ChangeNodeState(nodeRegex string, state string, reason string) util.CraneCmdError {
	nodeNames, ok := util.ParseHostList(nodeRegex)
	if !ok {
		log.Errorf("Invalid node pattern: %s.\n", nodeRegex)
		return util.ErrorCmdArg
	}

	if len(nodeNames) == 0 {
		log.Errorln("No node provided.")
		return util.ErrorCmdArg
	}

	var req = &protos.ModifyCranedStateRequest{}

	req.Uid = uint32(os.Getuid())
	req.CranedIds = nodeNames
	state = strings.ToLower(state)
	switch state {
	case "drain":
		if reason == "" {
			log.Errorln("You must specify a reason by '-r' or '--reason' when draining a node.")
			return util.ErrorCmdArg
		}
		req.NewState = protos.CranedControlState_CRANE_DRAIN
		req.Reason = reason
	case "resume":
		req.NewState = protos.CranedControlState_CRANE_NONE
	default:
		log.Errorf("Invalid state given: %s. Valid states are: drain, resume.\n", state)
		return util.ErrorCmdArg
	}

	reply, err := stub.ModifyNode(context.Background(), req)
	if err != nil {
		log.Errorf("Failed to modify node state: %v.\n", err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedNodes) == 0 {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}

	return SummarizeReply(reply)
}

func ModifyPartitionAcl(partition string, isAllowedList bool, accounts string) util.CraneCmdError {
	var accountList []string

	accountList, _ = util.ParseStringParamList(accounts, ",")

	req := protos.ModifyPartitionAclRequest{
		Uid:           userUid,
		Partition:     partition,
		IsAllowedList: isAllowedList,
		Accounts:      accountList,
	}

	reply, err := stub.ModifyPartitionAcl(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Faild to modify partition %s", partition)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}

	if !reply.GetOk() {
		fmt.Printf("Modify partition %s failed: %s.\n", partition, util.ErrMsg(reply.GetCode()))
		return util.ErrorBackend
	}

	fmt.Printf("Modify partition %s succeeded.\n", partition)
	return util.ErrorSuccess
}
