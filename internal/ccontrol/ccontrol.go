/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	log "github.com/sirupsen/logrus"
)

var (
	stub protos.CraneCtldClient
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
				"\tLastBusyTime=%s\n",
				nodeInfo.Hostname, stateStr,
				nodeInfo.ResTotal.AllocatableResInNode.CpuCoreLimit,
				math.Abs(nodeInfo.ResAlloc.AllocatableResInNode.CpuCoreLimit),
				math.Abs(nodeInfo.ResAvail.AllocatableResInNode.CpuCoreLimit),

				formatMemToMB(nodeInfo.ResTotal.AllocatableResInNode.MemoryLimitBytes),
				formatMemToMB(nodeInfo.ResAlloc.AllocatableResInNode.MemoryLimitBytes),
				formatMemToMB(nodeInfo.ResAvail.AllocatableResInNode.MemoryLimitBytes),

				formatDedicatedResource(nodeInfo.ResTotal.GetDedicatedResInNode()),
				formatDedicatedResource(nodeInfo.ResAlloc.GetDedicatedResInNode()),
				formatDedicatedResource(nodeInfo.ResAvail.GetDedicatedResInNode()),

				strings.Join(nodeInfo.PartitionNames, ","), nodeInfo.RunningTaskNum, CranedVersion,

				CranedOs,
				SystemBootTimeStr, CranedStartTimeStr,
				LastBusyTimeStr)
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
				"\tTotalNodes=%d AliveNodes=%d\n"+
				"\tTotalCPU=%.2f AvailCPU=%.2f AllocCPU=%.2f\n"+
				"\tTotalMem=%s AvailMem=%s AllocMem=%s\n"+
				"\tTotalGres=%s AvailGres=%s AllocGres=%s\n"+
				"\tHostList=%v\n\n",
				partitionInfo.Name, partitionInfo.State.String()[10:],
				partitionInfo.TotalNodes, partitionInfo.AliveNodes,
				math.Abs(partitionInfo.ResTotal.AllocatableRes.CpuCoreLimit),
				math.Abs(partitionInfo.ResAvail.AllocatableRes.CpuCoreLimit),
				math.Abs(partitionInfo.ResAlloc.AllocatableRes.CpuCoreLimit),
				formatMemToMB(partitionInfo.ResTotal.AllocatableRes.MemoryLimitBytes),
				formatMemToMB(partitionInfo.ResAvail.AllocatableRes.MemoryLimitBytes),
				formatMemToMB(partitionInfo.ResAlloc.AllocatableRes.MemoryLimitBytes),
				formatDeviceMap(partitionInfo.ResTotal.GetDeviceMap()),
				formatDeviceMap(partitionInfo.ResAvail.GetDeviceMap()),
				formatDeviceMap(partitionInfo.ResAlloc.GetDeviceMap()),
				partitionInfo.Hostlist)
		}
	}
	return util.ErrorSuccess
}

func ShowTasks(taskId uint32, queryAll bool) util.CraneCmdError {
	var req *protos.QueryTasksInfoRequest
	var taskIdList []uint32
	taskIdList = append(taskIdList, taskId)
	if queryAll {
		req = &protos.QueryTasksInfoRequest{}
	} else {
		req = &protos.QueryTasksInfoRequest{FilterTaskIds: taskIdList}
	}

	reply, err := stub.QueryTasksInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show the task")
		return util.ErrorNetwork
	}

	if !reply.GetOk() {
		log.Errorf("Failed to retrive the information of job %d", taskId)
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
			fmt.Printf("Job %d is not running.\n", taskId)
		}
	} else {
		checkStringEmpty := func(s string) string {
			if len(s) == 0 {
				return "unknown"
			} else {
				return s
			}
		}
		for _, taskInfo := range reply.TaskInfoList {
			timeSubmitStr := "unknown"
			timeStartStr := "unknown"
			timeEndStr := "unknown"
			runTimeStr := "unknown"

			var timeLimitStr string

			timeSubmit := taskInfo.SubmitTime.AsTime()
			if !timeSubmit.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
				timeSubmitStr = timeSubmit.In(time.Local).Format("2006-01-02 15:04:05")
			}

			timeStart := taskInfo.StartTime.AsTime()
			if !timeStart.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
				timeStartStr = timeStart.In(time.Local).Format("2006-01-02 15:04:05")
			}

			timeEnd := taskInfo.EndTime.AsTime()
			if timeEnd.After(timeStart) {
				timeEndStr = timeEnd.In(time.Local).Format("2006-01-02 15:04:05")
			}
			var resourcesType = "ReqRes"
			if taskInfo.Status == protos.TaskStatus_Running {
				timeEndStr = timeStart.Add(taskInfo.TimeLimit.AsDuration()).In(time.Local).Format("2006-01-02 15:04:05")

				runTimeDuration := taskInfo.ElapsedTime.AsDuration()

				days := int(runTimeDuration.Hours()) / 24
				hours := int(runTimeDuration.Hours()) % 24
				minutes := int(runTimeDuration.Minutes()) % 60
				seconds := int(runTimeDuration.Seconds()) % 60

				runTimeStr = fmt.Sprintf("%d-%02d:%02d:%02d", days, hours, minutes, seconds)
				resourcesType = "AllocRes"
			}

			if taskInfo.TimeLimit.Seconds >= util.InvalidDuration().Seconds {
				timeLimitStr = "unlimited"
				timeEndStr = "unknown"
			} else {
				timeLimitStr = util.SecondTimeFormat(taskInfo.TimeLimit.Seconds)
			}
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

			fmt.Printf("JobId=%v JobName=%v\n"+
				"\tUser=%s(%d) GroupId=%s(%d) Account=%v\n"+
				"\tJobState=%v RunTime=%v TimeLimit=%s SubmitTime=%v\n"+
				"\tStartTime=%v EndTime=%v Partition=%v NodeList=%v ExecutionHost=%v\n"+
				"\tCmdLine=\"%v\" Workdir=%v\n"+
				"\tPriority=%v Qos=%v CpusPerTask=%v MemPerNode=%vM \n"+
				"\t%s=node=%d cpu=%.2f gres=%s\n\t"+
				"\tReqNodeList=%v ExecludeNodeList=%v \n",
				taskInfo.TaskId, taskInfo.Name, craneUser.Username, taskInfo.Uid, group.Name, taskInfo.Gid,
				taskInfo.Account, taskInfo.Status.String(), runTimeStr, timeLimitStr, timeSubmitStr,
				timeStartStr, timeEndStr, taskInfo.Partition, checkStringEmpty(taskInfo.GetCranedList()),
				checkStringEmpty(util.HostNameListToStr(taskInfo.GetExecutionNode())),
				taskInfo.CmdLine, taskInfo.Cwd,
				taskInfo.Priority, taskInfo.Qos, taskInfo.ResView.AllocatableRes.CpuCoreLimit, formatMemToMB(taskInfo.ResView.AllocatableRes.MemoryLimitBytes),
				resourcesType, taskInfo.NodeNum, taskInfo.ResView.AllocatableRes.CpuCoreLimit*float64(taskInfo.NodeNum), formatDeviceMap(taskInfo.ResView.DeviceMap),
				checkStringEmpty(util.HostNameListToStr(taskInfo.GetReqNodes())), checkStringEmpty(util.HostNameListToStr(taskInfo.GetExcludeNodes())))
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

func SummarizeReply(proto interface{}) util.CraneCmdError {
	switch reply := proto.(type) {
	case *protos.ModifyTaskReply:
		if len(reply.ModifiedTasks) > 0 {
			fmt.Printf("Jobs %v modified successfully.\n", reply.ModifiedTasks)
		}
		if len(reply.NotModifiedTasks) > 0 {
			for i := 0; i < len(reply.NotModifiedTasks); i++ {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to modify job: %d. Reason: %s.\n",
					reply.NotModifiedTasks[i], reply.NotModifiedReasons[i])
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
				_, _ = fmt.Fprintf(os.Stderr, "Failed to modify node: %s. Reason: %s.\n",
					reply.NotModifiedNodes[i], reply.NotModifiedReasons[i])
			}
			return util.ErrorBackend
		}
		return util.ErrorSuccess
	default:
		return util.ErrorGeneric
	}
}

func ChangeTaskTimeLimit(taskStr string, timeLimit string) util.CraneCmdError {
	seconds, err := ParseTimeStrToSeconds(timeLimit)
	if err != nil {
		log.Errorln(err)
		return util.ErrorCmdArg
	}

	taskIds, err := util.ParseJobIdList(taskStr, ",")
	if err != nil {
		log.Errorln(err)
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

func ParseTimeStrToSeconds(time string) (int64, error) {
	re := regexp.MustCompile(`((.*)-)?(.*):(.*):(.*)`)
	result := re.FindAllStringSubmatch(time, -1)

	if result == nil || len(result) != 1 {
		return 0, fmt.Errorf("time format error")
	}
	var dd uint64
	if result[0][2] != "" {
		dd, _ = strconv.ParseUint(result[0][2], 10, 32)
	}
	hh, err := strconv.ParseUint(result[0][3], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("the hour time format error")
	}
	mm, err := strconv.ParseUint(result[0][4], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("the minute time format error")
	}
	ss, err := strconv.ParseUint(result[0][5], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("the second time format error")
	}
	seconds := int64(60*60*24*dd + 60*60*hh + 60*mm + ss)
	return seconds, nil
}

func HoldReleaseJobs(jobs string, hold bool) util.CraneCmdError {
	jobList, err := util.ParseJobIdList(jobs, ",")
	if err != nil {
		log.Errorln(err)
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
			parsedSeconds, err := ParseTimeStrToSeconds(FlagHoldTime)
			if err != nil {
				log.Errorln(err)
				return util.ErrorCmdArg
			}

			if parsedSeconds == 0 {
				log.Errorln("Hold time must be greater than 0.")
				return util.ErrorCmdArg
			}

			req.Value = &protos.ModifyTaskRequest_HoldSeconds{HoldSeconds: parsedSeconds}
		}
	} else {
		req.Value = &protos.ModifyTaskRequest_HoldSeconds{HoldSeconds: 0}
	}

	reply, err := stub.ModifyTask(context.Background(), req)
	if err != nil {
		log.Errorf("ModifyJob failed: " + err.Error())
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
