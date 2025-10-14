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
	"github.com/tidwall/sjson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	userUid uint32
	stub    protos.CraneCtldClient
)

type UpdateJobParamFlags int

const (
	CommentTypeFlag UpdateJobParamFlags = 1 << iota
	MailUserTypeFlag
	MailTypeTypeFlag
	PriorityTypeFlag
	TimelimitTypeFlag
	DeadlineTypeFlag
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

func ShowNodes(nodeName string, queryAll bool) error {
	req := &protos.QueryCranedInfoRequest{CranedName: nodeName}
	reply, err := stub.QueryCranedInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show nodes")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return nil
	}
	if len(reply.CranedInfoList) == 0 {
		if queryAll {
			fmt.Println("No node is available.")
		} else {
			return &util.CraneError{
				Code:    util.ErrorBackend,
				Message: fmt.Sprintf("Node %s not found.", nodeName),
			}
		}
	} else {
		for _, nodeInfo := range reply.CranedInfoList {
			stateStr := strings.ToLower(nodeInfo.ResourceState.String()[6:])
			if nodeInfo.ControlState != protos.CranedControlState_CRANE_NONE {
				stateStr += "(" + strings.ToLower(nodeInfo.ControlState.String()[6:]) + ")"
			}
			stateStr += "[" + strings.ToLower(nodeInfo.PowerState.String()[6:]) + "]"

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

				util.FormatMemToMB(nodeInfo.ResTotal.AllocatableResInNode.MemoryLimitBytes),
				util.FormatMemToMB(nodeInfo.ResAlloc.AllocatableResInNode.MemoryLimitBytes),
				util.FormatMemToMB(nodeInfo.ResAvail.AllocatableResInNode.MemoryLimitBytes),

				formatDedicatedResource(nodeInfo.ResTotal.GetDedicatedResInNode()),
				formatDedicatedResource(nodeInfo.ResAlloc.GetDedicatedResInNode()),
				formatDedicatedResource(nodeInfo.ResAvail.GetDedicatedResInNode()),

				strings.Join(nodeInfo.PartitionNames, ","), nodeInfo.RunningTaskNum, CranedVersion,

				CranedOs,
				SystemBootTimeStr, CranedStartTimeStr,
				LastBusyTimeStr)
		}
	}
	return nil
}

func ShowPartitions(partitionName string, queryAll bool) error {
	req := &protos.QueryPartitionInfoRequest{PartitionName: partitionName}
	reply, err := stub.QueryPartitionInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show partition")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return nil
	}

	if len(reply.PartitionInfoList) == 0 {
		if queryAll {
			fmt.Println("No partition is available.")
		} else {
			return &util.CraneError{
				Code:    util.ErrorBackend,
				Message: fmt.Sprintf("Partition %s not found.", partitionName),
			}
		}
	} else {
		for _, partitionInfo := range reply.PartitionInfoList {
			fmt.Printf("PartitionName=%v State=%v\n"+
				"\tAllowedAccounts=%s DeniedAccounts=%s\n"+
				"\tTotalNodes=%d AliveNodes=%d\n"+
				"\tTotalCPU=%.2f AvailCPU=%.2f AllocCPU=%.2f\n"+
				"\tTotalMem=%s AvailMem=%s AllocMem=%s\n"+
				"\tTotalGres=%s AvailGres=%s AllocGres=%s\n"+
				"\tDefaultMemPerCPU=%s MaxMemPerCPU=%s\n"+
				"\tHostList=%v\n\n",
				partitionInfo.Name, partitionInfo.State.String()[10:],
				formatAllowedAccounts(partitionInfo.AllowedAccounts),
				formatDeniedAccounts(partitionInfo.DeniedAccounts),
				partitionInfo.TotalNodes, partitionInfo.AliveNodes,
				math.Abs(partitionInfo.ResTotal.AllocatableRes.CpuCoreLimit),
				math.Abs(partitionInfo.ResAvail.AllocatableRes.CpuCoreLimit),
				math.Abs(partitionInfo.ResAlloc.AllocatableRes.CpuCoreLimit),
				util.FormatMemToMB(partitionInfo.ResTotal.AllocatableRes.MemoryLimitBytes),
				util.FormatMemToMB(partitionInfo.ResAvail.AllocatableRes.MemoryLimitBytes),
				util.FormatMemToMB(partitionInfo.ResAlloc.AllocatableRes.MemoryLimitBytes),
				formatDeviceMap(partitionInfo.ResTotal.GetDeviceMap()),
				formatDeviceMap(partitionInfo.ResAvail.GetDeviceMap()),
				formatDeviceMap(partitionInfo.ResAlloc.GetDeviceMap()),
				util.FormatMemToMB(partitionInfo.DefaultMemPerCpu),
				util.FormatMemToMB(partitionInfo.MaxMemPerCpu),
				partitionInfo.Hostlist)
		}
	}
	return nil
}

func ShowReservations(reservationName string, queryAll bool) error {
	req := &protos.QueryReservationInfoRequest{
		Uid:             uint32(os.Getuid()),
		ReservationName: reservationName,
	}
	reply, err := stub.QueryReservationInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show reservations")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return nil
	}

	if !reply.GetOk() {
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Failed to retrive the information of reservation %s: %s", reservationName, reply.GetReason()),
		}
	}

	if len(reply.ReservationInfoList) == 0 {
		if queryAll {
			fmt.Println("No reservation is available.")
		} else {
			return &util.CraneError{
				Code:    util.ErrorBackend,
				Message: fmt.Sprintf("Reservation %s not found.", reservationName),
			}
		}
	} else {
		for _, reservationInfo := range reply.ReservationInfoList {
			str := fmt.Sprintf("ReservationName=%v StartTime=%v Duration=%v\n", reservationInfo.ReservationName, reservationInfo.StartTime.AsTime().In(time.Local).Format("2006-01-02 15:04:05"), reservationInfo.Duration.AsDuration().String())
			if reservationInfo.Partition != "" && reservationInfo.CranedRegex != "" {
				str += fmt.Sprintf("Partition=%v CranedRegex=%v\n", reservationInfo.Partition, reservationInfo.CranedRegex)
			} else if reservationInfo.Partition != "" {
				str += fmt.Sprintf("Partition=%v\n", reservationInfo.Partition)
			} else if reservationInfo.CranedRegex != "" {
				str += fmt.Sprintf("CranedRegex=%v\n", reservationInfo.CranedRegex)
			}
			if len(reservationInfo.AllowedAccounts) > 0 {
				str += fmt.Sprintf("AllowedAccounts=%s\n", strings.Join(reservationInfo.AllowedAccounts, ","))
			}
			if len(reservationInfo.DeniedAccounts) > 0 {
				str += fmt.Sprintf("DeniedAccounts=%s\n", strings.Join(reservationInfo.DeniedAccounts, ","))
			}
			if len(reservationInfo.AllowedUsers) > 0 {
				str += fmt.Sprintf("AllowedUsers=%s\n", strings.Join(reservationInfo.AllowedUsers, ","))
			}
			if len(reservationInfo.DeniedUsers) > 0 {
				str += fmt.Sprintf("DeniedUsers=%s\n", strings.Join(reservationInfo.DeniedUsers, ","))
			}
			str += fmt.Sprintf("TotalCPU=%.2f AvailCPU=%.2f AllocCPU=%.2f\n", math.Abs(reservationInfo.ResTotal.AllocatableRes.CpuCoreLimit), math.Abs(reservationInfo.ResAvail.AllocatableRes.CpuCoreLimit), math.Abs(reservationInfo.ResAlloc.AllocatableRes.CpuCoreLimit))
			str += fmt.Sprintf("TotalMem=%s AvailMem=%s AllocMem=%s\n", util.FormatMemToMB(reservationInfo.ResTotal.AllocatableRes.MemoryLimitBytes), util.FormatMemToMB(reservationInfo.ResAvail.AllocatableRes.MemoryLimitBytes), util.FormatMemToMB(reservationInfo.ResAlloc.AllocatableRes.MemoryLimitBytes))
			str += fmt.Sprintf("TotalGres=%s AvailGres=%s AllocGres=%s\n", formatDeviceMap(reservationInfo.ResTotal.GetDeviceMap()), formatDeviceMap(reservationInfo.ResAvail.GetDeviceMap()), formatDeviceMap(reservationInfo.ResAlloc.GetDeviceMap()))
			fmt.Println(str)
		}
	}
	return nil
}

func ShowJobs(jobIds string, queryAll bool) error {
	var req *protos.QueryTasksInfoRequest
	var jobIdList []uint32
	var err error

	if !queryAll {
		jobIdList, err = util.ParseJobIdList(jobIds, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid job list specified: %s.", err),
			}
		}
	}

	req = &protos.QueryTasksInfoRequest{FilterTaskIds: jobIdList}
	reply, err := stub.QueryTasksInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show jobs")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if !reply.GetOk() {
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Failed to retrive the information of job %s", jobIds),
		}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return nil
	}

	if len(reply.TaskInfoList) == 0 {
		if queryAll {
			fmt.Println("No job is running.")
		} else {
			jobIdListString := util.ConvertSliceToString(jobIdList, ", ")
			fmt.Printf("Job %s is not running.\n", jobIdListString)
		}
		return nil
	}

	formatHostNameStr := func(s string) string {
		if len(s) == 0 {
			return "None"
		} else {
			return s
		}
	}

	formatJobExTraAttrsField := func(s, key string) string {
		if gjson.Valid(s) {
			return gjson.Get(s, key).String()
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

		// uid and gid (egid)
		craneUser, err := user.LookupId(strconv.Itoa(int(taskInfo.Uid)))
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorGeneric,
				Message: fmt.Sprintf("Failed to get username for UID %d: %s", taskInfo.Uid, err),
			}
		}
		group, err := user.LookupGroupId(strconv.Itoa(int(taskInfo.Gid)))
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorGeneric,
				Message: fmt.Sprintf("Failed to get groupname for GID %d: %s", taskInfo.Gid, err),
			}
		}

		printed[taskInfo.TaskId] = true

		fmt.Printf("JobId=%v JobName=%v\n"+
			"\tUser=%s(%d) GroupId=%s(%d) Account=%v\n"+
			"\tJobState=%v RunTime=%v TimeLimit=%s SubmitTime=%v\n"+
			"\tStartTime=%v EndTime=%v Partition=%v NodeList=%v ExecutionHost=%v\n"+
			"\tCmdLine=\"%v\" Workdir=%v\n"+
			"\tPriority=%v Qos=%v CpusPerTask=%v MemPerNode=%v\n"+
			"\tReqRes=node=%d cpu=%.2f mem=%v gres=%s\n",
			taskInfo.TaskId, taskInfo.Name, craneUser.Username, taskInfo.Uid, group.Name, taskInfo.Gid,
			taskInfo.Account, taskInfo.Status.String(), runTimeStr, timeLimitStr, timeSubmitStr,
			timeStartStr, timeEndStr, taskInfo.Partition, formatHostNameStr(taskInfo.GetCranedList()),
			formatHostNameStr(util.HostNameListToStr(taskInfo.GetExecutionNode())),
			taskInfo.CmdLine, taskInfo.Cwd,
			taskInfo.Priority, taskInfo.Qos, taskInfo.ReqResView.AllocatableRes.CpuCoreLimit,
			util.FormatMemToMB(taskInfo.ReqResView.AllocatableRes.MemoryLimitBytes),
			taskInfo.NodeNum, taskInfo.ReqResView.AllocatableRes.CpuCoreLimit*float64(taskInfo.NodeNum),
			util.FormatMemToMB(taskInfo.ReqResView.AllocatableRes.MemoryLimitBytes*uint64(taskInfo.NodeNum)),
			formatDeviceMap(taskInfo.ReqResView.DeviceMap),
		)

		if taskInfo.Status == protos.TaskStatus_Running {
			fmt.Printf("\tAllocRes=node=%d cpu=%.2f mem=%v gres=%s\n",
				taskInfo.NodeNum,
				taskInfo.AllocatedResView.AllocatableRes.CpuCoreLimit,
				util.FormatMemToMB(taskInfo.AllocatedResView.AllocatableRes.MemoryLimitBytes),
				formatDeviceMap(taskInfo.AllocatedResView.DeviceMap),
			)
		}

		fmt.Printf("\tReqNodeList=%v ExecludeNodeList=%v\n"+
			"\tExclusive=%v Comment=%v\n",
			formatHostNameStr(util.HostNameListToStr(taskInfo.GetReqNodes())),
			formatHostNameStr(util.HostNameListToStr(taskInfo.GetExcludeNodes())),
			strconv.FormatBool(taskInfo.Exclusive), formatJobExTraAttrsField(taskInfo.ExtraAttr, "comment"))

		mailUserStr := formatJobExTraAttrsField(taskInfo.ExtraAttr, "mail.user")
		mailTypeStr := formatJobExTraAttrsField(taskInfo.ExtraAttr, "mail.type")
		if mailUserStr != "" || mailTypeStr != "" {
			fmt.Printf("\tMailUser=%v MailType=%v\n", mailUserStr, mailTypeStr)
		}
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
			notRunningJobsString := util.ConvertSliceToString(notRunningJobs, ", ")
			fmt.Printf("Job %s is not running.\n", notRunningJobsString)
		}
	}

	return nil
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

func ShowConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Failed to read configuration file: %s", err),
		}
	}

	var config map[string]interface{}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Failed to unmarshal yaml configuration file: %s", err),
		}
	}
	if FlagJson {
		output, _ := json.Marshal(config)
		fmt.Println(string(output))
	} else {
		PrintFlattenYAML("", config)
	}

	return nil
}

func SummarizeReply(proto interface{}) error {
	switch reply := proto.(type) {
	case *protos.ModifyTaskReply:
		if len(reply.ModifiedTasks) > 0 {
			modifiedTasksString := util.ConvertSliceToString(reply.ModifiedTasks, ", ")
			fmt.Printf("Jobs %s modified successfully.\n", modifiedTasksString)
		}
		if len(reply.NotModifiedTasks) > 0 {
			for i := 0; i < len(reply.NotModifiedTasks); i++ {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to modify job: %d. Reason: %s.\n", reply.NotModifiedTasks[i], reply.NotModifiedReasons[i])
			}
			return &util.CraneError{Code: util.ErrorBackend}
		}
		return nil
	case *protos.ModifyCranedStateReply:
		if len(reply.ModifiedNodes) > 0 {
			nodeListString := util.ConvertSliceToString(reply.ModifiedNodes, ", ")
			fmt.Printf("Nodes %s modified successfully, please wait for a few minutes for the node state to fully update.\n", nodeListString)
		}
		if len(reply.NotModifiedNodes) > 0 {
			for i := 0; i < len(reply.NotModifiedNodes); i++ {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to modify node: %s. Reason: %s.\n", reply.NotModifiedNodes[i], reply.NotModifiedReasons[i])
			}
			return &util.CraneError{Code: util.ErrorBackend}
		}
		return nil
	case *protos.ModifyTasksExtraAttrsReply:
		if len(reply.ModifiedTasks) > 0 {
			modifiedTasksString := util.ConvertSliceToString(reply.ModifiedTasks, ", ")
			fmt.Printf("Jobs %s modified successfully.\n", modifiedTasksString)
		}
		if len(reply.NotModifiedTasks) > 0 {
			for i := 0; i < len(reply.NotModifiedTasks); i++ {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to modify job: %d. Reason: %s.\n", reply.NotModifiedTasks[i], reply.NotModifiedReasons[i])
			}
			return &util.CraneError{Code: util.ErrorBackend}
		}
		return nil
	default:
		return &util.CraneError{Code: util.ErrorGeneric}
	}
}

func ChangeTaskTimeLimit(taskStr string, timeLimit string) error {
	seconds, err := util.ParseDurationStrToSeconds(timeLimit)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: err.Error(),
		}
	}

	taskIds, err := util.ParseJobIdList(taskStr, ",")
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid job list specified: %s.\n", err),
		}
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
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedTasks) == 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return SummarizeReply(reply)
}

func ChangeDeadlineTime(taskStr string, deadline string) error {
	time, err := util.ParseTime(deadline)

	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: err.Error(),
		}
	}
	taskIds, err := util.ParseJobIdList(taskStr, ",")
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid job list specified: %s.\n", err),
		}
	}

	req := &protos.ModifyTaskRequest{
		Uid:       uint32(os.Getuid()),
		TaskIds:   taskIds,
		Attribute: protos.ModifyTaskRequest_Deadline,
		Value: &protos.ModifyTaskRequest_DeadlineTime{
			DeadlineTime: timestamppb.New(time),
		},
	}

	reply, err := stub.ModifyTask(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to change task deadline")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedTasks) == 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return SummarizeReply(reply)
}

func HoldReleaseJobs(jobs string, hold bool) error {
	jobList, err := util.ParseJobIdList(jobs, ",")
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid job list specified: %s.", err),
		}
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
				return &util.CraneError{
					Code:    util.ErrorCmdArg,
					Message: err.Error(),
				}
			}

			if seconds == 0 {
				return &util.CraneError{
					Code:    util.ErrorCmdArg,
					Message: "Hold time must be greater than 0.",
				}
			}

			req.Value = &protos.ModifyTaskRequest_HoldSeconds{HoldSeconds: seconds}
		}
	} else {
		req.Value = &protos.ModifyTaskRequest_HoldSeconds{HoldSeconds: 0}
	}

	reply, err := stub.ModifyTask(context.Background(), req)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorNetwork,
			Message: fmt.Sprintf("Failed to modify the job: %s", err),
		}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedTasks) == 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return SummarizeReply(reply)
}

func ChangeTaskPriority(taskStr string, priority float64) error {
	if priority < 0 {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "Priority must be greater than or equal to 0.",
		}
	}

	taskIds, err := util.ParseJobIdList(taskStr, ",")
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: err.Error(),
		}
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
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedTasks) == 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return SummarizeReply(reply)
}

func ChangeTaskExtraAttrs(taskStr string, valueMap map[UpdateJobParamFlags]string) error {
	jobIdList, err := util.ParseJobIdList(taskStr, ",")
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: err.Error(),
		}
	}

	req := &protos.QueryTasksInfoRequest{
		FilterTaskIds:               jobIdList,
		OptionIncludeCompletedTasks: false,
	}
	reply, err := stub.QueryTasksInfo(context.Background(), req)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorNetwork,
			Message: fmt.Sprintf("Failed to modify the job: %s", err),
		}
	}

	if !reply.GetOk() {
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Failed to retrive the information of job %s", taskStr),
		}
	}

	if len(reply.TaskInfoList) == 0 {
		jobIdListString := util.ConvertSliceToString(jobIdList, ", ")
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Job %s is completed or does not exist", jobIdListString),
		}
	}

	updateJobExtraAttr := func(origin string, JobParamvalMap map[UpdateJobParamFlags]string) (string, error) {
		var extraAttrsKeyMap = map[UpdateJobParamFlags]string{
			CommentTypeFlag:  "comment",
			MailUserTypeFlag: "mail.user",
			MailTypeTypeFlag: "mail.type",
		}
		var err error
		newJsonStr := origin
		for flag, key := range extraAttrsKeyMap {
			if newValue, exist := JobParamvalMap[flag]; exist {
				newJsonStr, err = sjson.Set(newJsonStr, key, newValue)
				if err != nil {
					return "", fmt.Errorf("set %s failed: %w", key, err)
				}
			}
		}
		return newJsonStr, nil
	}

	pdOrRJobMap := make(map[uint32]string)
	validJobList := map[uint32]bool{}
	for _, taskInfo := range reply.TaskInfoList {
		newJsonStr, err := updateJobExtraAttr(taskInfo.ExtraAttr, valueMap)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to set extra attributes JSON: %s", err),
			}
		}
		pdOrRJobMap[taskInfo.TaskId] = newJsonStr
		validJobList[taskInfo.TaskId] = true
	}

	notGetInfoJobs := []uint32{}
	for _, jobId := range jobIdList {
		if !validJobList[jobId] {
			notGetInfoJobs = append(notGetInfoJobs, jobId)
		}
	}
	if len(notGetInfoJobs) > 0 {
		notGetInfoJobsString := util.ConvertSliceToString(notGetInfoJobs, ", ")
		log.Warnf("Job %s is completed or does not exist.\n", notGetInfoJobsString)
	}

	request := &protos.ModifyTasksExtraAttrsRequest{
		Uid:            uint32(os.Getuid()),
		ExtraAttrsList: pdOrRJobMap,
	}

	rep, err := stub.ModifyTasksExtraAttrs(context.Background(), request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to change task extra attrs")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(rep))
		if len(rep.NotModifiedTasks) == 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return SummarizeReply(rep)
}

func ChangeNodeState(nodeRegex string, state string, reason string) error {
	nodeNames, ok := util.ParseHostList(nodeRegex)
	if !ok {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid node pattern: %s.", nodeRegex),
		}
	}

	if len(nodeNames) == 0 {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "No node provided.",
		}
	}

	var req = &protos.ModifyCranedStateRequest{}

	req.Uid = uint32(os.Getuid())
	req.CranedIds = nodeNames
	state = strings.ToLower(state)
	switch state {
	case "drain":
		if reason == "" {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: "You must specify a reason when draining a node.",
			}
		}
		req.NewState = protos.CranedControlState_CRANE_DRAIN
		req.Reason = reason
	case "resume":
		req.NewState = protos.CranedControlState_CRANE_NONE
	case "on":
		req.NewState = protos.CranedControlState_CRANE_POWERON
	case "off":
		req.NewState = protos.CranedControlState_CRANE_POWEROFF
	case "sleep":
		req.NewState = protos.CranedControlState_CRANE_SLEEP
	case "wake":
		req.NewState = protos.CranedControlState_CRANE_WAKE
	default:
		p := []string{"drain", "resume", "on", "off", "sleep", "wake"}
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid state given: %s. Valid states are: %s.", state, strings.Join(p, ", ")),
		}
	}

	reply, err := stub.ModifyNode(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to modify node state")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedNodes) == 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return SummarizeReply(reply)
}

func ModifyPartitionAcl(partition string, isAllowedList bool, accounts string) error {
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
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if !reply.GetOk() {
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Modify partition %s failed: %s.", partition, util.ErrMsg(reply.GetCode())),
		}
	}

	fmt.Printf("Modify partition %s succeeded.\n", partition)
	return nil
}

func CreateReservation() error {
	start_time, err := util.ParseTime(FlagStartTime)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: err.Error(),
		}
	}
	duration, err := util.ParseDurationStrToSeconds(FlagDuration)
	if err != nil || duration <= 0 {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "Invalid duration specified.",
		}
	}

	req := &protos.CreateReservationRequest{
		Uid:                  uint32(os.Getuid()),
		ReservationName:      FlagReservationName,
		StartTimeUnixSeconds: start_time.Unix(),
		DurationSeconds:      duration,
	}

	if FlagNodes != "" {
		req.CranedRegex = FlagNodes
	} else {
		if FlagPartitionName == "" {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: "Partition name must be specified when no node regex is given.",
			}
		}
	}

	if FlagPartitionName != "" {
		req.Partition = FlagPartitionName
	}

	if FlagNodeNum != 0 {
		req.NodeNum = FlagNodeNum
	}

	if FlagAccount != "" {
		req.AllowedAccounts, req.DeniedAccounts, err = util.ParsePosNegList(FlagAccount)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: err.Error(),
			}
		}
		if len(req.AllowedAccounts) > 0 && len(req.DeniedAccounts) > 0 {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: "You can only specify either allowed or disallowed accounts.",
			}
		}
		if len(req.AllowedAccounts) == 0 && len(req.DeniedAccounts) == 0 {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: "Account can not be empty.",
			}
		}
	}

	if FlagUser != "" {
		req.AllowedUsers, req.DeniedUsers, err = util.ParsePosNegList(FlagUser)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: err.Error(),
			}
		}
		if len(req.AllowedUsers) > 0 && len(req.DeniedUsers) > 0 {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: "You can only specify either allowed or disallowed users.",
			}
		}
	}

	reply, err := stub.CreateReservation(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to create reservation")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Printf("Reservation %s created successfully.\n", FlagReservationName)
	} else {
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Failed to create reservation: %s.", reply.GetReason()),
		}
	}
	return nil
}

func DeleteReservation(ReservationName string) error {
	req := &protos.DeleteReservationRequest{
		Uid:             uint32(os.Getuid()),
		ReservationName: ReservationName,
	}
	reply, err := stub.DeleteReservation(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to delete reservation")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	if reply.GetOk() {
		fmt.Printf("Reservation %s deleted successfully.\n", ReservationName)
	} else {
		log.Errorf("Failed to delete reservation: %s.\n", reply.GetReason())
		return &util.CraneError{Code: util.ErrorBackend}
	}
	return nil
}

func EnableAutoPowerControl(nodeRegex string, enableStr string) error {
	nodeNames, ok := util.ParseHostList(nodeRegex)
	if !ok {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid node pattern: %s", nodeRegex),
		}
	}

	if len(nodeNames) == 0 {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "No node provided",
		}
	}

	var enable bool
	enableStr = strings.ToLower(enableStr)
	switch enableStr {
	case "true", "yes", "1", "on", "enable":
		enable = true
	case "false", "no", "0", "off", "disable":
		enable = false
	default:
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid power-control value: %s. Valid values are: true/false, yes/no, 1/0, on/off, enable/disable", enableStr),
		}
	}

	req := &protos.EnableAutoPowerControlRequest{
		Uid:       uint32(os.Getuid()),
		CranedIds: nodeNames,
		Enable:    enable,
	}

	reply, err := stub.EnableAutoPowerControl(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to modify node power control setting")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotModifiedNodes) > 0 {
			return &util.CraneError{Code: util.ErrorBackend}
		}
		return nil
	}

	if len(reply.ModifiedNodes) > 0 {
		action := "enabled for"
		if !enable {
			action = "disabled for"
		}
		modifiedNodesString := strings.Join(reply.ModifiedNodes, ", ")
		fmt.Printf("Auto power control %s nodes %s successfully.\n", action, modifiedNodesString)
	}

	if len(reply.NotModifiedNodes) > 0 {
		for i := 0; i < len(reply.NotModifiedNodes); i++ {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to modify node: %s. Reason: %s.\n",
				reply.NotModifiedNodes[i], reply.NotModifiedReasons[i])
		}
		return &util.CraneError{Code: util.ErrorBackend}
	}

	return nil
}
