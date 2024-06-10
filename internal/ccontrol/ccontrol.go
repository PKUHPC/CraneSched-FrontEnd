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
	"fmt"
	"math"
	"os"
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

func ShowNodes(nodeName string, queryAll bool) util.CraneCmdError {
	req := &protos.QueryCranedInfoRequest{CranedName: nodeName}
	reply, err := stub.QueryCranedInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show nodes")
		return util.ErrorNetwork
	}

	var B2MBRatio uint64 = 1024 * 1024

	if queryAll {
		if len(reply.CranedInfoList) == 0 {
			fmt.Println("No node is available.")
		} else {
			for _, nodeInfo := range reply.CranedInfoList {
				stateStr := strings.ToLower(nodeInfo.ResourceState.String()[6:])
				if nodeInfo.ControlState != protos.CranedControlState_CRANE_NONE {
					stateStr += "(" + strings.ToLower(nodeInfo.ControlState.String()[6:]) + ")"
				}
				fmt.Printf("NodeName=%v State=%v CPU=%.2f AllocCPU=%.2f FreeCPU=%.2f\n"+
					"\tRealMemory=%dM AllocMem=%dM FreeMem=%dM\n"+
					"\tPatition=%s RunningJob=%d\n\n",
					nodeInfo.Hostname, stateStr, nodeInfo.Cpu,
					math.Abs(nodeInfo.AllocCpu),
					math.Abs(nodeInfo.FreeCpu),
					nodeInfo.RealMem/B2MBRatio, nodeInfo.AllocMem/B2MBRatio, nodeInfo.FreeMem/B2MBRatio,
					strings.Join(nodeInfo.PartitionNames, ","), nodeInfo.RunningTaskNum)
			}
		}
	} else {
		if len(reply.CranedInfoList) == 0 {
			fmt.Printf("Node %s not found.\n", nodeName)
		} else {
			for _, nodeInfo := range reply.CranedInfoList {
				stateStr := strings.ToLower(nodeInfo.ResourceState.String()[6:])
				if nodeInfo.ControlState != protos.CranedControlState_CRANE_NONE {
					stateStr += "(" + strings.ToLower(nodeInfo.ControlState.String()[6:]) + ")"
				}
				fmt.Printf("NodeName=%v State=%v CPU=%.2f AllocCPU=%.2f FreeCPU=%.2f\n"+
					"\tRealMemory=%dM AllocMem=%dM FreeMem=%dM\n"+
					"\tPatition=%s RunningJob=%d\n\n",
					nodeInfo.Hostname, stateStr, nodeInfo.Cpu, nodeInfo.AllocCpu, nodeInfo.FreeCpu,
					nodeInfo.RealMem/B2MBRatio, nodeInfo.AllocMem/B2MBRatio, nodeInfo.FreeMem/B2MBRatio,
					strings.Join(nodeInfo.PartitionNames, ","), nodeInfo.RunningTaskNum)
			}
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

	var B2MBRatio uint64 = 1024 * 1024

	if queryAll {
		if len(reply.PartitionInfo) == 0 {
			fmt.Println("No node is available.")
		} else {
			for _, partitionInfo := range reply.PartitionInfo {
				fmt.Printf("PartitionName=%v State=%v\n"+
					"\tTotalNodes=%d AliveNodes=%d\n"+
					"\tTotalCPU=%.2f AvailCPU=%.2f AllocCPU=%.2f\n"+
					"\tTotalMem=%dM AvailMem=%dM AllocMem=%dM\n\tHostList=%v\n\n",
					partitionInfo.Name, partitionInfo.State.String()[10:],
					partitionInfo.TotalNodes, partitionInfo.AliveNodes,
					partitionInfo.TotalCpu, partitionInfo.AvailCpu, partitionInfo.AllocCpu,
					partitionInfo.TotalMem/B2MBRatio, partitionInfo.AvailMem/B2MBRatio, partitionInfo.AllocMem/B2MBRatio, partitionInfo.Hostlist)
			}
		}
	} else {
		if len(reply.PartitionInfo) == 0 {
			fmt.Printf("Partition %s not found.\n", partitionName)
		} else {
			for _, partitionInfo := range reply.PartitionInfo {
				fmt.Printf("PartitionName=%v State=%v\n"+
					"\tTotalNodes=%d AliveNodes=%d\n"+
					"\tTotalCPU=%.2f AvailCPU=%.2f AllocCPU=%.2f\n"+
					"\tTotalMem=%dM AvailMem=%dM AllocMem=%dM\n\tHostList=%v\n\n",
					partitionInfo.Name, partitionInfo.State.String()[10:],
					partitionInfo.TotalNodes, partitionInfo.AliveNodes,
					partitionInfo.TotalCpu, partitionInfo.AvailCpu, partitionInfo.AllocCpu,
					partitionInfo.TotalMem/B2MBRatio, partitionInfo.AvailMem/B2MBRatio, partitionInfo.AllocMem/B2MBRatio, partitionInfo.Hostlist)
			}
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

	if len(reply.TaskInfoList) == 0 {
		if queryAll {
			fmt.Println("No job is running.")
		} else {
			fmt.Printf("Job %d is not running.\n", taskId)
		}

	} else {
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

			if taskInfo.Status == protos.TaskStatus_Running {
				timeEndStr = timeStart.Add(taskInfo.TimeLimit.AsDuration()).In(time.Local).Format("2006-01-02 15:04:05")

				runTimeDuration := taskInfo.ElapsedTime.AsDuration()

				days := int(runTimeDuration.Hours()) / 24
				hours := int(runTimeDuration.Hours()) % 24
				minutes := int(runTimeDuration.Minutes()) % 60
				seconds := int(runTimeDuration.Seconds()) % 60

				runTimeStr = fmt.Sprintf("%d-%02d:%02d:%02d", days, hours, minutes, seconds)
			}

			if taskInfo.TimeLimit.Seconds >= util.InvalidDuration().Seconds {
				timeLimitStr = "unlimited"
				timeEndStr = "unknown"
			} else {
				timeLimitStr = util.SecondTimeFormat(taskInfo.TimeLimit.Seconds)
			}

			fmt.Printf("JobId=%v JobName=%v\n\tUserId=%d GroupId=%d Account=%v\n\tJobState=%v RunTime=%v "+
				"TimeLimit=%s SubmitTime=%v\n\tStartTime=%v EndTime=%v Partition=%v NodeList=%v "+
				"NumNodes=%d\n\tCmdLine=\"%v\" Workdir=%v\n",
				taskInfo.TaskId, taskInfo.Name, taskInfo.Uid, taskInfo.Gid,
				taskInfo.Account, taskInfo.Status.String(), runTimeStr, timeLimitStr,
				timeSubmitStr, timeStartStr, timeEndStr, taskInfo.Partition,
				taskInfo.GetCranedList(), taskInfo.NodeNum, taskInfo.CmdLine, taskInfo.Cwd)
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
	PrintFlattenYAML("", config)

	return util.ErrorSuccess
}

func ChangeTaskTimeLimit(taskId uint32, timeLimit string) util.CraneCmdError {
	seconds, err := ParseTimeStrToSeconds(timeLimit)
	if err != nil {
		log.Errorln(err)
		return util.ErrorCmdArg
	}
	req := &protos.ModifyTaskRequest{
		Uid:       uint32(os.Getuid()),
		TaskId:    taskId,
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

	if reply.Ok {
		log.Println("Change time limit success.")
		return util.ErrorSuccess
	} else {
		log.Printf("Change time limit failed: %s.\n", reply.GetReason())
		return util.ErrorBackend
	}
}

func ParseTimeStrToSeconds(time string) (int64, error) {
	re := regexp.MustCompile(`((.*)-)?(.*):(.*):(.*)`)
	result := re.FindAllStringSubmatch(time, -1)

	if result == nil || len(result) != 1 {
		return 0, fmt.Errorf("Time format error")
	}
	var dd uint64
	if result[0][2] != "" {
		dd, _ = strconv.ParseUint(result[0][2], 10, 32)
	}
	hh, err := strconv.ParseUint(result[0][3], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("The hour time format error")
	}
	mm, err := strconv.ParseUint(result[0][4], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("The minute time format error")
	}
	ss, err := strconv.ParseUint(result[0][5], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("The second time format error")
	}
	seconds := int64(60*60*24*dd + 60*60*hh + 60*mm + ss)
	return seconds, nil
}

func HoldReleaseJobs(jobs string, hold bool) util.CraneCmdError {
	jobIdStrSplit := strings.Split(jobs, ",")
	craneError := util.ErrorSuccess
	var job_list []uint64
	for i := 0; i < len(jobIdStrSplit); i++ {
		jobId64, err := strconv.ParseUint(jobIdStrSplit[i], 10, 32)
		if err != nil {
			fmt.Println("Invalid job Id: " + jobIdStrSplit[i])
			craneError = util.ErrorCmdArg
		} else {
			job_list = append(job_list, jobId64)
		}
	}
	if craneError != util.ErrorSuccess {
		return craneError
	}
	for _, jobId := range job_list {
		err := HoldReleaseJob(uint32(jobId), hold)
		if err != util.ErrorSuccess {
			craneError = err
		}
	}
	return craneError
}

func HoldReleaseJob(jobId uint32, hold bool) util.CraneCmdError {
	var req *protos.ModifyTaskRequest
	holdType := "Hold"

	req = &protos.ModifyTaskRequest{
		Uid:       uint32(os.Getuid()),
		TaskId:    jobId,
		Attribute: protos.ModifyTaskRequest_Hold,
	}
	if hold {
		req.Value = &protos.ModifyTaskRequest_ManualHold{
			ManualHold: true,
		}
	} else {
		req.Value = &protos.ModifyTaskRequest_ReleaseJob{
			ReleaseJob: true,
		}
		holdType = "Release"
	}

	if FlagHoldTime != "" {
		seconds, err := ParseTimeStrToSeconds(FlagHoldTime)
		if err != nil {
			log.Errorln(err)
			return util.ErrorCmdArg
		}
		if seconds == 0 {
			log.Errorln("Hold time must be greater than 0.")
			return util.ErrorCmdArg
		}
		req.HoldTimeSeconds = seconds
	}
	reply, err := stub.ModifyTask(context.Background(), req)
	if err != nil {
		log.Errorf("ModifyJob failed: " + err.Error())
		return util.ErrorNetwork
	}
	if reply.Ok {
		fmt.Printf(holdType+" job %v success.\n", jobId)
		return util.ErrorSuccess
	} else {
		fmt.Printf(holdType+" job %v failed: %s\n", jobId, reply.GetReason())
		return util.ErrorBackend
	}
}

func ChangeTaskPriority(taskId uint32, priority float64) util.CraneCmdError {
	if priority < 0 {
		log.Errorln("Priority must be greater than or equal to 0.")
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
		TaskId:    taskId,
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

	if reply.Ok {
		log.Println("Change priority success.")
		return util.ErrorSuccess
	} else {
		log.Errorf("Change priority failed: %s.\n", reply.GetReason())
		return util.ErrorBackend
	}
}

func ChangeNodeState(nodeName string, state string, reason string) util.CraneCmdError {
	var req = &protos.ModifyCranedStateRequest{}
	if nodeName == "" {
		log.Errorln("No valid node name in update node command. Specify node names by -n or --name.")
		return util.ErrorCmdArg
	} else {
		req.CranedId = nodeName
	}

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
		log.Errorf("ModifyNode failed: %v\n", err)
		return util.ErrorNetwork
	}

	if reply.Ok {
		log.Println("Change node state success.")
		return util.ErrorSuccess
	} else {
		log.Printf("Change node state failed: %s.\n", reply.GetReason())
		return util.ErrorBackend
	}
}
