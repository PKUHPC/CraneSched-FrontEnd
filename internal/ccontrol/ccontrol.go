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
	"bufio"
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
				fmt.Printf("NodeName=%v State=%v CPU=%.2f AllocCPU=%.2f FreeCPU=%.2f\n"+
					"\tRealMemory=%dM AllocMem=%dM FreeMem=%dM\n"+
					"\tPatition=%s RunningJob=%d\n\n",
					nodeInfo.Hostname, nodeInfo.State.String()[6:], nodeInfo.Cpu,
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
				fmt.Printf("NodeName=%v State=%v CPU=%.2f AllocCPU=%.2f FreeCPU=%.2f\n"+
					"\tRealMemory=%dM AllocMem=%dM FreeMem=%dM\n"+
					"\tPatition=%s RunningJob=%d\n\n",
					nodeInfo.Hostname, nodeInfo.State.String()[6:], nodeInfo.Cpu, nodeInfo.AllocCpu, nodeInfo.FreeCpu,
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
			timeSubmit := taskInfo.SubmitTime.AsTime()
			timeSubmitStr := "unknown"
			if !timeSubmit.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
				timeSubmitStr = timeSubmit.In(time.Local).Format("2006-01-02 15:04:05")
			}
			timeStart := taskInfo.StartTime.AsTime()
			timeStartStr := "unknown"
			runTimeStr := "unknown"
			if !timeStart.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
				timeStartStr = timeStart.In(time.Local).Format("2006-01-02 15:04:05")
				runTimeDuration := time.Since(timeStart)

				days := int(runTimeDuration.Hours()) / 24
				hours := int(runTimeDuration.Hours()) % 24
				minutes := int(runTimeDuration.Minutes()) % 60
				seconds := int(runTimeDuration.Seconds()) % 60

				runTimeStr = fmt.Sprintf("%d-%02d:%02d:%02d", days, hours, minutes, seconds)
			}
			timeEnd := taskInfo.EndTime.AsTime()
			timeEndStr := "unknown"
			if timeEnd.After(timeStart) {
				timeEndStr = timeEnd.In(time.Local).Format("2006-01-02 15:04:05")
				runTimeDuration := timeEnd.Sub(timeStart)

				days := int(runTimeDuration.Hours()) / 24
				hours := int(runTimeDuration.Hours()) % 24
				minutes := int(runTimeDuration.Minutes()) % 60
				seconds := int(runTimeDuration.Seconds()) % 60

				runTimeStr = fmt.Sprintf("%d-%02d:%02d:%02d", days, hours, minutes, seconds)
			}
			if taskInfo.Status.String() == "Running" {
				timeEndStr = timeStart.Add(taskInfo.TimeLimit.AsDuration()).In(time.Local).Format("2006-01-02 15:04:05")
			}
			var timeLimitStr string
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
				taskInfo.CranedList, taskInfo.NodeNum, taskInfo.CmdLine, taskInfo.Cwd)
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
	re := regexp.MustCompile(`((.*)-)?(.*):(.*):(.*)`)
	result := re.FindAllStringSubmatch(timeLimit, -1)

	if result == nil || len(result) != 1 {
		log.Errorf("Time format error")
		return util.ErrorCmdArg
	}
	var dd uint64
	if result[0][2] != "" {
		dd, _ = strconv.ParseUint(result[0][2], 10, 32)
	}
	hh, err := strconv.ParseUint(result[0][3], 10, 32)
	if err != nil {
		log.Errorf("The hour time format error")
		return util.ErrorCmdArg
	}
	mm, err := strconv.ParseUint(result[0][4], 10, 32)
	if err != nil {
		log.Errorf("The minute time format error")
		return util.ErrorCmdArg
	}
	ss, err := strconv.ParseUint(result[0][5], 10, 32)
	if err != nil {
		log.Errorf("The second time format error")
		return util.ErrorCmdArg
	}

	seconds := int64(60*60*24*dd + 60*60*hh + 60*mm + ss)

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
		req.NewState = protos.CranedState_CRANE_DRAIN
		req.Reason = reason
	case "resume":
		req.NewState = protos.CranedState_CRANE_IDLE
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

func AddNode(name string, cpu float64, mem string, partition []string) {
	memInByte, err := util.ParseMemStringAsByte(mem)
	if err != nil {
		log.Fatal(err)
	}

	var req *protos.AddNodeRequest
	req = &protos.AddNodeRequest{
		Uid: uint32(os.Getuid()),
		Node: &protos.CranedInfo{
			Hostname:       name,
			Cpu:            cpu,
			RealMem:        memInByte,
			PartitionNames: partition,
		},
	}
	reply, err := stub.AddNode(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to add a new node")
	}

	if reply.Ok {
		fmt.Println("Add node success!")
	} else {
		fmt.Printf("Add node failed: %s\n", reply.GetReason())
	}
}

func AddPartition(name string, nodes string, priority int64, allowlist []string, denylist []string) {
	var req *protos.AddPartitionRequest
	req = &protos.AddPartitionRequest{
		Uid: uint32(os.Getuid()),
		Partition: &protos.PartitionInfo{
			Name:      name,
			Hostlist:  nodes,
			Priority:  priority,
			AllowList: allowlist,
			DenyList:  denylist,
		},
	}
	reply, err := stub.AddPartition(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to add a new partition")
	}

	if reply.Ok {
		fmt.Println("Add partition success!")
	} else {
		fmt.Printf("Add partition failed: %s\n", reply.GetReason())
	}
}

func DeleteNode(name string) {
	var queryReq *protos.QueryCranedInfoRequest
	queryReq = &protos.QueryCranedInfoRequest{
		CranedName: name,
	}
	queryReply, err := stub.QueryCranedInfo(context.Background(), queryReq)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query craned info")
	}
	if len(queryReply.CranedInfoList) == 0 {
		fmt.Printf("Node %s not found.\n", name)
	} else {
		if queryReply.CranedInfoList[0].RunningTaskNum > 0 {
			for {
				fmt.Printf("There are still %d jobs running on this node. Are you sure you want to delete this node? (yes/no) ", queryReply.CranedInfoList[0].RunningTaskNum)
				reader := bufio.NewReader(os.Stdin)
				response, err := reader.ReadString('\n')
				if err != nil {
					util.GrpcErrorPrintf(err, "Failed to read from stdin")
				}

				response = strings.TrimSpace(response)
				if strings.ToLower(response) == "no" || strings.ToLower(response) == "n" {
					fmt.Println("Operation canceled.")
					os.Exit(0)
				} else if strings.ToLower(response) == "yes" || strings.ToLower(response) == "y" {
					break
				} else {
					fmt.Println("Unknown reply, please re-enter!")
				}
			}
		}
	}

	var req *protos.DeleteNodeRequest
	req = &protos.DeleteNodeRequest{
		Uid:  uint32(os.Getuid()),
		Name: name,
	}
	reply, err := stub.DeleteNode(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to delete node")
	}

	if reply.Ok {
		fmt.Println("Delete node success!")
	} else {
		fmt.Printf("Delete node %s failed: %s\n", name, reply.GetReason())
	}
}

func DeletePartition(name string) {
	var req *protos.DeletePartitionRequest
	req = &protos.DeletePartitionRequest{
		Uid:  uint32(os.Getuid()),
		Name: name,
	}
	reply, err := stub.DeletePartition(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to delete partition!")
	}

	if reply.Ok {
		fmt.Println("Delete partition success!")
	} else {
		fmt.Printf("Delete partition %s failed: %s\n", name, reply.GetReason())
	}
}

func UpdateNode(name string, cpu float64, mem string) {
	memInByte := uint64(0)
	if mem != "" {
		memParsed, err := util.ParseMemStringAsByte(mem)
		if err != nil {
			log.Fatal(err)
		}
		memInByte = memParsed
	}

	var req *protos.UpdateNodeRequest
	req = &protos.UpdateNodeRequest{
		Uid: uint32(os.Getuid()),
		Node: &protos.CranedInfo{
			Hostname: name,
			Cpu:      cpu,
			RealMem:  memInByte,
		},
	}
	reply, err := stub.UpdateNode(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to update node!")
	}

	if reply.Ok {
		fmt.Println("Update node success!")
	} else {
		fmt.Printf("Update node failed: %s\n", reply.GetReason())
	}
}

func UpdatePartition(name string, nodes string, priority int64, allowlist []string, denylist []string) {
	var req *protos.UpdatePartitionRequest
	req = &protos.UpdatePartitionRequest{
		Uid: uint32(os.Getuid()),
		Partition: &protos.PartitionInfo{
			Name:      name,
			Hostlist:  nodes,
			Priority:  priority,
			AllowList: allowlist,
			DenyList:  denylist,
		},
	}
	reply, err := stub.UpdatePartition(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to update partition")
	}

	if reply.Ok {
		fmt.Println("Update partition success!")
	} else {
		fmt.Printf("Update partition failed: %s\n", reply.GetReason())
	}
}
