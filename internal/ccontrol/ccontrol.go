package ccontrol

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var (
	stub protos.CraneCtldClient
)

func ShowNodes(nodeName string, queryAll bool) {
	var req *protos.QueryCranedInfoRequest

	req = &protos.QueryCranedInfoRequest{CranedName: nodeName}
	reply, err := stub.QueryCranedInfo(context.Background(), req)
	if err != nil {
		panic("QueryNodeInfo failed: " + err.Error())
	}

	if queryAll {
		if len(reply.CranedInfoList) == 0 {
			fmt.Printf("No node is avalable.\n")
		} else {
			for _, nodeInfo := range reply.CranedInfoList {
				fmt.Printf("NodeName=%v State=%v CPUs=%.2f AllocCpus=%.2f FreeCpus=%.2f\n"+
					"\tRealMemory=%d AllocMem=%d FreeMem=%d\n"+
					"\tPatition=%s RunningTask=%d\n\n",
					nodeInfo.Hostname, nodeInfo.State.String(), nodeInfo.Cpus, nodeInfo.AllocCpus, nodeInfo.FreeCpus,
					nodeInfo.RealMem, nodeInfo.AllocMem, nodeInfo.FreeMem,
					strings.Join(nodeInfo.PartitionNames, ","), nodeInfo.RunningTaskNum)
			}
		}
	} else {
		if len(reply.CranedInfoList) == 0 {
			fmt.Printf("Node %s not found.\n", nodeName)
		} else {
			for _, nodeInfo := range reply.CranedInfoList {
				fmt.Printf("NodeName=%v State=%v CPUs=%.2f AllocCpus=%.2f FreeCpus=%.2f\n"+
					"\tRealMemory=%d AllocMem=%d FreeMem=%d\n"+
					"\tPatition=%s RunningTask=%d\n\n",
					nodeInfo.Hostname, nodeInfo.State.String(), nodeInfo.Cpus, nodeInfo.AllocCpus, nodeInfo.FreeCpus,
					nodeInfo.RealMem, nodeInfo.AllocMem, nodeInfo.FreeMem,
					strings.Join(nodeInfo.PartitionNames, ","), nodeInfo.RunningTaskNum)
			}
		}
	}
}

func ShowPartitions(partitionName string, queryAll bool) {
	var req *protos.QueryPartitionInfoRequest

	req = &protos.QueryPartitionInfoRequest{PartitionName: partitionName}
	reply, err := stub.QueryPartitionInfo(context.Background(), req)
	if err != nil {
		panic("QueryPartitionInfo failed: " + err.Error())
	}

	if queryAll {
		if len(reply.PartitionInfo) == 0 {
			fmt.Printf("No node is avalable.\n")
		} else {
			for _, partitionInfo := range reply.PartitionInfo {
				fmt.Printf("PartitionName=%v State=%v\n"+
					"\tTotalNodes=%d AliveNodes=%d\n"+
					"\tTotalCpus=%.2f AvailCpus=%.2f AllocCpus=%.2f\n"+
					"\tTotalMem=%d AvailMem=%d AllocMem=%d\n\tHostList=%v\n",
					partitionInfo.Name, partitionInfo.State.String(),
					partitionInfo.TotalNodes, partitionInfo.AliveNodes,
					partitionInfo.TotalCpus, partitionInfo.AvailCpus, partitionInfo.AllocCpus,
					partitionInfo.TotalMem, partitionInfo.AvailMem, partitionInfo.AllocMem, partitionInfo.Hostlist)
			}
		}
	} else {
		if len(reply.PartitionInfo) == 0 {
			fmt.Printf("Partition %s not found.\n", partitionName)
		} else {
			for _, partitionInfo := range reply.PartitionInfo {
				fmt.Printf("PartitionName=%v State=%v\n"+
					"\tTotalNodes=%d AliveNodes=%d\n"+
					"\tTotalCpus=%.2f AvailCpus=%.2f AllocCpus=%.2f\n"+
					"\tTotalMem=%d AvailMem=%d AllocMem=%d\n\tHostList=%v\n",
					partitionInfo.Name, partitionInfo.State.String(),
					partitionInfo.TotalNodes, partitionInfo.AliveNodes,
					partitionInfo.TotalCpus, partitionInfo.AvailCpus, partitionInfo.AllocCpus,
					partitionInfo.TotalMem, partitionInfo.AvailMem, partitionInfo.AllocMem, partitionInfo.Hostlist)
			}
		}
	}
}

func ShowTasks(taskId uint32, queryAll bool) {
	var req *protos.QueryTasksInfoRequest
	var taskIdList []uint32
	taskIdList = append(taskIdList, taskId)
	if queryAll {
		req = &protos.QueryTasksInfoRequest{}
	} else {
		req = &protos.QueryTasksInfoRequest{FilterTaskIds: taskIdList}
	}

	reply, err := stub.QueryTasksInfo(context.Background(), req)
	if err != nil || reply.GetOk() {
		panic("QueryTasksInfo failed: " + err.Error())
	}

	if len(reply.TaskInfoList) == 0 {
		if queryAll {
			fmt.Printf("No task is running.\n")
		} else {
			fmt.Printf("Task %d is not running.\n", taskId)
		}

	} else {
		for _, taskInfo := range reply.TaskInfoList {
			timeStart := taskInfo.StartTime.AsTime()
			timeStartStr := "unknown"
			if !timeStart.IsZero() {
				timeStartStr = timeStart.Local().String()
			}
			timeEnd := taskInfo.EndTime.AsTime()
			timeEndStr := "unknown"
			runTime := "unknown"
			if timeEnd.After(timeStart) {
				timeEndStr = timeEnd.Local().String()
				runTime = timeEnd.Sub(timeStart).String()
			}

			fmt.Printf("JobId=%v JobName=%v\n\tUserId=%d GroupId=%d Account=%v\n\tJobState=%v RunTime=%v "+
				"TimeLimit=%s SubmitTime=%v\n\tStartTime=%v EndTime=%v Partition=%v NodeList=%v "+
				"NumNodes=%d\n\tCmdLine=%v Workdir=%v\n",
				taskInfo.TaskId, taskInfo.Name, taskInfo.Uid, taskInfo.Gid,
				taskInfo.Account, taskInfo.Status.String(), runTime, util.SecondTimeFormat(taskInfo.TimeLimit.Seconds),
				timeStartStr, timeStartStr, timeEndStr, taskInfo.Partition,
				taskInfo.CranedList, taskInfo.NodeNum, taskInfo.CmdLine, taskInfo.Cwd)
		}
	}
}

func ChangeTaskTimeLimit(taskId uint32, timeLimit string) {
	re := regexp.MustCompile(`((.*)-)?(.*):(.*):(.*)`)
	result := re.FindAllStringSubmatch(timeLimit, -1)

	if result == nil || len(result) != 1 {
		util.Error("Time format error")
	}
	var dd uint64
	if result[0][2] != "" {
		dd, _ = strconv.ParseUint(result[0][2], 10, 32)
	}
	hh, err := strconv.ParseUint(result[0][3], 10, 32)
	if err != nil {
		util.Error("The hour time format error")
	}
	mm, err := strconv.ParseUint(result[0][4], 10, 32)
	if err != nil {
		util.Error("The minute time format error")
	}
	ss, err := strconv.ParseUint(result[0][5], 10, 32)
	if err != nil {
		util.Error("The second time format error")
	}

	seconds := int64(60*60*24*dd + 60*60*hh + 60*mm + ss)

	var req *protos.ModifyTaskRequest

	req = &protos.ModifyTaskRequest{
		Uid:       uint32(os.Getuid()),
		TaskId:    taskId,
		Attribute: protos.ModifyTaskRequest_TimeLimit,
		Value: &protos.ModifyTaskRequest_TimeLimitSeconds{
			TimeLimitSeconds: seconds,
		},
	}
	reply, err := stub.ModifyTask(context.Background(), req)
	if err != nil {
		panic("ModifyTask failed: " + err.Error())
	}

	if reply.Ok {
		fmt.Println("Change time limit success")
	} else {
		fmt.Printf("Chang time limit failed: %s\n", reply.GetReason())
	}
}
