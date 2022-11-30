package ccontrol

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
				fmt.Printf("NodeName=%v State=%v CPUs=%.2f AllocCpus=%.2f FreeCpus=%.2f\n\tRealMemory=%d AllocMem=%d FreeMem=%d\n\tPatition=%s RunningTask=%d\n\n", nodeInfo.Hostname, nodeInfo.State.String(), nodeInfo.Cpus, nodeInfo.AllocCpus, nodeInfo.FreeCpus, nodeInfo.RealMem, nodeInfo.AllocMem, nodeInfo.FreeMem, nodeInfo.PartitionName, nodeInfo.RunningTaskNum)
			}
		}
	} else {
		if len(reply.CranedInfoList) == 0 {
			fmt.Printf("Node %s not found.\n", nodeName)
		} else {
			for _, nodeInfo := range reply.CranedInfoList {
				fmt.Printf("NodeName=%v State=%v CPUs=%.2f AllocCpus=%.2f FreeCpus=%.2f\n\tRealMemory=%d AllocMem=%d FreeMem=%d\n\tPatition=%s RunningTask=%d\n\n", nodeInfo.Hostname, nodeInfo.State.String(), nodeInfo.Cpus, nodeInfo.AllocCpus, nodeInfo.FreeCpus, nodeInfo.RealMem, nodeInfo.AllocMem, nodeInfo.FreeMem, nodeInfo.PartitionName, nodeInfo.RunningTaskNum)
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
				fmt.Printf("PartitionName=%v State=%v\n\tTotalNodes=%d AliveNodes=%d\n\tTotalCpus=%.2f AvailCpus=%.2f AllocCpus=%.2f\n\tTotalMem=%d AvailMem=%d AllocMem=%d\n\tHostList=%v\n", partitionInfo.Name, partitionInfo.State.String(), partitionInfo.TotalNodes, partitionInfo.AliveNodes, partitionInfo.TotalCpus, partitionInfo.AvailCpus, partitionInfo.AllocCpus, partitionInfo.TotalMem, partitionInfo.AvailMem, partitionInfo.AllocMem, partitionInfo.Hostlist)
			}
		}
	} else {
		if len(reply.PartitionInfo) == 0 {
			fmt.Printf("Partition %s not found.\n", partitionName)
		} else {
			for _, partitionInfo := range reply.PartitionInfo {
				fmt.Printf("PartitionName=%v State=%v\n\tTotalNodes=%d AliveNodes=%d\n\tTotalCpus=%.2f AvailCpus=%.2f AllocCpus=%.2f\n\tTotalMem=%d AvailMem=%d AllocMem=%d\n\tHostList=%v\n", partitionInfo.Name, partitionInfo.State.String(), partitionInfo.TotalNodes, partitionInfo.AliveNodes, partitionInfo.TotalCpus, partitionInfo.AvailCpus, partitionInfo.AllocCpus, partitionInfo.TotalMem, partitionInfo.AvailMem, partitionInfo.AllocMem, partitionInfo.Hostlist)
			}
		}
	}
}

func ShowJobs(jobId uint32, queryAll bool) {
	var req *protos.QueryJobsInfoRequest
	req = &protos.QueryJobsInfoRequest{FindAll: queryAll, JobId: jobId}

	reply, err := stub.QueryJobsInfo(context.Background(), req)
	if err != nil {
		panic("QueryJobsInfo failed: " + err.Error())
	}

	if len(reply.TaskInfoList) == 0 {
		if queryAll {
			fmt.Printf("No job is running.\n")
		} else {
			fmt.Printf("Job %d is not running.\n", jobId)
		}

	} else {
		for _, jobInfo := range reply.TaskInfoList {
			timeStart := jobInfo.StartTime.AsTime()
			timeStartStr := "unknown"
			if !timeStart.IsZero() {
				timeStartStr = timeStart.Local().String()
			}
			timeEnd := jobInfo.EndTime.AsTime()
			timeEndStr := "unknown"
			runTime := "unknown"
			if timeEnd.After(timeStart) {
				timeEndStr = timeEnd.Local().String()
				runTime = timeEnd.Sub(timeStart).String()
			}

			fmt.Printf("JobId=%v JobName=%v\n\tUserId=%d GroupId=%d Account=%v\n\tJobState=%v RunTime=%v TimeLimit=%v SubmitTime=%v\n\tStartTime=%v EndTime=%v Partition=%v NodeList=%v NumNodes=%d\n\tCmdLine=%v Workdir=%v\n", jobInfo.TaskId, jobInfo.SubmitInfo.Name, jobInfo.SubmitInfo.Uid, jobInfo.Gid, jobInfo.Account, jobInfo.Status.String(), runTime, jobInfo.SubmitInfo.TimeLimit.String(), timeStartStr, timeStartStr, timeEndStr, jobInfo.SubmitInfo.PartitionName, jobInfo.CranedList, jobInfo.SubmitInfo.NodeNum, jobInfo.SubmitInfo.CmdLine, jobInfo.SubmitInfo.Cwd)
		}
	}
}

func Init() {
	//if len(os.Args) <= 1 {
	//	fmt.Println("Arg must > 1")
	//	os.Exit(1)
	//}

	config := util.ParseConfig()

	serverAddr := fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("Cannot connect to CraneCtld: " + err.Error())
	}

	stub = protos.NewCraneCtldClient(conn)
}
