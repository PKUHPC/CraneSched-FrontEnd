package main

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) <= 1 {
		fmt.Println("Arg must > 1")
		os.Exit(1)
	}

	path := "/etc/crane/config.yaml"
	config := util.ParseConfig(path)

	serverAddr := fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		panic("Cannot connect to CraneCtld: " + err.Error())
	}

	stub := protos.NewCraneCtldClient(conn)

	if os.Args[1] == "show" {
		if os.Args[2] == "node" {
			var req *protos.QueryCranedInfoRequest
			queryAll := false
			nodeName := ""

			if len(os.Args) <= 3 {
				req = &protos.QueryCranedInfoRequest{CranedName: ""}
				queryAll = true
			} else {
				nodeName = os.Args[3]
				req = &protos.QueryCranedInfoRequest{CranedName: nodeName}
			}

			reply, err := stub.QueryCranedInfo(context.Background(), req)
			if err != nil {
				panic("QueryNodeInfo failed: " + err.Error())
			}

			if queryAll {
				if len(reply.CranedInfoList) == 0 {
					fmt.Printf("No node is avalable.\n")
				} else {
					for _, nodeInfo := range reply.CranedInfoList {
						fmt.Printf("NodeName=%v State=%v CPUs=%d AllocCpus=%d FreeCpus=%d\n\tRealMemory=%d AllocMem=%d FreeMem=%d\n\tPatition=%s RunningTask=%d\n\n", nodeInfo.Hostname, nodeInfo.State.String(), nodeInfo.Cpus, nodeInfo.AllocCpus, nodeInfo.FreeCpus, nodeInfo.RealMem, nodeInfo.AllocMem, nodeInfo.FreeMem, nodeInfo.PartitionName, nodeInfo.RunningTaskNum)
					}
				}
			} else {
				if len(reply.CranedInfoList) == 0 {
					fmt.Printf("Node %s not found.\n", nodeName)
				} else {
					for _, nodeInfo := range reply.CranedInfoList {
						fmt.Printf("NodeName=%v State=%v CPUs=%d AllocCpus=%d FreeCpus=%d\n\tRealMemory=%d AllocMem=%d FreeMem=%d\n\tPatition=%s RunningTask=%d\n\n", nodeInfo.Hostname, nodeInfo.State.String(), nodeInfo.Cpus, nodeInfo.AllocCpus, nodeInfo.FreeCpus, nodeInfo.RealMem, nodeInfo.AllocMem, nodeInfo.FreeMem, nodeInfo.PartitionName, nodeInfo.RunningTaskNum)
					}
				}
			}
		} else if os.Args[2] == "partition" {
			var req *protos.QueryPartitionInfoRequest
			queryAll := false
			partitionName := ""

			if len(os.Args) <= 3 {
				req = &protos.QueryPartitionInfoRequest{PartitionName: ""}
				queryAll = true
			} else {
				partitionName = os.Args[3]
				req = &protos.QueryPartitionInfoRequest{PartitionName: partitionName}
			}

			reply, err := stub.QueryPartitionInfo(context.Background(), req)
			if err != nil {
				panic("QueryPartitionInfo failed: " + err.Error())
			}

			if queryAll {
				if len(reply.PartitionInfo) == 0 {
					fmt.Printf("No node is avalable.\n")
				} else {
					for _, partitionInfo := range reply.PartitionInfo {
						fmt.Printf("PartitionName=%v State=%v\n\tTotalNodes=%d AliveNodes=%d\n\tTotalCpus=%d AvailCpus=%d AllocCpus=%d FreeCpus=%d\n\tTotalMem=%d AvailMem=%d AllocMem=%d FreeMem=%d\n\tHostList=%v\n", partitionInfo.Name, partitionInfo.State.String(), partitionInfo.TotalNodes, partitionInfo.AliveNodes, partitionInfo.TotalCpus, partitionInfo.AvailCpus, partitionInfo.AllocCpus, partitionInfo.FreeCpus, partitionInfo.TotalMem, partitionInfo.AvailMem, partitionInfo.AllocMem, partitionInfo.FreeMem, partitionInfo.Hostlist)
					}
				}
			} else {
				if len(reply.PartitionInfo) == 0 {
					fmt.Printf("Partition %s not found.\n", partitionName)
				} else {
					for _, partitionInfo := range reply.PartitionInfo {
						fmt.Printf("PartitionName=%v State=%v\n\tTotalNodes=%d AliveNodes=%d\n\tTotalCpus=%d AvailCpus=%d AllocCpus=%d FreeCpus=%d\n\tTotalMem=%d AvailMem=%d AllocMem=%d FreeMem=%d\n\tHostList=%v\n", partitionInfo.Name, partitionInfo.State.String(), partitionInfo.TotalNodes, partitionInfo.AliveNodes, partitionInfo.TotalCpus, partitionInfo.AvailCpus, partitionInfo.AllocCpus, partitionInfo.FreeCpus, partitionInfo.TotalMem, partitionInfo.AvailMem, partitionInfo.AllocMem, partitionInfo.FreeMem, partitionInfo.Hostlist)
					}
				}
			}
		} else if os.Args[2] == "job" {
			var req *protos.QueryJobsInfoRequest
			queryAll := false
			var jobId uint32

			if len(os.Args) <= 3 {
				queryAll = true
			} else {
				int, _ := strconv.Atoi(os.Args[3])
				jobId = uint32(int)
			}
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
					timeLimit, err := ptypes.Duration(jobInfo.SubmitInfo.TimeLimit)
					if err != nil {
						fmt.Println(err)
					}
					timeLimitStr := timeLimit.String()
					timeStart, err := ptypes.Timestamp(jobInfo.StartTime)
					if err != nil {
						fmt.Println(err)
					}
					timeStartStr := "unknown"
					if !timeStart.IsZero() {
						timeStartStr = timeStart.Local().String()
					}
					timeEnd, err := ptypes.Timestamp(jobInfo.EndTime)
					if err != nil {
						fmt.Println(err)
					}
					timeEndStr := "unknown"
					runTime := "unknown"
					if timeEnd.After(timeStart) {
						timeEndStr = timeEnd.Local().String()
						runTime = timeEnd.Sub(timeStart).String()
					}

					fmt.Printf("JobId=%v JobName=%v\n\tUserId=%d GroupId=%d Account=%v\n\tJobState=%v RunTime=%v TimeLimit=%v SubmitTime=%v\n\tStartTime=%v EndTime=%v Partition=%v NodeList=%v NumNodes=%d\n\tCmdLine=%v Workdir=%v\n", jobInfo.TaskId, jobInfo.SubmitInfo.Name, jobInfo.SubmitInfo.Uid, jobInfo.Gid, jobInfo.Account, jobInfo.Status.String(), runTime, timeLimitStr, timeStartStr, timeStartStr, timeEndStr, jobInfo.SubmitInfo.PartitionName, jobInfo.CranedList, jobInfo.SubmitInfo.NodeNum, jobInfo.SubmitInfo.CmdLine, jobInfo.SubmitInfo.Cwd)
				}
			}
		}
	}
}
