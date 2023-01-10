package ccancel

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"os"
	"strconv"
	"strings"
)

var (
	stub protos.CraneCtldClient
)

func CancelTask() {
	req := &protos.CancelTaskRequest{OperatorUid: uint32(os.Getuid()), Partition: partition, TaskName: name}
	//
	//os.Hostname()只能是自己，否则不允许

	if partition == "" && name == "" && state == "" {
		taskId := strings.Split(os.Args[1], ",")
		var taskIds []uint32
		for i := 0; i < len(taskId); i++ {
			taskId64, err := strconv.ParseUint(taskId[i], 10, 32)
			if err != nil {
				fmt.Println("Invalid task Id: " + taskId[i])
				os.Exit(1)
			}
			taskIds = append(taskIds, uint32(taskId64))
		}
		req.TaskId = taskIds
	}
	for i := 0; i < len(GetTaskIds()); i++ {
		id := GetTaskIds()
		req.TaskId = append(req.TaskId, id[i])
	}

	reply, err := stub.CancelTask(context.Background(), req)
	if err != nil {
		log.Fatalf("Failed to send TerminateTask gRPC: %s", err.Error())
	}

	if len(reply.CancelledTasks) > 0 {
		cancelledTasks := strconv.FormatUint(uint64(reply.CancelledTasks[0]), 10)
		for i := 1; i < len(reply.CancelledTasks); i++ {
			cancelledTasks += ","
			cancelledTasks += strconv.FormatUint(uint64(reply.CancelledTasks[i]), 10)
		}
		fmt.Printf("Task %s cancelled successfully.\n", cancelledTasks)
	}

	if !reply.Ok {
		fmt.Printf("Failed to cancel task: %s\n", reply.Reason)
	}
}

func GetTaskIds() []uint32 {
	var taskIds []uint32
	request := protos.QueryJobsInPartitionRequest{FindAll: true}
	reply, err := stub.QueryJobsInPartition(context.Background(), &request)
	if err != nil {
		panic("QueryJobsInPartition failed: " + err.Error())
	}
	var reqState protos.TaskStatus
	reqState = -1
	switch strings.ToLower(state) {
	case "pd", "pending":
		reqState = protos.TaskStatus_Pending
	case "r", "running":
		reqState = protos.TaskStatus_Running
	default:
		reqState = -1
	}
	if state != "" {
		for i := 0; i < len(reply.TaskMetas); i++ {
			if reqState != reply.TaskStatus[i] {
				continue
			}
			taskIds = append(taskIds, reply.TaskIds[i])
		}
	}

	return taskIds
}

func Init() {
	config := util.ParseConfig()

	serverAddr := fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("Cannot connect to CraneCtld: " + err.Error())
	}

	stub = protos.NewCraneCtldClient(conn)
}
