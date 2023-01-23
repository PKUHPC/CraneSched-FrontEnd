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

	reqState := 10
	req := &protos.CancelTaskRequest{OperatorUid: uint32(os.Getuid()),
		Partition: partition,
		TaskName:  taskName,
		Account:   account,
		State:     protos.TaskStatus(reqState),
	}

	curUerName, _ := os.Hostname()
	if userName != "" && userName != curUerName {
		fmt.Println("Failed to cancel task: Permission Denied.")
		os.Exit(1)
	}

	if partition == "" && taskName == "" && state == "" && account == "" && nodes == "" && userName == "" {
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

	if state != "" {
		state = strings.ToLower(state)
		if state == "pd" || state == "pending" {
			req.State = protos.TaskStatus_Pending
		} else if state == "r" || state == "running" {
			req.State = protos.TaskStatus_Running
		} else {
			fmt.Printf("Invalid state, Valid job states are PENDING, RUNNING.")
			os.Exit(1)
		}
	}
	nodeList := strings.Split(nodes, ",")
	if nodes != "" {
		req.Nodes = nodeList
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
		fmt.Print("Failed to cancel task: %d, %s\n", reply.NotCancelledId, reply.Reason)
	}
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
