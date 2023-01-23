package ccancel

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

var (
	stub protos.CraneCtldClient
)

func CancelTask(args []string) {
	req := &protos.CancelTaskRequest{
		OperatorUid: uint32(os.Getuid()),

		FilterPartition: FlagPartition,
		FilterAccount:   FlagAccount,
		FilterTaskName:  FlagTaskName,
		FilterState:     protos.TaskStatus_Invalid,
	}

	curUerName, _ := os.Hostname()
	if FlagUserName != "" && FlagUserName != curUerName {
		fmt.Println("Failed to cancel task: Permission Denied.")
		os.Exit(1)
	}

	if len(args) > 0 {
		taskIdStrSplit := strings.Split(args[0], ",")
		var taskIds []uint32
		for i := 0; i < len(taskIdStrSplit); i++ {
			taskId64, err := strconv.ParseUint(taskIdStrSplit[i], 10, 32)
			if err != nil {
				fmt.Println("Invalid task Id: " + taskIdStrSplit[i])
				os.Exit(1)
			}
			taskIds = append(taskIds, uint32(taskId64))
		}
		req.FilterTaskIds = taskIds
	}

	if FlagState != "" {
		FlagState = strings.ToLower(FlagState)
		if FlagState == "pd" || FlagState == "pending" {
			req.FilterState = protos.TaskStatus_Pending
		} else if FlagState == "r" || FlagState == "running" {
			req.FilterState = protos.TaskStatus_Running
		} else {
			fmt.Printf("Invalid FlagState, Valid job states are PENDING, RUNNING.")
			os.Exit(1)
		}
	}

	if FlagNodes != "" {
		nodeList := strings.Split(FlagNodes, ",")
		req.FilterNodes = nodeList
	}

	reply, err := stub.CancelTask(context.Background(), req)
	if err != nil {
		log.Fatalf("Failed to send TerminateTask gRPC: %s", err.Error())
	}

	if len(reply.CancelledTasks) > 0 {
		cancelledTasksStr := strconv.FormatUint(uint64(reply.CancelledTasks[0]), 10)
		for i := 1; i < len(reply.CancelledTasks); i++ {
			cancelledTasksStr += ","
			cancelledTasksStr += strconv.FormatUint(uint64(reply.CancelledTasks[i]), 10)
		}
		fmt.Printf("Task %s cancelled successfully.\n", cancelledTasksStr)
	}

	if len(reply.NotCancelledTasks) > 0 {
		for i := 0; i < len(reply.NotCancelledTasks); i++ {
			fmt.Printf("Failed to cancel task: %d. Reason: %s\n",
				reply.NotCancelledTasks[i], reply.NotCancelledReasons[i])
		}
	}
}
