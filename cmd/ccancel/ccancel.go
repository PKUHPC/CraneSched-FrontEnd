package main

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"os"
	"strconv"
)

func main() {
	taskId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal("Invalid task id!")
	}

	path := "/etc/crane/config.yaml"
	config := util.ParseConfig(path)

	serverAddr := fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		panic("Cannot connect to CraneCtld: " + err.Error())
	}

	stub := protos.NewCraneCtldClient(conn)
	req := &protos.CancelTaskRequest{TaskId: uint32(taskId)}

	reply, err := stub.CancelTask(context.Background(), req)
	if err != nil {
		log.Fatalf("Failed to send TerminateTask gRPC: %s", err.Error())
	}

	if reply.Ok {
		fmt.Printf("Task #%d is terminating...\n", taskId)
	} else {
		fmt.Printf("Failed to terminating task #%d: %s\n", taskId, reply.Reason)
	}
}
