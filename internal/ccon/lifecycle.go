/**
 * Copyright (c) 2025 Peking University and Peking University
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

package ccon

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// stopExecute handles the stop command execution
func stopExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "stop requires exactly one argument: TASK_ID",
		}
	}

	f := GetFlags()
	taskIdStr := args[0]

	// Parse task ID
	taskId, err := strconv.ParseUint(taskIdStr, 10, 32)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid task ID '%s': must be a positive integer", taskIdStr),
		}
	}

	// First, query the task to verify it's a container task
	queryReq := &protos.QueryTasksInfoRequest{
		FilterTaskIds:   []uint32{uint32(taskId)},
		FilterTaskTypes: []protos.TaskType{protos.TaskType_Container},
	}

	queryReply, err := stub.QueryTasksInfo(context.Background(), queryReq)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query task information")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if !queryReply.GetOk() {
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: "Failed to query task information",
		}
	}

	// Check if the task exists and is a container task
	if len(queryReply.TaskInfoList) == 0 {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Task %d not found or is not a container task", taskId),
		}
	}

	// Create cancel request
	req := &protos.CancelTaskRequest{
		OperatorUid:     uint32(os.Getuid()),
		FilterTaskIds:   []uint32{uint32(taskId)},
		FilterPartition: "",
		FilterAccount:   "",
		FilterState:     protos.TaskStatus_Invalid,
		FilterUsername:  "",
		FilterNodes:     nil,
		FilterTaskName:  "",
	}

	// Send cancel request
	reply, err := stub.CancelTask(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to stop container task")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if f.Global.Json {
		outputJson("stop", "", f.Stop, reply)
		if len(reply.CancelledTasks) > 0 {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	// Handle non-JSON output
	if len(reply.CancelledTasks) > 0 {
		fmt.Printf("Container task %d stopped successfully\n", taskId)
		return nil
	}

	if len(reply.NotCancelledTasks) > 0 {
		reason := "Unknown error"
		if len(reply.NotCancelledReasons) > 0 {
			reason = reply.NotCancelledReasons[0]
		}
		log.Errorf("Failed to stop container task %d: %s", taskId, reason)
		return &util.CraneError{Code: util.ErrorBackend}
	}

	return nil
}

func rmExecute(cmd *cobra.Command, args []string) error {
	fmt.Printf("Error: 'rm' command is not supported in CraneSched.\n")
	fmt.Printf("Container tasks are automatically cleaned up when they complete.\n")
	fmt.Printf("Use 'ccon stop <task_id>' to cancel a running task.\n")

	return &util.CraneError{Code: util.ErrorGeneric}
}

func createExecute(cmd *cobra.Command, args []string) error {
	fmt.Printf("Error: 'create' command is not supported in CraneSched.\n")
	fmt.Printf("Use 'ccon run <image>' to submit a container task.\n")

	return &util.CraneError{Code: util.ErrorGeneric}
}

func startExecute(cmd *cobra.Command, args []string) error {
	fmt.Printf("Error: 'start' command is not supported in CraneSched.\n")
	fmt.Printf("Use 'ccon run <image>' to submit a new container task.\n")

	return &util.CraneError{Code: util.ErrorGeneric}
}

func restartExecute(cmd *cobra.Command, args []string) error {
	fmt.Printf("Error: 'restart' command is not supported in CraneSched.\n")
	fmt.Printf("Use 'ccon run <image>' to submit a new container task.\n")

	return &util.CraneError{Code: util.ErrorGeneric}
}
