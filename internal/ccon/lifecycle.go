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

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// stopExecute handles the stop command execution
func stopExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return util.NewCraneErr(util.ErrorCmdArg, "stop requires exactly one argument: CONTAINER (JOBID.STEPID)")
	}

	f := GetFlags()

	jobId, stepId, err := util.ParseJobIdStepIdStrict(args[0])
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "invalid step ID: %v", err)
	}

	idFilter := map[uint32]*protos.JobStepIds{}
	idFilter[uint32(jobId)] = &protos.JobStepIds{
		Steps: []uint32{uint32(stepId)},
	}

	// First, query the task to verify it's a container task
	queryReq := &protos.QueryTasksInfoRequest{
		FilterIds:       idFilter,
		FilterTaskTypes: []protos.TaskType{protos.TaskType_Container},
	}

	queryReply, err := stub.QueryTasksInfo(context.Background(), queryReq)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query task information")
		return util.NewCraneErr(util.ErrorNetwork, "")
	}

	if !queryReply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, "Failed to query task information")
	}

	// Check if the task exists and is a container task
	if len(queryReply.TaskInfoList) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Step %d.%d not found or is not a container", jobId, stepId))
	}

	// Create cancel request
	req := &protos.CancelTaskRequest{
		OperatorUid:     uint32(os.Getuid()),
		FilterIds:       idFilter,
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
		return util.NewCraneErr(util.ErrorNetwork, "")
	}

	if f.Global.Json {
		outputJson("stop", "", f.Stop, reply)
		if len(reply.NotCancelledJobSteps) > 0 {
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, "")
		}
	}

	// Handle non-JSON output
	if len(reply.CancelledSteps) > 0 {
		fmt.Printf("Container %d.%d stopped successfully\n", jobId, stepId)
		return nil
	}

	if len(reply.NotCancelledJobSteps) > 0 {
		reason := "Unknown error"
		if len(reply.NotCancelledJobSteps) > 0 {
			reason = reply.NotCancelledJobSteps[0].Reason
		}
		log.Errorf("Failed to stop container %d.%d: %s", jobId, stepId, reason)
		return util.NewCraneErr(util.ErrorBackend, "")
	}

	return nil
}

func rmExecute(cmd *cobra.Command, args []string) error {
	fmt.Printf("Error: 'rm' command is not supported in CraneSched.\n")
	fmt.Printf("Container tasks are automatically cleaned up when they complete.\n")
	fmt.Printf("Use 'ccon stop <task_id>' to cancel a running task.\n")

	return util.NewCraneErr(util.ErrorGeneric, "")
}

func createExecute(cmd *cobra.Command, args []string) error {
	fmt.Printf("Error: 'create' command is not supported in CraneSched.\n")
	fmt.Printf("Use 'ccon run <image>' to submit a container task.\n")

	return util.NewCraneErr(util.ErrorGeneric, "")
}

func startExecute(cmd *cobra.Command, args []string) error {
	fmt.Printf("Error: 'start' command is not supported in CraneSched.\n")
	fmt.Printf("Use 'ccon run <image>' to submit a new container task.\n")

	return util.NewCraneErr(util.ErrorGeneric, "")
}

func restartExecute(cmd *cobra.Command, args []string) error {
	fmt.Printf("Error: 'restart' command is not supported in CraneSched.\n")
	fmt.Printf("Use 'ccon run <image>' to submit a new container task.\n")

	return util.NewCraneErr(util.ErrorGeneric, "")
}
