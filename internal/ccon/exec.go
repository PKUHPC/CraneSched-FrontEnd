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

// execExecute handles the exec command execution
func execExecute(cmd *cobra.Command, args []string) error {
	if len(args) < 2 {
		return util.NewCraneErr(util.ErrorCmdArg, "exec requires at least two arguments: CONTAINER_TASK_ID COMMAND [ARG...]")
	}

	taskIdStr := args[0]
	command := args[1:]

	// Get flags
	f := GetFlags()

	// Parse task ID
	taskId, err := strconv.ParseUint(taskIdStr, 10, 32)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid task ID '%s': must be a positive integer", taskIdStr))
	}

	// First, query the task to verify it's a container task and is running
	queryReq := &protos.QueryTasksInfoRequest{
		FilterTaskIds:   []uint32{uint32(taskId)},
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
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Container task %d not found or is not a container task", taskId))
	}

	task := queryReply.TaskInfoList[0]

	// Check if the task is in a state that allows exec
	if task.Status != protos.TaskStatus_Running {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Cannot exec into task %d in state: %s", taskId, task.Status.String()))
	}

	// Determine stdin, stdout, stderr based on flags
	stdin := f.Exec.Interactive
	tty := f.Exec.Tty
	stdout := true // Always enable stdout for exec
	stderr := !tty // Enable stderr only in non-TTY mode

	// Call ExecInContainerTask RPC
	execReq := &protos.ExecInContainerTaskRequest{
		Uid:     uint32(os.Getuid()),
		TaskId:  uint32(taskId),
		Command: command,
		Stdin:   stdin,
		Tty:     tty,
		Stdout:  stdout,
		Stderr:  stderr,
	}

	log.Debugf("Calling ExecInContainerTask RPC for task %d with command %v, flags: stdin=%t, stdout=%t, stderr=%t, tty=%t",
		taskId, command, execReq.Stdin, execReq.Stdout, execReq.Stderr, execReq.Tty)

	reply, err := stub.ExecInContainerTask(context.Background(), execReq)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to exec into container task")
		return util.NewCraneErr(util.ErrorNetwork, "")
	}

	if !reply.Ok {
		err = util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Exec failed: %s", reply.GetStatus().GetDescription()))
	}

	if f.Global.Json {
		outputJson("exec", "", f.Exec, reply)
		return err
	}

	// Handle RPC response
	if reply.Ok {
		if !f.Global.Json {
			log.Debugf("Exec request successful for task %d\n", taskId)
			if tty {
				log.Debugf("Executing command in container task %d (TTY enabled)...\n", taskId)
			} else {
				log.Debugf("Executing command in container task %d...\n", taskId)
			}
		}

		// Create stream options based on flags
		streamOpts := StreamOptions{
			Stdin:     stdin,
			Stdout:    stdout,
			Stderr:    stderr,
			Tty:       tty,
			Transport: f.Exec.Transport,
		}

		// Start streaming
		ctx := context.Background()
		if err := StreamWithURL(ctx, reply.Url, streamOpts); err != nil {
			return util.WrapCraneErr(util.ErrorBackend, "Failed to establish stream connection: %v", err)
		}
	}

	return err
}
