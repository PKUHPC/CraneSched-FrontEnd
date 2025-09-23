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

// attachExecute handles the attach command execution
func attachExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "attach requires exactly one argument: CONTAINER_TASK_ID",
		}
	}

	taskIdStr := args[0]

	// Get flags and apply tty/stderr mutual exclusion logic
	f := GetFlags()

	// When --tty is set, --stderr should be disabled (TTY combines stdout and stderr)
	if f.Attach.Tty {
		if cmd.Flags().Changed("stderr") && f.Attach.Stderr {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: "Cannot use --stderr with --tty; stderr is combined into stdout in TTY mode",
			}
		}
		f.Attach.Stderr = false
	}

	// Parse task ID
	taskId, err := strconv.ParseUint(taskIdStr, 10, 32)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid task ID '%s': must be a positive integer", taskIdStr),
		}
	}

	// Get configuration
	config := util.ParseConfig(f.Global.ConfigPath)
	stub := util.GetStubToCtldByConfig(config)

	// First, query the task to verify it's a container task and is running
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
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Container task %d not found or is not a container task", taskId),
		}
	}

	task := queryReply.TaskInfoList[0]

	// Check if the task is in a state that allows attaching
	if task.Status != protos.TaskStatus_Running {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Cannot attach to task %d in state: %s", taskId, task.Status.String()),
		}
	}

	// Call AttachContainerTask RPC
	attachReq := &protos.AttachContainerTaskRequest{
		Uid:    uint32(os.Getuid()),
		TaskId: uint32(taskId),
		Stdin:  f.Attach.Stdin,
		Tty:    f.Attach.Tty,
		Stdout: f.Attach.Stdout,
		Stderr: f.Attach.Stderr,
	}

	log.Infof("Calling AttachContainerTask RPC for task %d with flags: stdin=%t, stdout=%t, stderr=%t, tty=%t, sig-proxy=%t",
		taskId, attachReq.Stdin, attachReq.Stdout, attachReq.Stderr, attachReq.Tty, f.Attach.SigProxy)

	attachReply, err := stub.AttachContainerTask(context.Background(), attachReq)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to attach to container task")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if !attachReply.Ok {
		err = &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Attach failed: %s", attachReply.Reason),
		}
	}

	if f.Global.Json {
		outputJson("attach", "", f.Attach, attachReply)
		return err
	}

	// Handle RPC response
	if attachReply.Ok {
		if !f.Global.Json {
			log.Debugf("Attach request successful for task %d\n", taskId)
			if f.Attach.Tty {
				log.Debugf("Attaching to container task %d (TTY enabled)...\n", taskId)
			} else {
				log.Debugf("Attaching to container task %d...\n", taskId)
			}
		}

		// Create stream options based on flags
		streamOpts := StreamOptions{
			Stdin:  f.Attach.Stdin,
			Stdout: f.Attach.Stdout,
			Stderr: f.Attach.Stderr,
			Tty:    f.Attach.Tty,
		}

		// Start streaming
		ctx := context.Background()
		if err := StreamWithURL(ctx, attachReply.Url, streamOpts); err != nil {
			return &util.CraneError{
				Code:    util.ErrorBackend,
				Message: fmt.Sprintf("Failed to establish stream connection: %v", err),
			}
		}
	}

	return err
}
