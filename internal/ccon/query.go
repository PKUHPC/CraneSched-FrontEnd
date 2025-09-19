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
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

func psExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "ps command does not accept any arguments",
		}
	}

	f := GetFlags()
	config := util.ParseConfig(f.Global.ConfigPath)
	stub := util.GetStubToCtldByConfig(config)

	request := protos.QueryTasksInfoRequest{
		FilterTaskTypes:             []protos.TaskType{protos.TaskType_Container},
		OptionIncludeCompletedTasks: f.Ps.All,
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query container tasks")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if !reply.GetOk() {
		return &util.CraneError{Code: util.ErrorBackend}
	}

	// Sort tasks by Task ID descending
	sort.Slice(reply.TaskInfoList, func(i, j int) bool {
		taskI := reply.TaskInfoList[i]
		taskJ := reply.TaskInfoList[j]

		return taskI.TaskId > taskJ.TaskId
	})

	if f.Global.Json {
		jsonData, _ := json.Marshal(reply.TaskInfoList)
		fmt.Println(string(jsonData))
		return nil
	}

	if f.Ps.Quiet {
		for _, task := range reply.TaskInfoList {
			fmt.Println(strconv.FormatUint(uint64(task.TaskId), 10))
		}
	} else {
		table := tablewriter.NewWriter(os.Stdout)
		util.SetBorderlessTable(table)
		table.SetHeader([]string{"JOBID", "IMAGE", "COMMAND", "CREATED", "STATUS", "PORTS", "NAMES"})

		for _, task := range reply.TaskInfoList {
			jobID := strconv.FormatUint(uint64(task.TaskId), 10)

			var image string
			if task.ContainerMeta != nil && task.ContainerMeta.Image != nil {
				image = task.ContainerMeta.Image.Image
			} else {
				image = "-"
			}

			var command string
			if task.ContainerMeta != nil {
				// Build command from container metadata
				var cmdParts []string
				if task.ContainerMeta.Command != "" {
					cmdParts = append(cmdParts, task.ContainerMeta.Command)
				}
				if len(task.ContainerMeta.Args) > 0 {
					cmdParts = append(cmdParts, task.ContainerMeta.Args...)
				}
				if len(cmdParts) > 0 {
					command = strings.Join(cmdParts, " ")
				}
			}
			command = truncateCommand(command, 40)

			status := strings.ToUpper(task.Status.String())
			createdStr := formatDuration(time.Since(task.SubmitTime.AsTime())) + " ago"

			var ports string
			if task.ContainerMeta != nil && len(task.ContainerMeta.Ports) > 0 {
				var portStrs []string
				for hostPort, containerPort := range task.ContainerMeta.Ports {
					portStrs = append(portStrs, fmt.Sprintf("%d:%d", hostPort, containerPort))
				}
				ports = strings.Join(portStrs, ", ")
			} else {
				ports = "-"
			}

			table.Append([]string{
				jobID,
				image,
				command,
				createdStr,
				status,
				ports,
				task.Name,
			})
		}
		table.Render()
	}

	return nil
}

func inspectExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "inspect requires exactly one argument: CONTAINER",
		}
	}

	jobIDStr := args[0]
	jobID, err := strconv.ParseUint(jobIDStr, 10, 32)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("invalid job ID: %s", jobIDStr),
		}
	}

	f := GetFlags()
	config := util.ParseConfig(f.Global.ConfigPath)
	stub := util.GetStubToCtldByConfig(config)

	request := protos.QueryTasksInfoRequest{
		FilterTaskIds:               []uint32{uint32(jobID)},
		FilterTaskTypes:             []protos.TaskType{protos.TaskType_Container},
		OptionIncludeCompletedTasks: true,
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query container task")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if !reply.GetOk() {
		return &util.CraneError{Code: util.ErrorBackend}
	}

	if len(reply.TaskInfoList) == 0 {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("container with job ID %s not found", jobIDStr),
		}
	}

	task := reply.TaskInfoList[0]

	jsonData, err := json.MarshalIndent(task, "", "  ")
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("failed to format data from backend: %v", err),
		}
	}

	fmt.Println(string(jsonData))
	return nil
}

func truncateCommand(command string, maxLen int) string {
	if command == "" {
		return "-"
	}

	// Remove extra whitespace and newlines
	command = strings.ReplaceAll(command, "\n", " ")
	command = strings.ReplaceAll(command, "\t", " ")

	// Replace multiple spaces with single space
	for strings.Contains(command, "  ") {
		command = strings.ReplaceAll(command, "  ", " ")
	}
	command = strings.TrimSpace(command)

	if len(command) <= maxLen {
		return command
	}

	return command[:maxLen-3] + "..."
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%d seconds", int(d.Seconds()))
	} else if d < time.Hour {
		return fmt.Sprintf("%d minutes", int(d.Minutes()))
	} else if d < 24*time.Hour {
		return fmt.Sprintf("%d hours", int(d.Hours()))
	} else {
		return fmt.Sprintf("%d days", int(d.Hours()/24))
	}
}
