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
		return util.NewCraneErr(util.ErrorCmdArg, "ps command does not accept any arguments")
	}

	f := GetFlags()
	request := protos.QueryTasksInfoRequest{
		FilterTaskTypes:             []protos.TaskType{protos.TaskType_Container},
		OptionIncludeCompletedTasks: f.Ps.All,
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query container tasks")
		return util.NewCraneErr(util.ErrorNetwork, "")
	}

	if !reply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, "")
	}

	// Flatten steps for container view
	type stepRow struct {
		jobId     uint32
		stepId    uint32
		image     string
		command   string
		createdAt time.Time
		status    protos.TaskStatus
		name      string
	}

	var rows []stepRow
	for _, task := range reply.TaskInfoList {
		for _, step := range task.StepInfoList {
			if step.StepId == 0 || step.ContainerMeta == nil {
				// Skip pod step or non-container step
				continue
			}

			row := stepRow{
				jobId:  task.TaskId,
				stepId: step.StepId,
				name:   step.Name,
				status: step.Status,
			}

			if step.ContainerMeta.Image != nil {
				row.image = step.ContainerMeta.Image.Image
			}
			if step.ContainerMeta != nil {
				var cmdParts []string
				if step.ContainerMeta.Command != "" {
					cmdParts = append(cmdParts, step.ContainerMeta.Command)
				}
				if len(step.ContainerMeta.Args) > 0 {
					cmdParts = append(cmdParts, step.ContainerMeta.Args...)
				}
				if len(cmdParts) > 0 {
					row.command = strings.Join(cmdParts, " ")
				}
			}
			if step.SubmitTime != nil {
				row.createdAt = step.SubmitTime.AsTime()
			} else if task.SubmitTime != nil {
				row.createdAt = task.SubmitTime.AsTime()
			}

			rows = append(rows, row)
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].jobId == rows[j].jobId {
			return rows[i].stepId > rows[j].stepId
		}
		return rows[i].jobId > rows[j].jobId
	})

	if f.Global.Json {
		outputJson("ps", "", f.Ps, rows)
		return nil
	}

	if f.Ps.Quiet {
		for _, row := range rows {
			fmt.Printf("%d.%d\n", row.jobId, row.stepId)
		}
		return nil
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	table.SetHeader([]string{"CONTAINER", "IMAGE", "COMMAND", "CREATED", "STATUS", "NAME"})

	for _, row := range rows {
		createdStr := "-"
		if !row.createdAt.IsZero() {
			createdStr = formatDuration(time.Since(row.createdAt)) + " ago"
		}
		table.Append([]string{
			fmt.Sprintf("%d.%d", row.jobId, row.stepId),
			row.image,
			truncateCommand(row.command, 20),
			createdStr,
			strings.ToUpper(row.status.String()),
			row.name,
		})
	}
	table.Render()
	return nil
}

func podExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "pod command does not accept any arguments")
	}

	f := GetFlags()
	request := protos.QueryTasksInfoRequest{
		FilterTaskTypes:             []protos.TaskType{protos.TaskType_Container},
		OptionIncludeCompletedTasks: f.Pod.All,
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query container pods")
		return util.NewCraneErr(util.ErrorNetwork, "")
	}

	if !reply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, "")
	}

	sort.Slice(reply.TaskInfoList, func(i, j int) bool {
		return reply.TaskInfoList[i].TaskId > reply.TaskInfoList[j].TaskId
	})

	if f.Global.Json {
		outputJson("pods", "", f.Pod, reply.TaskInfoList)
		return nil
	}

	if f.Pod.Quiet {
		for _, task := range reply.TaskInfoList {
			fmt.Println(task.TaskId)
		}
		return nil
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	table.SetHeader([]string{"JOBID", "POD", "PARTITION", "CREATED", "STATUS", "PORTS"})

	for _, task := range reply.TaskInfoList {
		createdStr := "-"
		if task.SubmitTime != nil {
			createdStr = formatDuration(time.Since(task.SubmitTime.AsTime())) + " ago"
		}

		podName := "-"
		if task.PodMeta != nil && task.PodMeta.Name != "" {
			podName = task.PodMeta.Name
		}

		var ports string
		if task.PodMeta != nil && len(task.PodMeta.Ports) > 0 {
			var portStrs []string
			for _, port := range task.PodMeta.Ports {
				portStrs = append(portStrs, fmt.Sprintf("%d:%d", port.HostPort, port.ContainerPort))
			}
			ports = strings.Join(portStrs, ", ")
		} else {
			ports = "-"
		}

		table.Append([]string{
			strconv.FormatUint(uint64(task.TaskId), 10),
			podName,
			task.Partition,
			createdStr,
			strings.ToUpper(task.Status.String()),
			ports,
		})
	}

	table.Render()
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

func inspectPodExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return util.NewCraneErr(util.ErrorCmdArg, "inspectp requires exactly one argument: POD")
	}

	jobIDStr := args[0]
	jobID, err := strconv.ParseUint(jobIDStr, 10, 32)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid job ID: %s", jobIDStr))
	}

	idFilter := map[uint32]*protos.JobStepIds{}
	idFilter[uint32(jobID)] = &protos.JobStepIds{}
	request := protos.QueryTasksInfoRequest{
		FilterIds:                   idFilter,
		FilterTaskTypes:             []protos.TaskType{protos.TaskType_Container},
		OptionIncludeCompletedTasks: true,
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query container pod")
		return util.NewCraneErr(util.ErrorNetwork, "")
	}

	if !reply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, "")
	}

	if len(reply.TaskInfoList) == 0 {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container pod %s not found", jobIDStr))
	}

	task := reply.TaskInfoList[0]
	jsonData, err := json.MarshalIndent(task, "", "  ")
	if err != nil {
		return util.WrapCraneErr(util.ErrorBackend, "failed to format data from backend: %v", err)
	}

	fmt.Println(string(jsonData))
	return nil
}

func inspectStepExecute(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return util.NewCraneErr(util.ErrorCmdArg, "inspect requires exactly one argument: CONTAINER (format: JOBID.STEPID)")
	}

	jobID, stepID, err := util.ParseJobIdStepIdStrict(args[0])
	if err != nil {
		return err
	}

	if stepID == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "step 0 is reserved for pods. Please use inspectp to query it.")
	}

	idFilter := map[uint32]*protos.JobStepIds{}
	idFilter[jobID] = &protos.JobStepIds{Steps: []uint32{stepID}}
	request := protos.QueryTasksInfoRequest{
		FilterIds:                   idFilter,
		FilterTaskTypes:             []protos.TaskType{protos.TaskType_Container},
		OptionIncludeCompletedTasks: true,
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query container step")
		return util.NewCraneErr(util.ErrorNetwork, "")
	}

	if !reply.GetOk() {
		return util.NewCraneErr(util.ErrorBackend, "")
	}

	if len(reply.TaskInfoList) == 0 {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container step %d.%d not found", jobID, stepID))
	}

	var targetStep *protos.StepInfo
	for _, step := range reply.TaskInfoList[0].StepInfoList {
		if step.StepId == stepID {
			targetStep = step
			break
		}
	}

	if targetStep == nil {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container step %d.%d not found", jobID, stepID))
	}

	jsonData, err := json.MarshalIndent(targetStep, "", "  ")
	if err != nil {
		return util.WrapCraneErr(util.ErrorBackend, "failed to format data from backend: %v", err)
	}

	fmt.Println(string(jsonData))
	return nil
}

// getContainerStep queries exact 1 specific container step and returns the task and step info.
func getContainerStep(jobID, stepID uint32, includeCompleted bool) (*protos.TaskInfo, *protos.StepInfo, error) {
	idFilter := map[uint32]*protos.JobStepIds{
		jobID: {Steps: []uint32{stepID}},
	}
	req := protos.QueryTasksInfoRequest{
		FilterIds:                   idFilter,
		FilterTaskTypes:             []protos.TaskType{protos.TaskType_Container},
		OptionIncludeCompletedTasks: includeCompleted,
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query container step")
		return nil, nil, util.NewCraneErr(util.ErrorNetwork, "")
	}
	if !reply.GetOk() {
		return nil, nil, util.NewCraneErr(util.ErrorBackend, "")
	}

	if len(reply.TaskInfoList) == 0 {
		return nil, nil, util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container %d.%d not found", jobID, stepID))
	}

	task := reply.TaskInfoList[0]
	var targetStep *protos.StepInfo
	for _, step := range task.StepInfoList {
		if step.StepId == stepID {
			targetStep = step
			break
		}
	}

	if targetStep == nil {
		return nil, nil, util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("container %d.%d not found", jobID, stepID))
	}

	return task, targetStep, nil
}
