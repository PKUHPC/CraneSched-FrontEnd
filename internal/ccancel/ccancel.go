/**
 * Copyright (c) 2024 Peking University and Peking University
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

package ccancel

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
)

var (
	stub protos.CraneCtldSecureClient
)

func CancelTask(args []string) error {
	req := &protos.CancelTaskRequest{
		OperatorUid: uint32(os.Getuid()),

		FilterPartition: FlagPartition,
		FilterAccount:   FlagAccount,
		FilterState:     protos.TaskStatus_Invalid,
		FilterUsername:  FlagUserName,
	}

	err := util.CheckJobNameLength(FlagJobName)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid job name: %v.", err),
		}
	}
	req.FilterTaskName = FlagJobName

	if len(args) > 0 {
		taskIds, err := util.ParseJobIdList(args[0], ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid job list specified: %v.\n", err),
			}
		}
		req.FilterTaskIds = taskIds
	}

	if FlagState != "" {
		stateList, err := util.ParseInRamTaskStatusList(FlagState)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: err.Error(),
			}
		}
		if len(stateList) == 1 {
			req.FilterState = stateList[0]
		}
	}

	req.FilterNodes = FlagNodes

	reply, err := stub.CancelTask(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to cancel tasks")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotCancelledTasks) > 0 {
			return &util.CraneError{Code: util.ErrorBackend}
		} else {
			return nil
		}
	}

	if len(reply.CancelledTasks) > 0 {
		cancelledTasksString := util.ConvertSliceToString(reply.CancelledTasks, ", ")
		fmt.Printf("Jobs %s cancelled successfully.\n", cancelledTasksString)
	}

	if len(reply.NotCancelledTasks) > 0 {
		for i := 0; i < len(reply.NotCancelledTasks); i++ {
			log.Errorf("Failed to cancel job: %d. Reason: %s.\n", reply.NotCancelledTasks[i], reply.NotCancelledReasons[i])
		}
		return &util.CraneError{Code: util.ErrorBackend}
	}
	return nil
}
