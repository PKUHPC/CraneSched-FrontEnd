/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
	stub protos.CraneCtldClient
)

func CancelTask(args []string) util.CraneCmdError {
	req := &protos.CancelTaskRequest{
		OperatorUid: uint32(os.Getuid()),

		FilterPartition: FlagPartition,
		FilterAccount:   FlagAccount,
		FilterTaskName:  FlagJobName,
		FilterState:     protos.TaskStatus_Invalid,
		FilterUsername:  FlagUserName,
	}

	if len(args) > 0 {
		taskIds, err := util.ParseJobIdList(args[0], ",")
		if err != nil {
			log.Errorln(err)
			return util.ErrorCmdArg
		}
		req.FilterTaskIds = taskIds
	}

	if FlagState != "" {
		stateList, err := util.ParseInRamTaskStatusList(FlagState)
		if err != nil {
			log.Errorln(err)
			return util.ErrorCmdArg
		}
		if len(stateList) == 1 {
			req.FilterState = stateList[0]
		}
	}

	req.FilterNodes = FlagNodes

	reply, err := stub.CancelTask(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to cancel tasks")
		os.Exit(util.ErrorNetwork)
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotCancelledTasks) > 0 {
			return util.ErrorBackend
		} else {
			return util.ErrorSuccess
		}
	}

	if len(reply.CancelledTasks) > 0 {
		fmt.Printf("Jobs %v cancelled successfully.\n", reply.CancelledTasks)
	}

	if len(reply.NotCancelledTasks) > 0 {
		for i := 0; i < len(reply.NotCancelledTasks); i++ {
			log.Errorf("Failed to cancel job: %d. Reason: %s.\n", reply.NotCancelledTasks[i], reply.NotCancelledReasons[i])
		}
		os.Exit(util.ErrorBackend)
	}
	return util.ErrorSuccess
}
