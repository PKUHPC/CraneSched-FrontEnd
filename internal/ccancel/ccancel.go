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
)

var (
	stub protos.CraneCtldClient
)

func CancelJob(args []string) error {
	req := &protos.CancelJobRequest{
		OperatorUid: uint32(os.Getuid()),

		FilterPartition: FlagPartition,
		FilterAccount:   FlagAccount,
		FilterState:     protos.JobStatus_Invalid,
		FilterUsername:  FlagUserName,
	}

	err := util.CheckJobNameLength(FlagJobName)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid job name: %v.", err))
	}
	req.FilterJobName = FlagJobName

	if len(args) > 0 {
		selectors, err := util.ParseJobIdSelectorList(args[0], ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid job list specified: %v.\n", err))
		}

		filterIds := make(map[uint32]*protos.JobStepIds)
		filterArrayTaskIds := make(map[uint32]*protos.ArrayTaskIds)

		for _, sel := range selectors {
			if _, ok := filterIds[sel.JobId]; !ok {
				filterIds[sel.JobId] = &protos.JobStepIds{}
			}
			if sel.StepId != nil {
				filterIds[sel.JobId].Steps = append(filterIds[sel.JobId].Steps, *sel.StepId)
			}

			if sel.ArrayTaskId != nil {
				if _, ok := filterArrayTaskIds[sel.JobId]; !ok {
					filterArrayTaskIds[sel.JobId] = &protos.ArrayTaskIds{}
				}
				filterArrayTaskIds[sel.JobId].ArrayTaskIds = append(
					filterArrayTaskIds[sel.JobId].ArrayTaskIds, *sel.ArrayTaskId)
			}
		}

		req.FilterIds = filterIds
		if len(filterArrayTaskIds) > 0 {
			req.FilterArrayTaskIds = filterArrayTaskIds
		}
	}

	if FlagState != "" {
		stateList, err := util.ParseInRamJobStatusList(FlagState)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, err.Error())
		}
		if len(stateList) == 1 {
			req.FilterState = stateList[0]
		}
	}

	req.FilterNodes = FlagNodes

	reply, err := stub.CancelJob(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to cancel jobs")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.NotCancelledJobSteps) > 0 {
			return util.NewCraneErr(util.ErrorBackend, "some jobs were not cancelled")
		} else {
			return nil
		}
	}

	if len(reply.CancelledSteps) > 0 {
		cancelledJobsString := util.JobStepListToString(reply.CancelledSteps)
		fmt.Printf("%s cancelled successfully.\n", cancelledJobsString)
	}

	if len(reply.NotCancelledJobSteps) > 0 {
		for jobId, stepErr := range reply.NotCancelledJobSteps {
			if len(stepErr.Reason) != 0 {
				fmt.Printf("Failed to cancel job: %d. Reason: %s.\n", jobId, stepErr.Reason)
			} else {
				for i := 0; i < len(stepErr.StepIds); i++ {
					fmt.Printf("Failed to cancel step: %d-%d. Reason: %s.\n", jobId, stepErr.StepIds[i], stepErr.StepReasons[i])

				}
			}
		}
		return util.NewCraneErr(util.ErrorBackend, "some jobs were not cancelled")
	}
	return nil
}
