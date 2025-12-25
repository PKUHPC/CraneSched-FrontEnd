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

package crun

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
)

func SetFieldsFromEnv(job *protos.TaskToCtld, step *protos.StepToCtld) error {
	env := os.Environ()
	for _, e := range env {
		pair := strings.SplitN(e, "=", 2)
		key := pair[0]
		value := pair[1]
		switch key {
		case "CRANE_JOB_ID", "CRANE_JOBID":
			jobId, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return util.NewCraneErr(util.ErrorInvalidFormat, "invalid CRANE_JOB_ID from env")
			}
			step.JobId = uint32(jobId)
		case "CRANE_ACCOUNT":
			if job != nil {
				job.Account = value
			}
		case "CRANE_CONF":
			if FlagConfigFilePath != util.DefaultConfigPath {
				FlagConfigFilePath = value
			}
		case "CRANE_CPUS_PER_TASK":
			cpusPerTask, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return util.NewCraneErr(util.ErrorInvalidFormat, "invalid CRANE_CPUS_PER_TASK from env")
			}
			if job != nil {
				job.CpusPerTask = cpusPerTask
			} else {
				step.ReqResourcesPerTask.AllocatableRes.CpuCoreLimit = cpusPerTask
			}

		case "CRANE_EXCLUSIVE":
			exclusive, err := strconv.ParseBool(value)
			if err != nil {
				return util.NewCraneErr(util.ErrorInvalidFormat, "invalid CRANE_EXCLUSIVE from env")
			}
			if job != nil {
				job.Exclusive = exclusive
			}
		case "CRANE_EXPORT_ENV":
			if job != nil {
				job.Env["CRANE_EXPORT_ENV"] = FlagExport
			} else {
				step.Env["CRANE_EXPORT_ENV"] = FlagExport
			}
		case "CRANE_GRES":
			gresMap, err := util.ParseGres(value)
			if err != nil {
				return util.NewCraneErr(util.ErrorInvalidFormat, "invalid CRANE_GRES from env")
			}
			if job != nil {
				job.ReqResources.DeviceMap = gresMap
			} else {
				step.ReqResourcesPerTask.DeviceMap = gresMap
			}
		case "CRANE_JOB_NAME":
			if job != nil {
				job.Name = value
			}
		case "CRANE_JOB_NUM_NODES":
			numNodes, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return util.NewCraneErr(util.ErrorInvalidFormat, "invalid CRANE_JOB_NUM_NODES from env")
			}
			if job != nil {
				job.NodeNum = uint32(numNodes)
			} else {
				step.NodeNum = uint32(numNodes)
			}
		case "CRANE_MEM_PER_NODE":
			memPerNode, err := util.ParseMemStringAsByte(value)
			if err != nil {
				return util.NewCraneErr(util.ErrorInvalidFormat, "invalid CRANE_MEM_PER_NODE from env")
			}
			if job != nil {
				job.ReqResources.AllocatableRes.MemoryLimitBytes = memPerNode
				job.ReqResources.AllocatableRes.MemorySwLimitBytes = memPerNode
			} else {
				step.ReqResourcesPerTask.AllocatableRes.MemoryLimitBytes = memPerNode
				step.ReqResourcesPerTask.AllocatableRes.MemorySwLimitBytes = memPerNode
			}
		case "CRANE_NTASKS_PER_NODE":
			ntasksPerNode, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return util.NewCraneErr(util.ErrorInvalidFormat, "invalid CRANE_NTASKS_PER_NODE from env")
			}
			if job != nil {
				job.NtasksPerNode = uint32(ntasksPerNode)
			} else {
				step.NtasksPerNode = uint32(ntasksPerNode)
			}
		case "CRANE_OPEN_MODE":
			var openModeAppend bool
			switch value {
			case util.OpenModeAppend:
				openModeAppend = true
			case util.OpenModeTruncate:
				openModeAppend = false
			default:
				return fmt.Errorf("invalid CRANE_OPEN_MODE from env: must be either '%s' or '%s'", util.OpenModeAppend, util.OpenModeTruncate)
			}
			if job != nil {
				job.GetIoMeta().OpenModeAppend = proto.Bool(openModeAppend)
			} else {
				step.GetIoMeta().OpenModeAppend = proto.Bool(openModeAppend)
			}
		case "CRANE_QOS":
			if job != nil {
				job.Qos = value
			}
		case "CRANE_RESERVATION":
			if job != nil {
				job.Reservation = value
			}
		case "CRANE_TIME_LIMIT":
			seconds, err := util.ParseDurationStrToSeconds(FlagTime)
			if err != nil {
				return util.NewCraneErr(util.ErrorInvalidFormat, "invalid CRANE_TIME_LIMIT from env")
			}
			if job != nil {
				job.TimeLimit.Seconds = seconds
			} else {
				step.TimeLimit.Seconds = seconds
			}
		}
	}
	return nil
}
