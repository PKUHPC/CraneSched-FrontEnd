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

package cbatch

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"bufio"
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type CbatchArg struct {
	name string
	val  string
}

// Merge and validate arguments from the file and the command line,
// then return the constructed TaskToCtld.
func ProcessCbatchArgs(cmd *cobra.Command, args []CbatchArg) (bool, *protos.TaskToCtld) {
	task := new(protos.TaskToCtld)
	task.TimeLimit = util.InvalidDuration()
	task.Resources = &protos.ResourceView{
		AllocatableRes: &protos.AllocatableResource{
			CpuCoreLimit:       1,
			MemoryLimitBytes:   0,
			MemorySwLimitBytes: 0,
		},
	}
	task.Payload = &protos.TaskToCtld_BatchMeta{
		BatchMeta: &protos.BatchTaskAdditionalMeta{},
	}

	task.CpusPerTask = 1
	task.NtasksPerNode = 1
	task.NodeNum = 1
	task.GetUserEnv = false
	task.Env = make(map[string]string)

	///*************set parameter values based on the file*******************************///
	for _, arg := range args {
		switch arg.name {
		case "--nodes", "-N":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				log.Errorf("Invalid argument: %v in script: %v", arg.name, err)
				return false, nil
			}
			task.NodeNum = uint32(num)
		case "--cpus-per-task", "-c":
			num, err := util.ParseFloatWithPrecision(arg.val, 10)
			if err != nil {
				log.Errorf("Invalid argument: %v in script: %v", arg.name, err)
				return false, nil
			}
			task.CpusPerTask = num
		case "--gres":
			gresMap := util.ParseGres(arg.val)
			task.Resources.DeviceMap = gresMap
		case "--ntasks-per-node":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				log.Errorf("Invalid argument: %v in script: %v", arg.name, err)
				return false, nil
			}
			task.NtasksPerNode = uint32(num)
		case "--time", "-t":
			seconds, err := util.ParseDurationStrToSeconds(arg.val)
			if err != nil {
				log.Errorf("Invalid argument: %v in script: %v", arg.name, err)
				return false, nil
			}
			task.TimeLimit.Seconds = seconds
		case "--mem":
			memInByte, err := util.ParseMemStringAsByte(arg.val)
			if err != nil {
				log.Errorf("Invalid argument: %v in script: %v", arg.name, err)
				return false, nil
			}
			task.Resources.AllocatableRes.MemoryLimitBytes = memInByte
			task.Resources.AllocatableRes.MemorySwLimitBytes = memInByte
		case "-p", "--partition":
			task.PartitionName = arg.val
		case "-J", "--job-name":
			task.Name = arg.val
		case "-A", "--account":
			task.Account = arg.val
		case "--qos", "Q":
			task.Qos = arg.val
		case "--chdir":
			task.Cwd = arg.val
		case "--exclude", "-x":
			task.Excludes = arg.val
		case "--nodelist", "-w":
			task.Nodelist = arg.val
		case "--get-user-env":
			if arg.val == "" {
				task.GetUserEnv = true
			} else {
				val, err := strconv.ParseBool(arg.val)
				if err != nil {
					log.Errorf("Invalid argument: %v in script: %v", arg.name, err)
					return false, nil
				}
				task.GetUserEnv = val
			}
		case "--export":
			task.Env["CRANE_EXPORT_ENV"] = arg.val
		case "-o", "--output":
			task.GetBatchMeta().OutputFilePattern = arg.val
		case "-e", "--error":
			task.GetBatchMeta().ErrorFilePattern = arg.val
		case "--extra-attr":
			// Merge the extra attributes read from the file with the existing ones.
			if !util.CheckTaskExtraAttr(arg.val) {
				log.Errorf("Invalid argument: %v in script: invalid JSON string", arg.name)
				return false, nil
			}
			task.ExtraAttr = util.AmendTaskExtraAttr(task.ExtraAttr, arg.val)
		case "--mail-type":
			extra, err := sjson.Set(task.ExtraAttr, "mail.type", arg.val)
			if err != nil {
				log.Errorf("Invalid argument: %v in script: %v", arg.name, err)
				return false, nil
			}
			task.ExtraAttr = extra
		case "--mail-user":
			extra, err := sjson.Set(task.ExtraAttr, "mail.user", arg.val)
			if err != nil {
				log.Errorf("Invalid argument: %v in script: %v", arg.name, err)
				return false, nil
			}
			task.ExtraAttr = extra
		default:
			log.Errorf("Invalid argument: unrecognized '%s' is given in the script", arg.name)
			return false, nil
		}
	}

	// ************* set parameter values based on the command line *********************
	// If the command line argument is set, it replaces the argument read from the file,
	// so the command line has a higher priority
	if cmd.Flags().Changed("nodes") {
		task.NodeNum = FlagNodes
	}
	if cmd.Flags().Changed("cpus-per-task") {
		task.CpusPerTask = FlagCpuPerTask
	}

	if cmd.Flags().Changed("ntasks-per-node") {
		task.NtasksPerNode = FlagNtasksPerNode
	}
	if cmd.Flags().Changed("gres") {
		task.Resources.DeviceMap = util.ParseGres(FlagGres)
	}

	if FlagTime != "" {
		seconds, err := util.ParseDurationStrToSeconds(FlagTime)
		if err != nil {
			log.Errorf("Invalid argument: invalid --time: %v", err)
			return false, nil
		}
		task.TimeLimit.Seconds = seconds
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			log.Errorf("Invalid argument: %v", err)
			return false, nil
		}
		task.Resources.AllocatableRes.MemoryLimitBytes = memInByte
		task.Resources.AllocatableRes.MemorySwLimitBytes = memInByte
	}

	if FlagPartition != "" {
		task.PartitionName = FlagPartition
	}
	if FlagJob != "" {
		task.Name = FlagJob
	}
	if FlagQos != "" {
		task.Qos = FlagQos
	}
	if FlagCwd != "" {
		task.Cwd = FlagCwd
	}
	if FlagAccount != "" {
		task.Account = FlagAccount
	}
	if FlagNodelist != "" {
		task.Nodelist = FlagNodelist
	}
	if FlagExcludes != "" {
		task.Excludes = FlagExcludes
	}
	if FlagGetUserEnv {
		task.GetUserEnv = true
	}
	if FlagExport != "" {
		task.Env["CRANE_EXPORT_ENV"] = FlagExport
	}
	if FlagStdoutPath != "" {
		task.GetBatchMeta().OutputFilePattern = FlagStdoutPath
	}
	if FlagStderrPath != "" {
		task.GetBatchMeta().ErrorFilePattern = FlagStderrPath
	}

	if FlagExtraAttr != "" {
		// Merge the extra attributes read from the file with the existing ones.
		if !util.CheckTaskExtraAttr(FlagExtraAttr) {
			log.Errorln("Invalid argument: invalid --extra-attr: invalid JSON string")
			return false, nil
		}
		task.ExtraAttr = util.AmendTaskExtraAttr(task.ExtraAttr, FlagExtraAttr)
	}
	if FlagMailType != "" {
		extra, err := sjson.Set(task.ExtraAttr, "mail.type", FlagMailType)
		if err != nil {
			log.Errorf("Invalid argument: invalid --mail-type: %v", err)
			return false, nil
		}
		task.ExtraAttr = extra
	}
	if FlagMailUser != "" {
		extra, err := sjson.Set(task.ExtraAttr, "mail.user", FlagMailUser)
		if err != nil {
			log.Errorf("Invalid argument: invalid --mail-user: %v", err)
			return false, nil
		}
		task.ExtraAttr = extra
	}

	// Set total limit of cpu cores
	task.Resources.AllocatableRes.CpuCoreLimit = task.CpusPerTask * float64(task.NtasksPerNode)

	// Check the validity of the parameters
	if err := util.CheckFileLength(task.GetBatchMeta().OutputFilePattern); err != nil {
		log.Errorf("Invalid argument: invalid output file path: %v", err)
		return false, nil
	}
	if err := util.CheckFileLength(task.GetBatchMeta().ErrorFilePattern); err != nil {
		log.Errorf("Invalid argument: invalid error file path: %v", err)
		return false, nil
	}
	if err := util.CheckTaskArgs(task); err != nil {
		log.Errorf("Invalid argument: %v", err)
		return false, nil
	}
	if task.ExtraAttr != "" {
		// Check attrs in task.ExtraAttr, e.g., mail.type, mail.user
		mailtype := gjson.Get(task.ExtraAttr, "mail.type")
		mailuser := gjson.Get(task.ExtraAttr, "mail.user")
		if mailtype.Exists() != mailuser.Exists() {
			log.Errorln("Invalid argument: incomplete mail arguments")
			return false, nil
		}
		if mailtype.Exists() && !util.CheckMailType(mailtype.String()) {
			log.Errorln("Invalid argument: invalid --mail-type")
			return false, nil
		}
	}

	return true, task
}

func SendRequest(task *protos.TaskToCtld) util.CraneCmdError {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)
	req := &protos.SubmitBatchTaskRequest{Task: task}

	reply, err := stub.SubmitBatchTask(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to submit the job")
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	if reply.GetOk() {
		fmt.Printf("Job id allocated: %d.\n", reply.GetTaskId())
		return util.ErrorSuccess
	} else {
		log.Errorf("Job allocation failed: %s.\n", reply.GetReason())
		return util.ErrorBackend
	}
}

func SendMultipleRequests(task *protos.TaskToCtld, count uint32) util.CraneCmdError {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)
	req := &protos.SubmitBatchTasksRequest{Task: task, Count: count}

	reply, err := stub.SubmitBatchTasks(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to submit tasks")
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.ReasonList) > 0 {
			return util.ErrorBackend
		} else {
			return util.ErrorSuccess
		}
	}

	if len(reply.TaskIdList) > 0 {
		taskIdListString := make([]string, len(reply.TaskIdList))
		for i, taskId := range reply.TaskIdList {
			taskIdListString[i] = strconv.FormatUint(uint64(taskId), 10)
		}
		fmt.Printf("Job id allocated: %s.\n", strings.Join(taskIdListString, ", "))
	}

	if len(reply.ReasonList) > 0 {
		log.Errorf("Job allocation failed: %s.\n", strings.Join(reply.ReasonList, ", "))
		return util.ErrorBackend
	}
	return util.ErrorSuccess
}

// Split the job script into two parts: the arguments and the shell script.
func ParseCbatchScript(path string, args *[]CbatchArg, sh *[]string) util.CraneCmdError {
	file, err := os.Open(path)
	if err != nil {
		log.Error(err)
		return util.ErrorCmdArg
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf("Failed to close %s.\n", file.Name())
		}
	}(file)

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	num := 0

	for scanner.Scan() {
		num++
		reC := regexp.MustCompile(`^#CBATCH`)
		reS := regexp.MustCompile(`^#SBATCH`)
		reL := regexp.MustCompile(`^#BSUB`)
		var processor LineProcessor
		if reC.MatchString(scanner.Text()) {
			processor = &cLineProcessor{}
		} else if reS.MatchString(scanner.Text()) {
			processor = &sLineProcessor{}
		} else if reL.MatchString(scanner.Text()) {
			processor = &lLineProcessor{}
		} else {
			processor = &defaultProcessor{}
		}
		err := processor.Process(scanner.Text(), sh, args)
		if err != nil {
			log.Errorf("Parsing error at line %v: %v\n", num, err.Error())
			return util.ErrorCmdArg
		}
	}

	if err := scanner.Err(); err != nil {
		log.Error(err)
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}
