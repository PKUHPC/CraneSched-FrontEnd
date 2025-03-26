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

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
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

	structExtraFromScript := util.JobExtraAttrs{}
	structExtraFromCli := util.JobExtraAttrs{}

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
			structExtraFromScript.ExtraAttr = arg.val
		case "--mail-type":
			structExtraFromScript.MailType = arg.val
		case "--mail-user":
			structExtraFromScript.MailUser = arg.val
		case "--comment":
			structExtraFromScript.Comment = arg.val
		default:
			log.Errorf("Invalid argument: unrecognized '%s' is given in the script", arg.name)
			return false, nil
		}
	}

	var extraFromScript string
	if err := structExtraFromScript.Marshal(&extraFromScript); err != nil {
		log.Errorf("Invalid argument: %v", err)
		return false, nil
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
		structExtraFromCli.ExtraAttr = FlagExtraAttr
	}
	if FlagMailType != "" {
		structExtraFromCli.MailType = FlagMailType
	}
	if FlagMailUser != "" {
		structExtraFromCli.MailUser = FlagMailUser
	}
	if FlagComment != "" {
		structExtraFromCli.Comment = FlagComment
	}

	// Set and check the extra attributes
	var extraFromCli string
	if err := structExtraFromCli.Marshal(&extraFromCli); err != nil {
		log.Errorf("Invalid argument: %v", err)
		return false, nil
	}
	task.ExtraAttr = util.AmendJobExtraAttrs(extraFromScript, extraFromCli)

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
		log.Errorf("Job allocation failed: %s.\n", util.ErrMsg(reply.GetCode()))
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
		if len(reply.GetCodeList()) > 0 {
			return util.ErrorBackend
		} else {
			return util.ErrorSuccess
		}
	}

	if len(reply.TaskIdList) > 0 {
		taskIdListString, err := util.ConvertSliceToString(reply.TaskIdList, ",")
        if err != nil {
            log.Errorf("The returned job list type is not supported: %v.\n", err)
            os.Exit(util.ErrorBackend)
        }
		fmt.Printf("Job id allocated: %s.\n", taskIdListString)
	}

	if len(reply.GetCodeList()) > 0 {
		for _, reason := range reply.GetCodeList() {
			log.Errorf("Job allocation failed: %s.\n", util.ErrMsg(reason))
		}

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
