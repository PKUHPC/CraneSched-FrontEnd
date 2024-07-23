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
	task.Resources = &protos.Resources{
		AllocatableResource: &protos.AllocatableResource{
			CpuCoreLimit:       1,
			MemoryLimitBytes:   1024 * 1024 * 1024 * 16,
			MemorySwLimitBytes: 1024 * 1024 * 1024 * 16,
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
				log.Print("Invalid " + arg.name)
				return false, nil
			}
			task.NodeNum = uint32(num)
		case "--cpus-per-task", "-c":
			num, err := util.ParseFloatWithPrecision(arg.val, 10)
			if err != nil {
				log.Print("Invalid " + arg.name)
				return false, nil
			}
			task.CpusPerTask = num
		case "--ntasks-per-node":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				log.Print("Invalid " + arg.name)
				return false, nil
			}
			task.NtasksPerNode = uint32(num)
		case "--time", "-t":
			ok := util.ParseDuration(arg.val, task.TimeLimit)
			if !ok {
				log.Print("Invalid " + arg.name)
				return false, nil
			}
		case "--mem":
			memInByte, err := util.ParseMemStringAsByte(arg.val)
			if err != nil {
				log.Error(err)
				return false, nil
			}
			task.Resources.AllocatableResource.MemoryLimitBytes = memInByte
			task.Resources.AllocatableResource.MemorySwLimitBytes = memInByte
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
					log.Print("Invalid " + arg.name)
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
		case "--mail-type":
			mailType, err := util.ParseMailType(arg.val)
			if err != nil {
				log.Error(err)
				return false, nil
			}
			task.MailType = mailType
		case "--mail-user":
			task.MailUser = arg.val
		default:
			log.Errorf("Invalid parameter '%s' given in the script file.\n", arg.name)
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

	// TODO: Should use Changed() to check if the flag is set.
	if FlagTime != "" {
		ok := util.ParseDuration(FlagTime, task.TimeLimit)
		if !ok {
			log.Errorln("Invalid --time")
			return false, nil
		}
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			log.Errorln(err)
			return false, nil
		}
		task.Resources.AllocatableResource.MemoryLimitBytes = memInByte
		task.Resources.AllocatableResource.MemorySwLimitBytes = memInByte
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
	if FlagMailType != "" {
		mailType, err := util.ParseMailType(FlagMailType)
		if err != nil {
			log.Error(err)
			return false, nil
		}
		task.MailType = mailType
	}
	if FlagMailUser != "" {
		task.MailUser = FlagMailUser
	}

	// Check the validity of the parameters
	if task.CpusPerTask <= 0 {
		log.Errorln("Invalid --cpus-per-task")
		return false, nil
	}
	if task.NtasksPerNode <= 0 {
		log.Errorln("Invalid --ntasks-per-node")
		return false, nil
	}
	if task.NodeNum <= 0 {
		log.Errorln("Invalid --nodes")
		return false, nil
	}
	if task.TimeLimit.AsDuration() <= 0 {
		log.Errorln("Invalid --time")
		return false, nil
	}
	if task.Resources.AllocatableResource.MemoryLimitBytes <= 0 {
		log.Errorln("Invalid --mem")
		return false, nil
	}
	if !util.CheckNodeList(task.Nodelist) {
		log.Errorln("Invalid --nodelist")
		return false, nil
	}
	if !util.CheckNodeList(task.Excludes) {
		log.Errorln("Invalid --exclude")
		return false, nil
	}

	if task.MailType != 0 && task.MailUser == "" {
		log.Errorln("Mail type is set but missing the mail user")
		return false, nil
	}

	if len(task.Name) > 30 {
		task.Name = task.Name[:30]
		log.Warnf("Job name exceeds 30 characters, trimmed to %v.\n", task.Name)
	}

	task.Resources.AllocatableResource.CpuCoreLimit = task.CpusPerTask * float64(task.NtasksPerNode)

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

func SplitEnvironEntry(env *string) (string, string, bool) {
	eq := strings.IndexByte(*env, '=')
	if eq == -1 {
		return *env, "", false
	} else {
		return (*env)[:eq], (*env)[eq+1:], true
	}
}

func SetPropagatedEnviron(task *protos.TaskToCtld) {
	systemEnv := make(map[string]string)
	for _, str := range os.Environ() {
		name, value, _ := SplitEnvironEntry(&str)
		systemEnv[name] = value

		// The CRANE_* environment variables are loaded anyway.
		if strings.HasPrefix(name, "CRANE_") {
			task.Env[name] = value
		}
	}

	// This value is used only to carry the value of --export flag.
	// Delete it once we get it.
	valueOfExportFlag, haveExportFlag := task.Env["CRANE_EXPORT_ENV"]
	if haveExportFlag {
		delete(task.Env, "CRANE_EXPORT_ENV")
	} else {
		// Default mode is ALL
		valueOfExportFlag = "ALL"
	}

	switch valueOfExportFlag {
	case "NIL":
	case "NONE":
		task.GetUserEnv = true
	case "ALL":
		task.Env = systemEnv

	default:
		// The case like "ALL,A=a,B=b", "NIL,C=c"
		task.GetUserEnv = true
		splitValueOfExportFlag := strings.Split(valueOfExportFlag, ",")
		for _, exportValue := range splitValueOfExportFlag {
			if exportValue == "ALL" {
				for k, v := range systemEnv {
					task.Env[k] = v
				}
			} else {
				k, v, ok := SplitEnvironEntry(&exportValue)
				// If user-specified value is empty, use system value instead.
				if ok {
					task.Env[k] = v
				} else {
					systemEnvValue, envExist := systemEnv[k]
					if envExist {
						task.Env[k] = systemEnvValue
					}
				}
			}
		}
	}
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
		var processor LineProcessor
		if reC.MatchString(scanner.Text()) {
			processor = &cLineProcessor{}
		} else if reS.MatchString(scanner.Text()) {
			processor = &sLineProcessor{}
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
