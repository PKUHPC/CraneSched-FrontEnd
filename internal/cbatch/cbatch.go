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
		case "--gres":
			gresMap := util.ParseGres(arg.val)
			task.Resources.DeviceMap = gresMap
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
		case "--extra-attr":
			// Merge the extra attributes read from the file with the existing ones.
			if !util.CheckTaskExtraAttr(arg.val) {
				log.Errorln("Invalid extra attributes: invalid JSON string.")
				return false, nil
			}
			task.ExtraAttr = util.AmendTaskExtraAttr(task.ExtraAttr, arg.val)
		case "--mail-type":
			extra, err := sjson.Set(task.ExtraAttr, "mail.type", arg.val)
			if err != nil {
				log.Errorf("Invalid mail type: %v.\n", err)
				return false, nil
			}
			task.ExtraAttr = extra
		case "--mail-user":
			extra, err := sjson.Set(task.ExtraAttr, "mail.user", arg.val)
			if err != nil {
				log.Errorf("Invalid mail user: %v.\n", err)
				return false, nil
			}
			task.ExtraAttr = extra
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
	if cmd.Flags().Changed("gres") {
		task.Resources.DeviceMap = util.ParseGres(FlagGres)
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
			log.Errorln("Invalid extra attributes: invalid JSON string.")
			return false, nil
		}
		task.ExtraAttr = util.AmendTaskExtraAttr(task.ExtraAttr, FlagExtraAttr)
	}
	if FlagMailType != "" {
		extra, err := sjson.Set(task.ExtraAttr, "mail.type", FlagMailType)
		if err != nil {
			log.Errorf("Invalid mail type: %v.\n", err)
			return false, nil
		}
		task.ExtraAttr = extra
	}
	if FlagMailUser != "" {
		extra, err := sjson.Set(task.ExtraAttr, "mail.user", FlagMailUser)
		if err != nil {
			log.Errorf("Invalid mail user: %v.\n", err)
			return false, nil
		}
		task.ExtraAttr = extra
	}

	// Check the validity of the parameters
	if len(task.Name) > util.MaxJobNameLength {
		log.Errorf("Job name exceeds %v characters.", util.MaxJobNameLength)
		return false, nil
	}
	if err := util.CheckFileLength(task.GetBatchMeta().OutputFilePattern); err != nil {
		log.Errorf("Invalid output file path: %v", err)
		return false, nil
	}
	if err := util.CheckFileLength(task.GetBatchMeta().ErrorFilePattern); err != nil {
		log.Errorf("Invalid error file path: %v", err)
		return false, nil
	}
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
	if task.Resources.AllocatableRes.MemoryLimitBytes <= 0 {
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
	if task.ExtraAttr != "" {
		// Check attrs in task.ExtraAttr, e.g., mail.type, mail.user
		mailtype := gjson.Get(task.ExtraAttr, "mail.type")
		mailuser := gjson.Get(task.ExtraAttr, "mail.user")
		if mailtype.Exists() != mailuser.Exists() {
			log.Errorln("Incomplete mail arguments")
			return false, nil
		}
		if mailtype.Exists() && !util.CheckMailType(mailtype.String()) {
			log.Errorln("Invalid --mail-type")
			return false, nil
		}
	}

	task.Resources.AllocatableRes.CpuCoreLimit = task.CpusPerTask * float64(task.NtasksPerNode)
	if task.Resources.AllocatableRes.CpuCoreLimit > 1e6 {
		log.Errorf("Request too many CPUs: %v", task.Resources.AllocatableRes.CpuCoreLimit)
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
