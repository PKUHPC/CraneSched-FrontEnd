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
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CbatchArg struct {
	name string
	val  string
}

// BuildCbatchJob reads flags and script file to build a job
func BuildCbatchJob(cmd *cobra.Command, args []string) (*protos.TaskToCtld, error) {
	task := new(protos.TaskToCtld)

	// Parse the script file or use wrapped script
	cbatchArgs := make([]CbatchArg, 0)
	shScript := ""

	if FlagWrappedScript == "" {
		shLines := make([]string, 0)
		if err := ParseCbatchScript(args[0], &cbatchArgs, &shLines); err != nil {
			return nil, fmt.Errorf("invalid argument: failed to parse script: %w", err)
		}
		task.Name = filepath.Base(args[0])
		shScript = strings.Join(shLines, "\n")
	} else {
		task.Name = util.DefaultWrappedJobName
		shScript = FlagWrappedScript
	}

	// Set the payload
	task.Payload = &protos.TaskToCtld_BatchMeta{
		BatchMeta: &protos.BatchTaskAdditionalMeta{
			ShScript: shScript,
		},
	}

	// Set default values
	task.CpusPerTask = 1
	task.NtasksPerNode = 1
	task.NodeNum = 1
	task.GetUserEnv = false
	task.Env = make(map[string]string)
	task.TimeLimit = util.InvalidDuration()
	task.ReqResources = &protos.ResourceView{
		AllocatableRes: &protos.AllocatableResource{
			CpuCoreLimit:       1,
			MemoryLimitBytes:   0,
			MemorySwLimitBytes: 0,
		},
	}

	structExtraFromScript := util.JobExtraAttrs{}
	structExtraFromCli := util.JobExtraAttrs{}

	///*************set parameter values based on the file*******************************///
	for _, arg := range cbatchArgs {
		switch arg.name {
		case "--nodes", "-N":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.NodeNum = uint32(num)
		case "--cpus-per-task", "-c":
			num, err := util.ParseFloatWithPrecision(arg.val, 10)
			if err != nil {
				return nil, fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.CpusPerTask = num
		case "--gres":
			gresMap := util.ParseGres(arg.val)
			task.ReqResources.DeviceMap = gresMap
		case "--ntasks-per-node":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.NtasksPerNode = uint32(num)
		case "--time", "-t":
			seconds, err := util.ParseDurationStrToSeconds(arg.val)
			if err != nil {
				return nil, fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.TimeLimit.Seconds = seconds
		case "--begin", "-b":
			beginTime, err := util.ParseTime(arg.val)
			if err != nil {
				return nil, fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.BeginTime = timestamppb.New(beginTime)
		case "--deadline":
			deadlineTime, err := util.ParseTime(arg.val)
			if err != nil {
				log.Errorf("Invalid argument: %v in script: %v", arg.name, err)
				return false, nil
			}
			task.DeadlineTime = timestamppb.New(deadlineTime)
		case "--mem":
			memInByte, err := util.ParseMemStringAsByte(arg.val)
			if err != nil {
				return nil, fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.ReqResources.AllocatableRes.MemoryLimitBytes = memInByte
			task.ReqResources.AllocatableRes.MemorySwLimitBytes = memInByte
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
					return nil, fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
				}
				task.GetUserEnv = val
			}
		case "--export":
			task.Env["CRANE_EXPORT_ENV"] = arg.val
		case "-o", "--output":
			task.GetBatchMeta().OutputFilePattern = arg.val
		case "-e", "--error":
			task.GetBatchMeta().ErrorFilePattern = arg.val
		case "--interpreter":
			task.GetBatchMeta().Interpreter = arg.val
		case "--container":
			task.Container = arg.val
		case "--extra-attr":
			structExtraFromScript.ExtraAttr = arg.val
		case "--mail-type":
			structExtraFromScript.MailType = arg.val
		case "--mail-user":
			structExtraFromScript.MailUser = arg.val
		case "--comment":
			structExtraFromScript.Comment = arg.val
		case "--open-mode":
			switch arg.val {
			case util.OpenModeAppend:
				task.GetBatchMeta().OpenModeAppend = proto.Bool(true)
			case util.OpenModeTruncate:
				task.GetBatchMeta().OpenModeAppend = proto.Bool(false)
			default:
				return nil, fmt.Errorf("invalid argument: --open-mode must be either '%s' or '%s'", util.OpenModeAppend, util.OpenModeTruncate)
			}
		case "-r", "--reservation":
			task.Reservation = arg.val
		case "--exclusive":
			val, err := strconv.ParseBool(arg.val)
			if err != nil {
				return nil, fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.Exclusive = val
		default:
			return nil, fmt.Errorf("invalid argument: unrecognized '%s' in script", arg.name)
		}
	}

	var extraFromScript string
	if err := structExtraFromScript.Marshal(&extraFromScript); err != nil {
		return nil, fmt.Errorf("invalid argument: failed to marshal extra attributes from script: %w", err)
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
		task.ReqResources.DeviceMap = util.ParseGres(FlagGres)
	}

	if FlagTime != "" {
		seconds, err := util.ParseDurationStrToSeconds(FlagTime)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: invalid --time value '%s': %w", FlagTime, err)
		}
		task.TimeLimit.Seconds = seconds
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: invalid --mem value '%s': %w", FlagMem, err)
		}
		task.ReqResources.AllocatableRes.MemoryLimitBytes = memInByte
		task.ReqResources.AllocatableRes.MemorySwLimitBytes = memInByte
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
	if FlagInterpreter != "" {
		task.GetBatchMeta().Interpreter = FlagInterpreter
	}
	if FlagContainer != "" {
		task.Container = FlagContainer
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
	if FlagExclusive {
		task.Exclusive = true
	}
	if FlagOpenMode != "" {
		switch FlagOpenMode {
		case util.OpenModeAppend:
			task.GetBatchMeta().OpenModeAppend = proto.Bool(true)
		case util.OpenModeTruncate:
			task.GetBatchMeta().OpenModeAppend = proto.Bool(false)
		default:
			return nil, fmt.Errorf("invalid argument: --open-mode must be either '%s' or '%s'", util.OpenModeAppend, util.OpenModeTruncate)
		}
	}
	if FlagBeginTime != "" {
		beginTime, err := util.ParseTime(FlagBeginTime)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: invalid --begin value '%s': %w", FlagBeginTime, err)
		}
		task.BeginTime = timestamppb.New(beginTime)
	}
	if FlagDeadlineTime != "" {
		deadlineTime, err := util.ParseTime(FlagDeadlineTime)
		if err != nil {
			log.Errorf("Invalid argument: invalid --deadline: %v", err)
			return false, nil
		}
		task.DeadlineTime = timestamppb.New(deadlineTime)
	}
	if FlagExclusive {
		task.Exclusive = true
	}
	if FlagHold {
		task.Hold = true
	}

	// Set and check the extra attributes
	var extraFromCli string
	if err := structExtraFromCli.Marshal(&extraFromCli); err != nil {
		return nil, fmt.Errorf("invalid argument: failed to marshal extra attributes from CLI: %w", err)
	}
	task.ExtraAttr = util.AmendJobExtraAttrs(extraFromScript, extraFromCli)
	if FlagReservation != "" {
		task.Reservation = FlagReservation
	}

	// Set total limit of cpu cores
	task.ReqResources.AllocatableRes.CpuCoreLimit = task.CpusPerTask * float64(task.NtasksPerNode)

	// Check the validity of the parameters
	if err := util.CheckFileLength(task.GetBatchMeta().OutputFilePattern); err != nil {
		return nil, fmt.Errorf("invalid argument: invalid output file path: %w", err)
	}
	if err := util.CheckFileLength(task.GetBatchMeta().ErrorFilePattern); err != nil {
		return nil, fmt.Errorf("invalid argument: invalid error file path: %w", err)
	}
	if err := util.CheckTaskArgs(task); err != nil {
		return nil, fmt.Errorf("invalid argument: %w", err)
	}

	return task, nil
}

func SendRequest(task *protos.TaskToCtld) error {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)
	req := &protos.SubmitBatchTaskRequest{Task: task}

	reply, err := stub.SubmitBatchTask(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to submit the job")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}
	if reply.GetOk() {
		fmt.Printf("Job id allocated: %d.\n", reply.GetTaskId())
		return nil
	} else {
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Job allocation failed: %s.", util.ErrMsg(reply.GetCode())),
		}
	}
}

func SendMultipleRequests(task *protos.TaskToCtld, count uint32) error {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)
	req := &protos.SubmitBatchTasksRequest{Task: task, Count: count}

	reply, err := stub.SubmitBatchTasks(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to submit tasks")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if len(reply.GetCodeList()) > 0 {
			return &util.CraneError{Code: util.ErrorBackend}
		} else {
			return nil
		}
	}

	if len(reply.TaskIdList) > 0 {
		taskIdListString := util.ConvertSliceToString(reply.TaskIdList, ", ")
		fmt.Printf("Job id allocated: %s.\n", taskIdListString)
	}

	if len(reply.GetCodeList()) > 0 {
		for _, reason := range reply.GetCodeList() {
			log.Errorf("Job allocation failed: %s.\n", util.ErrMsg(reason))
		}

		return &util.CraneError{Code: util.ErrorBackend}
	}
	return nil
}

// ParseCbatchScript Split the job script into two parts: the arguments and the shell script.
func ParseCbatchScript(path string, args *[]CbatchArg, sh *[]string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
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

		// Shebang
		if num == 1 && strings.HasPrefix(scanner.Text(), "#!") {
			*args = append(*args, CbatchArg{
				name: "--interpreter",
				val:  strings.TrimPrefix(scanner.Text(), "#!"),
			})
			*sh = append(*sh, scanner.Text())
			continue
		}

		// Arguments
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
			return fmt.Errorf("failed to process line %d: %w", num, err)
		}
	}

	return scanner.Err()
}
