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

var (
	setGresGpusFlag    = false
	setGpusPerNodeFlag = false
)

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

	if isPod, err := isPodJob(cmd, cbatchArgs); err != nil {
		return nil, err
	} else if isPod {
		task.Type = protos.TaskType_Container
	} else {
		task.Type = protos.TaskType_Batch
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
	podOpts := podOptions{}

	// Set args from the script
	if err := applyScriptArgs(cmd, cbatchArgs, task, &structExtraFromScript, &podOpts); err != nil {
		return nil, err
	}

	var extraFromScript string
	if err := structExtraFromScript.Marshal(&extraFromScript); err != nil {
		return nil, fmt.Errorf("invalid argument: failed to marshal extra attributes from script: %w", err)
	}

	// Set args from the command line flags
	// Command line has a higher priority
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
		gresMap := util.ParseGres(FlagGres)
		if _, exist := gresMap.NameTypeMap[util.GresGpuName]; exist {
			if setGpusPerNodeFlag {
				return nil, fmt.Errorf("invalid argument: cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
			}
			setGresGpusFlag = true
		}
		task.ReqResources.DeviceMap = gresMap
	}
	if cmd.Flags().Changed("gpus-per-node") {
		if setGresGpusFlag {
			return nil, fmt.Errorf("invalid argument: cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
		}
		setGpusPerNodeFlag = true
		gpuDeviceMap, err := util.ParseGpusPerNodeStr(FlagGpusPerNode)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: invalid --gpus-per-node value '%s': %w", FlagGpusPerNode, err)
		}
		task.ReqResources.DeviceMap = gpuDeviceMap
	}
	if cmd.Flags().Changed("wckey") {
		task.Wckey = &FlagWckey
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
	if FlagMemPerCpu != "" {
		memInBytePerCpu, err := util.ParseMemStringAsByte(FlagMemPerCpu)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: invalid --mem-per-cpu value '%s': %w", FlagMemPerCpu, err)
		}
		task.MemPerCpu = &memInBytePerCpu
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
	if FlagLicenses != "" {
		licCount, isLicenseOr, err := util.ParseLicensesString(FlagLicenses)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: invalid --licenses value '%s': %w", FlagLicenses, err)
		}
		task.LicensesCount = licCount
		task.IsLicensesOr = isLicenseOr
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
	if FlagExclusive {
		task.Exclusive = true
	}
	if FlagHold {
		task.Hold = true
	}
	if FlagReservation != "" {
		task.Reservation = FlagReservation
	}

	// Set pod meta if it's a container job
	if task.Type == protos.TaskType_Container {
		overridePodFromFlags(cmd, &podOpts)
		podMeta, err := buildPodMeta(task, &podOpts)
		if err != nil {
			return nil, fmt.Errorf("invalid container options: %v", err)
		}
		task.PodMeta = podMeta
	}

	// Set and check the extra attributes
	var extraFromCli string
	if err := structExtraFromCli.Marshal(&extraFromCli); err != nil {
		return nil, fmt.Errorf("invalid argument: failed to marshal extra attributes from CLI: %w", err)
	}
	task.ExtraAttr = util.AmendJobExtraAttrs(extraFromScript, extraFromCli)

	// Set total limit of cpu cores
	task.ReqResources.AllocatableRes.CpuCoreLimit = task.CpusPerTask * float64(task.NtasksPerNode)

	// Set the submit hostname
	submitHostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("Failed to get hostname of the submitting host: %v", err)
	}
	task.SubmitHostname = submitHostname
	if FlagDependency != "" {
		err := util.SetTaskDependencies(task, FlagDependency)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: failed to set dependencies: %w", err)
		}
	}

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

func applyScriptArgs(cmd *cobra.Command, cbatchArgs []CbatchArg, task *protos.TaskToCtld, extraFromScript *util.JobExtraAttrs, podOpts *podOptions) error {
	for _, arg := range cbatchArgs {
		switch arg.name {
		case "--nodes", "-N":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.NodeNum = uint32(num)
		case "--cpus-per-task", "-c":
			num, err := util.ParseFloatWithPrecision(arg.val, 10)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.CpusPerTask = num
		case "--gres":
			gresMap := util.ParseGres(arg.val)
			if _, exist := gresMap.NameTypeMap[util.GresGpuName]; exist {
				if setGpusPerNodeFlag {
					return fmt.Errorf("invalid argument: cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
				}
				setGresGpusFlag = true
			}
			task.ReqResources.DeviceMap = gresMap
		case "--gpus-per-node":
			if setGresGpusFlag {
				return fmt.Errorf("invalid argument: cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
			}
			setGpusPerNodeFlag = true
			gpuDeviceMap, err := util.ParseGpusPerNodeStr(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.ReqResources.DeviceMap = gpuDeviceMap
		case "--ntasks-per-node":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.NtasksPerNode = uint32(num)
		case "--time", "-t":
			seconds, err := util.ParseDurationStrToSeconds(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.TimeLimit.Seconds = seconds
		case "--begin", "-b":
			beginTime, err := util.ParseTime(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.BeginTime = timestamppb.New(beginTime)
		case "--mem":
			memInByte, err := util.ParseMemStringAsByte(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.ReqResources.AllocatableRes.MemoryLimitBytes = memInByte
			task.ReqResources.AllocatableRes.MemorySwLimitBytes = memInByte
		case "--mem-per-cpu":
			memInBytePerCpu, err := util.ParseMemStringAsByte(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.MemPerCpu = &memInBytePerCpu
		case "-p", "--partition":
			task.PartitionName = arg.val
		case "-J", "--job-name":
			task.Name = arg.val
		case "-A", "--account":
			task.Account = arg.val
		case "--qos", "-Q":
			task.Qos = arg.val
		case "--licenses", "-L":
			licCount, isLicenseOr, err := util.ParseLicensesString(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.LicensesCount = licCount
			task.IsLicensesOr = isLicenseOr
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
					return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
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
		case "--extra-attr":
			extraFromScript.ExtraAttr = arg.val
		case "--mail-type":
			extraFromScript.MailType = arg.val
		case "--mail-user":
			extraFromScript.MailUser = arg.val
		case "--comment":
			extraFromScript.Comment = arg.val
		case "--open-mode":
			switch arg.val {
			case util.OpenModeAppend:
				task.GetBatchMeta().OpenModeAppend = proto.Bool(true)
			case util.OpenModeTruncate:
				task.GetBatchMeta().OpenModeAppend = proto.Bool(false)
			default:
				return fmt.Errorf("invalid argument: --open-mode must be either '%s' or '%s'", util.OpenModeAppend, util.OpenModeTruncate)
			}
		case "-r", "--reservation":
			task.Reservation = arg.val
		case "--exclusive":
			val, err := strconv.ParseBool(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			task.Exclusive = val
		case "--wckey":
			wckey := arg.val
			task.Wckey = &wckey
		case "--pod":
			// No need to process here, already handled in isPodJob
		case "--pod-name":
			podOpts.name = arg.val
		case "--pod-port":
			podOpts.ports = append(podOpts.ports, arg.val)
		case "--pod-user":
			podOpts.user = arg.val
		case "--pod-userns":
			val, err := strconv.ParseBool(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			podOpts.userns = val
		case "--pod-host-network":
			val, err := strconv.ParseBool(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			podOpts.hostNet = val
		case "--dependency", "-d":
			err := util.SetTaskDependencies(task, arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: failed to set dependencies: %w", err)
			}
		default:
			return fmt.Errorf("invalid argument: unrecognized '%s' in script", arg.name)
		}
	}

	return nil
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
		if len(reply.GetReason()) > 0 {
			return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Job allocation failed: %s.", reply.GetReason()));
		}

		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Job allocation failed: %s.", util.ErrMsg(reply.GetCode())))
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
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
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
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Parsing error at line %d: %s", num, err))
		}

	}

	if err := scanner.Err(); err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to read the script file", err)
	}
	*args = FilterDummyArgs(*args)
	return nil
}

func FilterDummyArgs(args []CbatchArg) []CbatchArg {
	filteredArgs := make([]CbatchArg, 0, len(args))
	unsupportedFlags := map[string]string{
		"ntasks":            "The feature --ntasks/-n is not yet supported by Crane, the use is ignored.",
		"n":                 "The feature --ntasks/-n is not yet supported by Crane, the use is ignored.",
		"array":             "The feature --array/-a is not yet supported by Crane, the use is ignored.",
		"a":                 "The feature --array/-a is not yet supported by Crane, the use is ignored.",
		"no-requeue":        "The feature --no-requeue is not yet supported by Crane, the use is ignored.",
		"parsable":          "The feature --parsable is not yet supported by Crane, the use is ignored.",
		"gpus-per-node":     "The feature --gpus-per-node is not yet supported by Crane, the use is ignored.",
		"ntasks-per-socket": "The feature --ntasks-per-socket is not yet supported by Crane, the use is ignored.",
		"wckey":             "The feature --wckey is not yet supported by Crane, the use is ignored.",
		"cpu-freq":          "The feature --cpu-freq is not yet supported by Crane, the use is ignored.",
		"dependency":        "The feature --dependency/-d is not yet supported by Crane, the use is ignored.",
		"d":                 "The feature --dependency/-d is not yet supported by Crane, the use is ignored.",
		"priority":          "The feature --priority is not yet supported by Crane, the use is ignored.",
		"mem-per-cpu":       "The feature --mem-per-cpu is not yet supported by Crane, the use is ignored.",
		"threads-per-core":  "The feature --threads-per-core is not yet supported by Crane, the use is ignored.",
		"distribution":      "The feature --distribution/-m is not yet supported by Crane, the use is ignored.",
		"m":                 "The feature --distribution/-m is not yet supported by Crane, the use is ignored.",
		"input":             "The feature --input/-i is not yet supported by Crane, the use is ignored.",
		"i":                 "The feature --input/-i is not yet supported by Crane, the use is ignored.",
		"sockets-per-node":  "The feature --sockets-per-node is not yet supported by Crane, the use is ignored.",
		"cores-per-socket":  "The feature --cores-per-socket is not yet supported by Crane, the use is ignored.",
		"requeue":           "The feature --requeue is not yet supported by Crane, the use is ignored.",
		"wait":              "The feature --wait/-W is not yet supported by Crane, the use is ignored.",
		"W":                 "The feature --wait/-W is not yet supported by Crane, the use is ignored.",
	}

	for _, arg := range args {
		nameWithoutPrefix := strings.TrimLeft(arg.name, "-")
		if message, found := unsupportedFlags[nameWithoutPrefix]; found {
			fmt.Fprintln(os.Stderr, message)
		} else {
			filteredArgs = append(filteredArgs, arg)
		}
	}

	return filteredArgs
}
