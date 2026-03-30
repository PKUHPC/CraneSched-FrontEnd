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
func BuildCbatchJob(cmd *cobra.Command, args []string) (*protos.JobToCtld, error) {
	job := new(protos.JobToCtld)

	// Parse the script file or use wrapped script
	cbatchArgs := make([]CbatchArg, 0)
	shScript := ""

	if FlagWrappedScript == "" {
		shLines := make([]string, 0)
		if err := ParseCbatchScript(args[0], &cbatchArgs, &shLines); err != nil {
			return nil, fmt.Errorf("invalid argument: failed to parse script: %w", err)
		}
		job.Name = filepath.Base(args[0])
		shScript = strings.Join(shLines, "\n")
	} else {
		job.Name = util.DefaultWrappedJobName
		shScript = FlagWrappedScript
	}

	if isPod, err := isPodJob(cmd, cbatchArgs); err != nil {
		return nil, err
	} else if isPod {
		job.Type = protos.JobType_Container
	} else {
		job.Type = protos.JobType_Batch
	}

	// Set the payload
	job.Payload = &protos.JobToCtld_BatchMeta{
		BatchMeta: &protos.BatchJobAdditionalMeta{},
	}
	job.IoMeta = &protos.IoMeta{}
	job.ShScript = shScript

	// Set default values
	job.NtasksPerNode = 0
	job.NodeNum = 0
	job.Ntasks = 0
	job.GetUserEnv = false
	job.Env = make(map[string]string)
	job.TimeLimit = util.InvalidDuration()

	structExtraFromScript := util.JobExtraAttrs{}
	structExtraFromCli := util.JobExtraAttrs{}
	podOpts := podOptions{}

	// Set args from the script
	if err := applyScriptArgs(cmd, cbatchArgs, job, &structExtraFromScript, &podOpts); err != nil {
		return nil, err
	}

	var extraFromScript string
	if err := structExtraFromScript.Marshal(&extraFromScript); err != nil {
		return nil, fmt.Errorf("invalid argument: failed to marshal extra attributes from script: %w", err)
	}

	// Set args from the command line flags
	// Command line has a higher priority
	if cmd.Flags().Changed("nodes") {
		if FlagNodes == 0 {
			return nil, fmt.Errorf("invalid argument: --nodes must be > 0")
		}
		job.NodeNum = FlagNodes
	}
	if cmd.Flags().Changed("cpus-per-task") {
		cpuPerTask := float64(FlagCpuPerTask)
		job.CpusPerTask = &cpuPerTask
	}

	if cmd.Flags().Changed("ntasks-per-node") {
		if FlagNtasksPerNode == 0 {
			return nil, fmt.Errorf("invalid argument: --ntasks-per-node must be > 0")
		}
		job.NtasksPerNode = FlagNtasksPerNode
	}
	if cmd.Flags().Changed("ntasks") {
		if FlagNtasks == 0 {
			return nil, fmt.Errorf("invalid argument: --ntasks must be > 0")
		}
		job.Ntasks = FlagNtasks
	}
	if cmd.Flags().Changed("gres") {
		gresMap, err := util.ParseGres(FlagGres)
		if err != nil {
			return nil, err
		}
		if _, exist := gresMap.NameTypeMap[util.GresGpuName]; exist {
			if setGpusPerNodeFlag {
				return nil, fmt.Errorf("invalid argument: cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
			}
			setGresGpusFlag = true
		}
		job.GresPerNode = gresMap
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
		job.GresPerNode = gpuDeviceMap
	}
	if cmd.Flags().Changed("wckey") {
		job.Wckey = &FlagWckey
	}

	if FlagTime != "" {
		seconds, err := util.ParseDurationStrToSeconds(FlagTime)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: invalid --time value '%s': %w", FlagTime, err)
		}
		job.TimeLimit.Seconds = seconds
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: invalid --mem value '%s': %w", FlagMem, err)
		}
		job.MemPerNode = &memInByte
	}
	if FlagMemPerCpu != "" {
		memInBytePerCpu, err := util.ParseMemStringAsByte(FlagMemPerCpu)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: invalid --mem-per-cpu value '%s': %w", FlagMemPerCpu, err)
		}
		job.MemPerCpu = &memInBytePerCpu
	}
	if FlagPartition != "" {
		job.PartitionName = FlagPartition
	}
	if FlagJob != "" {
		job.Name = FlagJob
	}
	if FlagQos != "" {
		job.Qos = FlagQos
	}
	if FlagLicenses != "" {
		licCount, isLicenseOr, err := util.ParseLicensesString(FlagLicenses)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: invalid --licenses value '%s': %w", FlagLicenses, err)
		}
		job.LicensesCount = licCount
		job.IsLicensesOr = isLicenseOr
	}
	if FlagCwd != "" {
		job.Cwd = FlagCwd
	}
	if FlagAccount != "" {
		job.Account = FlagAccount
	}
	if FlagNodelist != "" {
		job.Nodelist = FlagNodelist
	}
	if FlagExcludes != "" {
		job.Excludes = FlagExcludes
	}
	if FlagGetUserEnv {
		job.GetUserEnv = true
	}
	if FlagExport != "" {
		job.Env["CRANE_EXPORT_ENV"] = FlagExport
	}
	if FlagStdinPath != "" {
		job.GetIoMeta().InputFilePattern = FlagStdinPath
	}
	if FlagStdoutPath != "" {
		job.GetIoMeta().OutputFilePattern = FlagStdoutPath
	}
	if FlagStderrPath != "" {
		job.GetIoMeta().ErrorFilePattern = FlagStderrPath
	}
	if FlagInterpreter != "" {
		job.GetBatchMeta().Interpreter = FlagInterpreter
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
			job.GetIoMeta().OpenModeAppend = proto.Bool(true)
		case util.OpenModeTruncate:
			job.GetIoMeta().OpenModeAppend = proto.Bool(false)
		default:
			return nil, fmt.Errorf("invalid argument: --open-mode must be either '%s' or '%s'", util.OpenModeAppend, util.OpenModeTruncate)
		}
	}
	if FlagBeginTime != "" {
		beginTime, err := util.ParseTime(FlagBeginTime)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: invalid --begin value '%s': %w", FlagBeginTime, err)
		}
		job.BeginTime = timestamppb.New(beginTime)
	}
	if FlagExclusive {
		job.Exclusive = true
	}
	if FlagHold {
		job.Hold = true
	}
	if FlagReservation != "" {
		job.Reservation = FlagReservation
	}

	if FlagSignal != "" {
		signals, err := util.ParseSignalParamString(FlagSignal)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: signal value '%s' : %w", FlagSignal, err)
		}
		for _, signal := range signals {
			job.Signals = append(job.Signals, signal)
		}
	}

	// Set pod meta if it's a container job
	if job.Type == protos.JobType_Container {
		overridePodFromFlags(cmd, &podOpts)
		podMeta, err := buildPodMeta(job, &podOpts)
		if err != nil {
			return nil, fmt.Errorf("invalid container options: %v", err)
		}
		job.PodMeta = podMeta
	}

	// Set and check the extra attributes
	var extraFromCli string
	if err := structExtraFromCli.Marshal(&extraFromCli); err != nil {
		return nil, fmt.Errorf("invalid argument: failed to marshal extra attributes from CLI: %w", err)
	}
	job.ExtraAttr = util.AmendJobExtraAttrs(extraFromScript, extraFromCli)

	// Set the submit hostname
	submitHostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("Failed to get hostname of the submitting host: %v", err)
	}
	job.SubmitHostname = submitHostname
	if FlagDependency != "" {
		err := util.SetJobDependencies(job, FlagDependency)
		if err != nil {
			return nil, fmt.Errorf("invalid argument: failed to set dependencies: %w", err)
		}
	}

	// Check the validity of the parameters
	if err := util.CheckFileLength(job.GetIoMeta().InputFilePattern); err != nil {
		return nil, fmt.Errorf("invalid argument: invalid input file path: %w", err)
	}
	if err := util.CheckFileLength(job.GetIoMeta().OutputFilePattern); err != nil {
		return nil, fmt.Errorf("invalid argument: invalid output file path: %w", err)
	}
	if err := util.CheckFileLength(job.GetIoMeta().ErrorFilePattern); err != nil {
		return nil, fmt.Errorf("invalid argument: invalid error file path: %w", err)
	}
	if err := util.CheckJobArgs(job); err != nil {
		return nil, fmt.Errorf("invalid argument: %w", err)
	}

	return job, nil
}

func applyScriptArgs(cmd *cobra.Command, cbatchArgs []CbatchArg, job *protos.JobToCtld, extraFromScript *util.JobExtraAttrs, podOpts *podOptions) error {
	for _, arg := range cbatchArgs {
		switch arg.name {
		case "--nodes", "-N":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			if num == 0 {
				return fmt.Errorf("invalid argument: %s must be > 0 in script", arg.name)
			}
			job.NodeNum = uint32(num)
		case "--cpus-per-task", "-c":
			num, err := util.ParseFloatWithPrecision(arg.val, 10)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			job.CpusPerTask = &num
		case "--gres":
			gresMap, err := util.ParseGres(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			if _, exist := gresMap.NameTypeMap[util.GresGpuName]; exist {
				if setGpusPerNodeFlag {
					return fmt.Errorf("invalid argument: cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
				}
				setGresGpusFlag = true
			}
			job.GresPerNode = gresMap
		case "--gpus-per-node":
			if setGresGpusFlag {
				return fmt.Errorf("invalid argument: cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
			}
			setGpusPerNodeFlag = true
			gpuDeviceMap, err := util.ParseGpusPerNodeStr(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			job.GresPerNode = gpuDeviceMap
		case "--ntasks-per-node":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			if num == 0 {
				return fmt.Errorf("invalid argument: %s must be > 0 in script", arg.name)
			}
			job.NtasksPerNode = uint32(num)
		case "--ntasks", "-n":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			if num == 0 {
				return fmt.Errorf("invalid argument: %s must be > 0 in script", arg.name)
			}
			job.Ntasks = uint32(num)
		case "--time", "-t":
			seconds, err := util.ParseDurationStrToSeconds(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			job.TimeLimit.Seconds = seconds
		case "--begin", "-b":
			beginTime, err := util.ParseTime(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			job.BeginTime = timestamppb.New(beginTime)
		case "--mem":
			memInByte, err := util.ParseMemStringAsByte(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			job.MemPerNode = &memInByte
		case "--mem-per-cpu":
			memInBytePerCpu, err := util.ParseMemStringAsByte(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			job.MemPerCpu = &memInBytePerCpu
		case "-p", "--partition":
			job.PartitionName = arg.val
		case "-J", "--job-name":
			job.Name = arg.val
		case "-A", "--account":
			job.Account = arg.val
		case "--qos", "-Q":
			job.Qos = arg.val
		case "--licenses", "-L":
			licCount, isLicenseOr, err := util.ParseLicensesString(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			job.LicensesCount = licCount
			job.IsLicensesOr = isLicenseOr
		case "--chdir":
			job.Cwd = arg.val
		case "--exclude", "-x":
			job.Excludes = arg.val
		case "--nodelist", "-w":
			job.Nodelist = arg.val
		case "--get-user-env":
			if arg.val == "" {
				job.GetUserEnv = true
			} else {
				val, err := strconv.ParseBool(arg.val)
				if err != nil {
					return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
				}
				job.GetUserEnv = val
			}
		case "--export":
			job.Env["CRANE_EXPORT_ENV"] = arg.val
		case "-i", "--input":
			job.GetIoMeta().InputFilePattern = arg.val
		case "-o", "--output":
			job.GetIoMeta().OutputFilePattern = arg.val
		case "-e", "--error":
			job.GetIoMeta().ErrorFilePattern = arg.val
		case "--interpreter":
			job.GetBatchMeta().Interpreter = arg.val
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
				job.GetIoMeta().OpenModeAppend = proto.Bool(true)
			case util.OpenModeTruncate:
				job.GetIoMeta().OpenModeAppend = proto.Bool(false)
			default:
				return fmt.Errorf("invalid argument: --open-mode must be either '%s' or '%s'", util.OpenModeAppend, util.OpenModeTruncate)
			}
		case "-r", "--reservation":
			job.Reservation = arg.val
		case "--exclusive":
			val, err := strconv.ParseBool(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			job.Exclusive = val
		case "--wckey":
			wckey := arg.val
			job.Wckey = &wckey
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
		case "--pod-dns":
			servers := strings.Split(arg.val, ",")
			for i, s := range servers {
				servers[i] = strings.TrimSpace(s)
				if err := util.CheckIpv4Format(servers[i]); err != nil {
					return fmt.Errorf("invalid argument: %s dns server '%s' in script: %w", arg.name, servers[i], err)
				}
			}
			podOpts.dns = append(podOpts.dns, servers...)
		case "--dependency", "-d":
			err := util.SetJobDependencies(job, arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: failed to set dependencies: %w", err)
			}
		case "-s", "--signal":
			signals, err := util.ParseSignalParamString(arg.val)
			if err != nil {
				return fmt.Errorf("invalid argument: %s value '%s' in script: %w", arg.name, arg.val, err)
			}
			for _, signal := range signals {
				job.Signals = append(job.Signals, signal)
			}
		default:
			return fmt.Errorf("invalid argument: unrecognized '%s' in script", arg.name)
		}
	}

	return nil
}

func SendRequest(job *protos.JobToCtld) error {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)
	req := &protos.SubmitBatchJobRequest{Job: job}

	reply, err := stub.SubmitBatchJob(context.Background(), req)
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
		fmt.Printf("Job id allocated: %d.\n", reply.GetJobId())
		return nil
	} else {
		if len(reply.GetReason()) > 0 {
			return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Job allocation failed: %s.", reply.GetReason()))
		}

		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Job allocation failed: %s.", util.ErrMsg(reply.GetCode())))
	}
}

func SendMultipleRequests(job *protos.JobToCtld, count uint32) error {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)
	req := &protos.SubmitBatchJobsRequest{Job: job, Count: count}

	reply, err := stub.SubmitBatchJobs(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to submit jobs")
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

	if len(reply.JobIdList) > 0 {
		jobIdListString := util.ConvertSliceToString(reply.JobIdList, ", ")
		fmt.Printf("Job id allocated: %s.\n", jobIdListString)
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
		"array":             "The feature --array/-a is not yet supported by Crane, the use is ignored.",
		"a":                 "The feature --array/-a is not yet supported by Crane, the use is ignored.",
		"no-requeue":        "The feature --no-requeue is not yet supported by Crane, the use is ignored.",
		"parsable":          "The feature --parsable is not yet supported by Crane, the use is ignored.",
		"gpus-per-node":     "The feature --gpus-per-node is not yet supported by Crane, the use is ignored.",
		"ntasks-per-socket": "The feature --ntasks-per-socket is not yet supported by Crane, the use is ignored.",
		"wckey":             "The feature --wckey is not yet supported by Crane, the use is ignored.",
		"cpu-freq":          "The feature --cpu-freq is not yet supported by Crane, the use is ignored.",
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
