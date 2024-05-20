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
)

type CbatchArg struct {
	name string
	val  string
}

func ProcessCbatchArg(args []CbatchArg) (bool, *protos.TaskToCtld) {
	task := new(protos.TaskToCtld)
	task = new(protos.TaskToCtld)
	task.TimeLimit = util.InvalidDuration()
	task.Resources = &protos.Resources{
		AllocatableResource: &protos.AllocatableResource{
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

	/*** Set parameter values based on the file ***/
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
			// FIXME: BUG? 'bitSize' argument is invalid, must be either 32 or 64 (SA1030)
			num, err := strconv.ParseFloat(arg.val, 10)
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
			isOk := util.ParseDuration(arg.val, task.TimeLimit)
			if !isOk {
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
			task.GetUserEnv = true
		case "--export":
			task.Env["CRANE_EXPORT_ENV"] = arg.val
		case "--container":
			task.Container = arg.val
		case "--interpreter":
			task.GetBatchMeta().Interpreter = arg.val
		case "-o", "--output":
			task.GetBatchMeta().OutputFilePattern = arg.val
		case "-e", "--error":
			task.GetBatchMeta().ErrorFilePattern = arg.val
		default:
			log.Fatalf("Invalid parameter given: %s\n", arg.name)
		}
	}

	/*** Set parameter values based on the command line ***/
	// If the command line argument is set, it replaces the argument read from the file,
	// so the command line has a higher priority
	if FlagNodes != 0 {
		task.NodeNum = FlagNodes
	}
	if FlagCpuPerTask != 0 {
		task.CpusPerTask = FlagCpuPerTask
	}
	if FlagNtasksPerNode != 0 {
		task.NtasksPerNode = FlagNtasksPerNode
	}
	if FlagTime != "" {
		if ok := util.ParseDuration(FlagTime, task.TimeLimit); !ok {
			log.Error("Invalid --time")
			return false, nil
		}
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			log.Error(err)
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
	if FlagGetUserEnv != "" {
		task.GetUserEnv = true
	}
	if FlagExport != "" {
		task.Env["CRANE_EXPORT_ENV"] = FlagExport
	}
	if FlagContainer != "" {
		task.Container = FlagContainer
	}
	if FlagInterpreter != "" {
		task.GetBatchMeta().Interpreter = FlagInterpreter
	}
	if FlagStdoutPath != "" {
		task.GetBatchMeta().OutputFilePattern = FlagStdoutPath
	}
	if FlagStderrPath != "" {
		task.GetBatchMeta().ErrorFilePattern = FlagStderrPath
	}

	if task.CpusPerTask <= 0 || task.NtasksPerNode == 0 || task.NodeNum == 0 {
		log.Error("Invalid --cpus-per-task, --ntasks-per-node or --node-num")
		return false, nil
	}

	task.Resources.AllocatableResource.CpuCoreLimit = task.CpusPerTask * float64(task.NtasksPerNode)

	return true, task
}

func ProcessLine(line string, sh *[]string, args *[]CbatchArg) bool {
	re := regexp.MustCompile(`^#CBATCH`)
	if re.MatchString(line) {
		split := strings.Fields(line)
		if len(split) == 3 {
			*args = append(*args, CbatchArg{name: split[1], val: split[2]})
		} else if len(split) == 2 {
			*args = append(*args, CbatchArg{name: split[1]})
		} else {
			return false
		}
	} else {
		*sh = append(*sh, line)
	}
	return true
}

func SendRequest(task *protos.TaskToCtld) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)
	req := &protos.SubmitBatchTaskRequest{Task: task}

	reply, err := stub.SubmitBatchTask(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to submit the task")
	}

	if reply.GetOk() {
		fmt.Printf("Task Id allocated: %d\n", reply.GetTaskId())
	} else {
		fmt.Printf("Task allocation failed: %s\n", reply.GetReason())
	}
}

func SendMultipleRequests(task *protos.TaskToCtld, count uint32) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)
	req := &protos.SubmitBatchTasksRequest{Task: task, Count: count}

	reply, err := stub.SubmitBatchTasks(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to submit tasks")
	}

	if len(reply.TaskIdList) > 0 {
		taskIdListString := make([]string, len(reply.TaskIdList))
		for i, taskId := range reply.TaskIdList {
			taskIdListString[i] = strconv.FormatUint(uint64(taskId), 10)
		}
		fmt.Printf("Task Id allocated: %s\n", strings.Join(taskIdListString, ", "))
	}

	if len(reply.ReasonList) > 0 {
		fmt.Printf("Failed reasons: %s\n", strings.Join(reply.ReasonList, ", "))
	}
}

func SplitEnvironEntry(env *string) (string, string) {
	eq := strings.IndexByte(*env, '=')
	if eq == -1 {
		return *env, ""
	} else {
		return (*env)[:eq], (*env)[eq+1:]
	}
}

func SetPropagatedEnviron(task *protos.TaskToCtld) {
	systemEnv := make(map[string]string)
	for _, str := range os.Environ() {
		name, value := SplitEnvironEntry(&str)
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
				k, v := SplitEnvironEntry(&exportValue)
				// If user-specified value is empty, use system value instead.
				if v != "" {
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

func Cbatch(jobFilePath string) {
	if FlagRepeat == 0 {
		log.Fatal("--repeat must >0")
	}

	file, err := os.Open(jobFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Warnf("Failed to close %s\n", file.Name())
		}
	}(file)

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	num := 0
	sh := make([]string, 0)
	args := make([]CbatchArg, 0)

	for scanner.Scan() {
		num++

		// Shebang line
		if (num == 1) && strings.HasPrefix(scanner.Text(), "#!") {
			args = append(args, CbatchArg{name: "--interpreter", val: scanner.Text()[2:]})
			continue
		}

		// #CBATCH line
		if ok := ProcessLine(scanner.Text(), &sh, &args); !ok {
			log.Fatalf("grammar error at line %v", num)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	// fmt.Printf("Invoking UID: %d\n\n", os.Getuid())
	// fmt.Printf("Shell script:\n%s\n\n", strings.Join(sh, "\n"))
	// fmt.Printf("Cbatch args:\n%v\n\n", args)

	ok, task := ProcessCbatchArg(args)
	if !ok {
		log.Fatalf("Invalid cbatch argument")
	}

	task.GetBatchMeta().ShScript = strings.Join(sh, "\n")
	task.Uid = uint32(os.Getuid())
	task.CmdLine = strings.Join(os.Args, " ")

	// Process the content of --export
	SetPropagatedEnviron(task)

	task.Type = protos.TaskType_Batch
	if task.Cwd == "" {
		// If container task, cwd is omitted and only
		// used to generate default output/error file path.
		task.Cwd, _ = os.Getwd()
	}

	if FlagRepeat == 1 {
		SendRequest(task)
	} else {
		SendMultipleRequests(task, FlagRepeat)
	}
}
