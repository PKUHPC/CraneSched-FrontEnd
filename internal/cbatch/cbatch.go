package cbatch

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"bufio"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"regexp"
	"strconv"
	"strings"
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
			if isOk == false {
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
		case "-o", "--output":
			task.GetBatchMeta().OutputFilePattern = arg.val
		case "-J", "--job-name":
			task.Name = arg.val
		case "-A", "--account":
			task.Account = arg.val
		case "--qos", "Q":
			task.Qos = arg.val
		case "--chdir":
			task.Cwd = arg.val
		case "--excludes", "-x":
			task.Excludes = strings.Split(arg.val, ",")
		case "--nodelist", "-w":
			task.Nodelist = strings.Split(arg.val, ",")
		}
	}

	///*************set parameter values based on the command line*********************///
	//If the command line argument is set, it replaces the argument read from the file, so the command line has a higher priority
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
		ok := util.ParseDuration(FlagTime, task.TimeLimit)
		if ok == false {
			log.Print("Invalid --time")
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
	if FlagOutput != "" {
		task.GetBatchMeta().OutputFilePattern = FlagOutput
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
		task.Nodelist = strings.Split(FlagNodelist, ",")
	}
	if FlagExcludes != "" {
		task.Excludes = strings.Split(FlagExcludes, ",")
	}

	if task.CpusPerTask <= 0 || task.NtasksPerNode == 0 || task.NodeNum == 0 {
		log.Print("Invalid --cpus-per-task, --ntasks-per-node or --node-num")
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
		panic("SubmitBatchTask failed: " + err.Error())
	}

	if reply.GetOk() {
		fmt.Printf("Task Id allocated: %d\n", reply.GetTaskId())
	} else {
		fmt.Printf("Task allocation failed: %s\n", reply.GetReason())
	}
}

func SendMultipleRequests(tasks []*protos.TaskToCtld) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)
	req := &protos.SubmitBatchTasksRequest{Tasks: tasks}

	reply, err := stub.SubmitBatchTasks(context.Background(), req)
	if err != nil {
		panic("SubmitBatchTask failed: " + err.Error())
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
			log.Printf("Failed to close %s\n", file.Name())
		}
	}(file)

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	num := 0
	sh := make([]string, 0)
	args := make([]CbatchArg, 0)

	for scanner.Scan() {
		num++
		success := ProcessLine(scanner.Text(), &sh, &args)
		if !success {
			err = fmt.Errorf("grammer error at line %v", num)
			fmt.Println(err.Error())
			os.Exit(1)
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
	task.Env = strings.Join(os.Environ(), "||")
	task.Type = protos.TaskType_Batch
	if task.Cwd == "" {
		task.Cwd, _ = os.Getwd()
	}

	if FlagRepeat == 1 {
		SendRequest(task)
	} else {
		tasks := make([]*protos.TaskToCtld, FlagRepeat)
		for i := uint32(0); i < FlagRepeat; i++ {
			tasks[i] = task
		}
		SendMultipleRequests(tasks)
	}
}
