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

func ProcessCbatchArg(args []CbatchArg) (bool, *protos.SubmitBatchTaskRequest) {
	req := new(protos.SubmitBatchTaskRequest)
	req.Task = new(protos.TaskToCtld)
	req.Task.TimeLimit = util.InvalidDuration()
	req.Task.Resources = &protos.Resources{
		AllocatableResource: &protos.AllocatableResource{
			CpuCoreLimit:       1,
			MemoryLimitBytes:   0,
			MemorySwLimitBytes: 0,
		},
	}
	req.Task.Payload = &protos.TaskToCtld_BatchMeta{
		BatchMeta: &protos.BatchTaskAdditionalMeta{},
	}

	req.Task.CpusPerTask = 1
	req.Task.NtasksPerNode = 1
	req.Task.NodeNum = 1

	///*************set parameter values based on the file*******************************///
	for _, arg := range args {
		switch arg.name {
		case "--nodes", "-N":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				log.Print("Invalid " + arg.name)
				return false, nil
			}
			req.Task.NodeNum = uint32(num)
		case "--cpus-per-task", "-c":
			num, err := strconv.ParseFloat(arg.val, 10)
			if err != nil {
				log.Print("Invalid " + arg.name)
				return false, nil
			}
			req.Task.CpusPerTask = num
		case "--ntasks-per-node":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				log.Print("Invalid " + arg.name)
				return false, nil
			}
			req.Task.NtasksPerNode = uint32(num)
		case "--time", "-t":
			isOk := util.ParseDuration(arg.val, req.Task.TimeLimit)
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
			req.Task.Resources.AllocatableResource.MemoryLimitBytes = memInByte
			req.Task.Resources.AllocatableResource.MemorySwLimitBytes = memInByte
		case "-p", "--partition":
			req.Task.PartitionName = arg.val
		case "-o", "--output":
			req.Task.GetBatchMeta().OutputFilePattern = arg.val
		case "-J", "--job-name":
			req.Task.Name = arg.val
		case "-A", "--account":
			req.Task.Account = arg.val
		case "--qos", "Q":
			req.Task.Qos = arg.val
		case "--chdir":
			req.Task.Cwd = arg.val
		}
	}

	///*************set parameter values based on the command line*********************///
	//If the command line argument is set, it replaces the argument read from the file, so the command line has a higher priority
	if FlagNodes != 0 {
		req.Task.NodeNum = FlagNodes
	}
	if FlagCpuPerTask != 0 {
		req.Task.CpusPerTask = FlagCpuPerTask
	}
	if FlagNtasksPerNode != 0 {
		req.Task.NtasksPerNode = FlagNtasksPerNode
	}
	if FlagTime != "" {
		ok := util.ParseDuration(FlagTime, req.Task.TimeLimit)
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
		req.Task.Resources.AllocatableResource.MemoryLimitBytes = memInByte
		req.Task.Resources.AllocatableResource.MemorySwLimitBytes = memInByte
	}
	if FlagPartition != "" {
		req.Task.PartitionName = FlagPartition
	}
	if FlagOutput != "" {
		req.Task.GetBatchMeta().OutputFilePattern = FlagOutput
	}
	if FlagJob != "" {
		req.Task.Name = FlagJob
	}
	if FlagQos != "" {
		req.Task.Qos = FlagQos
	}
	if FlagCwd != "" {
		req.Task.Cwd = FlagCwd
	}
	if FlagAccount != "" {
		req.Task.Account = FlagAccount
	}

	if req.Task.CpusPerTask <= 0 || req.Task.NtasksPerNode == 0 || req.Task.NodeNum == 0 {
		log.Print("Invalid --cpus-per-task, --ntasks-per-node or --node-num")
		return false, nil
	}

	req.Task.Resources.AllocatableResource.CpuCoreLimit = req.Task.CpusPerTask * float64(req.Task.NtasksPerNode)

	return true, req
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

func SendRequest(req *protos.SubmitBatchTaskRequest) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)

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

func Cbatch(jobFilePath string) {
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

	ok, req := ProcessCbatchArg(args)
	if !ok {
		log.Fatalf("Invalid cbatch argument")
	}

	req.Task.GetBatchMeta().ShScript = strings.Join(sh, "\n")
	req.Task.Uid = uint32(os.Getuid())
	req.Task.CmdLine = strings.Join(os.Args, " ")
	req.Task.Env = strings.Join(os.Environ(), "||")
	req.Task.Type = protos.TaskType_Batch
	if req.Task.Cwd == "" {
		req.Task.Cwd, _ = os.Getwd()
	}

	SendRequest(req)
}
