package cbatch

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"bufio"
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/duration"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
)

func INVALID_DURATION() *duration.Duration {
	return &duration.Duration{
		Seconds: 630720000000,
		Nanos:   0,
	}
}

type CbatchArg struct {
	name string
	val  string
}

func ProcessCbatchArg(args []CbatchArg) (bool, *protos.SubmitBatchTaskRequest) {
	req := new(protos.SubmitBatchTaskRequest)
	req.Task = new(protos.TaskToCtld)
	req.Task.TimeLimit = INVALID_DURATION()
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

	///*************set parameter values based on the command line*********************///
	req.Task.NodeNum = nodes
	req.Task.CpusPerTask = cpuPerTask
	req.Task.NtasksPerNode = ntasksPerNode
	if time != "" {
		isOk := SetTime(time, req)
		if isOk == false {
			return false, nil
		}
	}
	if mem != "" {
		isOk := SetMem(mem, req)
		if isOk == false {
			return false, nil
		}
	}
	if partition != "" {
		req.Task.PartitionName = partition
	}
	if output != "" {
		req.Task.GetBatchMeta().OutputFilePattern = output
	}
	if job != "" {
		req.Task.Name = job
	}
	///*************set parameter values based on the file*******************************///
	for _, arg := range args {
		switch arg.name {
		case "--nodes", "-N":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return false, nil
			}
			req.Task.NodeNum = uint32(num)
		case "--cpus-per-task", "-c":
			num, err := strconv.ParseFloat(arg.val, 10)
			if err != nil {
				return false, nil
			}
			req.Task.CpusPerTask = num
		case "--ntasks-per-node":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return false, nil
			}
			req.Task.NtasksPerNode = uint32(num)
		case "--time", "t":
			isOk := SetTime(arg.val, req)
			if isOk == false {
				return false, nil
			}
		case "--mem":
			isOk := SetMem(arg.val, req)
			if isOk == false {
				return false, nil
			}
		case "-p", "partition":
			req.Task.PartitionName = arg.val
		case "-o", "output":
			req.Task.GetBatchMeta().OutputFilePattern = arg.val
		case "-J", "job-name":
			req.Task.Name = arg.val
		}
	}

	if req.Task.CpusPerTask <= 0 || req.Task.NtasksPerNode == 0 || req.Task.NodeNum == 0 {
		return false, nil
	}

	req.Task.Resources.AllocatableResource.CpuCoreLimit = req.Task.CpusPerTask * float64(req.Task.NtasksPerNode)

	return true, req
}

func SetTime(time string, req *protos.SubmitBatchTaskRequest) bool {
	re := regexp.MustCompile(`(.*):(.*):(.*)`)
	result := re.FindAllStringSubmatch(time, -1)
	if result == nil || len(result) != 1 {
		return false
	}
	hh, err := strconv.ParseUint(result[0][1], 10, 32)
	if err != nil {
		return false
	}
	mm, err := strconv.ParseUint(result[0][2], 10, 32)
	if err != nil {
		return false
	}
	ss, err := strconv.ParseUint(result[0][3], 10, 32)
	if err != nil {
		return false
	}

	req.Task.TimeLimit.Seconds = int64(60*60*hh + 60*mm + ss)
	return true
}

func SetMem(mem string, req *protos.SubmitBatchTaskRequest) bool {
	re := regexp.MustCompile(`([0-9]+(\.?[0-9]+)?)([MmGg])`)
	result := re.FindAllStringSubmatch(mem, -1)
	if result == nil || len(result) != 1 {
		return false
	}
	sz, err := strconv.ParseFloat(result[0][1], 10)
	if err != nil {
		return false
	}
	switch result[0][len(result[0])-1] {
	case "M", "m":
		req.Task.Resources.AllocatableResource.MemorySwLimitBytes = uint64(1024 * 1024 * sz)
		req.Task.Resources.AllocatableResource.MemoryLimitBytes = uint64(1024 * 1024 * sz)
	case "G", "g":
		req.Task.Resources.AllocatableResource.MemorySwLimitBytes = uint64(1024 * 1024 * 1024 * sz)
		req.Task.Resources.AllocatableResource.MemoryLimitBytes = uint64(1024 * 1024 * 1024 * sz)
	}
	return true
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

func SendRequest(serverAddr string, req *protos.SubmitBatchTaskRequest) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("Cannot connect to CraneCtld: " + err.Error())
	}

	stub := protos.NewCraneCtldClient(conn)
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

	config := util.ParseConfig()
	serverAddr := fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)

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
		log.Fatal("Invalid cbatch argument")
	}

	req.Task.GetBatchMeta().ShScript = strings.Join(sh, "\n")
	req.Task.Uid = uint32(os.Getuid())
	req.Task.CmdLine = strings.Join(os.Args, " ")
	req.Task.Cwd, _ = os.Getwd()
	req.Task.Env = strings.Join(os.Environ(), "||")
	req.Task.Type = protos.TaskType_Batch

	// fmt.Printf("Req:\n%v\n\n", req)

	SendRequest(serverAddr, req)
}
