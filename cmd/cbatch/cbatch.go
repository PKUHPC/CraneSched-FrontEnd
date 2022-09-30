package main

import (
	"CraneFrontEnd/generated/protos"
	"bufio"
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/duration"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
	"io/ioutil"
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

type SbatchArg struct {
	name string
	val  string
}

type ServerAddr struct {
	ControlMachine      string `yaml:"ControlMachine"`
	CraneCtldListenPort string `yaml:"CraneCtldListenPort"`
}

func ProcessSbatchArg(args []SbatchArg) (bool, *protos.SubmitBatchTaskRequest) {
	req := new(protos.SubmitBatchTaskRequest)
	req.Task = new(protos.TaskToCtld)
	req.Task.NodeNum = 1
	req.Task.NtasksPerNode = 1
	req.Task.CpusPerTask = 1
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

	for _, arg := range args {
		switch arg.name {
		case "--nodes", "-N":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return false, nil
			}
			req.Task.NodeNum = uint32(num)
		case "--cpus-per-task", "-c":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return false, nil
			}
			req.Task.CpusPerTask = uint32(num)
		case "--ntasks-per-node":
			num, err := strconv.ParseUint(arg.val, 10, 32)
			if err != nil {
				return false, nil
			}
			req.Task.NtasksPerNode = uint32(num)
		case "--time":
			re := regexp.MustCompile(`(.*):(.*):(.*)`)
			result := re.FindAllStringSubmatch(arg.val, -1)
			if result == nil || len(result) != 1 {
				return false, nil
			}

			hh, err := strconv.ParseUint(result[0][1], 10, 32)
			if err != nil {
				return false, nil
			}
			mm, err := strconv.ParseUint(result[0][2], 10, 32)
			if err != nil {
				return false, nil
			}
			ss, err := strconv.ParseUint(result[0][3], 10, 32)
			if err != nil {
				return false, nil
			}

			req.Task.TimeLimit.Seconds = int64(60*60*hh + 60*mm + ss)
		case "--mem":
			re := regexp.MustCompile(`(.*)([MmGg])`)
			result := re.FindAllStringSubmatch(arg.val, -1)
			if result == nil || len(result) != 1 {
				return false, nil
			}
			sz, err := strconv.ParseUint(result[0][1], 10, 32)
			if err != nil {
				return false, nil
			}
			switch result[0][2] {
			case "M", "m":
				req.Task.Resources.AllocatableResource.MemorySwLimitBytes = 1024 * 1024 * sz
				req.Task.Resources.AllocatableResource.MemoryLimitBytes = 1024 * 1024 * sz
			case "G", "g":
				req.Task.Resources.AllocatableResource.MemorySwLimitBytes = 1024 * 1024 * 1024 * sz
				req.Task.Resources.AllocatableResource.MemoryLimitBytes = 1024 * 1024 * 1024 * sz
			}
		case "-p":
			req.Task.PartitionName = arg.val
		case "-o":
			req.Task.GetBatchMeta().OutputFilePattern = arg.val
		case "-J":
			req.Task.Name = arg.val
		}

	}

	req.Task.Resources.AllocatableResource.CpuCoreLimit = uint64(req.Task.CpusPerTask * req.Task.NtasksPerNode)

	return true, req
}

func ProcessLine(line string, sh *[]string, args *[]SbatchArg) bool {
	re := regexp.MustCompile(`^#CBATCH`)
	if re.MatchString(line) {
		split := strings.Fields(line)
		if len(split) == 3 {
			*args = append(*args, SbatchArg{name: split[1], val: split[2]})
		} else if len(split) == 2 {
			*args = append(*args, SbatchArg{name: split[1]})
		} else {
			return false
		}
	} else {
		*sh = append(*sh, line)
	}

	return true
}

func SendRequest(serverAddr string, req *protos.SubmitBatchTaskRequest) {
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
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

func main() {
	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Printf("Failed to close %s\n", file.Name())
		}
	}(file)

	confFile, err := ioutil.ReadFile("/etc/crane/config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	confTxt := ServerAddr{}
	err = yaml.Unmarshal(confFile, &confTxt)
	if err != nil {
		log.Fatal(err)
	}
	ip := confTxt.ControlMachine
	port := confTxt.CraneCtldListenPort

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	num := 0
	sh := make([]string, 0)
	args := make([]SbatchArg, 0)

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
	// fmt.Printf("Sbatch args:\n%v\n\n", args)

	ok, req := ProcessSbatchArg(args)
	if !ok {
		log.Fatal("Invalid sbatch argument")
	}

	req.Task.GetBatchMeta().ShScript = strings.Join(sh, "\n")
	req.Task.Uid = uint32(os.Getuid())
	req.Task.CmdLine = strings.Join(os.Args, " ")
	req.Task.Cwd, _ = os.Getwd()
	req.Task.Env = strings.Join(os.Environ(), "||")
	req.Task.Type = protos.TaskType_Batch

	// fmt.Printf("Req:\n%v\n\n", req)

	SendRequest(fmt.Sprintf("%s:%s", ip, port), req)
}
