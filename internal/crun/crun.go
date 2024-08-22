package crun

import "C"

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type StateOfCrun int

const (
	ConnectCfored StateOfCrun = 0
	ReqTaskId     StateOfCrun = 1
	WaitRes       StateOfCrun = 2
	WaitForward   StateOfCrun = 3
	Forwarding    StateOfCrun = 4
	TaskKilling   StateOfCrun = 5
	WaitAck       StateOfCrun = 6
)

type GlobalVariables struct {
	user *user.User
	cwd  string

	globalCtx       context.Context
	globalCtxCancel context.CancelFunc

	connectionBroken bool
}

var gVars GlobalVariables

type ReplyReceiveItem struct {
	reply *protos.StreamCforedCrunReply
	err   error
}

func ReplyReceiveRoutine(stream protos.CraneForeD_CrunStreamClient,
	replyChannel chan ReplyReceiveItem) {
	for {
		cforedReply, err := stream.Recv()
		replyChannel <- ReplyReceiveItem{
			reply: cforedReply,
			err:   err,
		}
		if err != nil {
			if err != io.EOF {
				log.Errorf("Failed to receive CforedReply: %s. "+
					"Connection to Cfored is broken. "+
					"ReplyReceiveRoutine is exiting...", err)
			}
			break
		}
	}
}

func StartCrunStream(task *protos.TaskToCtld) util.CraneCmdError {
	config := util.ParseConfig(FlagConfigFilePath)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	unixSocketPath := "unix:///" + config.CranedCforedSockPath
	conn, err := grpc.Dial(unixSocketPath, opts...)
	if err != nil {
		log.Errorf("Failed to connect to local unix socket %s: %s",
			unixSocketPath, err)
		return util.ErrorBackend
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Errorf("Failed to close grpc conn: %s", err)
			os.Exit(util.ErrorNetwork)
		}
	}(conn)

	client := protos.NewCraneForeDClient(conn)

	var stream protos.CraneForeD_CrunStreamClient
	var replyChannel chan ReplyReceiveItem

	var request *protos.StreamCrunRequest
	var taskId uint32

	state := ConnectCfored

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)
CrunStateMachineLoop:
	for {
		switch state {
		case ConnectCfored:
			log.Trace("Sending Task Req to Cfored")
			stream, err = client.CrunStream(gVars.globalCtx)
			if err != nil {
				log.Errorf("Failed to create CrunStream: %s.", err)
				break CrunStateMachineLoop
			}

			replyChannel = make(chan ReplyReceiveItem, 8)
			go ReplyReceiveRoutine(stream, replyChannel)

			request = &protos.StreamCrunRequest{
				Type: protos.StreamCrunRequest_TASK_REQUEST,
				Payload: &protos.StreamCrunRequest_PayloadTaskReq{
					PayloadTaskReq: &protos.StreamCrunRequest_TaskReq{
						Task:    task,
						CrunPid: int32(os.Getpid()),
					},
				},
			}

			if err := stream.Send(request); err != nil {
				log.Errorf("Failed to send Task Request to CrunStream: %s. "+
					"Connection to Crun is broken", err)
				gVars.connectionBroken = true
				break CrunStateMachineLoop
			}

			state = ReqTaskId

		case ReqTaskId:
			log.Trace("Waiting TaskId")
			select {
			case item := <-replyChannel:
				cforedReply, err := item.reply, item.err

				if err != nil {
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Errorf("Connection to Cfored broken when requesting "+
							"task id: %s. Exiting...", err)
						gVars.connectionBroken = true
						break CrunStateMachineLoop
					}
				}

				if cforedReply.Type != protos.StreamCforedCrunReply_TASK_ID_REPLY {
					log.Errorln("Expect type TASK_ID_REPLY")
					return util.ErrorBackend
				}
				payload := cforedReply.GetPayloadTaskIdReply()

				if payload.Ok {
					taskId = payload.TaskId
					log.Debugf("Task id allocated: %d\n", taskId)
					state = WaitRes
				} else {
					_, _ = fmt.Fprintf(os.Stderr, "Failed to allocate task id: %s\n", payload.FailureReason)
					break CrunStateMachineLoop
				}
			case sig := <-sigs:
				if sig == syscall.SIGINT {
					log.Tracef("SIGINT Received. Not allowed to cancel task when ReqTaskId")
				} else {
					log.Tracef("Unhanled sig %s", sig.String())
				}
			}

		case WaitRes:
			log.Trace("Waiting Res Alloc")
			select {
			case item := <-replyChannel:
				cforedReply, err := item.reply, item.err

				if err != nil { // Failure Edge
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Errorf("Connection to Cfored broken when waiting "+
							"resource allocated: %s. Exiting...", err)
						gVars.connectionBroken = true
						break CrunStateMachineLoop
					}
				}

				switch cforedReply.Type {
				case protos.StreamCforedCrunReply_TASK_RES_ALLOC_REPLY:
					cforedPayload := cforedReply.GetPayloadTaskAllocReply()
					Ok := cforedPayload.Ok

					if Ok {
						log.Debugf("Allocated craned nodes: %s\n", cforedPayload.AllocatedCranedRegex)
						state = WaitForward
					} else {
						log.Errorln("Failed to allocate task resource. Exiting...")
						break CrunStateMachineLoop
					}
				case protos.StreamCforedCrunReply_TASK_CANCEL_REQUEST:
					log.Tracef("Received Task Cancel Request when wait res")
					state = TaskKilling
				}

			case sig := <-sigs:
				if sig == syscall.SIGINT {
					log.Tracef("SIGINT Received. Cancelling the task...")
					state = TaskKilling
				} else {
					log.Tracef("Unhandled sig %s", sig.String())
					state = TaskKilling
				}
			}

		case WaitForward:

			select {
			case item := <-replyChannel:
				cforedReply, err := item.reply, item.err
				if err != nil { // Failure Edge
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Errorf("Connection to Cfored broken when waiting for forwarding user i/o "+
							"%s. Exiting...", err)
						gVars.connectionBroken = true
						break CrunStateMachineLoop
					}
				}
				switch cforedReply.Type {
				case protos.StreamCforedCrunReply_TASK_IO_FORWARD_READY:
					cforedPayload := cforedReply.GetPayloadTaskIoForwardReadyReply()
					Ok := cforedPayload.Ok
					if Ok {
						log.Tracef("Task io forward ready")
						state = Forwarding
					} else {
						log.Errorln("Failed to wait for task io forward ready. Exiting...")
						break CrunStateMachineLoop
					}
				case protos.StreamCforedCrunReply_TASK_CANCEL_REQUEST:
					state = TaskKilling
				default:
					log.Errorf("Received unhandeled msg type %s", cforedReply.Type.String())
					state = TaskKilling
				}

			case sig := <-sigs:
				if sig == syscall.SIGINT {
					state = TaskKilling
				} else {
					log.Tracef("Unhanled sig %s", sig.String())
					state = TaskKilling
				}
			}

		case Forwarding:
			taskFinishCtx, taskFinishCb := context.WithCancel(context.Background())
			chanInputFromTerm := make(chan string, 5)
			chanOutputFromRemote := make(chan string, 5)
			go IOForward(taskFinishCtx, taskFinishCb, chanInputFromTerm, chanOutputFromRemote)

			go func(msgToTask chan string) {
			forwardToCfored:
				for {
					select {
					case msg := <-msgToTask:
						request = &protos.StreamCrunRequest{
							Type: protos.StreamCrunRequest_TASK_IO_FORWARD,
							Payload: &protos.StreamCrunRequest_PayloadTaskIoForwardReq{
								PayloadTaskIoForwardReq: &protos.StreamCrunRequest_TaskIOForwardReq{
									TaskId: taskId,
									Msg:    msg,
								},
							},
						}
						if err := stream.Send(request); err != nil {
							log.Errorf("Failed to send Task Request to CrunStream: %s. "+
								"Connection to Crun is broken", err)
							gVars.connectionBroken = true
							break forwardToCfored
						}
					case <-taskFinishCtx.Done():
						break forwardToCfored
					}
				}
			}(chanInputFromTerm)

			for state == Forwarding {
				select {
				case <-taskFinishCtx.Done():
					request = &protos.StreamCrunRequest{
						Type: protos.StreamCrunRequest_TASK_COMPLETION_REQUEST,
						Payload: &protos.StreamCrunRequest_PayloadTaskCompleteReq{
							PayloadTaskCompleteReq: &protos.StreamCrunRequest_TaskCompleteReq{
								TaskId: taskId,
								Status: protos.TaskStatus_Completed,
							},
						},
					}

					log.Debug("Sending TASK_COMPLETION_REQUEST with COMPLETED state...")
					if err := stream.Send(request); err != nil {
						log.Errorf("The connection to Cfored was broken: %s. "+
							"Exiting...", err)
						gVars.connectionBroken = true
						break CrunStateMachineLoop
					} else {
						state = WaitAck
					}

				case item := <-replyChannel:
					cforedReply, err := item.reply, item.err
					if err != nil {
						switch err {
						case io.EOF:
							fallthrough
						default:
							log.Errorf("The connection to Cfored was broken: %s. "+
								"Killing task...", err)
							gVars.connectionBroken = true
							break CrunStateMachineLoop
						}
					} else {
						switch cforedReply.Type {
						case protos.StreamCforedCrunReply_TASK_IO_FORWARD:
							{
								chanOutputFromRemote <- cforedReply.GetPayloadTaskIoForwardReply().Msg
							}
						case protos.StreamCforedCrunReply_TASK_CANCEL_REQUEST:
							{
								taskFinishCtx.Done()
								log.Trace("Received TASK_CANCEL_REQUEST")
								state = TaskKilling
							}
						case protos.StreamCforedCrunReply_TASK_COMPLETION_ACK_REPLY:
							{
								log.Debug("Task completed.")
								break CrunStateMachineLoop
							}
						}
					}
				}
			}

		case TaskKilling:
			request = &protos.StreamCrunRequest{
				Type: protos.StreamCrunRequest_TASK_COMPLETION_REQUEST,
				Payload: &protos.StreamCrunRequest_PayloadTaskCompleteReq{
					PayloadTaskCompleteReq: &protos.StreamCrunRequest_TaskCompleteReq{
						TaskId: taskId,
						Status: protos.TaskStatus_Cancelled,
					},
				},
			}

			if gVars.connectionBroken {
				break CrunStateMachineLoop
			}

			log.Debug("Sending TASK_COMPLETION_REQUEST with CANCELLED state...")
			if err := stream.Send(request); err != nil {
				log.Errorf("The connection to Cfored was broken: %s. Exiting...", err)
				gVars.connectionBroken = true
				break CrunStateMachineLoop
			} else {
				state = WaitAck
			}

		case WaitAck:
			log.Debug("Waiting Ctld TASK_COMPLETION_ACK_REPLY")
			item := <-replyChannel
			cforedReply, err := item.reply, item.err

			if err != nil {
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Errorf("The connection to Cfored was broken: %s. "+
						"Exiting...", err)
					gVars.connectionBroken = true
					break CrunStateMachineLoop
				}
			}

			if cforedReply.Type != protos.StreamCforedCrunReply_TASK_COMPLETION_ACK_REPLY {
				log.Errorf("Expect TASK_COMPLETION_ACK_REPLY. bug get %s\n", cforedReply.Type.String())
				return util.ErrorBackend
			}

			if cforedReply.GetPayloadTaskCompletionAckReply().Ok {
				log.Debug("Task completed.")
			} else {
				log.Errorln("Failed to notify server of task completion")
				return util.ErrorBackend
			}

			break CrunStateMachineLoop
		}
	}
	// Check if connection finished normally
	if state != WaitAck || gVars.connectionBroken {
		return util.ErrorNetwork
	} else {
		return util.ErrorSuccess
	}
}

func forwardingSigintHandlerRoutine(sigintCb func(), wg *sync.WaitGroup) {
	defer wg.Done()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)

	lastSigint := time.Now().Add(-2 * time.Second)
loop:
	for {
		select {
		case sig := <-sigs:
			switch sig {
			/*
				multiple sigint will cancel this job
			*/
			case syscall.SIGINT:
				log.Tracef("Recv signal: %v", sig)
				now := time.Now()
				if lastSigint.Add(time.Second).After(now) {
					sigintCb()
					break loop
				} else {
					lastSigint = now
					fmt.Println("Send interrupt once more in 1s to abort.")
				}

			default:
				log.Tracef("Ignored signal: %v", sig)
			}
		}
	}
	log.Tracef("Signal processing goroutine exit.")
}

func fileWriterRoutine(fd uintptr, chanOutputFromTask chan string, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	file := os.NewFile(fd, "stdout")
	writer := bufio.NewWriter(file)

writing:
	for {
		select {
		case msg := <-chanOutputFromTask:
			_, err := writer.WriteString(msg)

			if err != nil {
				fmt.Printf("Failed to write to fd: %v\n", err)
				break writing
			}
			err = writer.Flush()
			if err != nil {
				fmt.Printf("Failed to flush to fd: %v\n", err)
				break writing
			}

		case <-ctx.Done():
			break writing
		}
	}
}

func fileReaderRoutine(fd uintptr, chanInputFromTerm chan string, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	file := os.NewFile(fd, "stdin")
	reader := bufio.NewReader(file)
reading:
	for {
		select {
		case <-ctx.Done():
			break reading

		default:
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break reading
				}
				fmt.Printf("Failed to read from fd: %v\n", err)
				break reading
			}
			chanInputFromTerm <- line
		}
	}
}

func IOForward(taskFinishCtx context.Context, taskFinishFunc context.CancelFunc,
	chanInputFromTerm chan string, chanOutputFromTask chan string) {

	var wg sync.WaitGroup
	wg.Add(3)

	go forwardingSigintHandlerRoutine(taskFinishFunc, &wg)
	go fileReaderRoutine(os.Stdin.Fd(), chanInputFromTerm, taskFinishCtx, &wg)
	go fileWriterRoutine(os.Stdout.Fd(), chanOutputFromTask, taskFinishCtx, &wg)

	wg.Wait()
}

func MainCrun(cmd *cobra.Command, args []string) util.CraneCmdError {
	util.InitLogger(FlagDebugLevel)

	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())

	var err error
	if gVars.cwd, err = os.Getwd(); err != nil {
		log.Errorf("Failed to get working directory: %s", err.Error())
		return util.ErrorBackend
	}

	if gVars.user, err = user.Current(); err != nil {
		log.Errorf("Failed to get current user: %s", err.Error())
		return util.ErrorBackend
	}

	uid, err := strconv.Atoi(gVars.user.Uid)
	if err != nil {
		log.Errorf("Failed to convert uid to int: %s", err.Error())
		return util.ErrorInvalidFormat
	}

	if len(args) == 0 {
		log.Errorf("Please specify program to run")
		return util.ErrorCmdArg
	}

	task := &protos.TaskToCtld{
		Name:          "Interactive",
		TimeLimit:     util.InvalidDuration(),
		PartitionName: "",
		Resources: &protos.ResourceView{
			AllocatableRes: &protos.AllocatableResource{
				CpuCoreLimit:       1,
				MemoryLimitBytes:   0,
				MemorySwLimitBytes: 0,
			},
		},
		Type:            protos.TaskType_Interactive,
		Uid:             uint32(uid),
		NodeNum:         1,
		NtasksPerNode:   1,
		CpusPerTask:     1,
		RequeueIfFailed: false,
		Payload: &protos.TaskToCtld_InteractiveMeta{
			InteractiveMeta: &protos.InteractiveTaskAdditionalMeta{},
		},
		CmdLine: strings.Join(args, " "),
		Cwd:     gVars.cwd,

		// Todo: use --export here!
		Env: make(map[string]string),
	}

	if FlagNodes > 0 {
		task.NodeNum = FlagNodes
	} else {
		log.Errorf("Invalid --nodes %d", FlagNodes)
		return util.ErrorCmdArg
	}
	if FlagCpuPerTask > 0 {
		task.CpusPerTask = FlagCpuPerTask
	} else {
		log.Errorf("Invalid --cpus-per-task %f", FlagCpuPerTask)
		return util.ErrorCmdArg
	}
	if FlagNtasksPerNode > 0 {
		task.NtasksPerNode = FlagNtasksPerNode
	} else {
		log.Errorf("Invalid --ntasks-per-node %d", FlagNtasksPerNode)
		return util.ErrorCmdArg
	}
	if FlagTime != "" {
		ok := util.ParseDuration(FlagTime, task.TimeLimit)
		if !ok {
			log.Errorln("Invalid --time")
			return util.ErrorCmdArg
		}
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			log.Errorln(err)
			return util.ErrorCmdArg
		}
		task.Resources.AllocatableRes.MemoryLimitBytes = memInByte
		task.Resources.AllocatableRes.MemorySwLimitBytes = memInByte
	}
	if FlagGres != "" {
		gresMap := util.ParseGres(FlagGres)
		task.Resources.DeviceMap = gresMap
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
	task.Resources.AllocatableRes.CpuCoreLimit = task.CpusPerTask * float64(task.NtasksPerNode)
	if task.Resources.AllocatableRes.CpuCoreLimit > 1e6 {
		log.Errorf("Request too many cpus: %f", task.Resources.AllocatableRes.CpuCoreLimit)
		return util.ErrorCmdArg
	}
	task.GetInteractiveMeta().ShScript = strings.Join(args, " ")
	term, exits := syscall.Getenv("TERM")
	if exits {
		task.GetInteractiveMeta().TermEnv = term
	}
	task.GetInteractiveMeta().InteractiveType = protos.InteractiveTaskType_Crun

	return StartCrunStream(task)
}
