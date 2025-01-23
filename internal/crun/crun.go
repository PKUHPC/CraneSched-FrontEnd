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

package crun

import "C"

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"

	"github.com/pkg/term/termios"
	"golang.org/x/sys/unix"

	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
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
	cwd string

	globalCtx       context.Context
	globalCtxCancel context.CancelFunc

	connectionBroken bool
}

var gVars GlobalVariables

type ReplyReceiveItem struct {
	reply *protos.StreamCrunReply
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

	ret := util.ErrorSuccess
CrunStateMachineLoop:
	for {
		switch state {
		case ConnectCfored:
			log.Trace("Sending Task Req to Cfored")
			stream, err = client.CrunStream(gVars.globalCtx)
			if err != nil {
				log.Errorf("Failed to create CrunStream: %s.", err)
				ret = util.ErrorNetwork
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
				ret = util.ErrorNetwork
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
						ret = util.ErrorNetwork
						break CrunStateMachineLoop
					}
				}

				if cforedReply.Type != protos.StreamCrunReply_TASK_ID_REPLY {
					log.Errorln("Expect type TASK_ID_REPLY")
					return util.ErrorBackend
				}
				payload := cforedReply.GetPayloadTaskIdReply()

				if payload.Ok {
					taskId = payload.TaskId
					fmt.Printf("Task id allocated: %d, waiting resources.\n", taskId)
					state = WaitRes
				} else {
					_, _ = fmt.Fprintf(os.Stderr, "Failed to allocate task id: %s\n", payload.FailureReason)
					ret = util.ErrorBackend
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
						ret = util.ErrorNetwork
						break CrunStateMachineLoop
					}
				}

				switch cforedReply.Type {
				case protos.StreamCrunReply_TASK_RES_ALLOC_REPLY:
					cforedPayload := cforedReply.GetPayloadTaskAllocReply()
					Ok := cforedPayload.Ok

					if Ok {
						fmt.Printf("Allocated craned nodes: %s\n", cforedPayload.AllocatedCranedRegex)
						state = WaitForward
					} else {
						log.Errorln("Failed to allocate task resource. Exiting...")
						ret = util.ErrorBackend
						break CrunStateMachineLoop
					}
				case protos.StreamCrunReply_TASK_CANCEL_REQUEST:
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
						ret = util.ErrorNetwork
						break CrunStateMachineLoop
					}
				}
				switch cforedReply.Type {
				case protos.StreamCrunReply_TASK_IO_FORWARD_READY:
					cforedPayload := cforedReply.GetPayloadTaskIoForwardReadyReply()
					Ok := cforedPayload.Ok
					if Ok {
						fmt.Println("Task io forward ready, waiting input.")
						state = Forwarding
					} else {
						log.Errorln("Failed to wait for task io forward ready. Exiting...")
						ret = util.ErrorBackend
						break CrunStateMachineLoop
					}
				case protos.StreamCrunReply_TASK_CANCEL_REQUEST:
					state = TaskKilling
				case protos.StreamCrunReply_TASK_COMPLETION_ACK_REPLY:
					// Task launch failed !
					fmt.Println("Task failed ")
					ret = util.ErrorGeneric
					break CrunStateMachineLoop
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

			if FlagPty {
				ptyAttr := unix.Termios{}
				err := termios.Tcgetattr(os.Stdin.Fd(), &ptyAttr)
				if err != nil {
					log.Errorf("Failed to get stdin attr: %s,killing", err.Error())
					state = TaskKilling
					ret = util.ErrorGeneric
					break CrunStateMachineLoop
				}
				originAttr := ptyAttr
				termios.Cfmakeraw(&ptyAttr)
				termios.Cfmakecbreak(&ptyAttr)
				err = termios.Tcsetattr(os.Stdin.Fd(), termios.TCSANOW, &ptyAttr)
				if err != nil {
					log.Errorf("Failed to get stdin attr: %s,killing", err.Error())
					state = TaskKilling
					ret = util.ErrorGeneric
					break CrunStateMachineLoop
				}
				defer func(fd uintptr, oldState *unix.Termios) {
					err := termios.Tcsetattr(fd, termios.TCSANOW, &originAttr)
					if err != nil {
						log.Errorf("Failed to restore stdin attr: %s,killing", err.Error())
						ret = util.ErrorGeneric
					}

				}(os.Stdin.Fd(), &originAttr)
			}
			chanInputFromTerm := make(chan string, 100)
			chanOutputFromRemote := make(chan string, 5)
			taskFinishCtx, taskFinishCb := context.WithCancel(context.Background())
			StartIOForward(taskFinishCtx, taskFinishCb, chanInputFromTerm, chanOutputFromRemote)

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
						ret = util.ErrorNetwork
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
							ret = util.ErrorNetwork
							gVars.connectionBroken = true
							break CrunStateMachineLoop
						}
					} else {
						switch cforedReply.Type {
						case protos.StreamCrunReply_TASK_IO_FORWARD:
							{
								chanOutputFromRemote <- cforedReply.GetPayloadTaskIoForwardReply().Msg
							}
						case protos.StreamCrunReply_TASK_CANCEL_REQUEST:
							{
								taskFinishCtx.Done()
								log.Trace("Received TASK_CANCEL_REQUEST")
								state = TaskKilling
							}
						case protos.StreamCrunReply_TASK_COMPLETION_ACK_REPLY:
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
				log.Errorf("The connection to Cfored was broken. Exiting...")
				ret = util.ErrorNetwork
				break CrunStateMachineLoop
			}

			log.Debug("Sending TASK_COMPLETION_REQUEST with CANCELLED state...")
			if err := stream.Send(request); err != nil {
				log.Errorf("The connection to Cfored was broken: %s. Exiting...", err)
				gVars.connectionBroken = true
				ret = util.ErrorNetwork
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
					ret = util.ErrorNetwork
					break CrunStateMachineLoop
				}
			}

			if cforedReply.Type != protos.StreamCrunReply_TASK_COMPLETION_ACK_REPLY {
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
	return ret
}

func forwardingSigintHandlerRoutine(sigintCb func()) {
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

func fileWriterRoutine(fd uintptr, chanOutputFromTask chan string, ctx context.Context) {
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

func fileReaderRoutine(fd uintptr, chanInputFromTerm chan string, ctx context.Context) {

	file := os.NewFile(fd, "stdin")
	reader := bufio.NewReader(file)
reading:
	for {
		select {
		case <-ctx.Done():
			break reading

		default:
			if FlagPty {
				data, err := reader.ReadByte()
				if err != nil {
					if err == io.EOF {
						break reading
					}
					log.Errorf("Failed to read from fd: %v\n", err)
					break reading
				}
				chanInputFromTerm <- string(data)
			} else {
				data, err := reader.ReadString('\n')
				if err != nil {
					if err == io.EOF {
						break reading
					}
					log.Errorf("Failed to read from fd: %v\n", err)
					break reading
				}
				chanInputFromTerm <- data
			}

		}
	}
}

func StartIOForward(taskFinishCtx context.Context, taskFinishFunc context.CancelFunc,
	chanInputFromTerm chan string, chanOutputFromTask chan string) {

	go forwardingSigintHandlerRoutine(taskFinishFunc)
	go fileReaderRoutine(os.Stdin.Fd(), chanInputFromTerm, taskFinishCtx)
	go fileWriterRoutine(os.Stdout.Fd(), chanOutputFromTask, taskFinishCtx)

}

func MainCrun(cmd *cobra.Command, args []string) util.CraneCmdError {
	util.InitLogger(FlagDebugLevel)

	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())

	var err error
	if gVars.cwd, err = os.Getwd(); err != nil {
		log.Errorf("Failed to get working directory: %s", err.Error())
		return util.ErrorBackend
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
		Uid:             uint32(os.Getuid()),
		Gid:             uint32(os.Getgid()),
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

	task.NodeNum = FlagNodes
	task.CpusPerTask = FlagCpuPerTask
	task.NtasksPerNode = FlagNtasksPerNode

	if FlagTime != "" {
		seconds, err := util.ParseDurationStrToSeconds(FlagTime)
		if err != nil {
			log.Errorf("Invalid argument: invalid --time: %v", err)
			return util.ErrorCmdArg
		}
		task.TimeLimit.Seconds = seconds
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			log.Errorf("Invalid argument: %v", err)
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
	if FlagGetUserEnv {
		task.GetUserEnv = true
	}
	if FlagExport != "" {
		task.Env["CRANE_EXPORT_ENV"] = FlagExport
	}

	// Set total limit of cpu cores
	task.Resources.AllocatableRes.CpuCoreLimit = task.CpusPerTask * float64(task.NtasksPerNode)

	// Check the validity of the parameters
	if err := util.CheckTaskArgs(task); err != nil {
		log.Errorf("Invalid argument: %v", err)
		return util.ErrorCmdArg
	}

	util.SetPropagatedEnviron(task)

	iaMeta := task.GetInteractiveMeta()
	iaMeta.Pty = FlagPty

	if FlagX11 {
		target, port, err := util.GetX11Display()
		if err != nil {
			log.Errorf("Error in reading X11 $DISPLAY: %v", err)
			return util.ErrorGeneric
		}

		cookie, err := util.GetX11AuthCookie()
		if err != nil {
			log.Errorf("Error in reading X11 xauth cookies: %v", err)
			return util.ErrorGeneric
		}

		iaMeta.X11 = true
		iaMeta.X11Meta = &protos.InteractiveTaskAdditionalMeta_X11Meta{
			Cookie: cookie,
			Target: target,
			Port:   uint32(port),
		}

		log.Debugf("X11 forwarding enabled (%v:%d). ", target, port)
	}

	iaMeta.ShScript = strings.Join(args, " ")
	termEnv, exits := syscall.Getenv("TERM")
	if exits {
		iaMeta.TermEnv = termEnv
	}
	iaMeta.InteractiveType = protos.InteractiveTaskType_Crun

	return StartCrunStream(task)
}
