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

package calloc

import "C"
import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"
)

type GlobalVariables struct {
	user      *user.User
	cwd       string
	shellPath string

	globalCtx       context.Context
	globalCtxCancel context.CancelFunc

	connectionBroken bool
}

var gVars GlobalVariables

type StateOfCalloc int

const (
	ConnectCfored StateOfCalloc = 0
	ReqTaskId     StateOfCalloc = 1
	WaitRes       StateOfCalloc = 2
	TaskRunning   StateOfCalloc = 3
	TaskKilling   StateOfCalloc = 4
	WaitAck       StateOfCalloc = 5
)

type ReplyReceiveItem struct {
	reply *protos.StreamCforedReply
	err   error
}

func ReplyReceiveRoutine(stream protos.CraneForeD_CallocStreamClient,
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

func StartCallocStream(task *protos.TaskToCtld) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	unixSocketPath := "unix:///" + util.DefaultCforedUnixSocketPath
	conn, err := grpc.Dial(unixSocketPath, opts...)
	if err != nil {
		log.Fatalf("Failed to connect to local unix socket %s: %s",
			unixSocketPath, err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Fatalf("Failed to close grpc conn: %s", err)
		}
	}(conn)

	client := protos.NewCraneForeDClient(conn)

	var stream protos.CraneForeD_CallocStreamClient
	var replyChannel chan ReplyReceiveItem

	var request *protos.StreamCallocRequest
	var taskId uint32

	terminalExitChannel := make(chan bool, 1)
	cancelRequestChannel := make(chan bool, 1)

	state := ConnectCfored

CallocStateMachineLoop:
	for {
		switch state {
		case ConnectCfored:
			stream, err = client.CallocStream(gVars.globalCtx)
			if err != nil {
				log.Errorf("Failed to create CallocStream: %s.", err)
				break CallocStateMachineLoop
			}

			replyChannel = make(chan ReplyReceiveItem, 8)
			go ReplyReceiveRoutine(stream, replyChannel)

			request = &protos.StreamCallocRequest{
				Type: protos.StreamCallocRequest_TASK_REQUEST,
				Payload: &protos.StreamCallocRequest_PayloadTaskReq{
					PayloadTaskReq: &protos.StreamCallocRequest_TaskReq{
						Task:      task,
						CallocPid: int32(os.Getpid()),
					},
				},
			}

			if err := stream.Send(request); err != nil {
				log.Errorf("Failed to send Task Request to CallocStream: %s. "+
					"Connection to calloc is broken", err)
				gVars.connectionBroken = true
				break CallocStateMachineLoop
			}

			state = ReqTaskId

		case ReqTaskId:
			item := <-replyChannel
			cforedReply, err := item.reply, item.err

			if err != nil {
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Errorf("Connection to Cfored broken when requesting "+
						"task id: %s. Exiting...", err)
					gVars.connectionBroken = true
					break CallocStateMachineLoop
				}
			}

			if cforedReply.Type != protos.StreamCforedReply_TASK_ID_REPLY {
				log.Fatal("Expect type TASK_ID_REPLY")
			}
			payload := cforedReply.GetPayloadTaskIdReply()

			if payload.Ok {
				taskId = payload.TaskId
				fmt.Printf("Task id allocated: %d\n", taskId)

				state = WaitRes
			} else {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to allocate task id: %s\n", payload.FailureReason)
				break CallocStateMachineLoop
			}

		case WaitRes:
			item := <-replyChannel
			cforedReply, err := item.reply, item.err

			if err != nil { // Failure Edge
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Errorf("Connection to Cfored broken when waiting "+
						"resource allocated: %s. Exiting...", err)
					gVars.connectionBroken = true
					break CallocStateMachineLoop
				}
			}

			if cforedReply.Type != protos.StreamCforedReply_TASK_RES_ALLOC_REPLY {
				log.Fatal("Expect TASK_RES_ALLOC_REPLY")
			}
			cforedPayload := cforedReply.GetPayloadTaskAllocReply()
			Ok := cforedPayload.Ok

			if Ok {
				fmt.Printf("Allocated craned nodes: %s\n", cforedPayload.AllocatedCranedRegex)
				state = TaskRunning
			} else {
				fmt.Println("Failed to allocate task resource. Exiting...")
				break CallocStateMachineLoop
			}

		case TaskRunning:
			go StartTerminal(gVars.shellPath, cancelRequestChannel, terminalExitChannel)

			select {
			case <-terminalExitChannel:
				request = &protos.StreamCallocRequest{
					Type: protos.StreamCallocRequest_TASK_COMPLETION_REQUEST,
					Payload: &protos.StreamCallocRequest_PayloadTaskCompleteReq{
						PayloadTaskCompleteReq: &protos.StreamCallocRequest_TaskCompleteReq{
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
					break CallocStateMachineLoop
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
						state = TaskKilling
					}
				} else {
					if cforedReply.Type !=
						protos.StreamCforedReply_TASK_CANCEL_REQUEST {
						log.Fatal("Expect TASK_CANCEL_REQUEST")
					} else {
						log.Trace("Received TASK_CANCEL_REQUEST")
						state = TaskKilling
					}
				}
			}

		case TaskKilling:
			cancelRequestChannel <- true

			<-terminalExitChannel
			request = &protos.StreamCallocRequest{
				Type: protos.StreamCallocRequest_TASK_COMPLETION_REQUEST,
				Payload: &protos.StreamCallocRequest_PayloadTaskCompleteReq{
					PayloadTaskCompleteReq: &protos.StreamCallocRequest_TaskCompleteReq{
						TaskId: taskId,
						Status: protos.TaskStatus_Cancelled,
					},
				},
			}

			if gVars.connectionBroken {
				break CallocStateMachineLoop
			}

			log.Debug("Sending TASK_COMPLETION_REQUEST with CANCELLED state...")
			if err := stream.Send(request); err != nil {
				log.Errorf("The connection to Cfored was broken: %s. "+
					"Exiting...", err)
				gVars.connectionBroken = true
				break CallocStateMachineLoop
			} else {
				state = WaitAck
			}

		case WaitAck:
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
					break CallocStateMachineLoop
				}
			}

			if cforedReply.Type != protos.StreamCforedReply_TASK_COMPLETION_ACK_REPLY {
				log.Fatal("Expect TASK_COMPLETION_ACK_REPLY. Received: %s", cforedReply.Type.String())
			}

			if cforedReply.GetPayloadTaskCompletionAckReply().Ok {
				println("Task completed.")
			} else {
				log.Fatal("Failed to notify server of task completion")
			}

			break CallocStateMachineLoop
		}
	}
}

func main(cmd *cobra.Command, args []string) {
	var err error

	switch FlagDebugLevel {
	case "trace":
		util.InitLogger(log.TraceLevel)
	case "debug":
		util.InitLogger(log.DebugLevel)
	case "info":
		fallthrough
	default:
		util.InitLogger(log.InfoLevel)
	}

	log.Tracef("Positional args: %v\n", args)

	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())

	if gVars.cwd, err = os.Getwd(); err != nil {
		log.Fatalf("Failed to get working directory: %s", err.Error())
	}

	if gVars.user, err = user.Current(); err != nil {
		log.Fatalf("Failed to get current user: %s", err.Error())
	}

	uid, err := strconv.Atoi(gVars.user.Uid)
	if err != nil {
		log.Fatalf("Failed to convert uid to int: %s", err.Error())
	}

	if gVars.shellPath, err = util.NixShell(gVars.user.Uid); err != nil {
		log.Fatalf("Failed to get default shell of user %s: %s",
			gVars.user.Name, err.Error())
	}

	task := &protos.TaskToCtld{
		Name:          "Interactive",
		TimeLimit:     util.InvalidDuration(),
		PartitionName: "CPU",
		Resources: &protos.Resources{
			AllocatableResource: &protos.AllocatableResource{
				CpuCoreLimit:       1,
				MemoryLimitBytes:   1024 * 1024 * 128,
				MemorySwLimitBytes: 1024 * 1024 * 128,
			},
		},
		Type:            protos.TaskType_Interactive,
		Uid:             uint32(uid),
		NodeNum:         1,
		NtasksPerNode:   1,
		CpusPerTask:     1,
		RequeueIfFailed: false,
		Payload:         &protos.TaskToCtld_InteractiveMeta{InteractiveMeta: nil},
		CmdLine:         strings.Join(os.Args, " "),
		Cwd:             gVars.cwd,
		Env:             "",
	}

	if FlagNodes != 0 {
		task.NodeNum = FlagNodes
	}
	if FlagCpuPerTask != 0 {
		task.CpusPerTask = FlagCpuPerTask
	}
	if FlagNtasksPerNode != 0 {
		task.NtasksPerNode = FlagNtasksPerNode
	}
	if FlagGres != "" {
		gresMap := util.ParseGres(FlagGres)
		for gresName, gresType := range gresMap {
			deviceCountMap := &protos.DeviceCountMap{DeviceCountMap: gresType}
			task.GresCountMap[gresName] = deviceCountMap
		}
	}
	if FlagTime != "" {
		ok := util.ParseDuration(FlagTime, task.TimeLimit)
		if !ok {
			log.Fatalf("Invalid --time format.")
		}
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			log.Fatal(err)
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

	if task.CpusPerTask <= 0 || task.NtasksPerNode == 0 || task.NodeNum == 0 {
		log.Fatal("Invalid --cpus-per-task, --ntasks-per-node or --node-num")
	}

	StartCallocStream(task)
}
