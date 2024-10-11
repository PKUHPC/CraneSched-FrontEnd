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
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func StartCallocStream(task *protos.TaskToCtld) util.CraneCmdError {
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
				log.Errorln("Expect type TASK_ID_REPLY")
				return util.ErrorBackend
			}
			payload := cforedReply.GetPayloadTaskIdReply()

			if payload.Ok {
				taskId = payload.TaskId
				fmt.Printf("Task id allocated: %d\n", taskId)

				state = WaitRes
			} else {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to allocate task id: %s.\n", payload.FailureReason)
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

			switch cforedReply.Type {
			case protos.StreamCforedReply_TASK_RES_ALLOC_REPLY:
				cforedPayload := cforedReply.GetPayloadTaskAllocReply()
				Ok := cforedPayload.Ok

				if Ok {
					fmt.Printf("Allocated craned nodes: %s.\n", cforedPayload.AllocatedCranedRegex)
					state = TaskRunning
				} else {
					fmt.Println("Failed to allocate task resource. Exiting...")
					break CallocStateMachineLoop
				}

			case protos.StreamCforedReply_TASK_CANCEL_REQUEST:
				log.Tracef("Receive cancel request when wait res")
				state = TaskKilling
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
					switch cforedReply.Type {
					case protos.StreamCforedReply_TASK_CANCEL_REQUEST:
						state = TaskKilling

					case protos.StreamCforedReply_TASK_COMPLETION_ACK_REPLY:
						fmt.Println("Task failed ")
					}
				}

				cancelRequestChannel <- true
				<-terminalExitChannel
				if cforedReply.Type == protos.StreamCforedReply_TASK_COMPLETION_ACK_REPLY {
					break CallocStateMachineLoop
				}
			}

		case TaskKilling:

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
				log.Errorf("Expect TASK_COMPLETION_ACK_REPLY. Received: %s", cforedReply.Type.String())
				return util.ErrorBackend
			}

			if cforedReply.GetPayloadTaskCompletionAckReply().Ok {
				println("Task completed.")
			} else {
				log.Errorln("Failed to notify server of task completion")
				return util.ErrorBackend
			}

			break CallocStateMachineLoop
		}
	}
	// Check if connection finished normally
	if state != WaitAck || gVars.connectionBroken {
		return util.ErrorNetwork
	} else {
		return util.ErrorSuccess
	}
}

func MainCalloc(cmd *cobra.Command, args []string) util.CraneCmdError {
	util.InitLogger(FlagDebugLevel)

	var err error
	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())

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

	if gVars.shellPath, err = util.NixShell(gVars.user.Uid); err != nil {
		log.Errorf("Failed to get default shell of user %s: %s",
			gVars.user.Name, err.Error())
		return util.ErrorBackend
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
		Payload:         &protos.TaskToCtld_InteractiveMeta{InteractiveMeta: nil},
		CmdLine:         strings.Join(os.Args, " "),
		Cwd:             gVars.cwd,

		// Todo: Propagate Env by --export here!
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
	if FlagGres != "" {
		gresMap := util.ParseGres(FlagGres)
		task.Resources.DeviceMap = gresMap
	}
	if FlagTime != "" {
		ok := util.ParseDuration(FlagTime, task.TimeLimit)
		if !ok {
			log.Errorln("Invalid --time format.")
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
	util.SetPropagatedEnviron(task)
	task.Resources.AllocatableRes.CpuCoreLimit = task.CpusPerTask * float64(task.NtasksPerNode)
	if task.Resources.AllocatableRes.CpuCoreLimit > 1e6 {
		log.Errorf("Request too many CPUs: %v", task.Resources.AllocatableRes.CpuCoreLimit)
		return util.ErrorCmdArg
	}

	return StartCallocStream(task)
}
