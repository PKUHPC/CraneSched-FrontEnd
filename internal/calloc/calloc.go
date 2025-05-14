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

package calloc

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
	reply *protos.StreamCallocReply
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

func StartCallocStream(task *protos.TaskToCtld) error {
	config := util.ParseConfig(FlagConfigFilePath)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithKeepaliveParams(util.ClientKeepAliveParams))
	opts = append(opts, grpc.WithConnectParams(util.ClientConnectParams))

	unixSocketPath := "unix:///" + config.CranedCforedSockPath
	conn, err := grpc.NewClient(unixSocketPath, opts...)
	if err != nil {
		return util.NewCraneErr(util.ErrorBackend,
			fmt.Sprintf("Failed to connect to local unix socket %s: %s.", unixSocketPath, err))
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Errorf("Failed to close grpc conn: %s.", err)
			os.Exit(util.ErrorNetwork)
		}
	}(conn)

	client := protos.NewCraneForeDClient(conn)

	var stream protos.CraneForeD_CallocStreamClient
	var replyChannel chan ReplyReceiveItem

	var request *protos.StreamCallocRequest
	var jobId uint32
	var stepId uint32

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

			if cforedReply.Type != protos.StreamCallocReply_TASK_ID_REPLY {
				return util.NewCraneErr(util.ErrorBackend, "Expect type TASK_ID_REPLY.")
			}
			payload := cforedReply.GetPayloadTaskIdReply()

			if payload.Ok {
				jobId = payload.JobId
				stepId = payload.StepId
				fmt.Printf("Task id allocated: %d\n", jobId)

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
			case protos.StreamCallocReply_TASK_RES_ALLOC_REPLY:
				cforedPayload := cforedReply.GetPayloadTaskAllocReply()
				Ok := cforedPayload.Ok

				if Ok {
					fmt.Printf("Allocated craned nodes: %s.\n", cforedPayload.AllocatedCranedRegex)
					state = TaskRunning
				} else {
					fmt.Println("Failed to allocate job resource. Exiting...")
					break CallocStateMachineLoop
				}

			case protos.StreamCallocReply_TASK_CANCEL_REQUEST:
				log.Tracef("Receive cancel request when wait res")
				state = TaskKilling
			}

		case TaskRunning:
			go StartTerminal(gVars.shellPath, jobId, cancelRequestChannel, terminalExitChannel)

			select {
			case <-terminalExitChannel:
				request = &protos.StreamCallocRequest{
					Type: protos.StreamCallocRequest_TASK_COMPLETION_REQUEST,
					Payload: &protos.StreamCallocRequest_PayloadTaskCompleteReq{
						PayloadTaskCompleteReq: &protos.StreamCallocRequest_TaskCompleteReq{
							JobId:  jobId,
							StepId: stepId,
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
					case protos.StreamCallocReply_TASK_CANCEL_REQUEST:
						state = TaskKilling

					case protos.StreamCallocReply_TASK_COMPLETION_ACK_REPLY:
						fmt.Println("Job failed ")
					}
				}

				cancelRequestChannel <- true
				<-terminalExitChannel
				if cforedReply.Type == protos.StreamCallocReply_TASK_COMPLETION_ACK_REPLY {
					break CallocStateMachineLoop
				}
			}

		case TaskKilling:

			request = &protos.StreamCallocRequest{
				Type: protos.StreamCallocRequest_TASK_COMPLETION_REQUEST,
				Payload: &protos.StreamCallocRequest_PayloadTaskCompleteReq{
					PayloadTaskCompleteReq: &protos.StreamCallocRequest_TaskCompleteReq{
						JobId:  jobId,
						StepId: stepId,
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

			if cforedReply.Type != protos.StreamCallocReply_TASK_COMPLETION_ACK_REPLY {
				return util.NewCraneErr(util.ErrorBackend,
					fmt.Sprintf("Expect type TASK_COMPLETION_ACK_REPLY. Received: %s", cforedReply.Type.String()))
			}

			if cforedReply.GetPayloadTaskCompletionAckReply().Ok {
				println("Task completed.")
			} else {
				return util.NewCraneErr(util.ErrorBackend, "Failed to notify server of task completion")
			}

			break CallocStateMachineLoop
		}
	}
	// Check if connection finished normally
	if state != WaitAck || gVars.connectionBroken {
		return &util.CraneError{Code: util.ErrorNetwork}
	} else {
		return nil
	}
}

func MainCalloc(cmd *cobra.Command, args []string) error {
	util.InitLogger(FlagDebugLevel)

	var err error
	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())

	if gVars.cwd, err = os.Getwd(); err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "Failed to get working directory: %s", err)
	}

	if gVars.user, err = user.Current(); err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "Failed to get current user: %s", err)
	}

	// Get egid using os.Getgid() instead of using user.Current()
	gid := os.Getgid()

	uid, err := strconv.Atoi(gVars.user.Uid)
	if err != nil {
		return util.NewCraneErr(util.ErrorInvalidFormat, fmt.Sprintf("Failed to convert uid to int: %s", err))
	}

	if gVars.shellPath, err = util.NixShell(gVars.user.Uid); err != nil {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to get default shell of user %s: %s",
			gVars.user.Name, err))
	}

	task := &protos.TaskToCtld{
		Name:          "Interactive",
		TimeLimit:     util.InvalidDuration(),
		PartitionName: "",
		ReqResources: &protos.ResourceView{
			AllocatableRes: &protos.AllocatableResource{
				CpuCoreLimit:       1,
				MemoryLimitBytes:   0,
				MemorySwLimitBytes: 0,
			},
		},
		Type:            protos.TaskType_Interactive,
		Uid:             uint32(uid),
		Gid:             uint32(gid),
		NodeNum:         1,
		NtasksPerNode:   1,
		CpusPerTask:     1,
		RequeueIfFailed: false,
		Payload:         &protos.TaskToCtld_InteractiveMeta{InteractiveMeta: nil},
		CmdLine:         strings.Join(os.Args, " "),
		Cwd:             gVars.cwd,

		Env: make(map[string]string),
	}

	structExtraFromCli := &util.JobExtraAttrs{}

	task.NodeNum = FlagNodes
	task.CpusPerTask = FlagCpuPerTask
	task.NtasksPerNode = FlagNtasksPerNode

	setGresGpusFlag := false
	if FlagGres != "" {
		gresMap := util.ParseGres(FlagGres)
		task.ReqResources.DeviceMap = gresMap
		if _, exist := task.ReqResources.DeviceMap.NameTypeMap[util.GresGpuName]; exist {
			setGresGpusFlag = true
		}
	}
	if FlagTime != "" {
		seconds, err := util.ParseDurationStrToSeconds(FlagTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: invalid --time: %s", err))
		}
		task.TimeLimit.Seconds = seconds
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s", err))
		}
		task.ReqResources.AllocatableRes.MemoryLimitBytes = memInByte
		task.ReqResources.AllocatableRes.MemorySwLimitBytes = memInByte
	}
	if FlagMemPerCpu != "" {
		memInBytePerCpu, err := util.ParseMemStringAsByte(FlagMemPerCpu)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg,
				fmt.Sprintf("Invalid argument: invalid --mem-per-cpu value '%s': %v", FlagMemPerCpu, err))
		}
		task.MemPerCpu = &memInBytePerCpu
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
	if FlagReservation != "" {
		task.Reservation = FlagReservation
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
	if FlagExclusive {
		task.Exclusive = true
	}
	if FlagHold {
		task.Hold = true
	}
	if cmd.Flags().Changed("wckey") {
		task.Wckey = &FlagWckey
	}
	if FlagGpusPerNode != "" {
		if setGresGpusFlag {
			return util.NewCraneErr(util.ErrorCmdArg, "Cannot specify both --gres gpus and --gpus-per-node flags simultaneously")
		}
		gpuDeviceMap, err := util.ParseGpusPerNodeStr(FlagGpusPerNode)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid argument: invalid --gpus-per-node: %s", err),
			}

		}
		task.ReqResources.DeviceMap = gpuDeviceMap
	}

	if FlagLicenses != "" {
		licCount, isLicenseOr, err := util.ParseLicensesString(FlagLicenses)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid argument: %s", err)
		}
		task.LicensesCount = licCount
		task.IsLicensesOr = isLicenseOr
	}

	// Marshal extra attributes
	if err := structExtraFromCli.Marshal(&task.ExtraAttr); err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s", err))
	}

	// Set total limit of cpu cores
	task.ReqResources.AllocatableRes.CpuCoreLimit = task.CpusPerTask * float64(task.NtasksPerNode)

	// Check the validity of the parameters
	if err := util.CheckTaskArgs(task); err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s", err))
	}
	util.SetPropagatedEnviron(&task.Env, &task.GetUserEnv)

	return StartCallocStream(task)
}
