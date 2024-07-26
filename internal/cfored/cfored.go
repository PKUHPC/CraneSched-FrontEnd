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

package cfored

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
)

var DefaultProcId = 0

type GlobalVariables struct {
	hostName string

	ctldConnected atomic.Bool

	globalCtx       context.Context
	globalCtxCancel context.CancelFunc

	// Prevent the situation that de-multiplexer the two maps when
	// Cfored <--> Ctld state machine just removes the channel of a calloc
	// from ctldReplyChannelMapByProcId and hasn't added the channel
	// to ctldReplyChannelMapByTaskId. In such a situation, Cfored <--> Ctld
	// state machine may think the channel of a calloc is not in both maps
	// but the channel is actually being moving from one map to another map.
	ctldReplyChannelMapMtx sync.Mutex

	// Used by Cfored <--> Ctld state machine to de-multiplex messages
	// Used for calloc/crun with task id not allocated.
	// A calloc is identified by its pid.
	ctldReplyChannelMapByPid map[int32]chan *protos.StreamCtldReply

	// Used by Cfored <--> Ctld state machine to de-multiplex messages from CraneCtld.
	// Cfored <--> Ctld state machine GUARANTEES that NO `nil` will be sent into these channels.
	// Used for calloc/crun with task id allocated.
	ctldReplyChannelMapByTaskIdProcId map[uint32] /*TaskId*/ map[uint32] /*ProcId*/ chan *protos.StreamCtldReply

	// Used by Calloc/Crun <--> Cfored state machine to multiplex messages
	// these messages will be sent to CraneCtld
	cforedRequestCtldChannel chan *protos.StreamCforedRequest

	pidTaskIdMapMtx sync.RWMutex

	pidTaskIdMap map[int32]uint32
}

var gVars GlobalVariables

type StateOfCallocServer int
type StateOfCrunServer int
type StateOfCranedServer int

const (
	WaitTaskIdAllocReq     StateOfCallocServer = 0
	WaitCtldAllocTaskId    StateOfCallocServer = 1
	WaitCtldAllocRes       StateOfCallocServer = 2
	WaitCallocComplete     StateOfCallocServer = 3
	WaitCallocCancel       StateOfCallocServer = 4
	WaitCtldAck            StateOfCallocServer = 5
	CancelTaskOfDeadCalloc StateOfCallocServer = 6
)

const (
	CrunWaitProcIdAllocReq  StateOfCrunServer = 0
	CrunWaitCtldAllocProcId StateOfCrunServer = 1
	CrunWaitCtldAllocRes    StateOfCrunServer = 2
	CrunWaitIOForward       StateOfCrunServer = 3
	CrunWaitTaskComplete    StateOfCrunServer = 4
	CrunWaitTaskCancel      StateOfCrunServer = 5
	CrunWaitForwardEnd      StateOfCrunServer = 6
	CrunForwardEnd          StateOfCrunServer = 7
	CancelTaskOfDeadCrun    StateOfCrunServer = 8
)

func (cforedServer *GrpcCforedServer) CrunStream(toCrunStream protos.CraneForeD_CrunStreamServer) error {
	var crunPid int32
	var taskId uint32
	var procId uint32
	nestedTask := false
	var reply *protos.StreamCforedCrunReply

	var execCranedIds []string
	cranedNum := atomic.Uint32{}
	cranedNum.Store(0)
	crunRequestChannel := make(chan grpcMessage[protos.StreamCrunRequest], 8)
	go grpcStreamReceiver[protos.StreamCrunRequest](toCrunStream, crunRequestChannel)

	ctldReplyChannel := make(chan *protos.StreamCtldReply, 2)
	TaskIoRequestChannel := make(chan *protos.StreamCforedTaskIORequest, 2)
	taskId = math.MaxUint32
	crunPid = -1

	state := CrunWaitProcIdAllocReq

CforedCrunStateMachineLoop:
	for {
		switch state {
		case CrunWaitProcIdAllocReq:
			log.Debug("[Cfored<->Crun] Enter State WAIT_Proc_ID_ALLOC_REQ")

			item := <-crunRequestChannel
			crunRequest, err := item.message, item.err
			if err != nil { // Failure Edge
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Fatal(err)
					return nil
				}
			}

			log.Debug("[Cfored<->Crun] Receive ProcIdAllocReq")

			if crunRequest.Type != protos.StreamCrunRequest_TASK_REQUEST {
				log.Fatal("[Cfored<->Crun] Expect TASK_REQUEST")
			}

			if !gVars.ctldConnected.Load() {
				reply = &protos.StreamCforedCrunReply{
					Type: protos.StreamCforedCrunReply_TASK_ID_REPLY,
					Payload: &protos.StreamCforedCrunReply_PayloadTaskIdReply{
						PayloadTaskIdReply: &protos.StreamCforedCrunReply_TaskIdReply{
							Ok:            false,
							FailureReason: "Cfored is not connected to CraneCtld.",
						},
					},
				}

				if err := toCrunStream.Send(reply); err != nil {
					// It doesn't matter even if the connection is broken here.
					// Just print a log.
					log.Error(err)
				}

				// No need to cleaning any data
				break CforedCrunStateMachineLoop
			} else {
				crunPid = crunRequest.GetPayloadTaskReq().CrunPid

				gVars.ctldReplyChannelMapMtx.Lock()
				gVars.ctldReplyChannelMapByPid[crunPid] = ctldReplyChannel
				gVars.ctldReplyChannelMapMtx.Unlock()

				task := crunRequest.GetPayloadTaskReq().Task
				task.GetInteractiveMeta().CforedName = gVars.hostName
				var payload *protos.StreamCforedRequest_TaskReq
				if crunRequest.GetPayloadTaskReq().TaskId != nil {
					nestedTask = true
					taskId = crunRequest.GetPayloadTaskReq().GetTaskId()
					payload = &protos.StreamCforedRequest_TaskReq{
						CforedName: gVars.hostName,
						Pid:        crunPid,
						Task:       task,
						TaskId:     proto.Uint32(taskId),
					}
				} else {
					payload = &protos.StreamCforedRequest_TaskReq{
						CforedName: gVars.hostName,
						Pid:        crunPid,
						Task:       task,
					}
				}
				cforedRequest := &protos.StreamCforedRequest{
					Type: protos.StreamCforedRequest_TASK_REQUEST,
					Payload: &protos.StreamCforedRequest_PayloadTaskReq{
						PayloadTaskReq: payload,
					},
				}

				gVars.cforedRequestCtldChannel <- cforedRequest

				state = CrunWaitCtldAllocProcId
			}

		case CrunWaitCtldAllocProcId:
			log.Debug("[Cfored<->Crun] Enter State WAIT_CTLD_ALLOC_Proc_ID")

			select {
			case item := <-crunRequestChannel:
				crunRequest, err := item.message, item.err
				if crunRequest != nil || err == nil {
					log.Fatal("[Cfored<->Crun] Expect only nil (crun connection broken) here!")
				}
				log.Debug("[Cfored<->Crun] Connection to crun was broken.")

				//todo: there is no task_id for this state
				state = CancelTaskOfDeadCrun

			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_ID_REPLY {
					log.Fatal("[Cfored<->Crun] Expect type TASK_ID_REPLY")
				}

				Ok := ctldReply.GetPayloadTaskIdReply().Ok
				taskId = ctldReply.GetPayloadTaskIdReply().TaskId
				procId = ctldReply.GetPayloadTaskIdReply().ProcId
				log.Debugf("[Cfored<->Crun] taskId %d procId %d", taskId, procId)
				if nestedTask {
					execCranedIds = ctldReply.GetPayloadTaskIdReply().Nodes.GetCranedIds()
					log.Debugf("[Cfored<->Crun] execCranedId %v", execCranedIds)
				}
				reply = &protos.StreamCforedCrunReply{
					Type: protos.StreamCforedCrunReply_TASK_ID_REPLY,
					Payload: &protos.StreamCforedCrunReply_PayloadTaskIdReply{
						PayloadTaskIdReply: &protos.StreamCforedCrunReply_TaskIdReply{
							Ok:            Ok,
							TaskId:        taskId,
							ProcId:        procId,
							FailureReason: ctldReply.GetPayloadTaskIdReply().FailureReason,
						},
					},
				}

				gVars.ctldReplyChannelMapMtx.Lock()
				delete(gVars.ctldReplyChannelMapByPid, crunPid)
				if Ok {
					if _, exits := gVars.ctldReplyChannelMapByTaskIdProcId[taskId]; !exits {
						gVars.ctldReplyChannelMapByTaskIdProcId[taskId] = make(map[uint32]chan *protos.StreamCtldReply)
					}
					gVars.ctldReplyChannelMapByTaskIdProcId[taskId][procId] = ctldReplyChannel
					gVars.pidTaskIdMapMtx.Lock()
					gVars.pidTaskIdMap[crunPid] = taskId
					gVars.pidTaskIdMapMtx.Unlock()

					gCranedChanKeeper.setRemoteIoToCrunChannel(taskId, procId, TaskIoRequestChannel)
				}

				gVars.ctldReplyChannelMapMtx.Unlock()
				if err := toCrunStream.Send(reply); err != nil {
					log.Debug("[Cfored<->Crun] Connection to crun was broken.")
					state = CancelTaskOfDeadCrun
				} else {
					if Ok {
						if nestedTask {
							state = CrunWaitIOForward
						} else {
							state = CrunWaitCtldAllocRes
						}
					} else {
						// channel was already removed from gVars.ctldReplyChannelMapByPid
						break CforedCrunStateMachineLoop
					}
				}
			}

		case CrunWaitCtldAllocRes:
			log.Debug("[Cfored<->Crun] Enter State WAIT_CTLD_ALLOC_RES")

			select {
			case item := <-crunRequestChannel:
				crunRequest, err := item.message, item.err
				if crunRequest != nil || err == nil {
					if crunRequest.Type != protos.StreamCrunRequest_TASK_COMPLETION_REQUEST {
						log.Fatal("[Cfored<--Crun] Expect only nil (crun connection broken) here!")
					}
					log.Debug("[Cfored<->Crun] Crun exited too early with TASK_COMPLETION_REQUEST. Cancelling it...")
					toCtldRequest := &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
							PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
								CforedName:      gVars.hostName,
								TaskId:          taskId,
								InteractiveType: protos.InteractiveTaskType_Crun,
							},
						},
					}
					gVars.cforedRequestCtldChannel <- toCtldRequest
					state = CrunWaitForwardEnd
				} else {
					log.Debug("[Cfored<->Crun] Connection to crun was broken.")
				}

			case ctldReply := <-ctldReplyChannel:
				switch ctldReply.Type {
				case protos.StreamCtldReply_TASK_RES_ALLOC_REPLY:
					ctldPayload := ctldReply.GetPayloadTaskResAllocReply()
					reply = &protos.StreamCforedCrunReply{
						Type: protos.StreamCforedCrunReply_TASK_RES_ALLOC_REPLY,
						Payload: &protos.StreamCforedCrunReply_PayloadTaskAllocReply{
							PayloadTaskAllocReply: &protos.StreamCforedCrunReply_TaskResAllocatedReply{
								Ok:                   ctldPayload.Ok,
								AllocatedCranedRegex: ctldPayload.AllocatedCranedRegex,
							},
						},
					}

					// TODO: Difference
					execCranedIds = ctldPayload.GetCranedIds()

					if err := toCrunStream.Send(reply); err != nil {
						log.Debug("[Cfored<->Crun] Connection to crun was broken.")
						state = CancelTaskOfDeadCrun
					} else {
						state = CrunWaitIOForward
					}

				case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
					state = CrunWaitTaskCancel

				default:
					log.Fatal("[Cfored<->Crun] Expect type " +
						"TASK_RES_ALLOC_REPLY or TASK_CANCEL_REQUEST")
				}
			}

		case CrunWaitIOForward:
			log.Debug("[Cfored<->Crun] Enter State WAIT_TASK_IO_FORWARD")

			cranedNum.Store(uint32(len(execCranedIds)))
			stopWaiting := atomic.Bool{}
			stopWaiting.Store(false)
			readyChannel := make(chan bool, 1)
			go gCranedChanKeeper.waitCranedChannelsReady(execCranedIds, readyChannel, &stopWaiting, taskId, procId)

			select {
			case ctldReply := <-ctldReplyChannel:
				switch ctldReply.Type {
				case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
					state = CrunWaitTaskCancel
				case protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY:
					log.Warningf("[Cfored<->Crun] task %d completed and failed to wait craned ready", ctldReply.GetPayloadTaskCompletionAck().TaskId)
					reply = &protos.StreamCforedCrunReply{
						Type: protos.StreamCforedCrunReply_TASK_IO_FORWARD_READY,
						Payload: &protos.StreamCforedCrunReply_PayloadTaskIoForwardReadyReply{
							PayloadTaskIoForwardReadyReply: &protos.StreamCforedCrunReply_TaskIOForwardReadyReply{
								Ok: false,
							},
						},
					}
					state = CrunForwardEnd
					break
				}
				stopWaiting.Store(true)

			case item := <-crunRequestChannel:
				crunRequest, err := item.message, item.err
				if err != nil {
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Debug("[Cfored<->Crun] Connection to crun was broken.")
						stopWaiting.Store(true)
						state = CancelTaskOfDeadCrun
					}
				} else {
					if crunRequest.Type != protos.StreamCrunRequest_TASK_COMPLETION_REQUEST {
						log.Fatal("[Cfored<->Crun] Expect TASK_COMPLETION_REQUEST")
					}

					log.Debug("[Cfored<->Crun] Receive TaskCompletionRequest")
					toCtldRequest := &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
							PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
								CforedName:      gVars.hostName,
								TaskId:          taskId,
								InteractiveType: protos.InteractiveTaskType_Crun,
							},
						},
					}
					gVars.cforedRequestCtldChannel <- toCtldRequest
					stopWaiting.Store(true)
					state = CrunForwardEnd
				}

			case <-readyChannel:
				reply = &protos.StreamCforedCrunReply{
					Type: protos.StreamCforedCrunReply_TASK_IO_FORWARD_READY,
					Payload: &protos.StreamCforedCrunReply_PayloadTaskIoForwardReadyReply{
						PayloadTaskIoForwardReadyReply: &protos.StreamCforedCrunReply_TaskIOForwardReadyReply{
							Ok: true,
						},
					},
				}

				if err := toCrunStream.Send(reply); err != nil {
					log.Debugf("[Cfored<->Crun] Failed to send CancelRequest to crun: %s. "+
						"The connection to crun was broken.", err.Error())
					state = CancelTaskOfDeadCrun
				} else {
					state = CrunWaitTaskComplete
				}
			}

		case CrunWaitTaskComplete:
			log.Debug("[Cfored<->Crun] Enter State Crun_Wait_Task_Complete")
		forwarding:
			for {
				select {
				case ctldReply := <-ctldReplyChannel:
					switch ctldReply.Type {
					case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
						{
							state = CrunWaitTaskCancel
							break forwarding
						}
					}

				case item := <-crunRequestChannel:
					crunRequest, err := item.message, item.err
					if err != nil {
						switch err {
						case io.EOF:
							fallthrough
						default:
							log.Debug("[Cfored<->Crun] Connection to crun was broken.")
							state = CancelTaskOfDeadCrun
							break forwarding
						}
					} else {
						switch crunRequest.Type {
						case protos.StreamCrunRequest_TASK_IO_FORWARD:
							log.Debug("[Crun->Cfored->Craned] Receive TASK_IO_FORWARD Request")
							gCranedChanKeeper.forwardCrunRequestToCranedChannels(crunRequest, execCranedIds)

						case protos.StreamCrunRequest_TASK_COMPLETION_REQUEST:
							log.Debug("[Cfored<->Crun] Receive TaskCompletionRequest")
							toCtldRequest := &protos.StreamCforedRequest{
								Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
								Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
									PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
										CforedName:      gVars.hostName,
										TaskId:          taskId,
										InteractiveType: protos.InteractiveTaskType_Crun,
									},
								},
							}
							gVars.cforedRequestCtldChannel <- toCtldRequest
						default:
							log.Fatalf("[Cfored<->Crun] Expect TASK_COMPLETION_REQUEST or TASK_IO_FORWARD but get %s", crunRequest.Type.String())
							break forwarding
						}
					}

				case taskMsg := <-TaskIoRequestChannel:
					if taskMsg == nil {
						log.Errorf("[Cfored<->Crun] Task %d Proc %d Craned down,cancel task", taskId, procId)
						//connection err
						cranedNum.Store(cranedNum.Load() - 1)
						state = CrunWaitTaskCancel
						break forwarding
					}
					if taskMsg.Type == protos.StreamCforedTaskIORequest_CRANED_PROC_OUTPUT {
						if taskMsg.GetPayloadProcOutputReq().End {
							num := cranedNum.Load() - 1
							if num == 0 {
								state = CrunForwardEnd
								break forwarding
							} else {
								cranedNum.Store(num)
								continue
							}
						}
						reply = &protos.StreamCforedCrunReply{
							Type: protos.StreamCforedCrunReply_TASK_IO_FORWARD,
							Payload: &protos.StreamCforedCrunReply_PayloadTaskIoForwardReply{
								PayloadTaskIoForwardReply: &protos.StreamCforedCrunReply_TaskIOForwardReply{
									Msg: taskMsg.GetPayloadProcOutputReq().Msg,
								},
							},
						}
						log.Tracef("[Cfored<->Crun] fowarding msg %s to crun for taskid %d Proc %d", taskMsg.GetPayloadProcOutputReq().Msg, taskId, procId)

						if err := toCrunStream.Send(reply); err != nil {
							log.Debugf("[Cfored<->Crun] Failed to send Request to crun: %s. "+
								"The connection to crun was broken.", err.Error())
							state = CancelTaskOfDeadCrun
							break forwarding
						}
					} else {
						log.Fatal("[Cfored<->Crun] Expect Type CRANED_TASK_OUTPUT")
						break forwarding
					}
				}
			}

		case CrunWaitTaskCancel:
			log.Debug("[Cfored<->Crun] Enter State WAIT_CRUN_CANCEL. Sending TASK_CANCEL_REQUEST...")

			reply = &protos.StreamCforedCrunReply{
				Type: protos.StreamCforedCrunReply_TASK_CANCEL_REQUEST,
				Payload: &protos.StreamCforedCrunReply_PayloadTaskCancelRequest{
					PayloadTaskCancelRequest: &protos.StreamCforedCrunReply_TaskCancelRequest{
						TaskId: taskId,
					},
				},
			}

			if err := toCrunStream.Send(reply); err != nil {
				log.Debugf("[Cfored<->Crun] Failed to send CancelRequest to crun: %s. "+
					"The connection to crun was broken.", err.Error())
				state = CancelTaskOfDeadCrun
			} else {
				item := <-crunRequestChannel
				crunRequest, err := item.message, item.err
				if err != nil { // Failure Edge
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Debug("[Cfored<->Crun] Connection to crun was broken.")
						state = CancelTaskOfDeadCrun
					}
				} else {
					for {
						if crunRequest.Type != protos.StreamCrunRequest_TASK_COMPLETION_REQUEST {
							log.Warningf("[Cfored<->Crun] Expect TASK_COMPLETION_REQUEST but %s received. Ignoring it...", crunRequest.Type)
						} else {
							log.Tracef("[Cfored<->Crun] TASK_COMPLETION_REQUEST of task #%d received",
								taskId)
							break
						}
					}

					log.Debug("[Cfored<->Crun] Receive TaskCompletionRequest")

					toCtldRequest := &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
							PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
								CforedName:      gVars.hostName,
								TaskId:          taskId,
								InteractiveType: protos.InteractiveTaskType_Crun,
							},
						},
					}
					gVars.cforedRequestCtldChannel <- toCtldRequest

					state = CrunWaitForwardEnd
				}
			}

		case CrunWaitForwardEnd:
			log.Debug("[Cfored<->Crun] Enter State Crun_Wait_Forward_End")
			if cranedNum.Load() == 0 {
				state = CrunForwardEnd
				break
			}
		WaitForwardEnd:
			for {
				select {
				case taskMsg := <-TaskIoRequestChannel:
					if taskMsg == nil {
						num := cranedNum.Load() - 1
						if num == 0 {
							state = CrunForwardEnd
							break WaitForwardEnd
						} else {
							cranedNum.Store(num)
							continue
						}
					}
					if taskMsg.Type == protos.StreamCforedTaskIORequest_CRANED_PROC_OUTPUT {
						if taskMsg.GetPayloadProcOutputReq().End {
							num := cranedNum.Load() - 1
							if num == 0 {
								state = CrunForwardEnd
								break WaitForwardEnd
							} else {
								cranedNum.Store(num)
								continue
							}
						}

						reply = &protos.StreamCforedCrunReply{
							Type: protos.StreamCforedCrunReply_TASK_IO_FORWARD,
							Payload: &protos.StreamCforedCrunReply_PayloadTaskIoForwardReply{
								PayloadTaskIoForwardReply: &protos.StreamCforedCrunReply_TaskIOForwardReply{
									Msg: taskMsg.GetPayloadProcOutputReq().Msg,
								},
							},
						}
						log.Tracef("[Cfored<->Crun] fowarding msg %s to crun for taskid %d Proc %d", taskMsg.GetPayloadProcOutputReq().Msg, taskId, procId)

						if err := toCrunStream.Send(reply); err != nil {
							log.Debugf("[Cfored<->Crun] Failed to send Request to crun: %s. "+
								"The connection to crun was broken.", err.Error())
							state = CancelTaskOfDeadCrun
							break WaitForwardEnd
						}
					} else {
						log.Fatal("[Cfored<->Crun] Expect Type CRANED_TASK_OUTPUTT")
						break WaitForwardEnd
					}
				}
			}

		case CrunForwardEnd:
			log.Debug("[Cfored<->Crun] Enter State ForwardEnd")

			reply = &protos.StreamCforedCrunReply{
				Type: protos.StreamCforedCrunReply_Proc_FORWARD_END,
				Payload: &protos.StreamCforedCrunReply_PayloadProcForwardEndReply{
					PayloadProcForwardEndReply: &protos.StreamCforedCrunReply_ProcForwardEndReply{
						Ok: true,
					},
				},
			}
			if err := toCrunStream.Send(reply); err != nil {
				log.Debugf("[Cfored<->Crun] Failed to send Proc_FORWARD_END to crun: %s. "+
					"The connection to crun was broken.", err.Error())
			}
			gVars.ctldReplyChannelMapMtx.Lock()
			delete(gVars.ctldReplyChannelMapByTaskIdProcId[taskId], procId)
			if len(gVars.ctldReplyChannelMapByTaskIdProcId[taskId]) == 0 {
				delete(gVars.ctldReplyChannelMapByTaskIdProcId, taskId)
			}
			gVars.ctldReplyChannelMapMtx.Unlock()

			if err := toCrunStream.Send(reply); err != nil {
				log.Errorf("[Cfored<->Crun] Failed to send CompletionAck to crun: %s. "+
					"The connection to crun was broken.", err.Error())
			}

			gCranedChanKeeper.crunTaskStopAndRemoveChannel(taskId, procId, execCranedIds)
			break CforedCrunStateMachineLoop

		case CancelTaskOfDeadCrun:
			log.Debug("[Cfored<->Crun] Enter State CANCEL_TASK_OF_DEAD_CRUN")

			toCtldRequest := &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
				Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
					PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
						CforedName:      gVars.hostName,
						TaskId:          taskId,
						InteractiveType: protos.InteractiveTaskType_Crun,
					},
				},
			}
			gVars.cforedRequestCtldChannel <- toCtldRequest

			for {
				ctldReply := <-ctldReplyChannel
				if ctldReply.Type != protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY {
					log.Tracef("[Cfored<->Crun] Expect TASK_COMPLETION_ACK_REPLY, "+
						"but %s received. Just ignore it...", ctldReply.Type)
				} else {
					break
				}
			}

			gVars.ctldReplyChannelMapMtx.Lock()
			if taskId != math.MaxUint32 {
				delete(gVars.ctldReplyChannelMapByTaskIdProcId[taskId], procId)
				if len(gVars.ctldReplyChannelMapByTaskIdProcId[taskId]) == 0 {
					delete(gVars.ctldReplyChannelMapByTaskIdProcId, taskId)
				}

				gVars.pidTaskIdMapMtx.Lock()
				delete(gVars.pidTaskIdMap, crunPid)
				gVars.pidTaskIdMapMtx.Unlock()

				gCranedChanKeeper.crunTaskStopAndRemoveChannel(taskId, procId, execCranedIds)
			} else {
				delete(gVars.ctldReplyChannelMapByPid, crunPid)
			}
			gVars.ctldReplyChannelMapMtx.Unlock()

			break CforedCrunStateMachineLoop
		}
	}

	return nil

}

func (cforedServer *GrpcCforedServer) CallocStream(toCallocStream protos.CraneForeD_CallocStreamServer) error {
	var callocPid int32
	var taskId uint32
	var procId uint32
	var reply *protos.StreamCforedReply

	requestChannel := make(chan grpcMessage[protos.StreamCallocRequest], 8)
	go grpcStreamReceiver[protos.StreamCallocRequest](toCallocStream, requestChannel)

	ctldReplyChannel := make(chan *protos.StreamCtldReply, 2)

	taskId = math.MaxUint32
	callocPid = -1

	state := WaitTaskIdAllocReq

CforedStateMachineLoop:
	for {
		switch state {
		case WaitTaskIdAllocReq:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_TASK_ID_ALLOC_REQ")

			item := <-requestChannel
			callocRequest, err := item.message, item.err
			if err != nil { // Failure Edge
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Fatal(err)
					return nil
				}
			}

			log.Debug("[Cfored<->Calloc] Receive TaskIdAllocReq")

			if callocRequest.Type != protos.StreamCallocRequest_TASK_REQUEST {
				log.Fatal("[Cfored<->Calloc] Expect TASK_REQUEST")
			}

			if !gVars.ctldConnected.Load() {
				reply = &protos.StreamCforedReply{
					Type: protos.StreamCforedReply_TASK_ID_REPLY,
					Payload: &protos.StreamCforedReply_PayloadTaskIdReply{
						PayloadTaskIdReply: &protos.StreamCforedReply_TaskIdReply{
							Ok:            false,
							FailureReason: "Cfored is not connected to CraneCtld.",
						},
					},
				}

				if err := toCallocStream.Send(reply); err != nil {
					// It doesn't matter even if the connection is broken here.
					// Just print a log.
					log.Error(err)
				}

				// No need to cleaning any data
				break CforedStateMachineLoop
			} else {
				callocPid = callocRequest.GetPayloadTaskReq().CallocPid

				gVars.ctldReplyChannelMapMtx.Lock()
				gVars.ctldReplyChannelMapByPid[callocPid] = ctldReplyChannel
				gVars.ctldReplyChannelMapMtx.Unlock()

				task := callocRequest.GetPayloadTaskReq().Task
				task.GetInteractiveMeta().CforedName = gVars.hostName
				cforedRequest := &protos.StreamCforedRequest{
					Type: protos.StreamCforedRequest_TASK_REQUEST,
					Payload: &protos.StreamCforedRequest_PayloadTaskReq{
						PayloadTaskReq: &protos.StreamCforedRequest_TaskReq{
							CforedName: gVars.hostName,
							Pid:        callocPid,
							Task:       task,
						},
					},
				}

				gVars.cforedRequestCtldChannel <- cforedRequest

				state = WaitCtldAllocTaskId
			}

		case WaitCtldAllocTaskId:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CTLD_ALLOC_TASK_ID")

			select {
			case item := <-requestChannel:
				callocRequest, err := item.message, item.err
				if callocRequest != nil || err == nil {
					log.Fatal("[Cfored<->Calloc] Expect only nil (calloc connection broken) here!")
				}
				log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")

				state = CancelTaskOfDeadCalloc

			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_ID_REPLY {
					log.Fatal("[Cfored<->Calloc] Expect type TASK_ID_REPLY")
				}

				Ok := ctldReply.GetPayloadTaskIdReply().Ok
				taskId = ctldReply.GetPayloadTaskIdReply().TaskId
				procId = ctldReply.GetPayloadTaskIdReply().ProcId
				reply = &protos.StreamCforedReply{
					Type: protos.StreamCforedReply_TASK_ID_REPLY,
					Payload: &protos.StreamCforedReply_PayloadTaskIdReply{
						PayloadTaskIdReply: &protos.StreamCforedReply_TaskIdReply{
							Ok:            Ok,
							TaskId:        taskId,
							FailureReason: ctldReply.GetPayloadTaskIdReply().FailureReason,
						},
					},
				}

				gVars.ctldReplyChannelMapMtx.Lock()
				delete(gVars.ctldReplyChannelMapByPid, callocPid)
				if Ok {
					if _, exits := gVars.ctldReplyChannelMapByTaskIdProcId[taskId]; !exits {
						gVars.ctldReplyChannelMapByTaskIdProcId[taskId] = make(map[uint32]chan *protos.StreamCtldReply)
					}
					gVars.ctldReplyChannelMapByTaskIdProcId[taskId][procId] = ctldReplyChannel
					gVars.pidTaskIdMapMtx.Lock()
					gVars.pidTaskIdMap[callocPid] = taskId
					gVars.pidTaskIdMapMtx.Unlock()
				}
				gVars.ctldReplyChannelMapMtx.Unlock()

				if err := toCallocStream.Send(reply); err != nil {
					log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
					state = CancelTaskOfDeadCalloc
				} else {
					if Ok {
						state = WaitCtldAllocRes
					} else {
						// channel was already removed from gVars.ctldReplyChannelMapByPid
						break CforedStateMachineLoop
					}
				}
			}

		case WaitCtldAllocRes:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CTLD_ALLOC_RES")

			select {
			case item := <-requestChannel:
				callocRequest, err := item.message, item.err
				if callocRequest != nil || err == nil {
					log.Fatal("[Cfored<->Calloc] Expect only nil (calloc connection broken) here!")
				}
				log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")

				state = CancelTaskOfDeadCalloc

			case ctldReply := <-ctldReplyChannel:
				switch ctldReply.Type {
				case protos.StreamCtldReply_TASK_RES_ALLOC_REPLY:
					ctldPayload := ctldReply.GetPayloadTaskResAllocReply()
					reply = &protos.StreamCforedReply{
						Type: protos.StreamCforedReply_TASK_RES_ALLOC_REPLY,
						Payload: &protos.StreamCforedReply_PayloadTaskAllocReply{
							PayloadTaskAllocReply: &protos.StreamCforedReply_TaskResAllocatedReply{
								Ok:                   ctldPayload.Ok,
								AllocatedCranedRegex: ctldPayload.AllocatedCranedRegex,
							},
						},
					}

					if err := toCallocStream.Send(reply); err != nil {
						log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
						state = CancelTaskOfDeadCalloc
					} else {
						state = WaitCallocComplete
					}

				case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
					reply = &protos.StreamCforedReply{
						Type: protos.StreamCforedReply_TASK_CANCEL_REQUEST,
						Payload: &protos.StreamCforedReply_PayloadTaskCancelRequest{
							PayloadTaskCancelRequest: &protos.StreamCforedReply_TaskCancelRequest{
								TaskId: ctldReply.GetPayloadTaskCancelRequest().TaskId,
							},
						},
					}

					if err := toCallocStream.Send(reply); err != nil {
						log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
						state = CancelTaskOfDeadCalloc
					} else {
						state = WaitCallocCancel
					}

				default:
					log.Fatal("[Cfored<->Calloc] Expect type " +
						"TASK_ID_ALLOC_REPLY or TASK_CANCEL_REQUEST")
				}
			}

		case WaitCallocComplete:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CALLOC_COMPLETE")

			select {
			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_CANCEL_REQUEST {
					log.Fatal("[Cfored<->Calloc] Expect Type TASK_CANCEL_REQUEST")
				}

				state = WaitCallocCancel

			case item := <-requestChannel:
				callocRequest, err := item.message, item.err
				if err != nil {
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
						state = CancelTaskOfDeadCalloc
					}
				} else {
					if callocRequest.Type != protos.StreamCallocRequest_TASK_COMPLETION_REQUEST {
						log.Fatal("[Cfored<->Calloc] Expect TASK_COMPLETION_REQUEST")
					}

					log.Debug("[Cfored<->Calloc] Receive TaskCompletionRequest")
					toCtldRequest := &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
							PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
								CforedName:      gVars.hostName,
								TaskId:          taskId,
								InteractiveType: protos.InteractiveTaskType_Calloc,
							},
						},
					}
					gVars.cforedRequestCtldChannel <- toCtldRequest

					state = WaitCtldAck
				}
			}

		case WaitCallocCancel:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CALLOC_CANCEL. " +
				"Sending TASK_CANCEL_REQUEST...")

			reply = &protos.StreamCforedReply{
				Type: protos.StreamCforedReply_TASK_CANCEL_REQUEST,
				Payload: &protos.StreamCforedReply_PayloadTaskCancelRequest{
					PayloadTaskCancelRequest: &protos.StreamCforedReply_TaskCancelRequest{
						TaskId: taskId,
					},
				},
			}

			if err := toCallocStream.Send(reply); err != nil {
				log.Debugf("[Cfored<->Calloc] Failed to send CancelRequest to calloc: %s. "+
					"The connection to calloc was broken.", err.Error())
				state = CancelTaskOfDeadCalloc
			} else {
				item := <-requestChannel
				callocRequest, err := item.message, item.err
				if err != nil { // Failure Edge
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
						state = CancelTaskOfDeadCalloc
					}
				} else {
					if callocRequest.Type != protos.StreamCallocRequest_TASK_COMPLETION_REQUEST {
						log.Fatal("[Cfored<->Calloc] Expect TASK_COMPLETION_REQUEST")
					}

					log.Debug("[Cfored<->Calloc] Receive TaskCompletionRequest")

					toCtldRequest := &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
							PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
								CforedName:      gVars.hostName,
								TaskId:          taskId,
								InteractiveType: protos.InteractiveTaskType_Calloc,
							},
						},
					}
					gVars.cforedRequestCtldChannel <- toCtldRequest

					state = WaitCtldAck
				}
			}

		case WaitCtldAck:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CTLD_ACK")
			ctldReply := <-ctldReplyChannel
			if ctldReply.Type != protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY {
				log.Warningf("[Cfored<->Calloc] Expect TASK_COMPLETION_ACK_REPLY, "+
					"but %s received.", ctldReply.Type)
			} else {
				log.Tracef("[Cfored<->Calloc] TASK_COMPLETION_ACK_REPLY of task #%d received",
					ctldReply.GetPayloadTaskCompletionAck().GetTaskId())
			}

			reply = &protos.StreamCforedReply{
				Type: protos.StreamCforedReply_TASK_COMPLETION_ACK_REPLY,
				Payload: &protos.StreamCforedReply_PayloadTaskCompletionAckReply{
					PayloadTaskCompletionAckReply: &protos.StreamCforedReply_TaskCompletionAckReply{
						Ok: true,
					},
				},
			}
			err := toCallocStream.Send(reply)
			if err != nil {
				log.Debugf("[Cfored<->Calloc] Failed to send TASK_COMPLETION_ACK_REPLY to calloc: %s. "+
					"The connection to calloc was broken.", err.Error())
			}

			gVars.ctldReplyChannelMapMtx.Lock()
			delete(gVars.ctldReplyChannelMapByTaskIdProcId[taskId], procId)
			if len(gVars.ctldReplyChannelMapByTaskIdProcId[taskId]) == 0 {
				delete(gVars.ctldReplyChannelMapByTaskIdProcId, taskId)
			}
			gVars.ctldReplyChannelMapMtx.Unlock()

			break CforedStateMachineLoop

		case CancelTaskOfDeadCalloc:
			log.Debug("[Cfored<->Calloc] Enter State CANCEL_TASK_OF_DEAD_CALLOC")

			toCtldRequest := &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
				Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
					PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
						CforedName:      gVars.hostName,
						TaskId:          taskId,
						InteractiveType: protos.InteractiveTaskType_Calloc,
					},
				},
			}
			gVars.cforedRequestCtldChannel <- toCtldRequest
			ctldReply := <-ctldReplyChannel
			if ctldReply.Type != protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY {
				log.Fatalf("[Cfored<->Calloc] Expect TASK_COMPLETION_ACK_REPLY, "+
					"but %s received.", ctldReply.Type)
			}

			gVars.ctldReplyChannelMapMtx.Lock()
			if taskId != math.MaxUint32 {
				delete(gVars.ctldReplyChannelMapByTaskIdProcId[taskId], procId)
				if len(gVars.ctldReplyChannelMapByTaskIdProcId[taskId]) == 0 {
					delete(gVars.ctldReplyChannelMapByTaskIdProcId, taskId)
				}
				gVars.pidTaskIdMapMtx.Lock()
				delete(gVars.pidTaskIdMap, callocPid)
				gVars.pidTaskIdMapMtx.Unlock()
			} else {
				delete(gVars.ctldReplyChannelMapByPid, callocPid)
			}
			gVars.ctldReplyChannelMapMtx.Unlock()

			break CforedStateMachineLoop
		}
	}

	return nil
}

func StartCfored() {
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

	config := util.ParseConfig(FlagConfigFilePath)

	util.DetectNetworkProxy()

	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())
	defer gVars.globalCtxCancel()

	gVars.ctldConnected.Store(false)

	gVars.cforedRequestCtldChannel = make(chan *protos.StreamCforedRequest, 8)

	gVars.ctldReplyChannelMapByPid = make(map[int32]chan *protos.StreamCtldReply)
	gVars.ctldReplyChannelMapByTaskIdProcId = make(map[uint32]map[uint32]chan *protos.StreamCtldReply)
	gVars.pidTaskIdMap = make(map[int32]uint32)

	gCranedChanKeeper = NewCranedChannelKeeper()

	hostName, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %s", err.Error())
	}
	gVars.hostName = hostName

	var wgAllRoutines sync.WaitGroup

	ctldClient := &GrpcCtldClient{
		ctldClientStub:   util.GetStubToCtldByConfig(config),
		ctldReplyChannel: make(chan *protos.StreamCtldReply, 8),
	}
	wgAllRoutines.Add(1)
	go ctldClient.StartCtldClientStream(&wgAllRoutines)

	startGrpcServer(config, &wgAllRoutines)

	log.Debug("Waiting all go routines to exit...")
	wgAllRoutines.Wait()
}
