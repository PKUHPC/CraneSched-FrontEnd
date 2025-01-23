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

package cfored

import (
	"CraneFrontEnd/generated/protos"
	"io"
	"math"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

type StateOfCrunServer int

const (
	CrunWaitTaskIdAllocReq  StateOfCrunServer = 0
	CrunWaitCtldAllocTaskId StateOfCrunServer = 1
	CrunWaitCtldAllocRes    StateOfCrunServer = 2
	CrunWaitIOForward       StateOfCrunServer = 3
	CrunWaitTaskComplete    StateOfCrunServer = 4
	CrunWaitTaskCancel      StateOfCrunServer = 5
	CrunWaitCtldAck         StateOfCrunServer = 6
	CancelTaskOfDeadCrun    StateOfCrunServer = 7
)

func (cforedServer *GrpcCforedServer) CrunStream(toCrunStream protos.CraneForeD_CrunStreamServer) error {
	var crunPid int32
	var taskId uint32
	var reply *protos.StreamCrunReply

	var execCranedIds []string
	var crunPty bool
	//Whether crun down before get taskid
	crunDownWithoutTaskId := false
	crunRequestChannel := make(chan grpcMessage[protos.StreamCrunRequest], 8)
	go grpcStreamReceiver[protos.StreamCrunRequest](toCrunStream, crunRequestChannel)

	ctldReplyChannel := make(chan *protos.StreamCtldReply, 2)
	TaskIoRequestChannel := make(chan *protos.StreamTaskIORequest, 2)
	taskId = math.MaxUint32
	crunPid = -1
	forwardEstablished := atomic.Bool{}
	forwardEstablished.Store(false)

	state := CrunWaitTaskIdAllocReq

CforedCrunStateMachineLoop:
	for {
		switch state {
		case CrunWaitTaskIdAllocReq:
			log.Debug("[Cfored<->Crun] Enter State WAIT_TASK_ID_ALLOC_REQ")

			item := <-crunRequestChannel
			crunRequest, err := item.message, item.err
			// If crun down before req task id, do nothing
			if err != nil { // Failure Edge
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Fatal(err)
					return nil
				}
			}

			log.Debug("[Cfored<->Crun] Receive TaskIdAllocReq")

			if crunRequest.Type != protos.StreamCrunRequest_TASK_REQUEST {
				log.Fatal("[Cfored<->Crun] Expect TASK_REQUEST")
			}

			if !gVars.ctldConnected.Load() {
				reply = &protos.StreamCrunReply{
					Type: protos.StreamCrunReply_TASK_ID_REPLY,
					Payload: &protos.StreamCrunReply_PayloadTaskIdReply{
						PayloadTaskIdReply: &protos.StreamCrunReply_TaskIdReply{
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
				interactiveMeta := task.GetInteractiveMeta()
				interactiveMeta.CforedName = gVars.hostName
				crunPty = interactiveMeta.Pty
				cforedRequest := &protos.StreamCforedRequest{
					Type: protos.StreamCforedRequest_TASK_REQUEST,
					Payload: &protos.StreamCforedRequest_PayloadTaskReq{
						PayloadTaskReq: &protos.StreamCforedRequest_TaskReq{
							CforedName: gVars.hostName,
							Pid:        crunPid,
							Task:       task,
						},
					},
				}

				gVars.cforedRequestCtldChannel <- cforedRequest

				state = CrunWaitCtldAllocTaskId
			}

		case CrunWaitCtldAllocTaskId:
			log.Debug("[Cfored<->Crun] Enter State WAIT_CTLD_ALLOC_TASK_ID")

			select {
			case item := <-crunRequestChannel:
				crunRequest, err := item.message, item.err
				if err != nil {
					// EOF meaning Crun closed, otherwise RPC error.
					// Crun dead, cfored will cancel this task, keep waiting task id
					crunDownWithoutTaskId = true
					log.Debug("[Cfored<->Crun] Connection to crun was broken.")
				} else if crunRequest != nil || err == nil {
					log.Fatal("[Cfored<->Crun] Expect only nil (crun connection broken) here!")
				}

			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_ID_REPLY {
					log.Fatal("[Cfored<->Crun] Expect type TASK_ID_REPLY")
				}

				Ok := ctldReply.GetPayloadTaskIdReply().Ok
				taskId = ctldReply.GetPayloadTaskIdReply().TaskId

				gVars.ctldReplyChannelMapMtx.Lock()
				delete(gVars.ctldReplyChannelMapByPid, crunPid)
				if Ok {
					gVars.ctldReplyChannelMapByTaskId[taskId] = ctldReplyChannel
					gVars.pidTaskIdMapMtx.Lock()
					gVars.pidTaskIdMap[crunPid] = taskId
					gVars.pidTaskIdMapMtx.Unlock()

					gCranedChanKeeper.setRemoteIoToCrunChannel(taskId, TaskIoRequestChannel)
				}

				gVars.ctldReplyChannelMapMtx.Unlock()
				if crunDownWithoutTaskId {
					// Crun was down when CrunWaitCtldAllocTaskId, just cancel task.
					state = CancelTaskOfDeadCrun
				} else {
					reply = &protos.StreamCrunReply{
						Type: protos.StreamCrunReply_TASK_ID_REPLY,
						Payload: &protos.StreamCrunReply_PayloadTaskIdReply{
							PayloadTaskIdReply: &protos.StreamCrunReply_TaskIdReply{
								Ok:            Ok,
								TaskId:        taskId,
								FailureReason: ctldReply.GetPayloadTaskIdReply().FailureReason,
							},
						},
					}
					if err := toCrunStream.Send(reply); err != nil {
						log.Debug("[Cfored<->Crun] Connection to crun was broken.")
						state = CancelTaskOfDeadCrun
					} else {
						if Ok {
							state = CrunWaitCtldAllocRes
						} else {
							// Crun task req failed
							// channel was already removed from gVars.ctldReplyChannelMapByPid
							break CforedCrunStateMachineLoop
						}
					}
				}
			}

		case CrunWaitCtldAllocRes:
			log.Debug("[Cfored<->Crun] Enter State WAIT_CTLD_ALLOC_RES")

			select {
			case item := <-crunRequestChannel:
				crunRequest, err := item.message, item.err
				if err != nil {
					log.Debug("[Cfored<->Crun] Connection to crun was broken.")
					state = CancelTaskOfDeadCrun
				} else if crunRequest != nil || err == nil {
					if crunRequest.Type != protos.StreamCrunRequest_TASK_COMPLETION_REQUEST {
						log.Fatal("[Cfored<--Crun] Expect only nil (crun connection broken) here!")
					} else {
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
						state = CrunWaitCtldAck
					}

				}
			case ctldReply := <-ctldReplyChannel:
				switch ctldReply.Type {
				case protos.StreamCtldReply_TASK_RES_ALLOC_REPLY:
					ctldPayload := ctldReply.GetPayloadTaskResAllocReply()
					reply = &protos.StreamCrunReply{
						Type: protos.StreamCrunReply_TASK_RES_ALLOC_REPLY,
						Payload: &protos.StreamCrunReply_PayloadTaskAllocReply{
							PayloadTaskAllocReply: &protos.StreamCrunReply_TaskResAllocatedReply{
								Ok:                   ctldPayload.Ok,
								AllocatedCranedRegex: ctldPayload.AllocatedCranedRegex,
							},
						},
					}

					// TODO: Difference
					if crunPty {
						// For crun with pty, only execute on first node
						execCranedIds = []string{ctldPayload.GetCranedIds()[0]}
					} else {
						execCranedIds = ctldPayload.GetCranedIds()
					}

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
						"TASK_ID_ALLOC_REPLY or TASK_CANCEL_REQUEST")
				}
			}

		case CrunWaitIOForward:
			log.Debug("[Cfored<->Crun] Enter State WAIT_TASK_IO_FORWARD")

			stopWaiting := atomic.Bool{}
			stopWaiting.Store(false)
			readyChannel := make(chan bool, 1)
			go gCranedChanKeeper.waitCranedChannelsReady(execCranedIds, readyChannel, &stopWaiting, taskId)

			select {
			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_CANCEL_REQUEST {
					log.Fatalf("[Cfored<->Crun] Expect type TASK_CANCEL_REQUEST but got %s, ignored", ctldReply.Type)
				} else {
					state = CrunWaitTaskCancel
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
					state = CrunWaitCtldAck
				}

			case <-readyChannel:
				reply = &protos.StreamCrunReply{
					Type: protos.StreamCrunReply_TASK_IO_FORWARD_READY,
					Payload: &protos.StreamCrunReply_PayloadTaskIoForwardReadyReply{
						PayloadTaskIoForwardReadyReply: &protos.StreamCrunReply_TaskIOForwardReadyReply{
							Ok: true,
						},
					},
				}
				forwardEstablished.Store(true)

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
					if ctldReply.Type != protos.StreamCtldReply_TASK_CANCEL_REQUEST {
						log.Warningf("[Cfored<->Crun] Expect type TASK_CANCEL_REQUEST but got %s, ignored", ctldReply.Type)
					} else {
						log.Debug("[Cfored<->Crun] Receive TaskCancelRequest")
						state = CrunWaitTaskCancel
						break forwarding
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
							log.Debugf("[Crun->Cfored->Craned] Receive TASK_IO_FORWARD Request to task #%d, msg:\"%s\"", crunRequest.GetPayloadTaskIoForwardReq().GetTaskId(), crunRequest.GetPayloadTaskIoForwardReq().GetMsg())
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
							state = CrunWaitCtldAck
							break forwarding
						default:
							log.Fatal("[Cfored<->Crun] Expect TASK_COMPLETION_REQUEST or TASK_IO_FORWARD")
							break forwarding
						}
					}

				case taskMsg := <-TaskIoRequestChannel:
					if taskMsg == nil {
						log.Errorf("[Cfored<->Crun] One of task #%d Craneds down. Cancelling the task...", taskId)
						// IO Channel from Craned was shut down unexpectedly.
						state = CrunWaitTaskCancel
						break forwarding
					}

					if taskMsg.Type == protos.StreamTaskIORequest_CRANED_TASK_OUTPUT {
						reply = &protos.StreamCrunReply{
							Type: protos.StreamCrunReply_TASK_IO_FORWARD,
							Payload: &protos.StreamCrunReply_PayloadTaskIoForwardReply{
								PayloadTaskIoForwardReply: &protos.StreamCrunReply_TaskIOForwardReply{
									Msg: taskMsg.GetPayloadTaskOutputReq().Msg,
								},
							},
						}
						log.Tracef("[Cfored<->Crun] fowarding msg %s to crun for taskid %d", taskMsg.GetPayloadTaskOutputReq().GetMsg(), taskId)
						if err := toCrunStream.Send(reply); err != nil {
							log.Debugf("[Cfored<->Crun] Failed to send CancelRequest to calloc: %s. "+
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
			log.Debug("[Cfored<->Crun] Enter State WAIT_CRUN_CANCEL. Sending TASK_CANCEL_REQUEST to Crun...")

			reply = &protos.StreamCrunReply{
				Type: protos.StreamCrunReply_TASK_CANCEL_REQUEST,
				Payload: &protos.StreamCrunReply_PayloadTaskCancelRequest{
					PayloadTaskCancelRequest: &protos.StreamCrunReply_TaskCancelRequest{
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

					state = CrunWaitCtldAck
				}
			}

		case CrunWaitCtldAck:
			log.Debug("[Cfored<->Crun] Enter State WAIT_CTLD_ACK")

			ctldReply := <-ctldReplyChannel
			if ctldReply.Type != protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY {
				log.Warningf("[Cfored<->Crun] Expect TASK_COMPLETION_ACK_REPLY, "+
					"but %s received. Ignoring it...", ctldReply.Type)
				break
			} else {
				log.Tracef("[Cfored<->Crun] TASK_COMPLETION_ACK_REPLY of task #%d received",
					ctldReply.GetPayloadTaskCompletionAck().GetTaskId())
			}

			reply = &protos.StreamCrunReply{
				Type: protos.StreamCrunReply_TASK_COMPLETION_ACK_REPLY,
				Payload: &protos.StreamCrunReply_PayloadTaskCompletionAckReply{
					PayloadTaskCompletionAckReply: &protos.StreamCrunReply_TaskCompletionAckReply{
						Ok: true,
					},
				},
			}

			gVars.ctldReplyChannelMapMtx.Lock()
			delete(gVars.ctldReplyChannelMapByTaskId, taskId)
			gVars.ctldReplyChannelMapMtx.Unlock()
			gCranedChanKeeper.crunTaskStopAndRemoveChannel(taskId, execCranedIds, forwardEstablished.Load())

			if err := toCrunStream.Send(reply); err != nil {
				log.Errorf("[Cfored<->Crun] Failed to send CompletionAck to crun: %s. "+
					"The connection to crun was broken.", err.Error())
			}

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
						"but %s received. Just ignore it...", ctldReply.Type.String())
				} else {
					break
				}
			}

			gVars.ctldReplyChannelMapMtx.Lock()
			if taskId != math.MaxUint32 {
				delete(gVars.ctldReplyChannelMapByTaskId, taskId)

				gVars.pidTaskIdMapMtx.Lock()
				delete(gVars.pidTaskIdMap, crunPid)
				gVars.pidTaskIdMapMtx.Unlock()

				gCranedChanKeeper.crunTaskStopAndRemoveChannel(taskId, execCranedIds, forwardEstablished.Load())
			} else {
				log.Fatal("Task id should not equal MaxUint32 in CancelTaskOfDeadCrun")
			}
			gVars.ctldReplyChannelMapMtx.Unlock()

			break CforedCrunStateMachineLoop
		}
	}

	return nil

}
