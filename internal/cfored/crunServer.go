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
			log.Infof("[Cfored<->Crun][Pid #%d] Enter State WAIT_TASK_ID_ALLOC_REQ", crunPid)

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
				log.Infof("[Cfored<->Crun]Cfored not connected to CraneCtld")
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
			log.Infof("[Cfored<->Crun][Pid #%d] Enter State WAIT_CTLD_ALLOC_TASK_ID", crunPid)

			select {
			case item := <-crunRequestChannel:
				crunRequest, err := item.message, item.err
				if err != nil {
					// EOF meaning Crun closed, otherwise RPC error.
					// Crun dead, cfored will cancel this task, keep waiting task id
					crunDownWithoutTaskId = true
					log.Debug("[Crun->Cfored] Connection to crun was broken.")
					//Will wait for ctldReplyChannel to get task id
				} else if crunRequest != nil || err == nil {
					log.Fatal("[Crun->Cfored] Expect only nil (crun connection broken) here!")
				}

			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_ID_REPLY {
					log.Fatalf("[Ctld->Cfored->Crun][Pid#%d] Expect type TASK_ID_REPLY", crunPid)
				}

				Ok := ctldReply.GetPayloadTaskIdReply().Ok
				log.Tracef("[Ctld->Cfored->Crun][Pid#%d] Receive TaskIdReply, Ok: %v", crunPid, Ok)
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
					break
				}

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
					log.Debugf("[Cfored<->Crun][Job #%d] Connection to crun was broken.", taskId)
					state = CancelTaskOfDeadCrun
					break
				}

				if Ok {
					state = CrunWaitCtldAllocRes
				} else {
					// Crun task req failed
					// channel was already removed from gVars.ctldReplyChannelMapByPid
					log.Infof("[Cfored<->Crun][Job #%d] Task request failed", taskId)
					break CforedCrunStateMachineLoop
				}

			}

		case CrunWaitCtldAllocRes:
			log.Infof("[Cfored<->Crun][Job #%d] Enter State WAIT_CTLD_ALLOC_RES", taskId)

			select {
			case item := <-crunRequestChannel:
				crunRequest, err := item.message, item.err
				if err != nil {
					log.Debugf("[Crun->Cfored][Job #%d] Connection to crun was broken.", taskId)
					state = CancelTaskOfDeadCrun
					break
				}

				if crunRequest != nil || err == nil {
					if crunRequest.Type != protos.StreamCrunRequest_TASK_COMPLETION_REQUEST {
						log.Fatalf("[Crun->Cfored][Job #%d] Expect TaskCompletionRequest here!", taskId)
					} else {
						log.Debugf("[Crun->Cfored][Job #%d] Receive TaskCompletionRequest", taskId)
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
					log.Tracef("[Ctld->Cfored->Crun][Job #%d] Receive TaskResAllocReply with node %v",
						taskId, execCranedIds)

					if err := toCrunStream.Send(reply); err != nil {
						log.Debug("[Cfored<->Crun] Connection to crun was broken.")
						state = CancelTaskOfDeadCrun
					} else {
						state = CrunWaitIOForward
					}

				case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
					log.Debugf("[Ctld->Cfored->Crun][Job #%d] Receive TaskCancelRequest", taskId)
					state = CrunWaitTaskCancel

				default:
					log.Fatalf("[Ctld->Cfored->Crun][Job #%d] Expect type "+
						"TASK_RES_ALLOC_REPLY or TASK_CANCEL_REQUEST but got %s", taskId, ctldReply.Type)
				}
			}

		case CrunWaitIOForward:
			log.Infof("[Cfored<->Crun][Job #%d] Enter State WAIT_TASK_IO_FORWARD", taskId)

			stopWaiting := atomic.Bool{}
			stopWaiting.Store(false)
			readyChannel := make(chan bool, 1)
			go gCranedChanKeeper.waitCranedChannelsReady(execCranedIds, readyChannel, &stopWaiting, taskId)

			select {
			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_CANCEL_REQUEST {
					log.Fatalf("[Ctld->Cfored->Crun][Job #%d] Expect type TASK_CANCEL_REQUEST but got %s, ignored",
						taskId, ctldReply.Type)
				} else {
					log.Debugf("[Ctld->Cfored->Crun][Job #%d] Receive TaskCancelRequest", taskId)
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
						log.Debugf("[Crun->Cfored][Job #%d] Connection to crun was broken.", taskId)
						stopWaiting.Store(true)
						state = CancelTaskOfDeadCrun
					}
					break
				}

				if crunRequest.Type != protos.StreamCrunRequest_TASK_COMPLETION_REQUEST {
					log.Fatalf("[Crun->Cfored][Job #%d] Expect TASK_COMPLETION_REQUEST", taskId)
				}

				log.Debugf("[Crun->Cfored->Ctld][Job #%d] Receive TaskCompletionRequest", taskId)
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
					log.Debugf("[Cfored<->Crun][Job #%d] Failed to send TASK_IO_FORWARD_READY to crun: %s. "+
						"The connection to crun was broken.", taskId, err.Error())
					state = CancelTaskOfDeadCrun
				} else {
					state = CrunWaitTaskComplete
				}
			}

		case CrunWaitTaskComplete:
			log.Debugf("[Cfored<->Crun][Job #%d] Enter State Crun_Wait_Task_Complete", taskId)
		forwarding:
			for {
				select {
				case ctldReply := <-ctldReplyChannel:
					if ctldReply.Type != protos.StreamCtldReply_TASK_CANCEL_REQUEST {
						log.Warningf("[Ctld->Cfored->Crun][Job #%d] Expect type TASK_CANCEL_REQUEST but got %s, ignored",
							taskId, ctldReply.Type)
					} else {
						log.Debugf("[Ctld->Cfored->Crun][Job #%d] Receive TaskCancelRequest", taskId)
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
							log.Debugf("[Crun->Cfored][Job #%d] Connection to crun was broken.", taskId)
							state = CancelTaskOfDeadCrun
							break forwarding
						}
					} else {
						switch crunRequest.Type {
						case protos.StreamCrunRequest_TASK_IO_FORWARD:
							log.Debugf("[Crun->Cfored->Craned][Job #%d] Receive TASK_IO_FORWARD Request to task, msg size[%d]",
								crunRequest.GetPayloadTaskIoForwardReq().GetTaskId(),
								len(crunRequest.GetPayloadTaskIoForwardReq().GetMsg()))
							gCranedChanKeeper.forwardCrunRequestToCranedChannels(crunRequest, execCranedIds)

						case protos.StreamCrunRequest_TASK_X11_FORWARD:
							log.Debugf("[Crun->Cfored->Craned][Job #%d] Receive Local TASK_X11_FORWARD to remote task",
								crunRequest.GetPayloadTaskX11ForwardReq().GetTaskId())
							gCranedChanKeeper.forwardCrunRequestToCranedChannels(crunRequest, execCranedIds)

						case protos.StreamCrunRequest_TASK_COMPLETION_REQUEST:
							log.Debug("[Crun->Cfored->Ctld] Receive TaskCompletionRequest")
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
							log.Fatalf("[Crun->Cfored][Job #%d] Expect TASK_COMPLETION_REQUEST or TASK_IO_FORWARD", taskId)
							break forwarding
						}
					}

				case taskMsg := <-TaskIoRequestChannel:
					if taskMsg == nil {
						log.Errorf("[Craned->Cfored->Crun] One of task #%d Craneds [%v] down. Cancelling the task...",
							taskId, execCranedIds)
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
						log.Tracef("[Craned->Cfored->Crun][Job #%d] fowarding msg size[%d]",
							taskId, len(taskMsg.GetPayloadTaskOutputReq().GetMsg()))
						if err := toCrunStream.Send(reply); err != nil {
							log.Debugf("[Cfored->Crun] Failed to send TASK_IO_FORWARD to crun: %s. "+
								"The connection to crun was broken.", err.Error())
							state = CancelTaskOfDeadCrun
							break forwarding
						}
					} else if taskMsg.Type == protos.StreamTaskIORequest_CRANED_TASK_X11_OUTPUT {
						reply = &protos.StreamCrunReply{
							Type: protos.StreamCrunReply_TASK_X11_FORWARD,
							Payload: &protos.StreamCrunReply_PayloadTaskX11ForwardReply{
								PayloadTaskX11ForwardReply: &protos.StreamCrunReply_TaskX11ForwardReply{
									Msg: taskMsg.GetPayloadTaskX11OutputReq().Msg,
								},
							},
						}
						log.Tracef("[Craned->Cfored->Crun][Job #%d] fowarding x11 msg size[%d]",
							taskId, len(taskMsg.GetPayloadTaskX11OutputReq().Msg))
						if err := toCrunStream.Send(reply); err != nil {
							log.Debugf("[Cfored<->Crun] Failed to send TASK_X11_FORWARD to crun: %s. "+
								"The connection to crun was broken.", err.Error())
							state = CancelTaskOfDeadCrun
							break forwarding
						}
					} else {
						log.Fatalf("[Craned->Cfored->Crun][Job #%d] Expect Type CRANED_TASK_OUTPUT", taskId)
						break forwarding
					}
				}
			}

		case CrunWaitTaskCancel:
			log.Debugf("[Cfored<->Crun][Job #%d] Enter State WAIT_CRUN_CANCEL. Sending TASK_CANCEL_REQUEST to Crun...", taskId)

			reply = &protos.StreamCrunReply{
				Type: protos.StreamCrunReply_TASK_CANCEL_REQUEST,
				Payload: &protos.StreamCrunReply_PayloadTaskCancelRequest{
					PayloadTaskCancelRequest: &protos.StreamCrunReply_TaskCancelRequest{
						TaskId: taskId,
					},
				},
			}

			if err := toCrunStream.Send(reply); err != nil {
				log.Debugf("[Cfored->Crun][Job #%d] Failed to send CancelRequest to crun: %s. "+
					"The connection to crun was broken.", taskId, err.Error())
				state = CancelTaskOfDeadCrun
				break
			}

			crunErr := false
			for {
				item := <-crunRequestChannel
				crunRequest, err := item.message, item.err
				if err != nil { // Failure Edge
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Debugf("[Cfored<->Crun][Job #%d] Connection to crun was broken.", taskId)
						state = CancelTaskOfDeadCrun
						crunErr = true
					}
					break
				}
				if crunRequest.Type != protos.StreamCrunRequest_TASK_COMPLETION_REQUEST {
					log.Warningf("[Crun->Cfored][Job #%d] Expect TASK_COMPLETION_REQUEST but %s received. Ignoring it...",
						taskId, crunRequest.Type)
				} else {
					log.Tracef("[Crun->Cfored][Job #%d] TASK_COMPLETION_REQUEST received",
						taskId)
					break
				}
			}
			if crunErr {
				break
			}
			log.Debugf("[Crun->Cfored][Job #%d] Receive TaskCompletionRequest", taskId)

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

		case CrunWaitCtldAck:
			log.Infof("[Cfored<->Crun][Job #%d] Enter State WAIT_CTLD_ACK", taskId)

			ctldReply := <-ctldReplyChannel
			if ctldReply.Type != protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY {
				log.Warningf("[Ctld->Cfored->Crun][Job #%d] Expect TASK_COMPLETION_ACK_REPLY, "+
					"but %s received. Ignoring it...", taskId, ctldReply.Type)
				break
			} else {
				log.Tracef("[Ctld->fored->Crun][Job #%d] TASK_COMPLETION_ACK_REPLY received",
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
			log.Infof("[Cfored<->Crun][Job #%d] Job completed successfully", taskId)

			break CforedCrunStateMachineLoop

		case CancelTaskOfDeadCrun:
			log.Infof("[Cfored<->Crun][Job #%d] Enter State CANCEL_TASK_OF_DEAD_CRUN", taskId)

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

			log.Infof("[Cfored<->Crun][Job #%d] Job cancelled due to Crun down", taskId)

			break CforedCrunStateMachineLoop
		}
	}

	return nil

}
