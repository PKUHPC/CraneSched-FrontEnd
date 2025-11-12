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
	"CraneFrontEnd/internal/util"
	"io"
	"math"
	"sync/atomic"

	"google.golang.org/grpc/peer"

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
	var jobId uint32
	var stepId uint32
	var reply *protos.StreamCrunReply
	var step StepIdentifier

	var execCranedIds []string
	var crunPty bool
	//Whether crun down before get taskid
	crunDownWithoutTaskId := false
	crunRequestChannel := make(chan grpcMessage[protos.StreamCrunRequest], 8)
	go grpcStreamReceiver[protos.StreamCrunRequest](toCrunStream, crunRequestChannel)

	ctldReplyChannel := make(chan *protos.StreamCtldReply, 2)
	TaskIoRequestChannel := make(chan *protos.StreamTaskIORequest, 2)
	jobId = math.MaxUint32
	crunPid = -1
	forwardEstablished := atomic.Bool{}
	forwardEstablished.Store(false)

	state := CrunWaitTaskIdAllocReq

CforedCrunStateMachineLoop:
	for {
		switch state {
		case CrunWaitTaskIdAllocReq:
			log.Infof("[Cfored<->Crun] Enter State WAIT_TASK_ID_ALLOC_REQ")

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

			if crunRequest.Type != protos.StreamCrunRequest_TASK_REQUEST {
				log.Fatalf("[Cfored<-Crun] Expect TASK_REQUEST but got %s", crunRequest.Type)
				break
			}

			log.Debug("[Cfored<-Crun] Receive TASK_REQUEST")

			ctx := toCrunStream.Context()
			p, ok := peer.FromContext(ctx)
			if ok {
				if auth, ok := p.AuthInfo.(*util.UnixPeerAuthInfo); ok {
					uid := crunRequest.GetPayloadTaskReq().Task.Uid
					if uid != auth.UID {
						log.Warnf("Security: UID mismatch - peer UID %d does not match task UID %d", auth.UID, crunRequest.GetPayloadTaskReq().Task.Uid)
						reply = &protos.StreamCrunReply{
							Type: protos.StreamCrunReply_TASK_ID_REPLY,
							Payload: &protos.StreamCrunReply_PayloadTaskIdReply{
								PayloadTaskIdReply: &protos.StreamCrunReply_TaskIdReply{
									Ok:            false,
									FailureReason: "Permission denied: caller UID does not match task UID",
								},
							},
						}

						if err := toCrunStream.Send(reply); err != nil {
							log.Error(err)
						}

						break CforedCrunStateMachineLoop
					}
				}
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
				jobId = ctldReply.GetPayloadTaskIdReply().JobId
				stepId = ctldReply.GetPayloadTaskIdReply().StepId
				step = StepIdentifier{JobId: jobId, StepId: stepId}
				log.Tracef("[Ctld->Cfored->Crun][Pid#%d] Receive TaskIdReply, Ok: %v, JobId #%d, StepId #%d",
					crunPid, Ok, jobId, stepId)

				gVars.ctldReplyChannelMapMtx.Lock()
				delete(gVars.ctldReplyChannelMapByPid, crunPid)
				if Ok {
					gVars.ctldReplyChannelMapByStep[step] = ctldReplyChannel
					gVars.pidStepMapMtx.Lock()
					gVars.pidStepMap[crunPid] = step
					gVars.pidStepMapMtx.Unlock()
					gSupervisorChanKeeper.setRemoteIoToCrunChannel(jobId, stepId, TaskIoRequestChannel)
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
							JobId:         jobId,
							StepId:        stepId,
							FailureReason: ctldReply.GetPayloadTaskIdReply().FailureReason,
						},
					},
				}

				if err := toCrunStream.Send(reply); err != nil {
					log.Debugf("[Cfored<->Crun][Step #%d.%d] Connection to crun was broken.", jobId, stepId)
					state = CancelTaskOfDeadCrun
					break
				}

				if Ok {
					state = CrunWaitCtldAllocRes
				} else {
					// Crun task req failed
					// channel was already removed from gVars.ctldReplyChannelMapByPid
					log.Infof("[Cfored<->Crun][Pid #%d] Task request failed", crunPid)
					break CforedCrunStateMachineLoop
				}

			}

		case CrunWaitCtldAllocRes:
			log.Infof("[Cfored<->Crun][Step #%d.%d] Enter State WAIT_CTLD_ALLOC_RES", jobId, stepId)

			select {
			case item := <-crunRequestChannel:
				crunRequest, err := item.message, item.err
				if err != nil {
					log.Debugf("[Crun->Cfored][Step #%d.%d] Connection to crun was broken.", jobId, stepId)
					state = CancelTaskOfDeadCrun
					break
				}

				if crunRequest != nil || err == nil {
					if crunRequest.Type != protos.StreamCrunRequest_TASK_COMPLETION_REQUEST {
						log.Fatalf("[Crun->Cfored][Step #%d.%d] Expect TaskCompletionRequest here!", jobId, stepId)
					} else {
						log.Debugf("[Crun->Cfored][Step #%d.%d] Receive TaskCompletionRequest", jobId, stepId)
						toCtldRequest := &protos.StreamCforedRequest{
							Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
							Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
								PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
									CforedName:      gVars.hostName,
									JobId:           jobId,
									StepId:          stepId,
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
					log.Tracef("[Ctld->Cfored->Crun][Step #%d.%d] Receive TaskResAllocReply with node %v",
						jobId, stepId, execCranedIds)

					if err := toCrunStream.Send(reply); err != nil {
						log.Debug("[Cfored->Crun] Connection to crun was broken.")
						state = CancelTaskOfDeadCrun
					} else {
						state = CrunWaitIOForward
					}

				case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
					log.Debugf("[Ctld->Cfored->Crun][Step #%d.%d] Receive TaskCancelRequest", jobId, stepId)
					state = CrunWaitTaskCancel

				default:
					log.Fatalf("[Ctld->Cfored->Crun][Step #%d.%d] Expect type "+
						"TASK_RES_ALLOC_REPLY or TASK_CANCEL_REQUEST but got %s", jobId, stepId, ctldReply.Type)
				}
			}

		case CrunWaitIOForward:
			log.Infof("[Cfored<->Crun][Step #%d.%d] Enter State WAIT_TASK_IO_FORWARD.", jobId, stepId)

			stopWaiting := atomic.Bool{}
			stopWaiting.Store(false)
			readyChannel := make(chan bool, 1)
			go gSupervisorChanKeeper.waitSupervisorChannelsReady(execCranedIds, readyChannel, &stopWaiting, jobId, stepId)

			select {
			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_CANCEL_REQUEST {
					log.Fatalf("[Ctld->Cfored->Crun][Step #%d.%d] Expect type TASK_CANCEL_REQUEST but got %s, ignored",
						jobId, stepId, ctldReply.Type)
				} else {
					log.Debugf("[Ctld->Cfored->Crun][Step #%d.%d] Receive TaskCancelRequest", jobId, stepId)
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
						log.Debugf("[Crun->Cfored][Step #%d.%d] Connection to crun was broken.", jobId, stepId)
						stopWaiting.Store(true)
						state = CancelTaskOfDeadCrun
					}
					break
				}

				if crunRequest.Type != protos.StreamCrunRequest_TASK_COMPLETION_REQUEST {
					log.Fatalf("[Crun->Cfored][Step #%d.%d] Expect TASK_COMPLETION_REQUEST.", jobId, stepId)
				}

				log.Debugf("[Crun->Cfored->Ctld][Step #%d.%d] Receive TaskCompletionRequest", jobId, stepId)
				toCtldRequest := &protos.StreamCforedRequest{
					Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
					Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
						PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
							CforedName:      gVars.hostName,
							JobId:           jobId,
							StepId:          stepId,
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
					log.Debugf("[Cfored<->Crun][Step #%d.%d] Failed to send TASK_IO_FORWARD_READY to crun: %s. "+
						"The connection to crun was broken.", jobId, stepId, err.Error())
					state = CancelTaskOfDeadCrun
				} else {
					state = CrunWaitTaskComplete
				}
			}

		case CrunWaitTaskComplete:
			log.Debugf("[Cfored<->Crun][Step #%d.%d] Enter State Crun_Wait_Task_Complete", jobId, stepId)
		forwarding:
			for {
				select {
				case ctldReply := <-ctldReplyChannel:
					if ctldReply.Type != protos.StreamCtldReply_TASK_CANCEL_REQUEST {
						log.Warningf("[Ctld->Cfored->Crun][Step #%d.%d] Expect type TASK_CANCEL_REQUEST but got %s, ignored",
							jobId, stepId, ctldReply.Type)
					} else {
						log.Debugf("[Ctld->Cfored->Crun][Step #%d.%d] Receive TaskCancelRequest", jobId, stepId)
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
							log.Debugf("[Crun->Cfored][Step #%d.%d] Connection to crun was broken.", jobId, stepId)
							state = CancelTaskOfDeadCrun
							break forwarding
						}
					} else {
						switch crunRequest.Type {
						case protos.StreamCrunRequest_TASK_IO_FORWARD:
							log.Debugf("[Crun->Cfored->Supervisor][Step #%d.%d] Receive TASK_IO_FORWARD Request to"+
								" task, msg size[%d], EOF [%v]", jobId, stepId,
								len(crunRequest.GetPayloadTaskIoForwardReq().GetMsg()),
								crunRequest.GetPayloadTaskIoForwardReq().Eof)
							gSupervisorChanKeeper.forwardCrunRequestToSupervisor(jobId, stepId, crunRequest)

						case protos.StreamCrunRequest_TASK_X11_FORWARD:
							log.Debugf("[Crun->Cfored->Supervisor][Step #%d.%d] Receive Local TASK_X11_FORWARD to remote task",
								jobId, stepId)
							gSupervisorChanKeeper.forwardCrunRequestToSupervisor(jobId, stepId, crunRequest)

						case protos.StreamCrunRequest_TASK_COMPLETION_REQUEST:
							log.Debugf("[Crun->Cfored->Ctld][Step #%d.%d] Receive TaskCompletionRequest", jobId, stepId)
							toCtldRequest := &protos.StreamCforedRequest{
								Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
								Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
									PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
										CforedName:      gVars.hostName,
										JobId:           jobId,
										StepId:          stepId,
										InteractiveType: protos.InteractiveTaskType_Crun,
									},
								},
							}
							gVars.cforedRequestCtldChannel <- toCtldRequest
							state = CrunWaitCtldAck
							break forwarding
						default:
							log.Fatalf("[Crun->Cfored][Step #%d.%d] Expect TASK_COMPLETION_REQUEST or TASK_IO_FORWARD",
								jobId, stepId)
							break forwarding
						}
					}

				case taskMsg := <-TaskIoRequestChannel:
					if taskMsg == nil {
						log.Errorf("[Supervisor->Cfored->Crun][Step #%d.%d] One of Craneds [%v] down. Cancelling the task...",
							jobId, stepId, execCranedIds)
						// IO Channel from Craned was shut down unexpectedly.
						state = CrunWaitTaskCancel
						break forwarding
					}

					if taskMsg.Type == protos.StreamTaskIORequest_TASK_OUTPUT {
						reply = &protos.StreamCrunReply{
							Type: protos.StreamCrunReply_TASK_IO_FORWARD,
							Payload: &protos.StreamCrunReply_PayloadTaskIoForwardReply{
								PayloadTaskIoForwardReply: &protos.StreamCrunReply_TaskIOForwardReply{
									Msg: taskMsg.GetPayloadTaskOutputReq().Msg,
								},
							},
						}
						log.Tracef("[Supervisor->Cfored->Crun][Step #%d.%d] fowarding msg size[%d]",
							jobId, stepId, len(taskMsg.GetPayloadTaskOutputReq().GetMsg()))
					} else if taskMsg.Type == protos.StreamTaskIORequest_TASK_X11_OUTPUT {
						req := taskMsg.GetPayloadTaskX11OutputReq()
						reply = &protos.StreamCrunReply{
							Type: protos.StreamCrunReply_TASK_X11_FORWARD,
							Payload: &protos.StreamCrunReply_PayloadTaskX11ForwardReply{
								PayloadTaskX11ForwardReply: &protos.StreamCrunReply_TaskX11ForwardReply{
									Msg: req.Msg,
								},
							},
						}
						log.Tracef("[Supervisor->Cfored->Crun][Step #%d.%d]  fowarding x11 msg size[%d]",
							jobId, stepId, len(taskMsg.GetPayloadTaskX11OutputReq().Msg))
					} else if taskMsg.Type == protos.StreamTaskIORequest_TASK_EXIT_STATUS {
						req := taskMsg.GetPayloadTaskExitStatusReq()
						reply = &protos.StreamCrunReply{
							Type: protos.StreamCrunReply_TASK_EXIT_STATUS,
							Payload: &protos.StreamCrunReply_PayloadTaskExitStatusReply{
								PayloadTaskExitStatusReply: &protos.StreamCrunReply_TaskExitStatusReply{
									TaskId:   req.TaskId,
									ExitCode: req.ExitCode,
									Signaled: req.Signaled,
								},
							},
						}
						log.Tracef("[Supervisor->Cfored->Crun][Step #%d.%d][Task #%d] fowarding task exit msg ",
							jobId, stepId, req.TaskId)
					} else {
						log.Fatalf("[Supervisor->Cfored->Crun][Step #%d.%d] Expect Type TASK_OUTPUT or TASK_X11_OUTPUT or TASK_EXIT_STATUS.",
							jobId, stepId)
						break forwarding
					}
					if err := toCrunStream.Send(reply); err != nil {
						log.Debugf("[Cfored->Crun][Step #%d.%d] Failed to send %s to crun: %s. "+
							"The connection to crun was broken.", jobId, stepId, taskMsg.Type.String(), err.Error())
						state = CancelTaskOfDeadCrun
						break forwarding
					}
				}
			}

		case CrunWaitTaskCancel:
			log.Debugf("[Cfored<->Crun][Step #%d.%d] Enter State WAIT_CRUN_CANCEL. Sending TASK_CANCEL_REQUEST to Crun...",
				jobId, stepId)

			reply = &protos.StreamCrunReply{
				Type: protos.StreamCrunReply_TASK_CANCEL_REQUEST,
				Payload: &protos.StreamCrunReply_PayloadTaskCancelRequest{
					PayloadTaskCancelRequest: &protos.StreamCrunReply_TaskCancelRequest{},
				},
			}

			if err := toCrunStream.Send(reply); err != nil {
				log.Debugf("[Cfored->Crun][Step #%d.%d]  Failed to send CancelRequest to crun: %s. "+
					"The connection to crun was broken.", jobId, stepId, err.Error())
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
						log.Debugf("[Cfored<->Crun][Step #%d.%d]  Connection to crun was broken.", jobId, stepId)
						state = CancelTaskOfDeadCrun
						crunErr = true
					}
					break
				}
				if crunRequest.Type != protos.StreamCrunRequest_TASK_COMPLETION_REQUEST {
					log.Warningf("[Crun->Cfored][Step #%d.%d] Expect TASK_COMPLETION_REQUEST but %s received. Ignoring it...",
						jobId, stepId, crunRequest.Type)
				} else {
					log.Tracef("[Crun->Cfored][Step #%d.%d] TASK_COMPLETION_REQUEST received",
						jobId, stepId)
					break
				}
			}
			if crunErr {
				break
			}
			log.Debugf("[Crun->Cfored][Step #%d.%d] Receive TaskCompletionRequest", jobId, stepId)

			toCtldRequest := &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
				Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
					PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
						CforedName:      gVars.hostName,
						JobId:           jobId,
						StepId:          stepId,
						InteractiveType: protos.InteractiveTaskType_Crun,
					},
				},
			}
			gVars.cforedRequestCtldChannel <- toCtldRequest

			state = CrunWaitCtldAck

		case CrunWaitCtldAck:
			log.Infof("[Cfored<->Crun][Step #%d.%d] Enter State WAIT_CTLD_ACK", jobId, stepId)

			ctldReply := <-ctldReplyChannel
			if ctldReply.Type != protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY {
				log.Warningf("[Ctld->Cfored->Crun][Step #%d.%d]  Expect TASK_COMPLETION_ACK_REPLY, "+
					"but %s received. Ignoring it...", jobId, stepId, ctldReply.Type)
				break
			} else {
				log.Tracef("[Ctld->Cfored->Crun][Step #%d.%d]  TASK_COMPLETION_ACK_REPLY received",
					jobId, stepId)
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
			delete(gVars.ctldReplyChannelMapByStep, step)
			gVars.ctldReplyChannelMapMtx.Unlock()
			gSupervisorChanKeeper.crunTaskStopAndRemoveChannel(jobId, stepId)

			if err := toCrunStream.Send(reply); err != nil {
				log.Errorf("[Cfored->Crun] Failed to send CompletionAck to crun: %s. "+
					"The connection to crun was broken.", err.Error())
			} else {
				log.Debug("[Cfored->Crun] TASK_COMPLETION_ACK_REPLY sent to Crun")
			}
			log.Infof("[Cfored<->Crun][Step #%d.%d] Step completed successfully", jobId, stepId)

			break CforedCrunStateMachineLoop

		case CancelTaskOfDeadCrun:
			log.Infof("[Cfored<->Crun][Step #%d.%d] Enter State CANCEL_TASK_OF_DEAD_CRUN", jobId, stepId)

			toCtldRequest := &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
				Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
					PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
						CforedName:      gVars.hostName,
						JobId:           jobId,
						StepId:          stepId,
						InteractiveType: protos.InteractiveTaskType_Crun,
					},
				},
			}
			gVars.cforedRequestCtldChannel <- toCtldRequest

			for {
				ctldReply := <-ctldReplyChannel
				if ctldReply.Type != protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY {
					log.Tracef("[Cfored<->Crun] Expect TASK_COMPLETION_ACK_REPLY from Ctld, "+
						"but %s received. Just ignore it...", ctldReply.Type.String())
				} else {
					break
				}
			}

			gVars.ctldReplyChannelMapMtx.Lock()
			if jobId != math.MaxUint32 {
				delete(gVars.ctldReplyChannelMapByStep, step)

				gVars.pidStepMapMtx.Lock()
				delete(gVars.pidStepMap, crunPid)
				gVars.pidStepMapMtx.Unlock()

				gSupervisorChanKeeper.crunTaskStopAndRemoveChannel(jobId, stepId)
			} else {
				log.Fatal("Task id should not equal MaxUint32 in CancelTaskOfDeadCrun")
			}
			gVars.ctldReplyChannelMapMtx.Unlock()

			log.Infof("[Cfored<->Crun][Step #%d.%d] Job cancelled due to Crun down", jobId, stepId)

			break CforedCrunStateMachineLoop
		}
	}

	return nil

}
