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
	"context"
	"google.golang.org/grpc"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type GrpcCtldClient struct {
	config           *util.Config
	ctldConn         *grpc.ClientConn
	ctldClientStub   protos.CraneCtldForInternalClient
	ctldReplyChannel chan *protos.StreamCtldReply
}

type StateOfCtldClient int

const (
	StartReg        StateOfCtldClient = 0
	WaitReg         StateOfCtldClient = 1
	WaitChannelReq  StateOfCtldClient = 2
	WaitAllFrontEnd StateOfCtldClient = 3
	GracefulExit    StateOfCtldClient = 4
)

func (client *GrpcCtldClient) CtldReplyReceiveRoutine(stream protos.CraneCtldForInternal_CforedStreamClient) {
	for {
		m := new(protos.StreamCtldReply)
		if err := stream.RecvMsg(m); err != nil {
			client.ctldReplyChannel <- nil
			log.Infof("CtlClientStreamError: %s. "+
				"Exiting CtldReplyReceiveRoutine...", err.Error())
			break
		}
		client.ctldReplyChannel <- m
	}
}

func (client *GrpcCtldClient) StartCtldClientStream(wg *sync.WaitGroup) {
	var request *protos.StreamCforedRequest
	var stream protos.CraneCtldForInternal_CforedStreamClient
	var err error
	client.ctldConn = nil

	state := StartReg
CtldClientStateMachineLoop:
	for {
		switch state {
		case StartReg:
			log.Tracef("[Cfored<->Ctld] Enter START_REG state.")

			select {
			case <-gVars.globalCtx.Done():
				// SIGINT or SIGTERM received.
				break CtldClientStateMachineLoop
			default: // SIGINT or SIGTERM received.
			}
			if client.ctldConn != nil {
				err = client.ctldConn.Close()
				if err != nil {
					log.Errorf("Failed to close to ctld connection: %s", err.Error())
				}
			}

			client.ctldConn, client.ctldClientStub = util.GetStubToCtldForInternalByConfig(client.config)
			stream, err = client.ctldClientStub.CforedStream(context.Background())
			if err != nil {
				log.Errorf("[Cfored<->Ctld] Cannot connect to CraneCtld: %s. "+
					"Waiting for 1 second and reconnect...", err.Error())
				time.Sleep(time.Second * 1)
				continue CtldClientStateMachineLoop
			}
			go client.CtldReplyReceiveRoutine(stream)

			request = &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_CFORED_REGISTRATION,
				Payload: &protos.StreamCforedRequest_PayloadCforedReg{
					PayloadCforedReg: &protos.StreamCforedRequest_CforedReg{
						CforedName: gVars.hostName,
					},
				},
			}

			if err := stream.Send(request); err != nil {
				log.Error("[Cfored<->Ctld] Failed to send registration msg to ctld. " +
					"Wait for 1 second and reconnect...")
				time.Sleep(time.Second * 1)
			} else {
				state = WaitReg
			}

		case WaitReg:
			log.Tracef("[Cfored<->Ctld] Enter WAIT_REG state.")

			select {
			case reply := <-client.ctldReplyChannel:
				if reply == nil {
					log.Error("[Cfored<->Ctld] Failed to receive registration msg from ctld. " +
						"Wait for 1 second and reconnect...")
					state = StartReg
					time.Sleep(time.Second * 1)
				} else {
					if reply.Type != protos.StreamCtldReply_CFORED_REGISTRATION_ACK {
						log.Fatal("[Cfored<->Ctld] Expect CFORED_REGISTRATION_ACK type.")
					}

					if reply.GetPayloadCforedRegAck().Ok {
						log.Infof("[Cfored<->Ctld] Cfored %s successfully registered.", gVars.hostName)

						gVars.ctldConnected.Store(true)
						state = WaitChannelReq
					} else {
						log.Errorf("[Cfored<->Ctld] Failed to register with CraneCtld: %s. Exiting...",
							reply.GetPayloadCforedRegAck().FailureReason)
						gVars.globalCtxCancel()
						break CtldClientStateMachineLoop
					}
				}

			case <-gVars.globalCtx.Done():
				break CtldClientStateMachineLoop
			}

		case WaitChannelReq:
			log.Tracef("[Cfored<->Ctld] Enter WAIT_CHANNEL_REQ state.")

			var jobId uint32
			var stepId uint32

		WaitChannelReqLoop:
			for {
				select {
				case <-gVars.globalCtx.Done():
					state = WaitAllFrontEnd
					break WaitChannelReqLoop

				// Multiplex requests from calloc/crun to ctld.
				case request = <-gVars.cforedRequestCtldChannel:
					if err := stream.Send(request); err != nil {
						log.Error("[Cfored<->Ctld] Failed to forward msg to ctld. " +
							"Connection to ctld is broken.")

						gVars.ctldConnected.Store(false)
						state = WaitAllFrontEnd
						break WaitChannelReqLoop
					}

				// De-multiplex requests from ctl to calloc/crun.
				case ctldReply := <-client.ctldReplyChannel:
					if ctldReply == nil {
						log.Trace("[Cfored<->Ctld] Failed to receive msg from ctld. " +
							"Connection to cfored is broken.")

						gVars.ctldConnected.Store(false)
						state = WaitAllFrontEnd
						break WaitChannelReqLoop
					}

					switch ctldReply.Type {

					case protos.StreamCtldReply_TASK_ID_REPLY:
						frontPid := ctldReply.GetPayloadTaskIdReply().Pid

						gVars.ctldReplyChannelMapMtx.Lock()

						toFrontCtlReplyChannel, ok := gVars.ctldReplyChannelMapByPid[frontPid]
						if ok {
							toFrontCtlReplyChannel <- ctldReply
						} else {
							log.Fatalf("[Cfored<->Ctld] Front pid %d shall exist "+
								"in ctldReplyChannelMapByPid!", frontPid)
						}

						gVars.ctldReplyChannelMapMtx.Unlock()

					case protos.StreamCtldReply_TASK_META_REPLY:
						frontPid := ctldReply.GetPayloadTaskMetaReply().CattachPid

						gVars.ctldReplyChannelMapMtx.Lock()
						toFrontCtlReplyChannel, ok := gVars.ctldReplyChannelMapByPid[frontPid]
						if ok {
							toFrontCtlReplyChannel <- ctldReply
						} else {
							log.Fatalf("[Cfored<->Ctld] Front pid %d shall exist "+
								"in ctldReplyChannelMapByPid!", frontPid)
						}
						gVars.ctldReplyChannelMapMtx.Unlock()

					case protos.StreamCtldReply_TASK_RES_ALLOC_REPLY:
						fallthrough
					case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
						fallthrough
					case protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY:
						switch ctldReply.Type {
						case protos.StreamCtldReply_TASK_RES_ALLOC_REPLY:
							jobId = ctldReply.GetPayloadTaskResAllocReply().JobId
							stepId = ctldReply.GetPayloadTaskResAllocReply().StepId
						case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
							jobId = ctldReply.GetPayloadTaskCancelRequest().JobId
							stepId = ctldReply.GetPayloadTaskCancelRequest().StepId
						case protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY:
							jobId = ctldReply.GetPayloadTaskCompletionAck().JobId
							stepId = ctldReply.GetPayloadTaskCompletionAck().StepId
						}

						log.Tracef("[Cfored<->Ctld][Step #%d.%d] %s message received.", jobId, stepId, ctldReply.Type)

						gVars.ctldReplyChannelMapMtx.Lock()

						toFeCtlReplyChannel, ok := gVars.ctldReplyChannelMapByStep[StepIdentifier{JobId: jobId, StepId: stepId}]
						if ok {
							toFeCtlReplyChannel <- ctldReply
						} else {
							log.Warningf("[Cfored<->Ctld][Step #%d.%d] shall exist in "+
								"ctldReplyChannelMapByStep!", jobId, stepId)
							// TODO: is true?
							if ctldReply.Type == protos.StreamCtldReply_TASK_CANCEL_REQUEST {
								log.Debugf("[Cfored<->Ctld] sending TASK_COMPLETION_REQUEST directly. Job Id #%d ", jobId)
								toCtldRequest := &protos.StreamCforedRequest{
									Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
									Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
										PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
											CforedName:      gVars.hostName,
											JobId:          jobId,
											InteractiveType: protos.InteractiveTaskType_Crun,
										},
									},
								}
								gVars.cforedRequestCtldChannel <- toCtldRequest
							}
						}

						gVars.ctldReplyChannelMapMtx.Unlock()
					}
				}
			}

		case WaitAllFrontEnd:
			log.Tracef("[Cfored<->Ctld] Enter WAIT_ALL_FRONT_END state.")

			gVars.ctldConnected.Store(false)
			gVars.ctldReplyChannelMapMtx.Lock()

			for pid, c := range gVars.ctldReplyChannelMapByPid {
				reply := &protos.StreamCtldReply{
					Type: protos.StreamCtldReply_TASK_ID_REPLY,
					Payload: &protos.StreamCtldReply_PayloadTaskIdReply{
						PayloadTaskIdReply: &protos.StreamCtldReply_TaskIdReply{
							Pid:           pid,
							Ok:            false,
							FailureReason: "Cfored is not connected to CraneCtld.",
						},
					},
				}
				c <- reply
			}

			gVars.ctldReplyChannelMapByPid = make(map[int32]chan *protos.StreamCtldReply)

			for step, c := range gVars.ctldReplyChannelMapByStep {
				reply := &protos.StreamCtldReply{
					Type: protos.StreamCtldReply_TASK_CANCEL_REQUEST,
					Payload: &protos.StreamCtldReply_PayloadTaskCancelRequest{
						PayloadTaskCancelRequest: &protos.StreamCtldReply_TaskCancelRequest{
							JobId:  step.JobId,
							StepId: step.StepId,
						},
					},
				}
				c <- reply
			}

			for step, c := range gVars.ctldReplyChannelMapByStep {
				reply := &protos.StreamCtldReply{
					Type: protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY,
					Payload: &protos.StreamCtldReply_PayloadTaskCompletionAck{
						PayloadTaskCompletionAck: &protos.StreamCtldReply_TaskCompletionAckReply{
							JobId:  step.JobId,
							StepId: step.StepId,
						},
					},
				}
				c <- reply
			}

			num := len(gVars.ctldReplyChannelMapByStep)
			count := 0

			if num > 0 {
				log.Debugf("[Cfored<->Ctld] Sending cancel request to %d front ends "+
					"with task id allocated.", num)
				for {
					request = <-gVars.cforedRequestCtldChannel
					if request.Type != protos.StreamCforedRequest_TASK_COMPLETION_REQUEST {
						log.Fatal("[Cfored<->Ctld] Expect type TASK_COMPLETION_REQUEST")
					}

					jobId := request.GetPayloadTaskCompleteReq().JobId
					stepId := request.GetPayloadTaskCompleteReq().StepId

					toCallocCtlReplyChannel, ok := gVars.ctldReplyChannelMapByStep[StepIdentifier{JobId: jobId, StepId: stepId}]
					if ok {
						toCallocCtlReplyChannel <- &protos.StreamCtldReply{
							Type: protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY,
							Payload: &protos.StreamCtldReply_PayloadTaskCompletionAck{
								PayloadTaskCompletionAck: &protos.StreamCtldReply_TaskCompletionAckReply{
									JobId:  jobId,
									StepId: stepId,
								},
							},
						}
					} else {
						log.Fatalf("[Cfored<->Ctld][Step #%d.%d] Step shall exist in ctldReplyChannelMapByStep!", jobId, stepId)
					}

					count += 1
					log.Debugf("[Cfored<->Ctld][Step #%d.%d] Receive task completion request. %d/%d front ends is cancelled",
						jobId, stepId, count, num)

					if count >= num {
						break
					}
				}
			}

			gVars.ctldReplyChannelMapByStep = make(map[StepIdentifier]chan *protos.StreamCtldReply)

			gVars.ctldReplyChannelMapMtx.Unlock()

			select {
			case <-gVars.globalCtx.Done():
				state = GracefulExit
			default:
				state = StartReg
			}

		case GracefulExit:
			request = &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_CFORED_GRACEFUL_EXIT,
				Payload: &protos.StreamCforedRequest_PayloadGracefulExitReq{
					PayloadGracefulExitReq: &protos.StreamCforedRequest_GracefulExitReq{
						CforedName: gVars.hostName,
					},
				},
			}

			if err = stream.Send(request); err != nil {
				log.Errorf("[Cfored<->Ctld] Failed to send graceful exit msg to ctld: %s. "+
					"Exiting...", err)
				break CtldClientStateMachineLoop
			} else {
				ctldReply := <-client.ctldReplyChannel
				if ctldReply == nil {
					log.Trace("[Cfored<->Ctld] Failed to receive msg from ctld. " +
						"Connection to cfored is broken.")

					gVars.ctldConnected.Store(false)
					break CtldClientStateMachineLoop
				}

				if ctldReply.Type != protos.StreamCtldReply_CFORED_GRACEFUL_EXIT_ACK {
					log.Fatal("[Cfored<->Ctld] Expect CFORED_GRACEFUL_EXIT_ACK type.")
				}

				log.Debugf("Receive CFORED_GRACEFUL_EXIT_ACK with ok = %t",
					ctldReply.GetPayloadGracefulExitAck().Ok)
				break CtldClientStateMachineLoop
			}
		}
	}

	wg.Done()
}
