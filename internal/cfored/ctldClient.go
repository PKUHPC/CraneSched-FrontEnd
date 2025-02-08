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
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type GrpcCtldClient struct {
	ctldClientStub   protos.CraneCtldClient
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

func (client *GrpcCtldClient) CtldReplyReceiveRoutine(stream protos.CraneCtld_CforedStreamClient) {
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
	var stream protos.CraneCtld_CforedStreamClient
	var err error

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

			var taskId uint32

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

					case protos.StreamCtldReply_TASK_RES_ALLOC_REPLY:
						fallthrough
					case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
						fallthrough
					case protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY:
						switch ctldReply.Type {
						case protos.StreamCtldReply_TASK_RES_ALLOC_REPLY:
							taskId = ctldReply.GetPayloadTaskResAllocReply().TaskId
						case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
							taskId = ctldReply.GetPayloadTaskCancelRequest().TaskId
						case protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY:
							taskId = ctldReply.GetPayloadTaskCompletionAck().TaskId
						}

						log.Tracef("[Cfored<->Ctld] %s message received. Task Id %d", ctldReply.Type, taskId)

						gVars.ctldReplyChannelMapMtx.Lock()

						toFeCtlReplyChannel, ok := gVars.ctldReplyChannelMapByTaskId[taskId]
						if ok {
							toFeCtlReplyChannel <- ctldReply
						} else {
							log.Warningf("[Cfored<->Ctld] Task Id %d shall exist in "+
								"ctldReplyChannelMapByTaskId!", taskId)
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

			for taskId, c := range gVars.ctldReplyChannelMapByTaskId {
				reply := &protos.StreamCtldReply{
					Type: protos.StreamCtldReply_TASK_CANCEL_REQUEST,
					Payload: &protos.StreamCtldReply_PayloadTaskCancelRequest{
						PayloadTaskCancelRequest: &protos.StreamCtldReply_TaskCancelRequest{
							TaskId: taskId,
						},
					},
				}
				c <- reply
			}

			num := len(gVars.ctldReplyChannelMapByTaskId)
			count := 0

			if num > 0 {
				log.Debugf("[Cfored<->Ctld] Sending cancel request to %d front ends "+
					"with task id allocated.", num)
				for {
					request = <-gVars.cforedRequestCtldChannel
					if request.Type != protos.StreamCforedRequest_TASK_COMPLETION_REQUEST {
						log.Fatal("[Cfored<->Ctld] Expect type TASK_COMPLETION_REQUEST")
					}

					taskId := request.GetPayloadTaskCompleteReq().TaskId

					toCallocCtlReplyChannel, ok := gVars.ctldReplyChannelMapByTaskId[taskId]
					if ok {
						toCallocCtlReplyChannel <- &protos.StreamCtldReply{
							Type: protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY,
							Payload: &protos.StreamCtldReply_PayloadTaskCompletionAck{
								PayloadTaskCompletionAck: &protos.StreamCtldReply_TaskCompletionAckReply{
									TaskId: taskId,
								},
							},
						}
					} else {
						log.Fatalf("[Cfored<->Ctld] Task Id %d shall exist in "+
							"ctldReplyChannelMapByTaskId!", taskId)
					}

					count += 1
					log.Debugf("[Cfored<->Ctld] Receive task completion request of task id %d. "+
						"%d/%d front ends is cancelled", request.GetPayloadTaskCompleteReq().TaskId, count, num)

					if count >= num {
						break
					}
				}
			}

			gVars.ctldReplyChannelMapByTaskId = make(map[uint32]chan *protos.StreamCtldReply)

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
