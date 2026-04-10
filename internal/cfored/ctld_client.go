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
	"sync"
	"time"

	"google.golang.org/grpc"

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

					case protos.StreamCtldReply_JOB_ID_REPLY:
						frontPid := ctldReply.GetPayloadJobIdReply().Pid

						gVars.ctldReplyChannelMapMtx.Lock()

						toFrontCtlReplyChannel, ok := gVars.ctldReplyChannelMapByPid[frontPid]
						if ok {
							toFrontCtlReplyChannel <- ctldReply
						} else {
							log.Fatalf("[Cfored<->Ctld] Front pid %d shall exist "+
								"in ctldReplyChannelMapByPid!", frontPid)
						}

						gVars.ctldReplyChannelMapMtx.Unlock()

					case protos.StreamCtldReply_STEP_META_REPLY:
						frontPid := ctldReply.GetPayloadStepMetaReply().CattachPid

						gVars.ctldReplyChannelMapMtx.Lock()
						// Use the dedicated cattach-by-pid map (see ctldReplyChannelMapForCattachByPid).
						toFrontCtlReplyChannel, ok := gVars.ctldReplyChannelMapForCattachByPid[frontPid]
						if ok {
							toFrontCtlReplyChannel <- ctldReply
						} else {
							// The cattach client may have disconnected before ctld replied.
							// The channel was removed in CattachWaitStepMeta; just drop this reply.
							log.Warnf("[Cfored<->Ctld] STEP_META_REPLY for cattach pid %d "+
								"not found in ctldReplyChannelMapForCattachByPid "+
								"(cattach may have disconnected). Dropping.", frontPid)
						}
						gVars.ctldReplyChannelMapMtx.Unlock()

					case protos.StreamCtldReply_JOB_RES_ALLOC_REPLY:
						fallthrough
					case protos.StreamCtldReply_JOB_CANCEL_REQUEST:
						fallthrough
					case protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY:
						switch ctldReply.Type {
						case protos.StreamCtldReply_JOB_RES_ALLOC_REPLY:
							jobId = ctldReply.GetPayloadJobResAllocReply().JobId
							stepId = ctldReply.GetPayloadJobResAllocReply().StepId
						case protos.StreamCtldReply_JOB_CANCEL_REQUEST:
							jobId = ctldReply.GetPayloadJobCancelRequest().JobId
							stepId = ctldReply.GetPayloadJobCancelRequest().StepId
						case protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY:
							jobId = ctldReply.GetPayloadJobCompletionAck().JobId
							stepId = ctldReply.GetPayloadJobCompletionAck().StepId
						}

						log.Tracef("[Cfored<->Ctld][Step #%d.%d] %s message received.", jobId, stepId, ctldReply.Type)

						gVars.ctldReplyChannelMapMtx.Lock()

						toFeCtlReplyChannel, ok := gVars.ctldReplyChannelMapByStep[StepIdentifier{JobId: jobId, StepId: stepId}]
						if ok {
							toFeCtlReplyChannel <- ctldReply
						} else {
							log.Warningf("[Cfored<->Ctld][Step #%d.%d] shall exist in "+
								"ctldReplyChannelMapByStep!", jobId, stepId)
							if ctldReply.Type == protos.StreamCtldReply_JOB_CANCEL_REQUEST {
								log.Debugf("[Cfored<->Ctld] sending TASK_COMPLETION_REQUEST directly. Job Id #%d ", jobId)
								toCtldRequest := &protos.StreamCforedRequest{
									Type: protos.StreamCforedRequest_JOB_COMPLETION_REQUEST,
									Payload: &protos.StreamCforedRequest_PayloadJobCompleteReq{
										PayloadJobCompleteReq: &protos.StreamCforedRequest_JobCompleteReq{
											CforedName:      gVars.hostName,
											JobId:           jobId,
											StepId:          stepId,
											InteractiveType: protos.InteractiveJobType_Crun,
										},
									},
								}
								gVars.cforedRequestCtldChannel <- toCtldRequest
							}
						}
						// cattach only focus on JOB_COMPLETION_ACK_REPLY
						if ctldReply.Type == protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY {
							toCattachCtlReplyChannelMap, ok := gVars.ctldReplyChannelMapForCattachByStep[StepIdentifier{JobId: jobId, StepId: stepId}]
							if ok {
								for _, toCattachCtlReplyChannel := range toCattachCtlReplyChannelMap {
									toCattachCtlReplyChannel <- ctldReply
								}
							}
						}

						gVars.ctldReplyChannelMapMtx.Unlock()
					}
				}
			}

		case WaitAllFrontEnd:
			log.Tracef("[Cfored<->Ctld] Enter WAIT_ALL_FRONT_END state.")

			gVars.ctldConnected.Store(false)

			// Snapshot all maps and immediately replace them with empty ones while
			// holding the mutex. This keeps the critical section short (no blocking
			// channel sends), avoiding the deadlock where CrunWaitCtldAck or
			// CancelJobOfDeadCrun needs the mutex to clean up while this goroutine
			// holds it and is waiting for JOB_COMPLETION_REQUEST.
			gVars.ctldReplyChannelMapMtx.Lock()
			byPidSnapshot := gVars.ctldReplyChannelMapByPid
			cattachByPidSnapshot := gVars.ctldReplyChannelMapForCattachByPid
			byStepSnapshot := gVars.ctldReplyChannelMapByStep
			cattachByStepSnapshot := gVars.ctldReplyChannelMapForCattachByStep
			gVars.ctldReplyChannelMapByPid = make(map[int32]chan *protos.StreamCtldReply)
			gVars.ctldReplyChannelMapForCattachByPid = make(map[int32]chan *protos.StreamCtldReply)
			gVars.ctldReplyChannelMapByStep = make(map[StepIdentifier]chan *protos.StreamCtldReply)
			gVars.ctldReplyChannelMapForCattachByStep = make(map[StepIdentifier]map[int32]chan *protos.StreamCtldReply)
			gVars.ctldReplyChannelMapMtx.Unlock()
			// Mutex is now released. All subsequent channel sends are done without
			// holding the mutex, preventing deadlock with goroutines that need the
			// mutex to perform their own cleanup (e.g., CrunWaitCtldAck).

			// Notify crun/calloc clients waiting for a job id.
			for pid, c := range byPidSnapshot {
				c <- &protos.StreamCtldReply{
					Type: protos.StreamCtldReply_JOB_ID_REPLY,
					Payload: &protos.StreamCtldReply_PayloadJobIdReply{
						PayloadJobIdReply: &protos.StreamCtldReply_JobIdReply{
							Pid:           pid,
							Ok:            false,
							FailureReason: "Cfored is not connected to CraneCtld.",
						},
					},
				}
			}

			// Notify cattach clients that are waiting for STEP_META_REPLY.
			// JOB_COMPLETION_ACK_REPLY is handled by CattachWaitStepMeta's first case.
			for _, c := range cattachByPidSnapshot {
				c <- &protos.StreamCtldReply{
					Type: protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY,
					Payload: &protos.StreamCtldReply_PayloadJobCompletionAck{
						PayloadJobCompletionAck: &protos.StreamCtldReply_JobCompletionAckReply{},
					},
				}
			}

			// For each active crun/calloc step: send JOB_CANCEL_REQUEST so the
			// goroutine initiates cancellation, then pre-send JOB_COMPLETION_ACK_REPLY
			// so it can exit without a ctld round-trip.
			//
			// Steps already in CrunWaitCtldAck or CancelJobOfDeadCrun will receive
			// JOB_CANCEL_REQUEST (ignored with a warning) then JOB_COMPLETION_ACK_REPLY
			// (accepted) and exit cleanly — without ever needing the mutex while we
			// are watching.
			//
			// We no longer wait for JOB_COMPLETION_REQUEST from each step. Any pending
			// JOB_COMPLETION_REQUESTs that step goroutines later enqueue into
			// cforedRequestCtldChannel will be forwarded to ctld when cfored reconnects.
			// Ctld should handle them idempotently (job already finished / not found).
			if len(byStepSnapshot) > 0 {
				log.Debugf("[Cfored<->Ctld] Notifying %d active steps to cancel.", len(byStepSnapshot))
			}
			for step, c := range byStepSnapshot {
				c <- &protos.StreamCtldReply{
					Type: protos.StreamCtldReply_JOB_CANCEL_REQUEST,
					Payload: &protos.StreamCtldReply_PayloadJobCancelRequest{
						PayloadJobCancelRequest: &protos.StreamCtldReply_JobCancelRequest{
							JobId:  step.JobId,
							StepId: step.StepId,
						},
					},
				}
			}
			for step, c := range byStepSnapshot {
				c <- &protos.StreamCtldReply{
					Type: protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY,
					Payload: &protos.StreamCtldReply_PayloadJobCompletionAck{
						PayloadJobCompletionAck: &protos.StreamCtldReply_JobCompletionAckReply{
							JobId:  step.JobId,
							StepId: step.StepId,
						},
					},
				}
			}

			// Notify cattach clients that are in IO forwarding state.
			for step, toCattachCtlReplyChannelMap := range cattachByStepSnapshot {
				for _, c := range toCattachCtlReplyChannelMap {
					c <- &protos.StreamCtldReply{
						Type: protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY,
						Payload: &protos.StreamCtldReply_PayloadJobCompletionAck{
							PayloadJobCompletionAck: &protos.StreamCtldReply_JobCompletionAckReply{
								JobId:  step.JobId,
								StepId: step.StepId,
							},
						},
					}
				}
			}

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
