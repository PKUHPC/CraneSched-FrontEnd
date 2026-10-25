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

// The channel belongs to this stream. An old receiver cannot publish a late
// reply or EOF into a subsequent registration attempt.
func (client *GrpcCtldClient) CtldReplyReceiveRoutine(ctx context.Context, stream protos.CraneCtldForInternal_CforedStreamClient, replies chan<- *protos.StreamCtldReply) {
	for {
		m, err := stream.Recv()
		if err != nil {
			log.Infof("CtlClientStreamError: %s. Exiting CtldReplyReceiveRoutine...", err)
			m = nil
		}
		select {
		case replies <- m:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// Route replies without holding the map lock across channel sends. If Ctld
// cancels an already-removed session, the caller sends its completion directly;
// enqueueing it on our own input channel could deadlock when that queue is full.
func dispatchCtldReply(reply *protos.StreamCtldReply) *protos.StreamCforedRequest {
	var channels []chan *protos.StreamCtldReply
	var completion *protos.StreamCforedRequest
	gVars.ctldReplyChannelMapMtx.Lock()
	switch reply.Type {
	case protos.StreamCtldReply_JOB_ID_REPLY:
		if c := gVars.ctldReplyChannelMapByPid[reply.GetPayloadJobIdReply().Pid]; c != nil {
			channels = append(channels, c)
		}
	case protos.StreamCtldReply_STEP_META_REPLY:
		if c := gVars.ctldReplyChannelMapForCattachByPid[reply.GetPayloadStepMetaReply().CattachPid]; c != nil {
			channels = append(channels, c)
		}
	case protos.StreamCtldReply_JOB_RES_ALLOC_REPLY,
		protos.StreamCtldReply_JOB_CANCEL_REQUEST,
		protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY:
		var step StepIdentifier
		switch reply.Type {
		case protos.StreamCtldReply_JOB_RES_ALLOC_REPLY:
			p := reply.GetPayloadJobResAllocReply()
			step = StepIdentifier{JobId: p.JobId, StepId: p.StepId}
		case protos.StreamCtldReply_JOB_CANCEL_REQUEST:
			p := reply.GetPayloadJobCancelRequest()
			step = StepIdentifier{JobId: p.JobId, StepId: p.StepId}
		case protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY:
			p := reply.GetPayloadJobCompletionAck()
			step = StepIdentifier{JobId: p.JobId, StepId: p.StepId}
		}
		if c := gVars.ctldReplyChannelMapByStep[step]; c != nil {
			channels = append(channels, c)
		} else if reply.Type == protos.StreamCtldReply_JOB_CANCEL_REQUEST {
			completion = &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_JOB_COMPLETION_REQUEST,
				Payload: &protos.StreamCforedRequest_PayloadJobCompleteReq{
					PayloadJobCompleteReq: &protos.StreamCforedRequest_JobCompleteReq{
						CforedName: gVars.hostName, JobId: step.JobId, StepId: step.StepId,
						InteractiveType: protos.InteractiveJobType_Crun,
					},
				},
			}
		}
		if reply.Type == protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY {
			for _, c := range gVars.ctldReplyChannelMapForCattachByStep[step] {
				channels = append(channels, c)
			}
		}
	default:
		log.Warnf("[Cfored<->Ctld] Unexpected reply %s", reply.Type)
	}
	gVars.ctldReplyChannelMapMtx.Unlock()

	for _, c := range channels {
		select {
		case c <- reply:
		case <-gVars.globalCtx.Done():
			return nil
		}
	}
	return completion
}

func (client *GrpcCtldClient) StartCtldClientStream(wg *sync.WaitGroup) {
	defer wg.Done()
	defer gVars.ctldConnected.Store(false)
	client.ctldConn, client.ctldClientStub = util.GetStubToCtldForInternalByConfig(client.config)
	defer client.ctldConn.Close()

	var stream protos.CraneCtldForInternal_CforedStreamClient
	var cancelStream context.CancelFunc
	defer func() {
		if cancelStream != nil {
			cancelStream()
		}
	}()
	// Completion can be retried: Ctld suppresses repeated step termination and
	// acknowledges steps that have already gone. Keep it until the real ACK,
	// including when Send succeeds but the connection loses the reply.
	pendingCompletions := make(map[StepIdentifier]*protos.StreamCforedRequest)
	sendRequest := func(request *protos.StreamCforedRequest) error {
		if request.Type == protos.StreamCforedRequest_JOB_COMPLETION_REQUEST {
			p := request.GetPayloadJobCompleteReq()
			pendingCompletions[StepIdentifier{JobId: p.JobId, StepId: p.StepId}] = request
		}
		// A flow-controlled Send must also unblock on process shutdown.
		stopCancel := context.AfterFunc(gVars.globalCtx, cancelStream)
		defer stopCancel()
		return stream.Send(request)
	}
	state := StartReg
	for {
		switch state {
		case StartReg:
			gVars.ctldConnected.Store(false)
			if gVars.globalCtx.Err() != nil {
				state = WaitAllFrontEnd
				continue
			}
			if cancelStream != nil {
				cancelStream()
				select {
				case <-gVars.globalCtx.Done():
					state = WaitAllFrontEnd
					continue
				case <-time.After(time.Second):
				}
			}
			log.Trace("[Cfored<->Ctld] Enter START_REG state.")
			ctx, cancel := context.WithCancel(context.Background())
			cancelStream = cancel
			// Opening a stream may wait for a transport; shutdown must interrupt it.
			stopCancel := context.AfterFunc(gVars.globalCtx, cancel)
			var err error
			stream, err = client.ctldClientStub.CforedStream(ctx)
			if err == nil {
				err = stream.Send(&protos.StreamCforedRequest{
					Type: protos.StreamCforedRequest_CFORED_REGISTRATION,
					Payload: &protos.StreamCforedRequest_PayloadCforedReg{
						PayloadCforedReg: &protos.StreamCforedRequest_CforedReg{CforedName: gVars.hostName},
					},
				})
			}
			stopCancel()
			if err != nil {
				log.Errorf("[Cfored<->Ctld] Cannot register with CraneCtld: %s. Retrying...", err)
				continue
			}
			client.ctldReplyChannel = make(chan *protos.StreamCtldReply, 8)
			go client.CtldReplyReceiveRoutine(ctx, stream, client.ctldReplyChannel)
			state = WaitReg

		case WaitReg, WaitChannelReq:
			// Keep admitted sessions and their routes across a transport failure.
			// Requests queued while disconnected are sent only after registration.
			var requests <-chan *protos.StreamCforedRequest
			if state == WaitChannelReq {
				requests = gVars.cforedRequestCtldChannel
			}
			select {
			case <-gVars.globalCtx.Done():
				state = WaitAllFrontEnd
			case request := <-requests:
				if err := sendRequest(request); err != nil {
					log.Errorf("[Cfored<->Ctld] Failed to forward %s: %s. Reconnecting...", request.Type, err)
					state = StartReg
				}
			case reply := <-client.ctldReplyChannel:
				if reply == nil {
					state = StartReg
					continue
				}
				if reply.Type == protos.StreamCtldReply_CFORED_REGISTRATION_ACK {
					if !reply.GetPayloadCforedRegAck().Ok {
						log.Errorf("[Cfored<->Ctld] Registration rejected: %s", reply.GetPayloadCforedRegAck().FailureReason)
						gVars.globalCtxCancel()
						state = WaitAllFrontEnd
					} else {
						log.Infof("[Cfored<->Ctld] Cfored %s successfully registered.", gVars.hostName)
						gVars.ctldConnected.Store(true)
						state = WaitChannelReq
						for _, request := range pendingCompletions {
							if err := sendRequest(request); err != nil {
								log.Errorf("[Cfored<->Ctld] Failed to retry completion: %s", err)
								state = StartReg
								break
							}
						}
					}
					continue
				}
				if reply.Type == protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY {
					p := reply.GetPayloadJobCompletionAck()
					delete(pendingCompletions, StepIdentifier{JobId: p.JobId, StepId: p.StepId})
				}
				// Ctld replaces its stream writer before sending registration ACK;
				// an existing session's callback may therefore arrive first.
				if request := dispatchCtldReply(reply); request != nil {
					if err := sendRequest(request); err != nil {
						log.Errorf("[Cfored<->Ctld] Failed to complete removed session: %s", err)
						state = StartReg
					}
				}
			}

		case WaitAllFrontEnd:
			log.Trace("[Cfored<->Ctld] Enter WAIT_ALL_FRONT_END state.")
			gVars.ctldConnected.Store(false)
			// Only process shutdown reaches this state. Handlers observe globalCtx,
			// release their own maps/I/O channels, and then decrement the counter.
			waitFrontEnds()
			state = GracefulExit

		case GracefulExit:
			if stream == nil || stream.Context().Err() != nil {
				return
			}
			request := &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_CFORED_GRACEFUL_EXIT,
				Payload: &protos.StreamCforedRequest_PayloadGracefulExitReq{
					PayloadGracefulExitReq: &protos.StreamCforedRequest_GracefulExitReq{CforedName: gVars.hostName},
				},
			}
			if err := stream.Send(request); err != nil {
				log.Errorf("[Cfored<->Ctld] Failed to notify CraneCtld of shutdown: %s", err)
				return
			}
			for reply := range client.ctldReplyChannel {
				if reply == nil || reply.Type == protos.StreamCtldReply_CFORED_GRACEFUL_EXIT_ACK {
					return
				}
				// Other session replies may already be queued ahead of the exit ACK.
			}
		}
	}
}
