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
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type RequestSupervisorChannel struct {
	valid                    *atomic.Bool
	crunRequestChannel       chan *protos.StreamCrunRequest
	cattachRequestChannelMap chan *protos.StreamCattachRequest
}

type StepIdentifier struct {
	JobId  uint32
	StepId uint32
}

type TaskIOBuffer struct {
	data     []*protos.StreamStepIORequest
	head     int
	size     int
	capacity int
}

func (t *TaskIOBuffer) Push(item *protos.StreamStepIORequest) {
	t.data[t.head] = item
	t.head = (t.head + 1) % t.capacity
	if t.size < t.capacity {
		t.size++
	}
}

func (t *TaskIOBuffer) GetHistory() []*protos.StreamStepIORequest {
	history := make([]*protos.StreamStepIORequest, t.size)
	for i := 0; i < t.size; i++ {
		idx := (t.head + t.capacity - t.size + i) % t.capacity
		history[i] = t.data[idx]
	}
	return history
}

type SupervisorChannelKeeper struct {
	toSupervisorChannelMtx sync.Mutex
	toSupervisorChannelCV  *sync.Cond

	// Request message from Crun/Cattach to Supervisor
	toSupervisorChannels map[StepIdentifier]map[string] /*CranedId*/ *RequestSupervisorChannel

	stepIORequestChannelMtx sync.Mutex
	// I/O message from Supervisor to Crun/Cattach
	stepIORequestChannelMap map[StepIdentifier]map[int32]chan *protos.StreamStepIORequest
	taskIOBufferMap         map[StepIdentifier]*TaskIOBuffer
}

var gSupervisorChanKeeper *SupervisorChannelKeeper

func NewCranedChannelKeeper() *SupervisorChannelKeeper {
	keeper := &SupervisorChannelKeeper{}
	keeper.toSupervisorChannelCV = sync.NewCond(&keeper.toSupervisorChannelMtx)
	keeper.toSupervisorChannels = make(map[StepIdentifier]map[string]*RequestSupervisorChannel)
	keeper.stepIORequestChannelMap = make(map[StepIdentifier]map[int32]chan *protos.StreamStepIORequest)
	keeper.taskIOBufferMap = make(map[StepIdentifier]*TaskIOBuffer)
	return keeper
}

func (keeper *SupervisorChannelKeeper) supervisorUpAndSetMsgToSupervisorChannel(jobId uint32, stepId uint32, cranedId string, msgChannel chan *protos.StreamCrunRequest, cattachMsgChannel chan *protos.StreamCattachRequest, valid *atomic.Bool) {
	keeper.toSupervisorChannelMtx.Lock()
	stepIdentity := StepIdentifier{JobId: jobId, StepId: stepId}
	if _, exist := keeper.toSupervisorChannels[stepIdentity]; !exist {
		keeper.toSupervisorChannels[stepIdentity] = make(map[string]*RequestSupervisorChannel)
	}
	keeper.toSupervisorChannels[stepIdentity][cranedId] = &RequestSupervisorChannel{crunRequestChannel: msgChannel, valid: valid, cattachRequestChannelMap: cattachMsgChannel}
	keeper.toSupervisorChannelCV.Broadcast()
	keeper.toSupervisorChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) supervisorDownAndRemoveChannelToSupervisor(jobId uint32, stepId uint32, cranedId string) {
	keeper.toSupervisorChannelMtx.Lock()
	stepIdentity := StepIdentifier{JobId: jobId, StepId: stepId}
	if _, exist := keeper.toSupervisorChannels[stepIdentity]; !exist {
		log.Errorf("Trying to remove a non-exist crun channel")
		return
	} else {
		if _, exist := keeper.toSupervisorChannels[stepIdentity][cranedId]; !exist {
			log.Errorf("Trying to remove a non-exist crun channel")
			return
		} else {
			delete(keeper.toSupervisorChannels[stepIdentity], cranedId)
		}
		if len(keeper.toSupervisorChannels[stepIdentity]) == 0 {
			delete(keeper.toSupervisorChannels, stepIdentity)
		}
	}
	keeper.toSupervisorChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) waitSupervisorChannelsReady(cranedIds []string, readyChan chan bool, stopWaiting *atomic.Bool, jobId uint32, stepId uint32) {
	log.Tracef("[Cfored<->Crun][Step #%d.%d] Waiting for step related craned [%v] up", jobId, stepId, cranedIds)
	keeper.toSupervisorChannelMtx.Lock()
	defer keeper.toSupervisorChannelMtx.Unlock()
	stepIdentity := StepIdentifier{JobId: jobId, StepId: stepId}
	for !stopWaiting.Load() {
		allReady := true
		for _, node := range cranedIds {
			_, exits := keeper.toSupervisorChannels[stepIdentity][node]
			if !exits {
				allReady = false
				break
			}
		}
		if !allReady {
			keeper.toSupervisorChannelCV.Wait() // gVars.toSupervisorChannelMtx is unlocked.
			// Once Wait() returns, the lock is held again.
		} else {
			log.Debugf("[Cfored<->Crun][Step #%d.%d] All related craned up now", jobId, stepId)
			readyChan <- true
			break
		}
	}
}

// broadcastStopWaiting wakes up any goroutine blocked in waitSupervisorChannelsReady
// after stopWaiting has been set to true externally. Without this broadcast, the goroutine
// would remain blocked in toSupervisorChannelCV.Wait() until the next supervisor registers.
func (keeper *SupervisorChannelKeeper) broadcastStopWaiting() {
	keeper.toSupervisorChannelMtx.Lock()
	keeper.toSupervisorChannelCV.Broadcast()
	keeper.toSupervisorChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) SupervisorCrashAndRemoveAllChannel(jobId uint32, stepId uint32, cranedId string) {
	stepIdentity := StepIdentifier{JobId: jobId, StepId: stepId}
	keeper.stepIORequestChannelMtx.Lock()
	channelMap, exist := keeper.stepIORequestChannelMap[stepIdentity]
	// Collect channel references while holding the lock, then send outside the lock
	// to avoid blocking other operations (e.g., setRemoteIoToFrontChannel,
	// crunJobStopAndRemoveChannel) while waiting for slow consumers.
	var channels []chan *protos.StreamStepIORequest
	if exist {
		for _, channel := range channelMap {
			channels = append(channels, channel)
		}
	} else {
		log.Warningf("[Supervisor->Cfored][Step #%d.%d] Supervisor on Craned %s"+
			" crashed but no crun/cattach found, skiping.", jobId, stepId, cranedId)
	}
	keeper.stepIORequestChannelMtx.Unlock()

	// Send nil (crash signal) to all front-ends outside the lock.
	for _, channel := range channels {
		channel <- nil
	}
}

func (keeper *SupervisorChannelKeeper) forwardCrunRequestToSupervisor(jobId uint32, stepId uint32, request *protos.StreamCrunRequest) {
	stepIdentity := StepIdentifier{JobId: jobId, StepId: stepId}
	keeper.toSupervisorChannelMtx.Lock()
	defer keeper.toSupervisorChannelMtx.Unlock()
	stepChannels, exist := keeper.toSupervisorChannels[stepIdentity]
	if !exist {
		log.Errorf("[Step #%d.%d] Trying to forward crun request to non-exist step.", jobId, stepId)
		return
	}
	for cranedId, supervisorChannel := range stepChannels {
		if !supervisorChannel.valid.Load() {
			log.Tracef("[Step #%d.%d] Ignoring crun request to invalid supervisor on Craned %s", jobId, stepId, cranedId)
			continue
		}
		select {
		case supervisorChannel.crunRequestChannel <- request:
		default:
			if len(supervisorChannel.crunRequestChannel) == cap(supervisorChannel.crunRequestChannel) {
				log.Errorf("[Step #%d.%d] toSupervisorChannel to supervisor on%s is full", jobId, stepId, cranedId)
			} else {
				log.Errorf("[Step #%d.%d] toSupervisorChannel to supervisor on%s write failed", jobId, stepId, cranedId)
			}
		}
	}
}

func (keeper *SupervisorChannelKeeper) forwardCrunRequestToSingleSupervisor(jobId uint32, stepId uint32,
	cranedId string, request *protos.StreamCrunRequest) {
	stepIdentity := StepIdentifier{JobId: jobId, StepId: stepId}
	keeper.toSupervisorChannelMtx.Lock()
	defer keeper.toSupervisorChannelMtx.Unlock()
	stepChannels, exist := keeper.toSupervisorChannels[stepIdentity]
	if !exist {
		log.Errorf("[Step #%d.%d] Trying to forward crun request to non-exist step.", jobId, stepId)
		return
	}
	supervisorChannel, exist := stepChannels[cranedId]
	if !exist {
		log.Errorf("[Step #%d.%d] Trying to forward crun request to non-exist craned %s.", jobId, stepId, cranedId)
	}

	select {
	case supervisorChannel.crunRequestChannel <- request:
	default:
		if len(supervisorChannel.crunRequestChannel) == cap(supervisorChannel.crunRequestChannel) {
			log.Errorf("[Step #%d.%d] toSupervisorChannel to supervisor on%s is full", jobId, stepId, cranedId)
		} else {
			log.Errorf("[Step #%d.%d] toSupervisorChannel to supervisor on%s write failed", jobId, stepId, cranedId)
		}
	}
}

func (keeper *SupervisorChannelKeeper) forwardCattachRequestToSupervisor(taskId uint32, stepId uint32, request *protos.StreamCattachRequest) {
	stepIdentity := StepIdentifier{JobId: taskId, StepId: stepId}
	keeper.toSupervisorChannelMtx.Lock()
	defer keeper.toSupervisorChannelMtx.Unlock()
	stepChannels, exist := keeper.toSupervisorChannels[stepIdentity]
	// TODO: when step non-exist, cattach front echo msg and close
	if !exist {
		log.Errorf("[Job #%d.%d] Trying to forward cattach request to non-exist step.", taskId, stepId)
		return
	}
	for cranedId, supervisorChannel := range stepChannels {
		if !supervisorChannel.valid.Load() {
			log.Tracef("[Job #%d.%d] Ignoring cattach request to invalid supervisor on Craned %s", taskId, stepId, cranedId)
			continue
		}
		select {
		case supervisorChannel.cattachRequestChannelMap <- request:
		default:
			if len(supervisorChannel.cattachRequestChannelMap) == cap(supervisorChannel.cattachRequestChannelMap) {
				log.Errorf("[Job #%d.%d] toSupervisorChannel to supervisor on%s is full", taskId, stepId, cranedId)
			} else {
				log.Errorf("[Job #%d.%d] toSupervisorChannel to supervisor on%s write failed", taskId, stepId, cranedId)
			}
		}
	}
}

func (keeper *SupervisorChannelKeeper) setRemoteIoToFrontChannel(frontId int32, jobId uint32, stepId uint32, ioToCrunChannel chan *protos.StreamStepIORequest) {
	keeper.stepIORequestChannelMtx.Lock()
	if keeper.stepIORequestChannelMap[StepIdentifier{JobId: jobId, StepId: stepId}] == nil {
		keeper.stepIORequestChannelMap[StepIdentifier{JobId: jobId, StepId: stepId}] = make(map[int32]chan *protos.StreamStepIORequest)
	}
	keeper.stepIORequestChannelMap[StepIdentifier{JobId: jobId, StepId: stepId}][frontId] = ioToCrunChannel
	if keeper.taskIOBufferMap[StepIdentifier{JobId: jobId, StepId: stepId}] == nil {
		keeper.taskIOBufferMap[StepIdentifier{JobId: jobId, StepId: stepId}] = &TaskIOBuffer{
			data:     make([]*protos.StreamStepIORequest, 10),
			head:     0,
			size:     0,
			capacity: 10,
		}
	}

	keeper.stepIORequestChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) getRemoteHistory(taskId uint32, stepId uint32) []*protos.StreamStepIORequest {
	keeper.stepIORequestChannelMtx.Lock()
	defer keeper.stepIORequestChannelMtx.Unlock()

	taskIOBuffer, exist := keeper.taskIOBufferMap[StepIdentifier{JobId: taskId, StepId: stepId}]
	if exist {
		return taskIOBuffer.GetHistory()
	}

	return []*protos.StreamStepIORequest{}
}

// forwardRemoteIoToFront forwards TASK_OUTPUT / TASK_EXIT_STATUS from Supervisor to all
// connected front-end clients (crun and cattach) and pushes to the history buffer so that
// late-joining cattach clients can replay the output.
//
// The lock is released before performing the blocking channel sends to prevent
// other operations (e.g., setRemoteIoToFrontChannel, crunJobStopAndRemoveChannel,
// cattachStopAndRemoveChannel) from being blocked while waiting for slow consumers.
func (keeper *SupervisorChannelKeeper) forwardRemoteIoToFront(jobId uint32, stepId uint32, ioToFront *protos.StreamStepIORequest) {
	keeper.stepIORequestChannelMtx.Lock()
	channelMap, exist := keeper.stepIORequestChannelMap[StepIdentifier{JobId: jobId, StepId: stepId}]
	// Collect channel references and push to history buffer while holding the lock.
	var channels []chan *protos.StreamStepIORequest
	if exist {
		for _, channel := range channelMap {
			channels = append(channels, channel)
		}
		if buf := keeper.taskIOBufferMap[StepIdentifier{JobId: jobId, StepId: stepId}]; buf != nil {
			buf.Push(ioToFront)
		}
	} else {
		log.Warningf("[Supervisor->Cfored->FrontEnd][Step #%d.%d]Trying forward to I/O to an unknown crun/cattach.", jobId, stepId)
	}
	keeper.stepIORequestChannelMtx.Unlock()

	// Send to all front-ends outside the lock to avoid blocking other lock holders.
	for _, channel := range channels {
		channel <- ioToFront
	}
}


func (keeper *SupervisorChannelKeeper) crunJobStopAndRemoveChannel(jobId uint32, stepId uint32) {
	keeper.stepIORequestChannelMtx.Lock()
	delete(keeper.stepIORequestChannelMap, StepIdentifier{JobId: jobId, StepId: stepId})
	delete(keeper.taskIOBufferMap, StepIdentifier{JobId: jobId, StepId: stepId})
	keeper.stepIORequestChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) cattachStopAndRemoveChannel(taskId uint32, stepId uint32, cattachPid int32) {
	keeper.stepIORequestChannelMtx.Lock()
	if keeper.stepIORequestChannelMap[StepIdentifier{JobId: taskId, StepId: stepId}] != nil {
		delete(keeper.stepIORequestChannelMap[StepIdentifier{JobId: taskId, StepId: stepId}], cattachPid)
	}
	keeper.stepIORequestChannelMtx.Unlock()
}

type GrpcCforedServer struct {
	protos.CraneForeDServer
}

type grpcMessage[T any] struct {
	message *T
	err     error
}

type grpcStreamServer[RecvT any] interface {
	Recv() (*RecvT, error)
	grpc.ServerStream
}

func grpcStreamReceiver[RecvT any](stream grpcStreamServer[RecvT], requestChannel chan grpcMessage[RecvT]) {
	for {
		callocRequest, err := stream.Recv()
		requestChannel <- grpcMessage[RecvT]{
			message: callocRequest,
			err:     err,
		}
		if err != nil {
			break
		}
	}
}

type StateOfCranedServer int

const (
	SupervisorReg   StateOfCranedServer = 0
	IOForwarding    StateOfCranedServer = 1
	SupervisorUnReg StateOfCranedServer = 2
)


func (cforedServer *GrpcCforedServer) StepIOStream(toSupervisorStream protos.CraneForeD_StepIOStreamServer) error {
	var cranedId string
	var jobId uint32
	var stepId uint32
	var reply *protos.StreamStepIOReply

	requestChannel := make(chan grpcMessage[protos.StreamStepIORequest], 8)
	go grpcStreamReceiver[protos.StreamStepIORequest](toSupervisorStream, requestChannel)

	pendingCrunReqToSupervisorChannel := make(chan *protos.StreamCrunRequest, 2)
	pendingCattachReqToSupervisorChannel := make(chan *protos.StreamCattachRequest, 2)

	var valid = &atomic.Bool{}
	valid.Store(true)
	state := SupervisorReg

CforedSupervisorStateMachineLoop:
	for {
		switch state {
		case SupervisorReg:
			log.Debugf("[Cfored<->Supervisor][Step #%d.%d] Enter State SupervisorReg", jobId, stepId)
			// Use a select so that a cfored shutdown (globalCtx cancelled) or a
			// stream-level error (e.g. code=Canceled from GracefulStop) can be
			// handled gracefully instead of calling log.Fatal / os.Exit(1).
			var item grpcMessage[protos.StreamStepIORequest]
			select {
			case item = <-requestChannel:
				// Proceed with normal registration handling below.
			case <-gVars.globalCtx.Done():
				log.Debugf("[Cfored<->Supervisor] Global context cancelled before supervisor registered, exiting.")
				break CforedSupervisorStateMachineLoop
			}
			cranedReq, err := item.message, item.err
			if err != nil { // Failure Edge
				switch err {
				case io.EOF:
					fallthrough
				default:
					// Do not call log.Fatal here — the error is expected during shutdown
					// (gRPC returns codes.Canceled when GracefulStop cancels the stream).
					log.Errorf("[Supervisor->Cfored] Stream error before registration completed: %s", err)
					break CforedSupervisorStateMachineLoop
				}
			}

			if cranedReq.Type != protos.StreamStepIORequest_SUPERVISOR_REGISTER {
				log.Errorf("[Supervisor->Cfored] Expect SUPERVISOR_REGISTER but got %s, exiting.", cranedReq.Type)
				break CforedSupervisorStateMachineLoop
			}

			cranedId = cranedReq.GetPayloadRegisterReq().GetCranedId()
			jobId = cranedReq.GetPayloadRegisterReq().GetJobId()
			stepId = cranedReq.GetPayloadRegisterReq().GetStepId()
			log.Debugf("[Supervisor->Cfored][Step #%d.%d] Receive SupervisorReg from node %s", jobId, stepId, cranedId)

			gSupervisorChanKeeper.supervisorUpAndSetMsgToSupervisorChannel(jobId, stepId, cranedId, pendingCrunReqToSupervisorChannel, pendingCattachReqToSupervisorChannel, valid)

			reply = &protos.StreamStepIOReply{
				Type: protos.StreamStepIOReply_SUPERVISOR_REGISTER_REPLY,
				Payload: &protos.StreamStepIOReply_PayloadSupervisorRegisterReply{
					PayloadSupervisorRegisterReply: &protos.StreamStepIOReply_SupervisorRegisterReply{
						Ok: true,
					},
				},
			}
			err = toSupervisorStream.Send(reply)
			if err != nil {
				log.Debugf("[Cfored->Supervisor][Step #%d.%d] Connection to Supervisor "+
					"on Craned %s was broken.", jobId, stepId, cranedId)
				state = SupervisorUnReg
			} else {
				state = IOForwarding
			}

		case IOForwarding:
			log.Debugf("[Cfored<->Supervisor][Step #%d.%d] Enter State IOForwarding on Craned %s", jobId, stepId, cranedId)
		supervisorIOForwarding:
			for {
				select {
				case item := <-requestChannel:
					// Msg from Supervisor
					supervisorReq, err := item.message, item.err
					if err != nil { // Failure Edge
						// Todo: do something when Supervisor down
						switch err {
						case io.EOF:
							fallthrough
						default:
							log.Errorf("[Supervisor->Cfored][Step #%d.%d] Superviso on Craned %s unexpected down",
								jobId, stepId, cranedId)
							gSupervisorChanKeeper.SupervisorCrashAndRemoveAllChannel(jobId, stepId, cranedId)
							state = SupervisorUnReg
							break supervisorIOForwarding
						}
					}

					log.Tracef("[Supervisor->Cfored][Step #%d.%d] Receive type %s", jobId, stepId, supervisorReq.Type.String())
					switch supervisorReq.Type {
					case protos.StreamStepIORequest_TASK_OUTPUT:
						log.Tracef("[Supervisor->Cfored][Step #%d.%d] Forwarding remote output", jobId, stepId)
						gSupervisorChanKeeper.forwardRemoteIoToFront(jobId, stepId, supervisorReq)

					case protos.StreamStepIORequest_STEP_X11_CONN:
						fallthrough
					case protos.StreamStepIORequest_STEP_X11_OUTPUT:
						fallthrough
					case protos.StreamStepIORequest_STEP_X11_EOF:
						log.Tracef("[Supervisor->Cfored][Step #%d.%d] Forwarding remote %s", jobId, stepId, supervisorReq.Type.String())
						gSupervisorChanKeeper.forwardRemoteIoToFront(jobId, stepId, supervisorReq)

					case protos.StreamStepIORequest_TASK_EXIT_STATUS:
						log.Tracef("[Supervisor->Cfored][Step #%d.%d] Forwarding remote exit status", jobId, stepId)
						gSupervisorChanKeeper.forwardRemoteIoToFront(jobId, stepId, supervisorReq)

					case protos.StreamStepIORequest_SUPERVISOR_UNREGISTER:
						log.Debugf("[Supervisor->Cfored][Step #%d.%d] Receive SupervisorUnReg from Craned %s",
							jobId, stepId, cranedId)

						reply = &protos.StreamStepIOReply{
							Type: protos.StreamStepIOReply_SUPERVISOR_UNREGISTER_REPLY,
							Payload: &protos.StreamStepIOReply_PayloadSupervisorUnregisterReply{
								PayloadSupervisorUnregisterReply: &protos.StreamStepIOReply_SupervisorUnregisterReply{
									Ok: true,
								},
							},
						}
						state = SupervisorUnReg
						valid.Store(false)
						err := toSupervisorStream.Send(reply)
						if err != nil {
							log.Debug("[Cfored->Supervisor] Connection to Supervisor was broken.")
						}
						break supervisorIOForwarding

					default:
						log.Fatalf("[Supervisor->Cfored][Step #%d.%d] Receive Unexpected %s",
							jobId, stepId, supervisorReq.Type.String())
						state = SupervisorUnReg
						break supervisorIOForwarding
					}

				case crunReq := <-pendingCrunReqToSupervisorChannel:
					// Msg from crun to craned
					switch crunReq.Type {
					case protos.StreamCrunRequest_TASK_IO_FORWARD:
						payload := crunReq.GetPayloadTaskIoForwardReq()
						msg := payload.GetMsg()
						log.Debugf("[Cfored->Supervisor][Step #%d.%d] forwarding input len [%d] EOF[%v] to craned %s",
							jobId, stepId, len(msg), payload.Eof, cranedId)
						reply = &protos.StreamStepIOReply{
							Type: protos.StreamStepIOReply_TASK_INPUT,
							Payload: &protos.StreamStepIOReply_PayloadTaskInputReq{
								PayloadTaskInputReq: &protos.StreamStepIOReply_TaskInputReq{
									Msg: msg,
									Eof: payload.Eof,
								},
							},
						}

					case protos.StreamCrunRequest_STEP_X11_FORWARD:
						payload := crunReq.GetPayloadStepX11ForwardReq()
						msg := payload.GetMsg()
						log.Debugf("[Cfored->Supervisor][Step #%d.%d][X11 #%d] forwarding len [%d] x11 to Suerpvisor on Craned %s",
							jobId, stepId, payload.LocalId, len(msg), cranedId)
						reply = &protos.StreamStepIOReply{
							Type: protos.StreamStepIOReply_STEP_X11_INPUT,
							Payload: &protos.StreamStepIOReply_PayloadStepX11InputReq{
								PayloadStepX11InputReq: &protos.StreamStepIOReply_StepX11InputReq{
									Msg:     msg,
									Eof:     payload.Eof,
									LocalId: payload.LocalId,
								},
							},
						}

					default:
						log.Fatalf("[Cfored<->Supervisor][Step #%d.%d] Receive Unexpected %s from crun ",
							jobId, stepId, crunReq.Type.String())
						break supervisorIOForwarding
					}
					if err := toSupervisorStream.Send(reply); err != nil {
						log.Debugf("[Cfored->Supervisor][Step #%d.%d] Connection to Supervisor "+
							"on Craned %s was broken.", jobId, stepId, cranedId)
						state = SupervisorUnReg
						break supervisorIOForwarding
					}

				case cattachReq := <-pendingCattachReqToSupervisorChannel:
					switch cattachReq.Type {
				case protos.StreamCattachRequest_TASK_IO_FORWARD:
					payload := cattachReq.GetPayloadTaskIoForwardReq()
					msg := payload.GetMsg()
					log.Debugf("[Cfored->Supervisor][Step #%d.%d] forwarding input len [%d] EOF[%v] to craned %s",
						jobId, stepId, len(msg), payload.Eof, cranedId)
					reply = &protos.StreamStepIOReply{
						Type: protos.StreamStepIOReply_TASK_INPUT,
						Payload: &protos.StreamStepIOReply_PayloadTaskInputReq{
							PayloadTaskInputReq: &protos.StreamStepIOReply_TaskInputReq{
								Msg:    msg,
								Eof:    payload.Eof,
								TaskId: payload.TaskId, // pass through optional task_id for --input-filter
							},
						},
					}
					if err := toSupervisorStream.Send(reply); err != nil {
							log.Debugf("[Cfored->Supervisor][Step #%d.%d] Connection to Supervisor "+
								"on Craned %s was broken.", jobId, stepId, cranedId)
							state = SupervisorUnReg
						}
					case protos.StreamCattachRequest_STEP_X11_FORWARD:
						payload := cattachReq.GetPayloadStepX11ForwardReq()
						msg := payload.GetMsg()
						log.Debugf("[Cfored->Supervisor][Step #%d.%d] forwarding len [%d] x11 to Supervisor "+
							"on Craned %s", jobId, stepId, len(msg), cranedId)
						reply = &protos.StreamStepIOReply{
							Type: protos.StreamStepIOReply_STEP_X11_INPUT,
							Payload: &protos.StreamStepIOReply_PayloadStepX11InputReq{
								PayloadStepX11InputReq: &protos.StreamStepIOReply_StepX11InputReq{
									Msg:     msg,
									Eof:     payload.Eof,
									LocalId: payload.LocalId,
								},
							},
						}
						if err := toSupervisorStream.Send(reply); err != nil {
							log.Debugf("[Cfored->Supervisor][Step #%d.%d] Connection to Supervisor "+
								"on Craned %s was broken.", jobId, stepId, cranedId)
							state = SupervisorUnReg
						}
					default:
						log.Fatalf("[Cfored<->Supervisor][Step #%d.%d] Receive Unexpected %s from cattach ",
							jobId, stepId, cattachReq.Type.String())
						break supervisorIOForwarding
					}

				case <-gVars.globalCtx.Done():
					// cfored is shutting down (SIGINT received).  Exit the forwarding loop
					// immediately so that GracefulStop() can complete quickly without waiting
					// for the supervisor to disconnect on its own.
					log.Infof("[Cfored<->Supervisor][Step #%d.%d] Global context cancelled, "+
						"exiting IO forwarding loop on Craned %s", jobId, stepId, cranedId)
					state = SupervisorUnReg
					break supervisorIOForwarding
				}
			}

		case SupervisorUnReg:
			log.Debugf("[Cfored<->Supervisor][Step #%d.%d] Enter State SupervisorUnReg", jobId, stepId)
			gSupervisorChanKeeper.supervisorDownAndRemoveChannelToSupervisor(jobId, stepId, cranedId)
			log.Debugf("[Cfored<->Supervisor][Step #%d.%d] Supervisor on Craned %s exit", jobId, stepId, cranedId)
			break CforedSupervisorStateMachineLoop
		}
	}
	return nil
}

func (cforedServer *GrpcCforedServer) QueryStepFromPort(ctx context.Context,
	request *protos.QueryStepFromPortRequest) (*protos.QueryStepFromPortReply, error) {

	var step StepIdentifier
	var ok bool

	pid, err := util.GetPidFromPort(uint16(request.Port))
	if err != nil {
		return &protos.QueryStepFromPortReply{Ok: false}, nil
	}

	for {
		gVars.pidStepMapMtx.RLock()
		step, ok = gVars.pidStepMap[int32(pid)]
		gVars.pidStepMapMtx.RUnlock()

		if ok {
			return &protos.QueryStepFromPortReply{
				Ok:    true,
				JobId: step.JobId,
			}, nil
		}

		pid, err = util.GetParentProcessID(pid)
		if err != nil || pid == 1 {
			return &protos.QueryStepFromPortReply{Ok: false}, nil
		}
	}
}

func startGrpcServer(config *util.Config, wgAllRoutines *sync.WaitGroup) {
	// 1. Unix gRPC Server
	unixSocket, err := util.GetUnixSocket(config.CranedCforedSockPath, 0666)
	if err != nil {
		log.Errorf("Failed to listen on unix socket: %s", err.Error())
		return
	}
	log.Tracef("Listening on unix socket %s", config.CranedCforedSockPath)

	var serverOptions []grpc.ServerOption
	if config.TlsConfig.Enabled {
		creds := &util.UnixPeerCredentials{}
		serverOptions = []grpc.ServerOption{
			grpc.KeepaliveParams(util.ServerKeepAliveParams),
			grpc.KeepaliveEnforcementPolicy(util.ServerKeepAlivePolicy),
			grpc.Creds(creds),
		}
	} else {
		serverOptions = []grpc.ServerOption{
			grpc.KeepaliveParams(util.ServerKeepAliveParams),
			grpc.KeepaliveEnforcementPolicy(util.ServerKeepAlivePolicy),
		}
	}

	unixGrpcServer := grpc.NewServer(serverOptions...)
	cforedServer := GrpcCforedServer{}
	protos.RegisterCraneForeDServer(unixGrpcServer, &cforedServer)

	// 2. TCP gRPC Server
	bindAddr := fmt.Sprintf("%s:%s", util.DefaultCforedServerListenAddress, util.DefaultCforedServerListenPort)
	tcpSocket, err := util.GetTCPSocket(bindAddr, config)
	if err != nil {
		log.Fatalf("Failed to listen on tcp socket: %s", err.Error())
	}
	log.Tracef("Listening on tcp socket %s:%s", util.DefaultCforedServerListenAddress, util.DefaultCforedServerListenPort)

	tcpServerOptions := []grpc.ServerOption{
		grpc.KeepaliveParams(util.ServerKeepAliveParams),
		grpc.KeepaliveEnforcementPolicy(util.ServerKeepAlivePolicy),
	}
	tcpGrpcServer := grpc.NewServer(tcpServerOptions...)
	protos.RegisterCraneForeDServer(tcpGrpcServer, &cforedServer)

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	wgAllRoutines.Add(2)
	go func() {
		defer wgAllRoutines.Done()
		if err := unixGrpcServer.Serve(unixSocket); err != nil {
			log.Errorf("Unix gRPC server stopped: %v", err)
		}
	}()
	go func() {
		defer wgAllRoutines.Done()
		if err := tcpGrpcServer.Serve(tcpSocket); err != nil {
			log.Errorf("TCP gRPC server stopped: %v", err)
		}
	}()

	wgAllRoutines.Add(1)
	go func(sigs chan os.Signal, unixServer *grpc.Server, tcpServer *grpc.Server, wg *sync.WaitGroup) {
		select {
		case sig := <-sigs:
			log.Infof("Receive signal: %s. Exiting...", sig.String())

			switch sig {
			case syscall.SIGINT:
				gVars.globalCtxCancel()
				// GracefulStop waits for all active RPCs to finish, which can block
				// indefinitely if any stream handler (e.g. CattachStream or CrunStream)
				// is stuck.  Give it 30 s and then force-stop both servers so the
				// process always terminates in a bounded time.
				const gracefulStopTimeout = 5 * time.Second
				done := make(chan struct{})
				go func() {
					unixServer.GracefulStop()
					tcpServer.GracefulStop()
					close(done)
				}()
				select {
				case <-done:
					log.Infof("Graceful stop completed.")
				case <-time.After(gracefulStopTimeout):
					log.Warningf("Graceful stop timed out after %s, forcing stop.", gracefulStopTimeout)
					unixServer.Stop()
					tcpServer.Stop()
				}
			case syscall.SIGTERM:
				unixServer.Stop()
				tcpServer.Stop()
			}
		case <-gVars.globalCtx.Done():
			break
		}
		wg.Done()
	}(signals, unixGrpcServer, tcpGrpcServer, wgAllRoutines)

}
