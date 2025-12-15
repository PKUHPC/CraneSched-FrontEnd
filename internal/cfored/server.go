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
	data     []*protos.StreamTaskIORequest
	head     int
	size     int
	capacity int
}

func (t *TaskIOBuffer) Push(item *protos.StreamTaskIORequest) {
	t.data[t.head] = item
	t.head = (t.head + 1) % t.capacity
	if t.size < t.capacity {
		t.size++
	}
}

func (t *TaskIOBuffer) GetHistory() []*protos.StreamTaskIORequest {
	history := make([]*protos.StreamTaskIORequest, t.size)
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

	taskIORequestChannelMtx sync.Mutex
	// I/O message from Supervisor to Crun/Cattach
	taskIORequestChannelMap map[StepIdentifier]map[int32]chan *protos.StreamTaskIORequest

	taskIOBufferMap map[StepIdentifier]*TaskIOBuffer
}

var gSupervisorChanKeeper *SupervisorChannelKeeper

func NewCranedChannelKeeper() *SupervisorChannelKeeper {
	keeper := &SupervisorChannelKeeper{}
	keeper.toSupervisorChannelCV = sync.NewCond(&keeper.toSupervisorChannelMtx)
	keeper.toSupervisorChannels = make(map[StepIdentifier]map[string]*RequestSupervisorChannel)
	keeper.taskIORequestChannelMap = make(map[StepIdentifier]map[int32]chan *protos.StreamTaskIORequest)
	keeper.taskIOBufferMap = make(map[StepIdentifier]*TaskIOBuffer)
	return keeper
}

func (keeper *SupervisorChannelKeeper) supervisorUpAndSetMsgToSupervisorChannel(taskId uint32, stepId uint32, cranedId string, msgChannel chan *protos.StreamCrunRequest, cattachMsgChannel chan *protos.StreamCattachRequest, valid *atomic.Bool) {
	keeper.toSupervisorChannelMtx.Lock()
	stepIdentity := StepIdentifier{JobId: taskId, StepId: stepId}
	if _, exist := keeper.toSupervisorChannels[stepIdentity]; !exist {
		keeper.toSupervisorChannels[stepIdentity] = make(map[string]*RequestSupervisorChannel)
	}
	keeper.toSupervisorChannels[stepIdentity][cranedId] = &RequestSupervisorChannel{crunRequestChannel: msgChannel, valid: valid, cattachRequestChannelMap: cattachMsgChannel}
	keeper.toSupervisorChannelCV.Broadcast()
	keeper.toSupervisorChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) supervisorDownAndRemoveChannelToSupervisor(taskId uint32, stepId uint32, cranedId string) {
	keeper.toSupervisorChannelMtx.Lock()
	stepIdentity := StepIdentifier{JobId: taskId, StepId: stepId}
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

func (keeper *SupervisorChannelKeeper) waitSupervisorChannelsReady(cranedIds []string, readyChan chan bool, stopWaiting *atomic.Bool, taskId uint32, stepId uint32) {
	log.Tracef("[Cfored<->Crun][Step #%d.%d] Waiting for step related craned [%v] up", taskId, stepId, cranedIds)
	keeper.toSupervisorChannelMtx.Lock()
	defer keeper.toSupervisorChannelMtx.Unlock()
	stepIdentity := StepIdentifier{JobId: taskId, StepId: stepId}
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
			log.Debugf("[Cfored<->Crun][Job #%d.%d] All related craned up now", taskId, stepId)
			readyChan <- true
			break
		}
	}
}

func (keeper *SupervisorChannelKeeper) SupervisorCrashAndRemoveAllChannel(taskId uint32, stepId uint32, cranedId string) {
	stepIdentity := StepIdentifier{JobId: taskId, StepId: stepId}
	keeper.taskIORequestChannelMtx.Lock()
	channelMap, exist := keeper.taskIORequestChannelMap[stepIdentity]

	if exist {
		for _, channel := range channelMap {
			channel <- nil
		}
	} else {
		log.Warningf("[Supervisor->Cfored][Job #%d.%d] Supervisor on Craned %s"+
			" crashed but no crun/cattach found, skiping.", taskId, stepId, cranedId)
	}
	keeper.taskIORequestChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) forwardCrunRequestToSupervisor(taskId uint32, stepId uint32, request *protos.StreamCrunRequest) {
	stepIdentity := StepIdentifier{JobId: taskId, StepId: stepId}
	keeper.toSupervisorChannelMtx.Lock()
	defer keeper.toSupervisorChannelMtx.Unlock()
	stepChannels, exist := keeper.toSupervisorChannels[stepIdentity]
	if !exist {
		log.Errorf("[Job #%d.%d] Trying to forward crun request to non-exist step.", taskId, stepId)
		return
	}
	for cranedId, supervisorChannel := range stepChannels {
		if !supervisorChannel.valid.Load() {
			log.Tracef("[Job #%d.%d] Ignoring crun request to invalid supervisor on Craned %s", taskId, stepId, cranedId)
			continue
		}
		select {
		case supervisorChannel.crunRequestChannel <- request:
		default:
			if len(supervisorChannel.crunRequestChannel) == cap(supervisorChannel.crunRequestChannel) {
				log.Errorf("[Job #%d.%d] toSupervisorChannel to supervisor on%s is full", taskId, stepId, cranedId)
			} else {
				log.Errorf("[Job #%d.%d] toSupervisorChannel to supervisor on%s write failed", taskId, stepId, cranedId)
			}
		}
	}
}

func (keeper *SupervisorChannelKeeper) forwardCattachRequestToSupervisor(taskId uint32, stepId uint32, request *protos.StreamCattachRequest) {
	stepIdentity := StepIdentifier{JobId: taskId, StepId: stepId}
	keeper.toSupervisorChannelMtx.Lock()
	defer keeper.toSupervisorChannelMtx.Unlock()
	stepChannels, exist := keeper.toSupervisorChannels[stepIdentity]
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

func (keeper *SupervisorChannelKeeper) setRemoteIoToCrunChannel(frontId int32, taskId uint32, stepId uint32, ioToCrunChannel chan *protos.StreamTaskIORequest) {
	keeper.taskIORequestChannelMtx.Lock()
	if keeper.taskIORequestChannelMap[StepIdentifier{JobId: taskId, StepId: stepId}] == nil {
		keeper.taskIORequestChannelMap[StepIdentifier{JobId: taskId, StepId: stepId}] = make(map[int32]chan *protos.StreamTaskIORequest)
	}
	keeper.taskIORequestChannelMap[StepIdentifier{JobId: taskId, StepId: stepId}][frontId] = ioToCrunChannel
	if keeper.taskIOBufferMap[StepIdentifier{JobId: taskId, StepId: stepId}] == nil {
		keeper.taskIOBufferMap[StepIdentifier{JobId: taskId, StepId: stepId}] = &TaskIOBuffer{
			data:     make([]*protos.StreamTaskIORequest, 10),
			head:     0,
			size:     0,
			capacity: 10,
		}
	}

	keeper.taskIORequestChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) getRemoteHistory(taskId uint32, stepId uint32) []*protos.StreamTaskIORequest {
	keeper.taskIORequestChannelMtx.Lock()
	defer keeper.taskIORequestChannelMtx.Unlock()

	taskIOBuffer, exist := keeper.taskIOBufferMap[StepIdentifier{taskId: taskId, StepId: stepId}]
	if exist {
		return taskIOBuffer.GetHistory()
	}

	return []*protos.StreamTaskIORequest{}
}

func (keeper *SupervisorChannelKeeper) forwardRemoteIoToCrun(taskId uint32, stepId uint32, ioToCrun *protos.StreamTaskIORequest) {
	keeper.taskIORequestChannelMtx.Lock()
	channelMap, exist := keeper.taskIORequestChannelMap[StepIdentifier{JobId: taskId, StepId: stepId}]
	if exist {
		// maybe too much msg, cfored will hang.
		for _, channel := range channelMap {
			channel <- ioToCrun
		}
		keeper.taskIOBufferMap[StepIdentifier{taskId: taskId, StepId: stepId}].Push(ioToCrun)
	} else {
		log.Warningf("[Supervisor->Cfored->FrontEnd][Job #%d.%d]Trying forward to I/O to an unknown crun/cattach.", taskId, stepId)
	}
	keeper.taskIORequestChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) crunTaskStopAndRemoveChannel(taskId uint32, stepId uint32) {
	keeper.taskIORequestChannelMtx.Lock()
	delete(keeper.taskIORequestChannelMap, StepIdentifier{JobId: taskId, StepId: stepId})
	delete(keeper.taskIOBufferMap, StepIdentifier{JobId: taskId, StepId: stepId})
	keeper.taskIORequestChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) cattachStopAndRemoveChannel(taskId uint32, stepId uint32, cattachPid int32) {
	keeper.taskIORequestChannelMtx.Lock()
	if (keeper.taskIORequestChannelMap[StepIdentifier{JobId: taskId, StepId: stepId}] != nil) {
		delete(keeper.taskIORequestChannelMap[StepIdentifier{JobId: taskId, StepId: stepId}], cattachPid)
	}
	keeper.taskIORequestChannelMtx.Unlock()
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

func (cforedServer *GrpcCforedServer) TaskIOStream(toSupervisorStream protos.CraneForeD_TaskIOStreamServer) error {
	var cranedId string
	var jobId uint32
	var stepId uint32
	var reply *protos.StreamTaskIOReply

	requestChannel := make(chan grpcMessage[protos.StreamTaskIORequest], 8)
	go grpcStreamReceiver[protos.StreamTaskIORequest](toSupervisorStream, requestChannel)

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
			item := <-requestChannel
			cranedReq, err := item.message, item.err
			if err != nil { // Failure Edge
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Fatal(err)
					return nil
				}
			}

			if cranedReq.Type != protos.StreamTaskIORequest_SUPERVISOR_REGISTER {
				log.Fatal("[Supervisor->Cfored] Expect SUPERVISOR_REGISTER")
			}

			cranedId = cranedReq.GetPayloadRegisterReq().GetCranedId()
			jobId = cranedReq.GetPayloadRegisterReq().GetJobId()
			stepId = cranedReq.GetPayloadRegisterReq().GetStepId()
			log.Debugf("[Supervisor->Cfored][Step #%d.%d] Receive SupervisorReg from node %s", jobId, stepId, cranedId)

			gSupervisorChanKeeper.supervisorUpAndSetMsgToSupervisorChannel(jobId, stepId, cranedId, pendingCrunReqToSupervisorChannel, pendingCattachReqToSupervisorChannel, valid)

			reply = &protos.StreamTaskIOReply{
				Type: protos.StreamTaskIOReply_SUPERVISOR_REGISTER_REPLY,
				Payload: &protos.StreamTaskIOReply_PayloadSupervisorRegisterReply{
					PayloadSupervisorRegisterReply: &protos.StreamTaskIOReply_SupervisorRegisterReply{
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
					case protos.StreamTaskIORequest_TASK_OUTPUT:
						log.Tracef("[Supervisor->Cfored][Step #%d.%d] Forwarding remote output", jobId, stepId)
						gSupervisorChanKeeper.forwardRemoteIoToCrun(jobId, stepId, supervisorReq)

					case protos.StreamTaskIORequest_TASK_X11_OUTPUT:
						log.Tracef("[Supervisor->Cfored][Step #%d.%d] Forwarding remote X11", jobId, stepId)
						gSupervisorChanKeeper.forwardRemoteIoToCrun(jobId, stepId, supervisorReq)

					case protos.StreamTaskIORequest_SUPERVISOR_UNREGISTER:
						log.Debugf("[Supervisor->Cfored][Step #%d.%d] Receive SupervisorUnReg from Craned %s",
							jobId, stepId, cranedId)

						reply = &protos.StreamTaskIOReply{
							Type: protos.StreamTaskIOReply_SUPERVISOR_UNREGISTER_REPLY,
							Payload: &protos.StreamTaskIOReply_PayloadSupervisorUnregisterReply{
								PayloadSupervisorUnregisterReply: &protos.StreamTaskIOReply_SupervisorUnregisterReply{
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
						reply = &protos.StreamTaskIOReply{
							Type: protos.StreamTaskIOReply_TASK_INPUT,
							Payload: &protos.StreamTaskIOReply_PayloadTaskInputReq{
								PayloadTaskInputReq: &protos.StreamTaskIOReply_TaskInputReq{
									Msg: msg,
									Eof: payload.Eof,
								},
							},
						}
						if err := toSupervisorStream.Send(reply); err != nil {
							log.Debugf("[Cfored->Supervisor][Step #%d.%d] Connection to Supervisor "+
								"on Craned %s was broken.", jobId, stepId, cranedId)
							state = SupervisorUnReg
						}
					case protos.StreamCrunRequest_TASK_X11_FORWARD:
						payload := crunReq.GetPayloadTaskX11ForwardReq()
						msg := payload.GetMsg()
						log.Debugf("[Cfored->Supervisor][Step #%d.%d] forwarding len [%d] x11 to Suerpvisor "+
							"on Craned %s", jobId, stepId, len(msg), cranedId)
						reply = &protos.StreamTaskIOReply{
							Type: protos.StreamTaskIOReply_TASK_X11_INPUT,
							Payload: &protos.StreamTaskIOReply_PayloadTaskX11InputReq{
								PayloadTaskX11InputReq: &protos.StreamTaskIOReply_TaskX11InputReq{
									Msg: msg,
								},
							},
						}
						if err := toSupervisorStream.Send(reply); err != nil {
							log.Debugf("[Cfored->Supervisor][Step #%d.%d] Connection to Supervisor "+
								"on Craned %s was broken.", jobId, stepId, cranedId)
							state = SupervisorUnReg
						}
					default:
						log.Fatalf("[Cfored<->Supervisor][Step #%d.%d] Receive Unexpected %s from crun ",
							jobId, stepId, crunReq.Type.String())
						break supervisorIOForwarding
					}

				case cattachReq := <-pendingCattachReqToSupervisorChannel:
					switch cattachReq.Type {
					case protos.StreamCattachRequest_TASK_IO_FORWARD:
						payload := cattachReq.GetPayloadTaskIoForwardReq()
						msg := payload.GetMsg()
						log.Debugf("[Cfored->Supervisor][Step #%d.%d] forwarding input len [%d] EOF[%v] to craned %s",
							jobId, stepId, len(msg), payload.Eof, cranedId)
						reply = &protos.StreamTaskIOReply{
							Type: protos.StreamTaskIOReply_TASK_INPUT,
							Payload: &protos.StreamTaskIOReply_PayloadTaskInputReq{
								PayloadTaskInputReq: &protos.StreamTaskIOReply_TaskInputReq{
									jobId:  jobId,
									stepId: stepId,
									Msg:    msg,
									Eof:    payload.Eof,
								},
							},
						}
						if err := toSupervisorStream.Send(reply); err != nil {
							log.Debugf("[Cfored->Supervisor][Step #%d.%d] Connection to Supervisor "+
								"on Craned %s was broken.", jobId, stepId, cranedId)
							state = SupervisorUnReg
						}
					case protos.StreamCattachRequest_TASK_X11_FORWARD:
						payload := cattachReq.GetPayloadTaskX11ForwardReq()
						msg := payload.GetMsg()
						log.Debugf("[Cfored->Supervisor][Step #%d.%d] forwarding len [%d] x11 to Suerpvisor "+
							"on Craned %s", jobId, stepId, len(msg), cranedId)
						reply = &protos.StreamTaskIOReply{
							Type: protos.StreamTaskIOReply_TASK_X11_INPUT,
							Payload: &protos.StreamTaskIOReply_PayloadTaskX11InputReq{
								PayloadTaskX11InputReq: &protos.StreamTaskIOReply_TaskX11InputReq{
									jobId:  jobId,
									StepId: stepId,
									Msg:    msg,
								},
							},
						}
						if err := toSupervisorStream.Send(reply); err != nil {
							log.Debugf("[Cfored->Supervisor][Step #%d.%d] Connection to Supervisor "+
								"on Craned %s was broken.", jobId, stepId, cranedId)
							state = SupervisorUnReg
						}
					default:
						log.Fatalf("[Cfored<->Supervisor][Step #%d.%d] Receive Unexpected %s from crun ",
							jobId, stepId, cattachReq.Type.String())
						break supervisorIOForwarding
					}
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
				unixServer.GracefulStop()
				tcpServer.GracefulStop()
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
