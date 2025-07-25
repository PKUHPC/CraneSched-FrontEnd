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

type CrunRequestSupervisorChannel struct {
	valid          *atomic.Bool
	requestChannel chan *protos.StreamCrunRequest
}

type StepIdentifier struct {
	taskId uint32
	StepId uint32
}

type SupervisorChannelKeeper struct {
	toSupervisorChannelMtx sync.Mutex
	toSupervisorChannelCV  *sync.Cond

	// Request message from Crun to Supervisor
	toSupervisorChannels map[StepIdentifier]map[string] /*CranedId*/ *CrunRequestSupervisorChannel

	taskIORequestChannelMtx sync.Mutex
	// I/O message from Supervisor to Crun
	taskIORequestChannelMap map[StepIdentifier]chan *protos.StreamTaskIORequest
}

var gSupervisorChanKeeper *SupervisorChannelKeeper

func NewCranedChannelKeeper() *SupervisorChannelKeeper {
	keeper := &SupervisorChannelKeeper{}
	keeper.toSupervisorChannelCV = sync.NewCond(&keeper.toSupervisorChannelMtx)
	keeper.toSupervisorChannels = make(map[StepIdentifier]map[string]*CrunRequestSupervisorChannel)
	keeper.taskIORequestChannelMap = make(map[StepIdentifier]chan *protos.StreamTaskIORequest)
	return keeper
}

func (keeper *SupervisorChannelKeeper) supervisorUpAndSetMsgToSupervisorChannel(taskId uint32, stepId uint32, cranedId string, msgChannel chan *protos.StreamCrunRequest, valid *atomic.Bool) {
	keeper.toSupervisorChannelMtx.Lock()
	stepIdentity := StepIdentifier{taskId: taskId, StepId: stepId}
	if _, exist := keeper.toSupervisorChannels[stepIdentity]; !exist {
		keeper.toSupervisorChannels[stepIdentity] = make(map[string]*CrunRequestSupervisorChannel)
	}
	keeper.toSupervisorChannels[stepIdentity][cranedId] = &CrunRequestSupervisorChannel{requestChannel: msgChannel, valid: valid}
	keeper.toSupervisorChannelCV.Broadcast()
	keeper.toSupervisorChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) supervisorDownAndRemoveChannelToSupervisor(taskId uint32, stepId uint32, cranedId string) {
	keeper.toSupervisorChannelMtx.Lock()
	stepIdentity := StepIdentifier{taskId: taskId, StepId: stepId}
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
	log.Tracef("[Cfored<->Crun][Job #%d.%d] Waiting for step related craned [%v] up", taskId, stepId, cranedIds)
	keeper.toSupervisorChannelMtx.Lock()
	defer keeper.toSupervisorChannelMtx.Unlock()
	stepIdentity := StepIdentifier{taskId: taskId, StepId: stepId}
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
	stepIdentity := StepIdentifier{taskId: taskId, StepId: stepId}
	keeper.taskIORequestChannelMtx.Lock()
	channel, exist := keeper.taskIORequestChannelMap[stepIdentity]

	if exist {
		channel <- nil
	} else {
		log.Warningf("[Supervisor->Cfored][Job #%d.%d] Supervisor on Craned %s"+
			" crashed but no crun found, skiping.", taskId, stepId, cranedId)
	}
	keeper.taskIORequestChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) forwardCrunRequestToSupervisor(taskId uint32, stepId uint32, request *protos.StreamCrunRequest) {
	stepIdentity := StepIdentifier{taskId: taskId, StepId: stepId}
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
		case supervisorChannel.requestChannel <- request:
		default:
			if len(supervisorChannel.requestChannel) == cap(supervisorChannel.requestChannel) {
				log.Errorf("[Job #%d.%d] toSupervisorChannel to supervisor on%s is full", taskId, stepId, cranedId)
			} else {
				log.Errorf("[Job #%d.%d] toSupervisorChannel to supervisor on%s write failed", taskId, stepId, cranedId)
			}
		}
	}
}

func (keeper *SupervisorChannelKeeper) setRemoteIoToCrunChannel(taskId uint32, stepId uint32, ioToCrunChannel chan *protos.StreamTaskIORequest) {
	keeper.taskIORequestChannelMtx.Lock()
	keeper.taskIORequestChannelMap[StepIdentifier{taskId: taskId, StepId: stepId}] = ioToCrunChannel
	keeper.taskIORequestChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) forwardRemoteIoToCrun(taskId uint32, stepId uint32, ioToCrun *protos.StreamTaskIORequest) {
	keeper.taskIORequestChannelMtx.Lock()
	channel, exist := keeper.taskIORequestChannelMap[StepIdentifier{taskId: taskId, StepId: stepId}]
	if exist {
		// maybe too much msg, cfored will hang.
		channel <- ioToCrun
	} else {
		log.Warningf("[Supervisor->Cfored->Crun][Job #%d.%d]Trying forward to I/O to an unknown crun.", taskId, stepId)
	}
	keeper.taskIORequestChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) crunTaskStopAndRemoveChannel(taskId uint32, stepId uint32) {
	keeper.taskIORequestChannelMtx.Lock()
	delete(keeper.taskIORequestChannelMap, StepIdentifier{taskId: taskId, StepId: stepId})
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
	var taskId uint32
	var stepId uint32
	var reply *protos.StreamTaskIOReply

	requestChannel := make(chan grpcMessage[protos.StreamTaskIORequest], 8)
	go grpcStreamReceiver[protos.StreamTaskIORequest](toSupervisorStream, requestChannel)

	pendingCrunReqToSupervisorChannel := make(chan *protos.StreamCrunRequest, 2)

	var valid = &atomic.Bool{}
	valid.Store(true)
	state := SupervisorReg

CforedSupervisorStateMachineLoop:
	for {
		switch state {
		case SupervisorReg:
			log.Debugf("[Cfored<->Supervisor] Enter State SupervisorReg")
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
			taskId = cranedReq.GetPayloadRegisterReq().GetTaskId()
			stepId = cranedReq.GetPayloadRegisterReq().GetStepId()
			log.Debugf("[Supervisor->Cfored][Step #%d.%d] Receive SupervisorReg from node %s", taskId, stepId, cranedId)

			gSupervisorChanKeeper.supervisorUpAndSetMsgToSupervisorChannel(taskId, stepId, cranedId, pendingCrunReqToSupervisorChannel, valid)

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
					"on Craned %s was broken.", taskId, stepId, cranedId)
				state = SupervisorUnReg
			} else {
				state = IOForwarding
			}

		case IOForwarding:
			log.Debugf("[Cfored<->Supervisor][Step #%d.%d] Enter State IOForwarding on Craned %s", taskId, stepId, cranedId)
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
								taskId, stepId, cranedId)
							gSupervisorChanKeeper.SupervisorCrashAndRemoveAllChannel(taskId, stepId, cranedId)
							state = SupervisorUnReg
							break supervisorIOForwarding
						}
					}

					log.Tracef("[Supervisor->Cfored][Step #%d.%d] Receive type %s", taskId, stepId, supervisorReq.Type.String())
					switch supervisorReq.Type {
					case protos.StreamTaskIORequest_TASK_OUTPUT:
						log.Tracef("[Supervisor->Cfored][Step #%d.%d] Forwarding remote output", taskId, stepId)
						gSupervisorChanKeeper.forwardRemoteIoToCrun(taskId, stepId, supervisorReq)

					case protos.StreamTaskIORequest_TASK_X11_OUTPUT:
						log.Tracef("[Supervisor->Cfored][Step #%d.%d] Forwarding remote X11", taskId, stepId)
						gSupervisorChanKeeper.forwardRemoteIoToCrun(taskId, stepId, supervisorReq)

					case protos.StreamTaskIORequest_SUPERVISOR_UNREGISTER:
						log.Debugf("[Supervisor->Cfored][Step #%d.%d] Receive SupervisorUnReg from Craned %s",
							taskId, stepId, cranedId)

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
							taskId, stepId, supervisorReq.Type.String())
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
							taskId, stepId, len(msg), payload.Eof, cranedId)
						reply = &protos.StreamTaskIOReply{
							Type: protos.StreamTaskIOReply_TASK_INPUT,
							Payload: &protos.StreamTaskIOReply_PayloadTaskInputReq{
								PayloadTaskInputReq: &protos.StreamTaskIOReply_TaskInputReq{
									TaskId: taskId,
									StepId: stepId,
									Msg:    msg,
									Eof:    payload.Eof,
								},
							},
						}
						if err := toSupervisorStream.Send(reply); err != nil {
							log.Debugf("[Cfored->Supervisor][Step #%d.%d] Connection to Supervisor "+
								"on Craned %s was broken.", taskId, stepId, cranedId)
							state = SupervisorUnReg
						}
					case protos.StreamCrunRequest_TASK_X11_FORWARD:
						payload := crunReq.GetPayloadTaskX11ForwardReq()
						msg := payload.GetMsg()
						log.Debugf("[Cfored->Supervisor][Step #%d.%d] forwarding len [%d] x11 to Suerpvisor "+
							"on Craned %s", taskId, stepId, len(msg), cranedId)
						reply = &protos.StreamTaskIOReply{
							Type: protos.StreamTaskIOReply_TASK_X11_INPUT,
							Payload: &protos.StreamTaskIOReply_PayloadTaskX11InputReq{
								PayloadTaskX11InputReq: &protos.StreamTaskIOReply_TaskX11InputReq{
									TaskId: taskId,
									StepId: stepId,
									Msg:    msg,
								},
							},
						}
						if err := toSupervisorStream.Send(reply); err != nil {
							log.Debugf("[Cfored->Supervisor][Step #%d.%d] Connection to Supervisor "+
								"on Craned %s was broken.", taskId, stepId, cranedId)
							state = SupervisorUnReg
						}
					default:
						log.Fatalf("[Cfored<->Supervisor][Step #%d.%d] Receive Unexpected %s from crun ",
							taskId, stepId, crunReq.Type.String())
						break supervisorIOForwarding
					}
				}
			}

		case SupervisorUnReg:
			log.Debugf("[Cfored<->Supervisor][Step #%d.%d] Enter State SupervisorUnReg", taskId, stepId)
			gSupervisorChanKeeper.supervisorDownAndRemoveChannelToSupervisor(taskId, stepId, cranedId)
			log.Debugf("[Cfored<->Supervisor][Step #%d.%d] Supervisor on Craned %s exit", taskId, stepId, cranedId)
			break CforedSupervisorStateMachineLoop
		}
	}
	return nil
}

func (cforedServer *GrpcCforedServer) QueryStepFromPort(ctx context.Context,
	request *protos.QueryStepFromPortRequest) (*protos.QueryStepFromPortReply, error) {

	var taskId uint32
	var ok bool

	pid, err := util.GetPidFromPort(uint16(request.Port))
	if err != nil {
		return &protos.QueryStepFromPortReply{Ok: false}, nil
	}

	for {
		gVars.pidTaskIdMapMtx.RLock()
		taskId, ok = gVars.pidTaskIdMap[int32(pid)]
		gVars.pidTaskIdMapMtx.RUnlock()

		if ok {
			return &protos.QueryStepFromPortReply{
				Ok:     true,
				TaskId: taskId,
			}, nil
		}

		pid, err = util.GetParentProcessID(pid)
		if err != nil || pid == 1 {
			return &protos.QueryStepFromPortReply{Ok: false}, nil
		}
	}
}

func startGrpcServer(config *util.Config, wgAllRoutines *sync.WaitGroup) {
	socket, err := util.GetUnixSocket(config.CranedCforedSockPath, 0666)
	if err != nil {
		log.Errorf("Failed to listen on unix socket: %s", err.Error())
		return
	}

	log.Tracef("Listening on unix socket %s", config.CranedCforedSockPath)

	var serverOptions []grpc.ServerOption
	if config.UseTls {
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

	grpcServer := grpc.NewServer(serverOptions...)

	cforedServer := GrpcCforedServer{}

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	wgAllRoutines.Add(1)
	go func(sigs chan os.Signal, server *grpc.Server, wg *sync.WaitGroup) {
		select {
		case sig := <-sigs:
			log.Infof("Receive signal: %s. Exiting...", sig.String())

			switch sig {
			case syscall.SIGINT:
				gVars.globalCtxCancel()
				server.GracefulStop()
			case syscall.SIGTERM:
				server.Stop()
			}
		case <-gVars.globalCtx.Done():
			break
		}
		wg.Done()
	}(signals, grpcServer, wgAllRoutines)

	protos.RegisterCraneForeDServer(grpcServer, &cforedServer)

	wgAllRoutines.Add(1)
	go func(wg *sync.WaitGroup) {
		bindAddr := fmt.Sprintf("%s:%s", util.DefaultCforedServerListenAddress, util.DefaultCforedServerListenPort)
		socket, err := util.GetTCPSocket(bindAddr, config)
		if err != nil {
			log.Fatalf("Failed to listen on tcp socket: %s", err.Error())
		}

		err = grpcServer.Serve(socket)
		if err != nil {
			log.Fatal(err)
		}

		wgAllRoutines.Done()
	}(wgAllRoutines)

	err = grpcServer.Serve(socket)
	if err != nil {
		log.Fatal(err)
	}
}
