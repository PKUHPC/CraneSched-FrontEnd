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
	taskIORequestChannelMap map[StepIdentifier]chan *protos.StreamCforedTaskIORequest
}

var gCranedChanKeeper *SupervisorChannelKeeper

func NewCranedChannelKeeper() *SupervisorChannelKeeper {
	keeper := &SupervisorChannelKeeper{}
	keeper.toSupervisorChannelCV = sync.NewCond(&keeper.toSupervisorChannelMtx)
	keeper.toSupervisorChannels = make(map[StepIdentifier]map[string]*CrunRequestSupervisorChannel)
	keeper.taskIORequestChannelMap = make(map[StepIdentifier]chan *protos.StreamCforedTaskIORequest)
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
	log.Tracef("[Cfored<->Crun] Waiting for task #%d step#%d's %d related craned up", taskId, stepId, len(cranedIds))
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
			log.Debug("[Cfored<->Crun] All related craned up now")
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
		log.Warningf("Task #%d Step#%d Supervisor on Craned %s crashed but no crun found, skiping.", taskId, stepId, cranedId)
	}
	keeper.taskIORequestChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) forwardCrunRequestToSupervisor(taskId uint32, stepId uint32, request *protos.StreamCrunRequest) {
	stepIdentity := StepIdentifier{taskId: taskId, StepId: stepId}
	keeper.toSupervisorChannelMtx.Lock()
	defer keeper.toSupervisorChannelMtx.Unlock()
	stepChannels, exist := keeper.toSupervisorChannels[stepIdentity]
	if !exist {
		log.Errorf("Trying to forward crun request to non-exist task #%d step#%d", taskId, stepId)
		return
	}
	for cranedId, supervisorChannel := range stepChannels {
		if !supervisorChannel.valid.Load() {
			log.Tracef("Ignoring crun request to invalid supervisor for task #%d step#%d on Craned %s", taskId, stepId, cranedId)
			continue
		}
		select {
		case supervisorChannel.requestChannel <- request:
		default:
			if len(supervisorChannel.requestChannel) == cap(supervisorChannel.requestChannel) {
				log.Errorf("toSupervisorChannel to supervisor task #%d step#%d on%s is full", taskId, stepId, cranedId)
			} else {
				log.Errorf("toSupervisorChannel to supervisor task #%d step#%d on%s write failed", taskId, stepId, cranedId)
			}
		}
	}
}

func (keeper *SupervisorChannelKeeper) setRemoteIoToCrunChannel(taskId uint32, stepId uint32, ioToCrunChannel chan *protos.StreamCforedTaskIORequest) {
	keeper.taskIORequestChannelMtx.Lock()
	keeper.taskIORequestChannelMap[StepIdentifier{taskId: taskId, StepId: stepId}] = ioToCrunChannel
	keeper.taskIORequestChannelMtx.Unlock()
}

func (keeper *SupervisorChannelKeeper) forwardRemoteIoToCrun(taskId uint32, stepId uint32, ioToCrun *protos.StreamCforedTaskIORequest) {
	keeper.taskIORequestChannelMtx.Lock()
	channel, exist := keeper.taskIORequestChannelMap[StepIdentifier{taskId: taskId, StepId: stepId}]
	if exist {
		channel <- ioToCrun
	} else {
		log.Warningf("Trying forward to I/O to an unknown crun of task #%d step#%d.", taskId, stepId)
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
	var reply *protos.StreamCforedTaskIOReply

	requestChannel := make(chan grpcMessage[protos.StreamCforedTaskIORequest], 8)
	go grpcStreamReceiver[protos.StreamCforedTaskIORequest](toSupervisorStream, requestChannel)

	pendingCrunReqToCranedChannel := make(chan *protos.StreamCrunRequest, 2)

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

			if cranedReq.Type != protos.StreamCforedTaskIORequest_SUPERVISOR_REGISTER {
				log.Fatal("[Cfored<-Supervisor] Expect SUPERVISOR_REGISTER")
			}

			cranedId = cranedReq.GetPayloadRegisterReq().GetCranedId()
			taskId = cranedReq.GetPayloadRegisterReq().GetTaskId()
			stepId = cranedReq.GetPayloadRegisterReq().GetStepId()
			log.Debugf("[Cfored<-Supervisor] Receive SupervisorReg from %s for task #%d step #%d", cranedId, taskId, stepId)

			gCranedChanKeeper.supervisorUpAndSetMsgToSupervisorChannel(taskId, stepId, cranedId, pendingCrunReqToCranedChannel, valid)

			reply = &protos.StreamCforedTaskIOReply{
				Type: protos.StreamCforedTaskIOReply_SUPERVISOR_REGISTER_REPLY,
				Payload: &protos.StreamCforedTaskIOReply_PayloadSupervisorRegisterReply{
					PayloadSupervisorRegisterReply: &protos.StreamCforedTaskIOReply_SupervisorRegisterReply{
						Ok: true,
					},
				},
			}
			err = toSupervisorStream.Send(reply)
			if err != nil {
				log.Debugf("[Cfored->Supervisor] Connection to Supervisor for task #%d step #%d on Craned %s was broken.", taskId, stepId, cranedId)
				state = SupervisorUnReg
			} else {
				state = IOForwarding
			}

		case IOForwarding:
			log.Debugf("[Cfored<->Supervisor] Enter State IOForwarding to Supervisor for task #%d step #%d on Craned %s", taskId, stepId, cranedId)
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
							log.Errorf("[Cfored<-Supervisor] Supervisor for task #%d step #%d on Craned %s unexpected down", taskId, stepId, cranedId)
							gCranedChanKeeper.SupervisorCrashAndRemoveAllChannel(0, 0, cranedId)
							state = SupervisorUnReg
							break supervisorIOForwarding
						}
					}

					log.Tracef("[Cfored<-Supervisor] Receive type %s", supervisorReq.Type.String())
					switch supervisorReq.Type {
					case protos.StreamCforedTaskIORequest_TASK_OUTPUT:
						gCranedChanKeeper.forwardRemoteIoToCrun(taskId, stepId, supervisorReq)

					case protos.StreamCforedTaskIORequest_SUPERVISOR_UNREGISTER:
						reply = &protos.StreamCforedTaskIOReply{
							Type: protos.StreamCforedTaskIOReply_SUPERVISOR_UNREGISTER_REPLY,
							Payload: &protos.StreamCforedTaskIOReply_PayloadSupervisorUnregisterReply{
								PayloadSupervisorUnregisterReply: &protos.StreamCforedTaskIOReply_SupervisorUnregisterReply{
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
						log.Fatalf("[Cfored<-Supervisor] Receive Unexpected %s", supervisorReq.Type.String())
						state = SupervisorUnReg
						break supervisorIOForwarding
					}

				case crunReq := <-pendingCrunReqToCranedChannel:
					// Msg from crun
					switch crunReq.Type {
					case protos.StreamCrunRequest_TASK_IO_FORWARD:
						payload := crunReq.GetPayloadTaskIoForwardReq()
						msg := payload.GetMsg()
						log.Debugf("[Cfored->Supervisor] forwarding task %d step%d input %s to Suerpvisor on Craned %s", taskId, stepId, msg, cranedId)
						reply = &protos.StreamCforedTaskIOReply{
							Type: protos.StreamCforedTaskIOReply_SUPERVISOR_TASK_INPUT,
							Payload: &protos.StreamCforedTaskIOReply_PayloadTaskInputReq{
								PayloadTaskInputReq: &protos.StreamCforedTaskIOReply_SupervisorTaskInputReq{
									TaskId: taskId,
									StepId: stepId,
									Msg:    msg,
								},
							},
						}
						if err := toSupervisorStream.Send(reply); err != nil {
							log.Debugf("[Cfored->Supervisor] Connection to Supervisor of task #%d step #%d on Craned %s was broken.", taskId, stepId, cranedId)
							state = SupervisorUnReg
						}
					default:
						log.Fatalf("[Cfored<->Supervisor] Receive Unexpected %s from crun of task #%d step #%d", crunReq.Type.String(), taskId, stepId)
						break supervisorIOForwarding
					}
				}
			}

		case SupervisorUnReg:
			log.Debugf("[Cfored<->Supervisor] Enter State SupervisorUnReg")
			gCranedChanKeeper.supervisorDownAndRemoveChannelToSupervisor(taskId, stepId, cranedId)
			log.Debugf("[Cfored<->Supervisor] Supervisor for task #%d step #%d on Craned %s exit", taskId, stepId, cranedId)
			break CforedSupervisorStateMachineLoop
		}
	}
	return nil
}

func (cforedServer *GrpcCforedServer) QueryTaskIdFromPort(ctx context.Context,
	request *protos.QueryTaskIdFromPortRequest) (*protos.QueryTaskIdFromPortReply, error) {

	var taskId uint32
	var ok bool

	pid, err := util.GetPidFromPort(uint16(request.Port))
	if err != nil {
		return &protos.QueryTaskIdFromPortReply{Ok: false}, nil
	}

	for {
		gVars.pidTaskIdMapMtx.RLock()
		taskId, ok = gVars.pidTaskIdMap[int32(pid)]
		gVars.pidTaskIdMapMtx.RUnlock()

		if ok {
			return &protos.QueryTaskIdFromPortReply{
				Ok:     true,
				TaskId: taskId,
			}, nil
		}

		pid, err = util.GetParentProcessID(pid)
		if err != nil || pid == 1 {
			return &protos.QueryTaskIdFromPortReply{Ok: false}, nil
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

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

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
