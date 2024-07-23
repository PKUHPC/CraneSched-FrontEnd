/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package cfored

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type CrunRequestCranedChannel struct {
	valid          *atomic.Bool
	requestChannel chan *protos.StreamCrunRequest
}

type CranedChannelKeeper struct {
	crunRequestChannelMtx sync.Mutex
	crunRequestChannelCV  *sync.Cond

	// Request message from Crun to craned
	crunRequestChannelMapByCranedId map[string]*CrunRequestCranedChannel

	taskIORequestChannelMtx sync.Mutex
	// I/O message from Craned to Crun
	taskIORequestChannelMapByTaskId map[uint32]chan *protos.StreamCforedTaskIORequest

	//for error handle
	taskIdProcIdMapMtx sync.Mutex
	taskIdSetByCraned  map[string] /*cranedId*/ map[uint32] /*taskId*/ bool
}

var gCranedChanKeeper *CranedChannelKeeper

func NewCranedChannelKeeper() *CranedChannelKeeper {
	keeper := &CranedChannelKeeper{}
	keeper.crunRequestChannelCV = sync.NewCond(&keeper.crunRequestChannelMtx)
	keeper.crunRequestChannelMapByCranedId = make(map[string]*CrunRequestCranedChannel)
	keeper.taskIORequestChannelMapByTaskId = make(map[uint32]chan *protos.StreamCforedTaskIORequest)
	keeper.taskIORequestChannelMapByTaskId = make(map[uint32]chan *protos.StreamCforedTaskIORequest)
	keeper.taskIdSetByCraned = make(map[string]map[uint32]bool)
	return keeper
}

func (keeper *CranedChannelKeeper) cranedUpAndSetMsgToCranedChannel(cranedId string, msgChannel chan *protos.StreamCrunRequest, valid *atomic.Bool) {
	keeper.crunRequestChannelMtx.Lock()
	keeper.crunRequestChannelMapByCranedId[cranedId] = &CrunRequestCranedChannel{requestChannel: msgChannel, valid: valid}
	keeper.crunRequestChannelCV.Broadcast()
	keeper.crunRequestChannelMtx.Unlock()
}

func (keeper *CranedChannelKeeper) cranedDownAndRemoveChannelToCraned(cranedId string) {
	keeper.crunRequestChannelMtx.Lock()
	delete(keeper.crunRequestChannelMapByCranedId, cranedId)
	keeper.crunRequestChannelMtx.Unlock()
}

func (keeper *CranedChannelKeeper) waitCranedChannelsReady(cranedIds []string, readyChan chan bool, stopWaiting *atomic.Bool, taskId uint32) {
	keeper.crunRequestChannelMtx.Lock()
	defer keeper.crunRequestChannelMtx.Unlock()
	for !stopWaiting.Load() {
		allReady := true
		for _, node := range cranedIds {
			_, exits := keeper.crunRequestChannelMapByCranedId[node]
			if !exits {
				allReady = false
				break
			}
		}

		if !allReady {
			keeper.crunRequestChannelCV.Wait() // gVars.crunRequestChannelMtx is unlocked.
			// Once Wait() returns, the lock is held again.
		} else {
			log.Debug("[Cfored<->Crun] All related craned up now")
			keeper.taskIdProcIdMapMtx.Lock()
			for _, cranedId := range cranedIds {
				if _, exist := keeper.taskIdSetByCraned[cranedId]; !exist {
					keeper.taskIdSetByCraned[cranedId] = make(map[uint32]bool)
				}
				keeper.taskIdSetByCraned[cranedId][taskId] = true
			}
			keeper.taskIdProcIdMapMtx.Unlock()
			readyChan <- true
			break
		}
	}
}

func (keeper *CranedChannelKeeper) CranedCrashAndRemoveAllChannel(cranedId string) {
	keeper.taskIdProcIdMapMtx.Lock()
	defer keeper.taskIdProcIdMapMtx.Unlock()

	if _, exist := keeper.taskIdSetByCraned[cranedId]; !exist {
		log.Errorf("Ignoring unexist craned %s unexpect down", cranedId)
	} else {
		keeper.taskIORequestChannelMtx.Lock()
		for taskId, _ := range keeper.taskIdSetByCraned[cranedId] {
			channel, exist := keeper.taskIORequestChannelMapByTaskId[taskId]
			if exist {
				channel <- nil
			} else {
				log.Warningf("Trying forward to I/O to an unknown crun of task #%d", taskId)
			}
		}
		keeper.taskIORequestChannelMtx.Unlock()
	}

}

func (keeper *CranedChannelKeeper) forwardCrunRequestToCranedChannels(request *protos.StreamCrunRequest, cranedIds []string) {
	keeper.crunRequestChannelMtx.Lock()
	for _, node := range cranedIds {
		chWrapper, exist := keeper.crunRequestChannelMapByCranedId[node]
		if !exist {
			log.Tracef("Ignoring crun request to nonexist craned %s", node)
			continue
		}
		if chWrapper.valid.Load() {
			select {
			case chWrapper.requestChannel <- request:
			default:
				if len(chWrapper.requestChannel) == cap(chWrapper.requestChannel) {
					log.Errorf("crunRequestChannel to craned %s is full", node)
				} else {
					log.Errorf("crunRequestChannel to craned %s write failed", node)
				}
			}
		} else {
			log.Tracef("Ignoring crun request to invalid craned %s", node)
		}

	}
	keeper.crunRequestChannelMtx.Unlock()
}

func (keeper *CranedChannelKeeper) setRemoteIoToCrunChannel(taskId uint32, ioToCrunChannel chan *protos.StreamCforedTaskIORequest) {
	keeper.taskIORequestChannelMtx.Lock()
	keeper.taskIORequestChannelMapByTaskId[taskId] = ioToCrunChannel
	keeper.taskIORequestChannelMtx.Unlock()
}

func (keeper *CranedChannelKeeper) forwardRemoteIoToCrun(taskId uint32, ioToCrun *protos.StreamCforedTaskIORequest) {
	keeper.taskIORequestChannelMtx.Lock()
	channel, exist := keeper.taskIORequestChannelMapByTaskId[taskId]
	if exist {
		channel <- ioToCrun
	} else {
		log.Warningf("Trying forward to I/O to an unknown crun of task #%d.", taskId)
	}
	keeper.taskIORequestChannelMtx.Unlock()
}

func (keeper *CranedChannelKeeper) crunTaskStopAndRemoveChannel(taskId uint32, cranedIds []string, forwardEstablished bool) {
	keeper.taskIORequestChannelMtx.Lock()
	delete(keeper.taskIORequestChannelMapByTaskId, taskId)
	keeper.taskIdProcIdMapMtx.Lock()
	if forwardEstablished {
		for _, cranedId := range cranedIds {
			if _, exist := keeper.taskIdSetByCraned[cranedId]; !exist {
				log.Errorf("CranedId %s should exist in CranedChannelKeeper", cranedId)
			} else {
				delete(keeper.taskIdSetByCraned[cranedId], taskId)
			}
		}
	}
	keeper.taskIdProcIdMapMtx.Unlock()
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

const (
	CranedReg    StateOfCranedServer = 0
	IOForwarding StateOfCranedServer = 1
	CranedUnReg  StateOfCranedServer = 2
)

func (cforedServer *GrpcCforedServer) TaskIOStream(toCranedStream protos.CraneForeD_TaskIOStreamServer) error {
	var cranedId string
	var reply *protos.StreamCforedTaskIOReply

	requestChannel := make(chan grpcMessage[protos.StreamCforedTaskIORequest], 8)
	go grpcStreamReceiver[protos.StreamCforedTaskIORequest](toCranedStream, requestChannel)

	pendingCrunReqToCranedChannel := make(chan *protos.StreamCrunRequest, 2)

	var valid = &atomic.Bool{}
	valid.Store(true)
	state := CranedReg

CforedCranedStateMachineLoop:
	for {
		switch state {
		case CranedReg:
			log.Debugf("[Cfored<->Craned] Enter State CranedReg")
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

			if cranedReq.Type != protos.StreamCforedTaskIORequest_CRANED_REGISTER {
				log.Fatal("[Cfored<->Craned] Expect CRANED_REGISTER")
			}

			cranedId = cranedReq.GetPayloadRegisterReq().GetCranedId()
			log.Debugf("[Cfored<->Craned] Receive CranedReg from %s", cranedId)

			gCranedChanKeeper.cranedUpAndSetMsgToCranedChannel(cranedId, pendingCrunReqToCranedChannel, valid)

			reply = &protos.StreamCforedTaskIOReply{
				Type: protos.StreamCforedTaskIOReply_CRANED_REGISTER_REPLY,
				Payload: &protos.StreamCforedTaskIOReply_PayloadCranedRegisterReply{
					PayloadCranedRegisterReply: &protos.StreamCforedTaskIOReply_CranedRegisterReply{
						Ok: true,
					},
				},
			}
			err = toCranedStream.Send(reply)
			if err != nil {
				log.Debug("[Cfored<->Craned] Connection to craned was broken.")
				state = CranedUnReg
			} else {
				state = IOForwarding
			}

		case IOForwarding:
			log.Debugf("[Cfored<->Craned] Enter State IOForwarding to craned %s", cranedId)
		cranedIOForwarding:
			for {
				select {
				case item := <-requestChannel:
					// Msg from craned
					cranedReq, err := item.message, item.err
					if err != nil { // Failure Edge
						// Todo: do something when craned down
						switch err {
						case io.EOF:
							fallthrough
						default:
							log.Errorf("[Cfored<->Craned] Craned %s unexpected down", cranedId)
							gCranedChanKeeper.CranedCrashAndRemoveAllChannel(cranedId)
							state = CranedUnReg
							break cranedIOForwarding
						}
					}

					log.Tracef("[Cfored<->Craned] Receive type %s", cranedReq.Type.String())
					switch cranedReq.Type {
					case protos.StreamCforedTaskIORequest_CRANED_TASK_OUTPUT:
						payload := cranedReq.GetPayloadTaskOutputReq()
						gCranedChanKeeper.forwardRemoteIoToCrun(payload.GetTaskId(), cranedReq)

					case protos.StreamCforedTaskIORequest_CRANED_UNREGISTER:
						reply = &protos.StreamCforedTaskIOReply{
							Type: protos.StreamCforedTaskIOReply_CRANED_UNREGISTER_REPLY,
							Payload: &protos.StreamCforedTaskIOReply_PayloadCranedUnregisterReply{
								PayloadCranedUnregisterReply: &protos.StreamCforedTaskIOReply_CranedUnregisterReply{
									Ok: true,
								},
							},
						}
						state = CranedUnReg
						valid.Store(false)
						err := toCranedStream.Send(reply)
						if err != nil {
							log.Debug("[Cfored<->Craned] Connection to craned was broken.")
						}
						break cranedIOForwarding

					default:
						log.Fatalf("[Cfored<->Craned] Receive Unexpected %s", cranedReq.Type.String())
						state = CranedUnReg
						break cranedIOForwarding
					}

				case crunReq := <-pendingCrunReqToCranedChannel:
					// Msg from crun
					switch crunReq.Type {
					case protos.StreamCrunRequest_TASK_IO_FORWARD:
						payload := crunReq.GetPayloadTaskIoForwardReq()
						taskId := payload.GetTaskId()
						msg := payload.GetMsg()
						log.Debugf("[Cfored<->Craned] forwarding task %d input %s to craned %s", taskId, msg, cranedId)
						reply = &protos.StreamCforedTaskIOReply{
							Type: protos.StreamCforedTaskIOReply_CRANED_TASK_INPUT,
							Payload: &protos.StreamCforedTaskIOReply_PayloadTaskInputReq{
								PayloadTaskInputReq: &protos.StreamCforedTaskIOReply_CranedTaskInputReq{
									TaskId: taskId,
									Msg:    msg,
								},
							},
						}
						if err := toCranedStream.Send(reply); err != nil {
							log.Debug("[Cfored<->Craned] Connection to craned was broken.")
							state = CranedUnReg
						}
					default:
						log.Fatalf("[Cfored<->Craned] Receive Unexpected %s", crunReq.Type.String())
						break cranedIOForwarding
					}
				}
			}

		case CranedUnReg:
			log.Debugf("[Cfored<->Craned] Enter State CranedUnReg")
			gCranedChanKeeper.cranedDownAndRemoveChannelToCraned(cranedId)
			log.Debugf("[Cfored<->Craned] Craned %s exit", cranedId)
			break CforedCranedStateMachineLoop
		}
	}
	return nil
}

func startGrpcServer(config *util.Config, wgAllRoutines *sync.WaitGroup) {
	unixSockPath := config.CranedGoUnixSockPath
	dir, err := filepath.Abs(filepath.Dir(unixSockPath))
	if err != nil {
		log.Fatalf("Failed to parse directory from %s: %s", unixSockPath, err.Error())
	}

	err = os.MkdirAll(dir, 0755)
	if err != nil {
		log.Fatalf("Failed to create directory for unix socket: %s", err.Error())
	}

	ok := util.RemoveFileIfExists(unixSockPath)
	if !ok {
		log.Fatalf("Error when removing existing unix socket!")
	}

	log.Tracef("Listening on unix socket %s", unixSockPath)
	unixListenSocket, err := net.Listen("unix", unixSockPath)
	if err != nil {
		log.Fatal(err)
	}
	if err = os.Chmod(unixSockPath, 0777); err != nil {
		log.Fatal(err)
	}

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
		tcpListenSocket, err := util.GetListenSocketByConfig(config)
		if err != nil {
			log.Fatalf("Failed to listen on tcp socket: %s", err.Error())
		}

		err = grpcServer.Serve(tcpListenSocket)
		if err != nil {
			log.Fatal(err)
		}

		wgAllRoutines.Done()
	}(wgAllRoutines)

	err = grpcServer.Serve(unixListenSocket)
	if err != nil {
		log.Fatal(err)
	}
}
