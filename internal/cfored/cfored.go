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
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type GlobalVariables struct {
	hostName string

	ctldConnected atomic.Bool

	globalCtx       context.Context
	globalCtxCancel context.CancelFunc

	// Prevent the situation that de-multiplexer the two maps when
	// Cfored <--> Ctld state machine just removes the channel of a calloc
	// from ctldReplyChannelMapByPid and hasn't added the channel
	// to ctldReplyChannelMapByTaskId. In such a situation, Cfored <--> Ctld
	// state machine may think the channel of a calloc is not in both maps
	// but the channel is actually being moving from one map to another map.
	ctldReplyChannelMapMtx sync.Mutex

	// Used by Cfored <--> Ctld state machine to de-multiplex messages
	// Used for calloc/crun with task id not allocated.
	// A calloc is identified by its pid.
	ctldReplyChannelMapByPid map[int32]chan *protos.StreamCtldReply

	// Used by Cfored <--> Ctld state machine to de-multiplex messages from CraneCtld.
	// Cfored <--> Ctld state machine GUARANTEES that NO `nil` will be sent into these channels.
	// Used for calloc/crun with task id allocated.
	ctldReplyChannelMapByTaskId map[uint32]chan *protos.StreamCtldReply

	// Used by Calloc/Crun <--> Cfored state machine to multiplex messages
	// these messages will be sent to CraneCtld
	cforedRequestCtldChannel chan *protos.StreamCforedRequest

	pidTaskIdMapMtx sync.RWMutex

	pidTaskIdMap map[int32]uint32

	crunRequestChannelMtx sync.Mutex
	crunRequestChannelCV  *sync.Cond
	// request message form crun to craned
	crunRequestChannelMapByCranedId map[string]chan *protos.StreamCrunRequest

	taskIORequestChannelMtx sync.Mutex
	// IO foward messsage form craned to crun
	taskIORequestChannelMapByTaskId map[uint32]chan *protos.StreamCfroedTaskIORequest
}

var gVars GlobalVariables

type StateOfCallocServer int
type StateOfCrunServer int
type StateOfCtldClient int
type StateOfCranedServer int

const (
	WaitTaskIdAllocReq     StateOfCallocServer = 0
	WaitCtldAllocTaskId    StateOfCallocServer = 1
	WaitCtldAllocRes       StateOfCallocServer = 2
	WaitCallocComplete     StateOfCallocServer = 3
	WaitCallocCancel       StateOfCallocServer = 4
	WaitCtldAck            StateOfCallocServer = 5
	CancelTaskOfDeadCalloc StateOfCallocServer = 6
)

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

const (
	StartReg       StateOfCtldClient = 0
	WaitReg        StateOfCtldClient = 1
	WaitChannelReq StateOfCtldClient = 2
	WaitAllCalloc  StateOfCtldClient = 3
	GracefulExit   StateOfCtldClient = 4
)

const (
	CranedReg    StateOfCranedServer = 0
	IOForwarding StateOfCranedServer = 1
	CranedUnReg  StateOfCranedServer = 2
)

type GrpcCtldClient struct {
	ctldClientStub   protos.CraneCtldClient
	ctldReplyChannel chan *protos.StreamCtldReply
}

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
			default:
				// SIGINT or SIGTERM received.
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

					if reply.GetPayloadCforedRegAck().Ok == true {
						log.Infof("[Cfored<->Ctld] Cfored %s successfully registered.", gVars.hostName)

						gVars.ctldConnected.Store(true)
						state = WaitChannelReq
					} else {
						log.Error("[Cfored<->Ctld] Failed to register with CraneCtld: %s. Exiting...",
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
					state = WaitAllCalloc
					break WaitChannelReqLoop

				// Multiplex requests from calloc/crun to ctld.
				case request = <-gVars.cforedRequestCtldChannel:
					if err := stream.Send(request); err != nil {
						log.Error("[Cfored<->Ctld] Failed to forward msg to ctld. " +
							"Connection to ctld is broken.")

						gVars.ctldConnected.Store(false)
						state = WaitAllCalloc
						break WaitChannelReqLoop
					}

				// De-multiplex requests from ctl to calloc/crun.
				case ctldReply := <-client.ctldReplyChannel:
					if ctldReply == nil {
						log.Trace("[Cfored<->Ctld] Failed to receive msg from ctld. " +
							"Connection to cfored is broken.")

						gVars.ctldConnected.Store(false)
						state = WaitAllCalloc
						break WaitChannelReqLoop
					}

					switch ctldReply.Type {

					case protos.StreamCtldReply_TASK_ID_REPLY:
						callocPid := ctldReply.GetPayloadTaskIdReply().Pid

						gVars.ctldReplyChannelMapMtx.Lock()

						toCallocCtlReplyChannel, ok := gVars.ctldReplyChannelMapByPid[callocPid]
						if ok {
							toCallocCtlReplyChannel <- ctldReply
						} else {
							log.Fatalf("[Cfored<->Ctld] Calloc pid %d shall exist "+
								"in ctldReplyChannelMapByPid!", callocPid)
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

						toCallocCtlReplyChannel, ok := gVars.ctldReplyChannelMapByTaskId[taskId]
						if ok {
							toCallocCtlReplyChannel <- ctldReply
						} else {
							log.Fatalf("[Cfored<->Ctld] Task Id %d shall exist in "+
								"ctldReplyChannelMapByTaskId!", taskId)
						}

						gVars.ctldReplyChannelMapMtx.Unlock()
					}
				}
			}

		case WaitAllCalloc:
			log.Tracef("[Cfored<->Ctld] Enter WAIT_ALL_CALLOC state.")

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
				log.Debugf("[Cfored<->Ctld] Sending cancel request to %d calloc "+
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
						"%d/%d calloc is cancelled", request.GetPayloadTaskCompleteReq().TaskId, count, num)

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

type GrpcCforedServer struct {
	protos.CraneForeDServer
}

type RequestReceiveItem[T any] struct {
	request *T
	err     error
}

func CallocRequestReceiveRoutine(stream protos.CraneForeD_CallocStreamServer,
	requestChannel chan RequestReceiveItem[protos.StreamCallocRequest]) {
	for {
		callocRequest, err := stream.Recv()
		requestChannel <- RequestReceiveItem[protos.StreamCallocRequest]{
			request: callocRequest,
			err:     err,
		}
		if err != nil {
			break
		}
	}
}

func CrunRequestReceiveRoutine(stream protos.CraneForeD_CrunStreamServer,
	requestChannel chan RequestReceiveItem[protos.StreamCrunRequest]) {
	for {
		crunRequest, err := stream.Recv()
		requestChannel <- RequestReceiveItem[protos.StreamCrunRequest]{
			request: crunRequest,
			err:     err,
		}
		if err != nil {
			break
		}
	}
}

func TaskIOFWDRequestReceiveRoutine(stream protos.CraneForeD_TaskIOStreamServer,
	requestChannel chan RequestReceiveItem[protos.StreamCfroedTaskIORequest]) {
	for {
		cranedRequest, err := stream.Recv()
		requestChannel <- RequestReceiveItem[protos.StreamCfroedTaskIORequest]{
			request: cranedRequest,
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

func (cforedServer *GrpcCforedServer) TaskIOStream(toCranedStream protos.CraneForeD_TaskIOStreamServer) error {
	var cranedId string
	var reply *protos.StreamCfroedTaskIOReply
	requestChannel := make(chan RequestReceiveItem[protos.StreamCfroedTaskIORequest], 8)
	go TaskIOFWDRequestReceiveRoutine(toCranedStream, requestChannel)
	crunRequestChannel := make(chan *protos.StreamCrunRequest, 2)
	state := CranedReg
CforedCranedStateMachineLoop:
	for {
		switch state {
		case CranedReg:
			log.Debugf("[Cfored<->Craned] Enter State CranedReg")
			item := <-requestChannel
			cranedReq, err := item.request, item.err
			if err != nil { // Failure Edge
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Fatal(err)
					return nil
				}
			}
			cranedId = cranedReq.GetPayloadRegisterReq().GetCranedId()
			log.Debugf("[Cfored<->Craned] Receive CranedReg from %s", cranedId)
			if cranedReq.Type != protos.StreamCfroedTaskIORequest_CRANED_REGISTER {
				log.Fatal("[Cfored<->Craned] Expect CRANED_REGISTER")
			}
			gVars.crunRequestChannelMtx.Lock()
			gVars.crunRequestChannelMapByCranedId[cranedId] = crunRequestChannel
			gVars.crunRequestChannelCV.Broadcast()
			gVars.crunRequestChannelMtx.Unlock()
			reply = &protos.StreamCfroedTaskIOReply{
				Type: protos.StreamCfroedTaskIOReply_CRANED_REGISTER_REPLY,
				Payload: &protos.StreamCfroedTaskIOReply_PayloadCranedRegisterReply{
					PayloadCranedRegisterReply: &protos.StreamCfroedTaskIOReply_CranedRegisterReply{
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
		cranedIOFrowarding:
			for {
				select {
				// msg from craned
				case item := <-requestChannel:
					cranedReq, err := item.request, item.err
					if err != nil { // Failure Edge
						//todo: do someting when craned down
						switch err {
						case io.EOF:
							fallthrough
						default:
							state = CranedUnReg
							break cranedIOFrowarding
						}
					}
					log.Tracef("[Cfored<->Craned] Receive type %s", cranedReq.Type.String())
					switch cranedReq.Type {
					case protos.StreamCfroedTaskIORequest_CRANED_TASK_OUTPUT:
						payload := cranedReq.GetPayloadTaskOutputReq()

						gVars.taskIORequestChannelMtx.Lock()
						channel, exist := gVars.taskIORequestChannelMapByTaskId[payload.GetTaskId()]
						if exist {
							channel <- cranedReq
						}
						gVars.taskIORequestChannelMtx.Unlock()
					case protos.StreamCfroedTaskIORequest_CRANED_UNREGISTER:
						reply = &protos.StreamCfroedTaskIOReply{
							Type: protos.StreamCfroedTaskIOReply_CRANED_UNREGISTER_REPLY,
							Payload: &protos.StreamCfroedTaskIOReply_PayloadCranedUnregisterReply{
								PayloadCranedUnregisterReply: &protos.StreamCfroedTaskIOReply_CranedUnregisterReply{
									Ok: true,
								},
							},
						}
						state = CranedUnReg
						err := toCranedStream.Send(reply)
						if err != nil {
							log.Debug("[Cfored<->Craned] Connection to craned was broken.")
						}
						break cranedIOFrowarding
					default:
						log.Fatal("[Cfored<->Craned] Receive Unexpected %s", cranedReq.Type.String())
						state = CranedUnReg
						break cranedIOFrowarding
					}
					// msg from crun
				case crunReq := <-crunRequestChannel:
					switch crunReq.Type {
					case protos.StreamCrunRequest_TASK_IO_FORWARD:
						payload := crunReq.GetPayloadTaskIoForwardReq()
						taskId := payload.GetTaskId()
						msg := payload.GetMsg()
						log.Debugf("[Cfored<->Craned] forwarding task %d input %s to craned %s", taskId, msg, cranedId)
						reply = &protos.StreamCfroedTaskIOReply{
							Type: protos.StreamCfroedTaskIOReply_CRANED_TASK_INPUT,
							Payload: &protos.StreamCfroedTaskIOReply_PayloadTaskInputReq{
								PayloadTaskInputReq: &protos.StreamCfroedTaskIOReply_CranedTaskInputReq{
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
						log.Fatal("[Cfored<->Craned] Receive Unexpected %s", crunReq.Type.String())
						break cranedIOFrowarding
					}
				}
			}
		case CranedUnReg:
			log.Debugf("[Cfored<->Craned] Enter State CranedUnReg")
			gVars.crunRequestChannelMtx.Lock()
			delete(gVars.crunRequestChannelMapByCranedId, cranedId)
			gVars.crunRequestChannelMtx.Unlock()
			break CforedCranedStateMachineLoop
		}
	}
	return nil
}

func (cforedServer *GrpcCforedServer) CrunStream(toCrunStream protos.CraneForeD_CrunStreamServer) error {
	var crunPid int32
	var taskId uint32
	var reply *protos.StreamCforedCrunReply
	var cranedIds []string
	requestChannel := make(chan RequestReceiveItem[protos.StreamCrunRequest], 8)
	go CrunRequestReceiveRoutine(toCrunStream, requestChannel)

	ctldReplyChannel := make(chan *protos.StreamCtldReply, 2)
	TaskIoRequestChannel := make(chan *protos.StreamCfroedTaskIORequest, 2)
	taskId = math.MaxUint32
	crunPid = -1

	state := CrunWaitTaskIdAllocReq

CforedCrunStateMachineLoop:
	for {
		switch state {
		case CrunWaitTaskIdAllocReq:
			log.Debug("[Cfored<->Crun] Enter State WAIT_TASK_ID_ALLOC_REQ")

			item := <-requestChannel
			crunRequest, err := item.request, item.err
			if err != nil { // Failure Edge
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Fatal(err)
					return nil
				}
			}

			log.Debug("[Cfored<->Crun] Receive TaskIdAllocReq")

			if crunRequest.Type != protos.StreamCrunRequest_TASK_REQUEST {
				log.Fatal("[Cfored<->Crun] Expect TASK_REQUEST")
			}

			if !gVars.ctldConnected.Load() {
				reply = &protos.StreamCforedCrunReply{
					Type: protos.StreamCforedCrunReply_TASK_ID_REPLY,
					Payload: &protos.StreamCforedCrunReply_PayloadTaskIdReply{
						PayloadTaskIdReply: &protos.StreamCforedCrunReply_TaskIdReply{
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
				break CforedCrunStateMachineLoop
			} else {
				crunPid = crunRequest.GetPayloadTaskReq().CrunPid

				gVars.ctldReplyChannelMapMtx.Lock()
				gVars.ctldReplyChannelMapByPid[crunPid] = ctldReplyChannel
				gVars.ctldReplyChannelMapMtx.Unlock()

				cforedRequest := &protos.StreamCforedRequest{
					Type: protos.StreamCforedRequest_TASK_REQUEST,
					Payload: &protos.StreamCforedRequest_PayloadTaskReq{
						PayloadTaskReq: &protos.StreamCforedRequest_TaskReq{
							CforedName: gVars.hostName,
							Pid:        crunPid,
							Task:       crunRequest.GetPayloadTaskReq().Task,
						},
					},
				}

				gVars.cforedRequestCtldChannel <- cforedRequest

				state = CrunWaitCtldAllocTaskId
			}

		case CrunWaitCtldAllocTaskId:
			log.Debug("[Cfored<->Crun] Enter State WAIT_CTLD_ALLOC_TASK_ID")

			select {
			case item := <-requestChannel:
				crunRequest, err := item.request, item.err
				if crunRequest != nil || err == nil {
					log.Fatal("[Cfored<->Crun] Expect only nil (calloc connection broken) here!")
				}
				log.Debug("[Cfored<->Crun] Connection to crun was broken.")

				state = CancelTaskOfDeadCrun

			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_ID_REPLY {
					log.Fatal("[Cfored<->Crun] Expect type TASK_ID_REPLY")
				}

				Ok := ctldReply.GetPayloadTaskIdReply().Ok
				taskId = ctldReply.GetPayloadTaskIdReply().TaskId

				reply = &protos.StreamCforedCrunReply{
					Type: protos.StreamCforedCrunReply_TASK_ID_REPLY,
					Payload: &protos.StreamCforedCrunReply_PayloadTaskIdReply{
						PayloadTaskIdReply: &protos.StreamCforedCrunReply_TaskIdReply{
							Ok:            Ok,
							TaskId:        taskId,
							FailureReason: ctldReply.GetPayloadTaskIdReply().FailureReason,
						},
					},
				}

				gVars.ctldReplyChannelMapMtx.Lock()
				delete(gVars.ctldReplyChannelMapByPid, crunPid)
				if Ok {
					gVars.ctldReplyChannelMapByTaskId[taskId] = ctldReplyChannel
					gVars.taskIORequestChannelMtx.Lock()
					gVars.taskIORequestChannelMapByTaskId[taskId] = TaskIoRequestChannel
					gVars.taskIORequestChannelMtx.Unlock()
					gVars.pidTaskIdMapMtx.Lock()
					gVars.pidTaskIdMap[crunPid] = taskId
					gVars.pidTaskIdMapMtx.Unlock()
				}

				gVars.ctldReplyChannelMapMtx.Unlock()
				if err := toCrunStream.Send(reply); err != nil {
					log.Debug("[Cfored<->Crun] Connection to crun was broken.")
					state = CancelTaskOfDeadCrun
				} else {
					if Ok {
						state = CrunWaitCtldAllocRes
					} else {
						// channel was already removed from gVars.ctldReplyChannelMapByPid
						break CforedCrunStateMachineLoop
					}
				}
			}

		case CrunWaitCtldAllocRes:
			log.Debug("[Cfored<->Crun] Enter State WAIT_CTLD_ALLOC_RES")

			select {
			case item := <-requestChannel:
				crunRequest, err := item.request, item.err
				if crunRequest != nil || err == nil {
					log.Fatal("[Cfored<->Crun] Expect only nil (crun connection broken) here!")
				}
				log.Debug("[Cfored<->Crun] Connection to calloc was broken.")

				state = CancelTaskOfDeadCrun

			case ctldReply := <-ctldReplyChannel:
				switch ctldReply.Type {
				case protos.StreamCtldReply_TASK_RES_ALLOC_REPLY:
					ctldPayload := ctldReply.GetPayloadTaskResAllocReply()
					reply = &protos.StreamCforedCrunReply{
						Type: protos.StreamCforedCrunReply_TASK_RES_ALLOC_REPLY,
						Payload: &protos.StreamCforedCrunReply_PayloadTaskAllocReply{
							PayloadTaskAllocReply: &protos.StreamCforedCrunReply_TaskResAllocatedReply{
								Ok:                   ctldPayload.Ok,
								AllocatedCranedRegex: ctldPayload.AllocatedCranedRegex,
							},
						},
					}
					cranedIds = ctldPayload.GetCranedIds()

					if err := toCrunStream.Send(reply); err != nil {
						log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
						state = CancelTaskOfDeadCrun
					} else {
						state = CrunWaitIOForward
					}

				case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
					state = CrunWaitTaskCancel

				default:
					log.Fatal("[Cfored<->Crun] Expect type " +
						"TASK_ID_ALLOC_REPLY or TASK_CANCEL_REQUEST")
				}
			}

		case CrunWaitIOForward:
			log.Debug("[Cfored<->Crun] Enter State WAIT_TASK_IO_FORWARD")
			waiting := atomic.Bool{}
			waiting.Store(true)
			readyChannel := make(chan bool, 1)
			go func() {
				gVars.crunRequestChannelMtx.Lock()
				defer gVars.crunRequestChannelMtx.Unlock()
				for waiting.Load() {
					allReady := true
					for _, node := range cranedIds {
						_, exits := gVars.crunRequestChannelMapByCranedId[node]
						if !exits {
							allReady = false
							break
						}
					}
					if allReady {
						break
					} else {
						gVars.crunRequestChannelCV.Wait()
					}
				}
				if waiting.Load() {
					log.Debug("[Cfored<->Crun] All related craned up now")
					readyChannel <- true
				}
			}()

			select {
			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_CANCEL_REQUEST {
					log.Fatal("[Cfored<->Crun] Expect Type TASK_CANCEL_REQUEST")
				}
				waiting.Store(false)

				state = CrunWaitTaskCancel

			case item := <-requestChannel:
				crunRequest, err := item.request, item.err
				if err != nil {
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Debug("[Cfored<->Crun] Connection to crun was broken.")
						waiting.Store(false)
						state = CancelTaskOfDeadCrun
					}
				} else {
					if crunRequest.Type != protos.StreamCrunRequest_TASK_COMPLETION_REQUEST {
						log.Fatal("[Cfored<->Crun] Expect TASK_COMPLETION_REQUEST")
					}

					log.Debug("[Cfored<->Crun] Receive TaskCompletionRequest")
					toCtldRequest := &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
							PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
								CforedName:   gVars.hostName,
								TaskId:       taskId,
								ReqFrontType: protos.InteractiveFrontEndType_CRUN,
							},
						},
					}
					gVars.cforedRequestCtldChannel <- toCtldRequest
					waiting.Store(false)
					state = CrunWaitCtldAck
				}
			case <-readyChannel:
				reply = &protos.StreamCforedCrunReply{
					Type: protos.StreamCforedCrunReply_TASK_IO_FORWARD_READY,
					Payload: &protos.StreamCforedCrunReply_PayloadTaskIoForwardReadyReply{
						PayloadTaskIoForwardReadyReply: &protos.StreamCforedCrunReply_TaskIOForwardReadyReply{
							Ok: true,
						},
					},
				}
				waiting.Store(false)
				if err := toCrunStream.Send(reply); err != nil {
					log.Debugf("[Cfored<->Crun] Failed to send CancelRequest to crun: %s. "+
						"The connection to calloc was broken.", err.Error())
					state = CancelTaskOfDeadCrun
				} else {
					state = CrunWaitTaskComplete
				}

			}

		case CrunWaitTaskComplete:
			log.Debug("[Cfored<->Crun] Enter State Crun_Wait_Task_Complete")
		forwarding:
			for {
				select {
				case ctldReply := <-ctldReplyChannel:
					switch ctldReply.Type {
					case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
						{
							state = CrunWaitTaskCancel
							break forwarding
						}
					case protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY:
						ctldReplyChannel <- ctldReply
						state = CrunWaitCtldAck
						break forwarding
					}

				case item := <-requestChannel:
					crunRequest, err := item.request, item.err
					if err != nil {
						switch err {
						case io.EOF:
							fallthrough
						default:
							log.Debug("[Cfored<->Crun] Connection to calloc was broken.")
							state = CancelTaskOfDeadCrun
							break forwarding
						}
					} else {
						switch crunRequest.Type {
						case protos.StreamCrunRequest_TASK_IO_FORWARD:
							log.Debug("[Cfored<->Crun] Receive TASK_IO_FORWARD Request")
							gVars.crunRequestChannelMtx.Lock()
							for _, node := range cranedIds {
								gVars.crunRequestChannelMapByCranedId[node] <- crunRequest
							}
							gVars.crunRequestChannelMtx.Unlock()
						case protos.StreamCrunRequest_TASK_COMPLETION_REQUEST:
							log.Debug("[Cfored<->Crun] Receive TaskCompletionRequest")
							toCtldRequest := &protos.StreamCforedRequest{
								Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
								Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
									PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
										CforedName:   gVars.hostName,
										TaskId:       taskId,
										ReqFrontType: protos.InteractiveFrontEndType_CRUN,
									},
								},
							}
							gVars.cforedRequestCtldChannel <- toCtldRequest
							state = CrunWaitCtldAck
							break forwarding
						default:
							log.Fatal("[Cfored<->Crun] Expect TASK_COMPLETION_REQUEST or TASK_IO_FORWARD")
							break forwarding
						}
					}

				case taskMsg := <-TaskIoRequestChannel:
					if taskMsg.Type == protos.StreamCfroedTaskIORequest_CRANED_TASK_OUTPUT {
						reply = &protos.StreamCforedCrunReply{
							Type: protos.StreamCforedCrunReply_TASK_IO_FORWARD,
							Payload: &protos.StreamCforedCrunReply_PayloadTaskIoForwardReply{
								PayloadTaskIoForwardReply: &protos.StreamCforedCrunReply_TaskIOForwardReply{
									Msg: taskMsg.GetPayloadTaskOutputReq().Msg,
								},
							},
						}
						log.Tracef("[Cfored<->Crun] fowarding msg %s to crun for taskid %d", taskMsg.GetPayloadTaskOutputReq().GetMsg(), taskId)
						if err := toCrunStream.Send(reply); err != nil {
							log.Debugf("[Cfored<->Crun] Failed to send CancelRequest to calloc: %s. "+
								"The connection to calloc was broken.", err.Error())
							state = CancelTaskOfDeadCrun
							break forwarding
						}
					} else {
						log.Fatal("[Cfored<->Crun] Expect Type CRANED_TASK_OUTPUTT")
						break forwarding
					}
				}
			}

		case CrunWaitTaskCancel:
			log.Debug("[Cfored<->Crun] Enter State WAIT_CRUN_CANCEL. " +
				"Sending TASK_CANCEL_REQUEST...")

			reply = &protos.StreamCforedCrunReply{
				Type: protos.StreamCforedCrunReply_TASK_CANCEL_REQUEST,
				Payload: &protos.StreamCforedCrunReply_PayloadTaskCancelRequest{
					PayloadTaskCancelRequest: &protos.StreamCforedCrunReply_TaskCancelRequest{
						TaskId: taskId,
					},
				},
			}

			if err := toCrunStream.Send(reply); err != nil {
				log.Debugf("[Cfored<->Crun] Failed to send CancelRequest to calloc: %s. "+
					"The connection to calloc was broken.", err.Error())
				state = CancelTaskOfDeadCrun
			} else {
				item := <-requestChannel
				crunRequest, err := item.request, item.err
				if err != nil { // Failure Edge
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Debug("[Cfored<->Crun] Connection to calloc was broken.")
						state = CancelTaskOfDeadCrun
					}
				} else {
					if crunRequest.Type != protos.StreamCrunRequest_TASK_COMPLETION_REQUEST {
						log.Fatal("[Cfored<->Crun] Expect TASK_COMPLETION_REQUEST")
					}

					log.Debug("[Cfored<->Crun] Receive TaskCompletionRequest")

					toCtldRequest := &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
							PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
								CforedName:   gVars.hostName,
								TaskId:       taskId,
								ReqFrontType: protos.InteractiveFrontEndType_CRUN,
							},
						},
					}
					gVars.cforedRequestCtldChannel <- toCtldRequest

					state = CrunWaitCtldAck
				}
			}

		case CrunWaitCtldAck:
			log.Debug("[Cfored<->Crun] Enter State WAIT_CTLD_ACK")

			ctldReply := <-ctldReplyChannel
			if ctldReply.Type != protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY {
				log.Fatalf("[Cfored<->Crun] Expect TASK_COMPLETION_ACK_REPLY, "+
					"but %s received.", ctldReply.Type)
			}

			reply = &protos.StreamCforedCrunReply{
				Type: protos.StreamCforedCrunReply_TASK_COMPLETION_ACK_REPLY,
				Payload: &protos.StreamCforedCrunReply_PayloadTaskCompletionAckReply{
					PayloadTaskCompletionAckReply: &protos.StreamCforedCrunReply_TaskCompletionAckReply{
						Ok: true,
					},
				},
			}

			gVars.ctldReplyChannelMapMtx.Lock()
			gVars.taskIORequestChannelMtx.Lock()
			delete(gVars.taskIORequestChannelMapByTaskId, taskId)
			delete(gVars.ctldReplyChannelMapByTaskId, taskId)
			gVars.taskIORequestChannelMtx.Unlock()
			gVars.ctldReplyChannelMapMtx.Unlock()

			if err := toCrunStream.Send(reply); err != nil {
				log.Errorf("[Cfored<->Crun] The stream to crun executing "+
					"task #%d is broken", taskId)
			}

			break CforedCrunStateMachineLoop

		case CancelTaskOfDeadCrun:
			log.Debug("[Cfored<->Crun] Enter State CANCEL_TASK_OF_DEAD_CRUN")

			toCtldRequest := &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
				Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
					PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
						CforedName:   gVars.hostName,
						TaskId:       taskId,
						ReqFrontType: protos.InteractiveFrontEndType_CRUN,
					},
				},
			}
			gVars.cforedRequestCtldChannel <- toCtldRequest

			gVars.ctldReplyChannelMapMtx.Lock()
			if taskId != math.MaxUint32 {
				delete(gVars.ctldReplyChannelMapByTaskId, taskId)

				gVars.pidTaskIdMapMtx.Lock()
				delete(gVars.pidTaskIdMap, crunPid)
				gVars.pidTaskIdMapMtx.Unlock()
				gVars.taskIORequestChannelMtx.Lock()
				delete(gVars.taskIORequestChannelMapByTaskId, taskId)
				gVars.taskIORequestChannelMtx.Unlock()
			} else {
				delete(gVars.ctldReplyChannelMapByPid, crunPid)
			}
			gVars.ctldReplyChannelMapMtx.Unlock()

			break CforedCrunStateMachineLoop
		}
	}

	return nil

}

func (cforedServer *GrpcCforedServer) CallocStream(toCallocStream protos.CraneForeD_CallocStreamServer) error {
	var callocPid int32
	var taskId uint32
	var reply *protos.StreamCforedReply

	requestChannel := make(chan RequestReceiveItem[protos.StreamCallocRequest], 8)
	go CallocRequestReceiveRoutine(toCallocStream, requestChannel)

	ctldReplyChannel := make(chan *protos.StreamCtldReply, 2)

	taskId = math.MaxUint32
	callocPid = -1

	state := WaitTaskIdAllocReq

CforedStateMachineLoop:
	for {
		switch state {
		case WaitTaskIdAllocReq:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_TASK_ID_ALLOC_REQ")

			item := <-requestChannel
			callocRequest, err := item.request, item.err
			if err != nil { // Failure Edge
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Fatal(err)
					return nil
				}
			}

			log.Debug("[Cfored<->Calloc] Receive TaskIdAllocReq")

			if callocRequest.Type != protos.StreamCallocRequest_TASK_REQUEST {
				log.Fatal("[Cfored<->Calloc] Expect TASK_REQUEST")
			}

			if !gVars.ctldConnected.Load() {
				reply = &protos.StreamCforedReply{
					Type: protos.StreamCforedReply_TASK_ID_REPLY,
					Payload: &protos.StreamCforedReply_PayloadTaskIdReply{
						PayloadTaskIdReply: &protos.StreamCforedReply_TaskIdReply{
							Ok:            false,
							FailureReason: "Cfored is not connected to CraneCtld.",
						},
					},
				}

				if err := toCallocStream.Send(reply); err != nil {
					// It doesn't matter even if the connection is broken here.
					// Just print a log.
					log.Error(err)
				}

				// No need to cleaning any data
				break CforedStateMachineLoop
			} else {
				callocPid = callocRequest.GetPayloadTaskReq().CallocPid

				gVars.ctldReplyChannelMapMtx.Lock()
				gVars.ctldReplyChannelMapByPid[callocPid] = ctldReplyChannel
				gVars.ctldReplyChannelMapMtx.Unlock()

				cforedRequest := &protos.StreamCforedRequest{
					Type: protos.StreamCforedRequest_TASK_REQUEST,
					Payload: &protos.StreamCforedRequest_PayloadTaskReq{
						PayloadTaskReq: &protos.StreamCforedRequest_TaskReq{
							CforedName: gVars.hostName,
							Pid:        callocPid,
							Task:       callocRequest.GetPayloadTaskReq().Task,
						},
					},
				}

				gVars.cforedRequestCtldChannel <- cforedRequest

				state = WaitCtldAllocTaskId
			}

		case WaitCtldAllocTaskId:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CTLD_ALLOC_TASK_ID")

			select {
			case item := <-requestChannel:
				callocRequest, err := item.request, item.err
				if callocRequest != nil || err == nil {
					log.Fatal("[Cfored<->Calloc] Expect only nil (calloc connection broken) here!")
				}
				log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")

				state = CancelTaskOfDeadCalloc

			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_ID_REPLY {
					log.Fatal("[Cfored<->Calloc] Expect type TASK_ID_REPLY")
				}

				Ok := ctldReply.GetPayloadTaskIdReply().Ok
				taskId = ctldReply.GetPayloadTaskIdReply().TaskId

				reply = &protos.StreamCforedReply{
					Type: protos.StreamCforedReply_TASK_ID_REPLY,
					Payload: &protos.StreamCforedReply_PayloadTaskIdReply{
						PayloadTaskIdReply: &protos.StreamCforedReply_TaskIdReply{
							Ok:            Ok,
							TaskId:        taskId,
							FailureReason: ctldReply.GetPayloadTaskIdReply().FailureReason,
						},
					},
				}

				gVars.ctldReplyChannelMapMtx.Lock()
				delete(gVars.ctldReplyChannelMapByPid, callocPid)
				if Ok {
					gVars.ctldReplyChannelMapByTaskId[taskId] = ctldReplyChannel

					gVars.pidTaskIdMapMtx.Lock()
					gVars.pidTaskIdMap[callocPid] = taskId
					gVars.pidTaskIdMapMtx.Unlock()
				}
				gVars.ctldReplyChannelMapMtx.Unlock()

				if err := toCallocStream.Send(reply); err != nil {
					log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
					state = CancelTaskOfDeadCalloc
				} else {
					if Ok {
						state = WaitCtldAllocRes
					} else {
						// channel was already removed from gVars.ctldReplyChannelMapByPid
						break CforedStateMachineLoop
					}
				}
			}

		case WaitCtldAllocRes:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CTLD_ALLOC_RES")

			select {
			case item := <-requestChannel:
				callocRequest, err := item.request, item.err
				if callocRequest != nil || err == nil {
					log.Fatal("[Cfored<->Calloc] Expect only nil (calloc connection broken) here!")
				}
				log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")

				state = CancelTaskOfDeadCalloc

			case ctldReply := <-ctldReplyChannel:
				switch ctldReply.Type {
				case protos.StreamCtldReply_TASK_RES_ALLOC_REPLY:
					ctldPayload := ctldReply.GetPayloadTaskResAllocReply()
					reply = &protos.StreamCforedReply{
						Type: protos.StreamCforedReply_TASK_RES_ALLOC_REPLY,
						Payload: &protos.StreamCforedReply_PayloadTaskAllocReply{
							PayloadTaskAllocReply: &protos.StreamCforedReply_TaskResAllocatedReply{
								Ok:                   ctldPayload.Ok,
								AllocatedCranedRegex: ctldPayload.AllocatedCranedRegex,
							},
						},
					}

					if err := toCallocStream.Send(reply); err != nil {
						log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
						state = CancelTaskOfDeadCalloc
					} else {
						state = WaitCallocComplete
					}

				case protos.StreamCtldReply_TASK_CANCEL_REQUEST:
					reply = &protos.StreamCforedReply{
						Type: protos.StreamCforedReply_TASK_CANCEL_REQUEST,
						Payload: &protos.StreamCforedReply_PayloadTaskCancelRequest{
							PayloadTaskCancelRequest: &protos.StreamCforedReply_TaskCancelRequest{
								TaskId: ctldReply.GetPayloadTaskCancelRequest().TaskId,
							},
						},
					}

					if err := toCallocStream.Send(reply); err != nil {
						log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
						state = CancelTaskOfDeadCalloc
					} else {
						state = WaitCallocCancel
					}

				default:
					log.Fatal("[Cfored<->Calloc] Expect type " +
						"TASK_ID_ALLOC_REPLY or TASK_CANCEL_REQUEST")
				}
			}

		case WaitCallocComplete:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CALLOC_COMPLETE")

			select {
			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_CANCEL_REQUEST {
					log.Fatal("[Cfored<->Calloc] Expect Type TASK_CANCEL_REQUEST")
				}

				state = WaitCallocCancel

			case item := <-requestChannel:
				callocRequest, err := item.request, item.err
				if err != nil {
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
						state = CancelTaskOfDeadCalloc
					}
				} else {
					if callocRequest.Type != protos.StreamCallocRequest_TASK_COMPLETION_REQUEST {
						log.Fatal("[Cfored<->Calloc] Expect TASK_COMPLETION_REQUEST")
					}

					log.Debug("[Cfored<->Calloc] Receive TaskCompletionRequest")
					toCtldRequest := &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
							PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
								CforedName:   gVars.hostName,
								TaskId:       taskId,
								ReqFrontType: protos.InteractiveFrontEndType_CALLOC,
							},
						},
					}
					gVars.cforedRequestCtldChannel <- toCtldRequest

					state = WaitCtldAck
				}
			}

		case WaitCallocCancel:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CALLOC_CANCEL. " +
				"Sending TASK_CANCEL_REQUEST...")

			reply = &protos.StreamCforedReply{
				Type: protos.StreamCforedReply_TASK_CANCEL_REQUEST,
				Payload: &protos.StreamCforedReply_PayloadTaskCancelRequest{
					PayloadTaskCancelRequest: &protos.StreamCforedReply_TaskCancelRequest{
						TaskId: taskId,
					},
				},
			}

			if err := toCallocStream.Send(reply); err != nil {
				log.Debugf("[Cfored<->Calloc] Failed to send CancelRequest to calloc: %s. "+
					"The connection to calloc was broken.", err.Error())
				state = CancelTaskOfDeadCalloc
			} else {
				item := <-requestChannel
				callocRequest, err := item.request, item.err
				if err != nil { // Failure Edge
					switch err {
					case io.EOF:
						fallthrough
					default:
						log.Debug("[Cfored<->Calloc] Connection to calloc was broken.")
						state = CancelTaskOfDeadCalloc
					}
				} else {
					if callocRequest.Type != protos.StreamCallocRequest_TASK_COMPLETION_REQUEST {
						log.Fatal("[Cfored<->Calloc] Expect TASK_COMPLETION_REQUEST")
					}

					log.Debug("[Cfored<->Calloc] Receive TaskCompletionRequest")

					toCtldRequest := &protos.StreamCforedRequest{
						Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
						Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
							PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
								CforedName:   gVars.hostName,
								TaskId:       taskId,
								ReqFrontType: protos.InteractiveFrontEndType_CALLOC,
							},
						},
					}
					gVars.cforedRequestCtldChannel <- toCtldRequest

					state = WaitCtldAck
				}
			}

		case WaitCtldAck:
			log.Debug("[Cfored<->Calloc] Enter State WAIT_CTLD_ACK")

			ctldReply := <-ctldReplyChannel
			if ctldReply.Type != protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY {
				log.Fatalf("[Cfored<->Calloc] Expect TASK_COMPLETION_ACK_REPLY, "+
					"but %s received.", ctldReply.Type)
			}

			reply = &protos.StreamCforedReply{
				Type: protos.StreamCforedReply_TASK_COMPLETION_ACK_REPLY,
				Payload: &protos.StreamCforedReply_PayloadTaskCompletionAckReply{
					PayloadTaskCompletionAckReply: &protos.StreamCforedReply_TaskCompletionAckReply{
						Ok: true,
					},
				},
			}

			gVars.ctldReplyChannelMapMtx.Lock()
			delete(gVars.ctldReplyChannelMapByTaskId, taskId)
			gVars.ctldReplyChannelMapMtx.Unlock()

			if err := toCallocStream.Send(reply); err != nil {
				log.Errorf("[Cfored<->Calloc] The stream to calloc executing "+
					"task #%d is broken", taskId)
			}

			break CforedStateMachineLoop

		case CancelTaskOfDeadCalloc:
			log.Debug("[Cfored<->Calloc] Enter State CANCEL_TASK_OF_DEAD_CALLOC")

			toCtldRequest := &protos.StreamCforedRequest{
				Type: protos.StreamCforedRequest_TASK_COMPLETION_REQUEST,
				Payload: &protos.StreamCforedRequest_PayloadTaskCompleteReq{
					PayloadTaskCompleteReq: &protos.StreamCforedRequest_TaskCompleteReq{
						CforedName:   gVars.hostName,
						TaskId:       taskId,
						ReqFrontType: protos.InteractiveFrontEndType_CALLOC,
					},
				},
			}
			gVars.cforedRequestCtldChannel <- toCtldRequest

			gVars.ctldReplyChannelMapMtx.Lock()
			if taskId != math.MaxUint32 {
				delete(gVars.ctldReplyChannelMapByTaskId, taskId)

				gVars.pidTaskIdMapMtx.Lock()
				delete(gVars.pidTaskIdMap, callocPid)
				gVars.pidTaskIdMapMtx.Unlock()
			} else {
				delete(gVars.ctldReplyChannelMapByPid, callocPid)
			}
			gVars.ctldReplyChannelMapMtx.Unlock()

			break CforedStateMachineLoop
		}
	}

	return nil
}

func StartCfored() {
	switch FlagDebugLevel {
	case "trace":
		util.InitLogger(log.TraceLevel)
	case "debug":
		util.InitLogger(log.DebugLevel)
	case "info":
		fallthrough
	default:
		util.InitLogger(log.InfoLevel)
	}

	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())
	defer gVars.globalCtxCancel()

	gVars.ctldConnected.Store(false)

	gVars.cforedRequestCtldChannel = make(chan *protos.StreamCforedRequest, 8)

	gVars.ctldReplyChannelMapByPid = make(map[int32]chan *protos.StreamCtldReply)
	gVars.ctldReplyChannelMapByTaskId = make(map[uint32]chan *protos.StreamCtldReply)
	gVars.pidTaskIdMap = make(map[int32]uint32)
	gVars.crunRequestChannelMapByCranedId = make(map[string]chan *protos.StreamCrunRequest)
	gVars.taskIORequestChannelMapByTaskId = make(map[uint32]chan *protos.StreamCfroedTaskIORequest)
	gVars.crunRequestChannelCV = sync.NewCond(&gVars.crunRequestChannelMtx)
	hostName, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %s", err.Error())
	}
	gVars.hostName = hostName

	sockDirPath := filepath.Dir(util.DefaultCforedUnixSocketPath)
	err = os.MkdirAll(sockDirPath, 0755)
	if err != nil {
		log.Fatal(err)
	}

	var wgAllRoutines sync.WaitGroup

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	unixListenSocket, err := net.Listen("unix", util.DefaultCforedUnixSocketPath)
	if err != nil {
		log.Fatal(err)
	}
	if err := os.Chmod(util.DefaultCforedUnixSocketPath, 0777); err != nil {
		log.Fatal(err)
	}

	config := util.ParseConfig(FlagConfigFilePath)
	tcpListenSocket, err := util.GetListenSocketByConfig(config)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	cforedServer := GrpcCforedServer{}

	wgAllRoutines.Add(1)
	go func(sigs chan os.Signal, server *grpc.Server, wg *sync.WaitGroup) {
		select {
		case sig := <-sigs:
			log.Infof("Receive signal: %s. Exiting...", sig.String())

			switch sig {
			case syscall.SIGINT:
				gVars.globalCtxCancel()
				server.GracefulStop()
				break
			case syscall.SIGTERM:
				server.Stop()
				break
			}
		case <-gVars.globalCtx.Done():
			break
		}
		wg.Done()
	}(sigs, grpcServer, &wgAllRoutines)

	ctldClient := &GrpcCtldClient{
		ctldClientStub:   util.GetStubToCtldByConfig(config),
		ctldReplyChannel: make(chan *protos.StreamCtldReply, 8),
	}
	wgAllRoutines.Add(1)
	go ctldClient.StartCtldClientStream(&wgAllRoutines)

	protos.RegisterCraneForeDServer(grpcServer, &cforedServer)

	wgAllRoutines.Add(1)
	go func(wg *sync.WaitGroup) {
		err := grpcServer.Serve(tcpListenSocket)
		if err != nil {
			log.Fatal(err)
		}

		wgAllRoutines.Done()
	}(&wgAllRoutines)

	err = grpcServer.Serve(unixListenSocket)
	if err != nil {
		log.Fatal(err)
	}

	log.Debug("Waiting all go routines to exit...")

	wgAllRoutines.Wait()
}
