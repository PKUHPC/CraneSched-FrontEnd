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
	// Used for calloc with task id not allocated.
	// A calloc is identified by its pid.
	ctldReplyChannelMapByPid map[int32]chan *protos.StreamCtldReply

	// Used by Cfored <--> Ctld state machine to de-multiplex messages from CraneCtld.
	// Cfored <--> Ctld state machine GUARANTEES that NO `nil` will be sent into these channels.
	// Used for calloc with task id allocated.
	ctldReplyChannelMapByTaskId map[uint32]chan *protos.StreamCtldReply

	// Used by Calloc <--> Cfored state machine to multiplex messages
	cforedRequestChannel chan *protos.StreamCforedRequest

	pidTaskIdMapMtx sync.RWMutex

	pidTaskIdMap map[int32]uint32
}

var gVars GlobalVariables

type StateOfCforedServer int
type StateOfCtldClient int

const (
	WaitTaskIdAllocReq     StateOfCforedServer = 0
	WaitCtldAllocTaskId    StateOfCforedServer = 1
	WaitCtldAllocRes       StateOfCforedServer = 2
	WaitCallocComplete     StateOfCforedServer = 3
	WaitCallocCancel       StateOfCforedServer = 4
	WaitCtldAck            StateOfCforedServer = 5
	CancelTaskOfDeadCalloc StateOfCforedServer = 6
)

const (
	StartReg       StateOfCtldClient = 0
	WaitReg        StateOfCtldClient = 1
	WaitChannelReq StateOfCtldClient = 2
	WaitAllCalloc  StateOfCtldClient = 3
	GracefulExit   StateOfCtldClient = 4
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

				// Multiplex requests from calloc to ctld.
				case request = <-gVars.cforedRequestChannel:
					if err := stream.Send(request); err != nil {
						log.Error("[Cfored<->Ctld] Failed to forward msg to ctld. " +
							"Connection to ctld is broken.")

						gVars.ctldConnected.Store(false)
						state = WaitAllCalloc
						break WaitChannelReqLoop
					}

				// De-multiplex requests from ctl to calloc.
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
					request = <-gVars.cforedRequestChannel
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

type RequestReceiveItem struct {
	request *protos.StreamCallocRequest
	err     error
}

func RequestReceiveRoutine(stream protos.CraneForeD_CallocStreamServer, requestChannel chan RequestReceiveItem) {
	for {
		callocRequest, err := stream.Recv()
		requestChannel <- RequestReceiveItem{
			request: callocRequest,
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
func (cforedServer *GrpcCforedServer) CallocStream(toCallocStream protos.CraneForeD_CallocStreamServer) error {
	var callocPid int32
	var taskId uint32
	var reply *protos.StreamCforedReply

	requestChannel := make(chan RequestReceiveItem, 8)
	go RequestReceiveRoutine(toCallocStream, requestChannel)

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

				gVars.cforedRequestChannel <- cforedRequest

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
								CforedName: gVars.hostName,
								TaskId:     taskId,
							},
						},
					}
					gVars.cforedRequestChannel <- toCtldRequest

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
								CforedName: gVars.hostName,
								TaskId:     taskId,
							},
						},
					}
					gVars.cforedRequestChannel <- toCtldRequest

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
						CforedName: gVars.hostName,
						TaskId:     taskId,
					},
				},
			}
			gVars.cforedRequestChannel <- toCtldRequest

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

	gVars.cforedRequestChannel = make(chan *protos.StreamCforedRequest, 8)

	gVars.ctldReplyChannelMapByPid = make(map[int32]chan *protos.StreamCtldReply)
	gVars.ctldReplyChannelMapByTaskId = make(map[uint32]chan *protos.StreamCtldReply)
	gVars.pidTaskIdMap = make(map[int32]uint32)

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
