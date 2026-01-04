package cfored

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"errors"
	"io"
	"math"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/peer"
)

type StateOfCattachServer int

const (
	CattachWaitConnectReq   StateOfCattachServer = 1
	CattachWaitTaskMeta     StateOfCattachServer = 2
	CattachWaitIOForward    StateOfCattachServer = 3
	CattachWaitTaskComplete StateOfCattachServer = 4
	DeadCattach             StateOfCattachServer = 5
	End                     StateOfCattachServer = 6
)

func (cforedServer *GrpcCforedServer) CattachStream(toCattachStream protos.CraneForeD_CattachStreamServer) error {
	var cattachPid int32
	var taskId uint32
	var stepId uint32
	var uid uint32
	var reply *protos.StreamCattachReply

	var execCranedIds []string
	var cattachPty bool

	RequestChannel := make(chan grpcMessage[protos.StreamCattachRequest], 8)
	go grpcStreamReceiver[protos.StreamCattachRequest](toCattachStream, RequestChannel)

	ctldReplyChannel := make(chan *protos.StreamCtldReply, 2)
	TaskIoRequestChannel := make(chan *protos.StreamTaskIORequest, 2)
	taskId = math.MaxUint32
	cattachPid = -1
	forwardEstablished := atomic.Bool{}
	forwardEstablished.Store(false)

	state := CattachWaitConnectReq

CforedCattachStateMachineLoop:
	for {
		switch state {
		case CattachWaitConnectReq:
			log.Infof("[Cfored<->Cattach] Enter State WAIT_CONNECT_REQ")
			item := <-RequestChannel
			cattachRequest, err := item.message, item.err
			if err != nil { // Failure Edge
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Fatal(err)
					return nil
				}
			}

			if cattachRequest.Type != protos.StreamCattachRequest_TASK_CONNECT_REQUEST {
				log.Fatalf("[Cfored<-Cattach] Expect TASK_CONNECT_REQUEST but got %s", cattachRequest.Type)
				break
			}

			log.Debug("[Cfored<-Cattach] Receive TASK_CONNECT_REQUEST")

			ctx := toCattachStream.Context()
			p, ok := peer.FromContext(ctx)
			if ok {
				if auth, ok := p.AuthInfo.(*util.UnixPeerAuthInfo); ok {
					uid = cattachRequest.GetPayloadTaskConnectReq().GetUid()
					if uid != auth.UID {
						log.Warnf("Security: UID mismatch - peer UID %d does not match task UID %d", auth.UID, uid)
						reply = &protos.StreamCattachReply{
							Type: protos.StreamCattachReply_TASK_CONNECT_REPLY,
							Payload: &protos.StreamCattachReply_PayloadTaskConnectReply{
								PayloadTaskConnectReply: &protos.StreamCattachReply_TaskConnectReply{
									Ok:            false,
									FailureReason: "Permission denied: caller UID does not match task UID",
								},
							},
						}

						if err := toCattachStream.Send(reply); err != nil {
							log.Error(err)
						}

						break CforedCattachStateMachineLoop
					}
				}
			}

			if !gVars.ctldConnected.Load() {
				reply = &protos.StreamCattachReply{
					Type: protos.StreamCattachReply_TASK_CONNECT_REPLY,
					Payload: &protos.StreamCattachReply_PayloadTaskConnectReply{
						PayloadTaskConnectReply: &protos.StreamCattachReply_TaskConnectReply{
							Ok:            false,
							FailureReason: "Cfored is not connected to CraneCtld.",
						},
					},
				}

				if err := toCattachStream.Send(reply); err != nil {
					// It doesn't matter even if the connection is broken here.
					// Just print a log.
					log.Error(err)
				}

				// No need to cleaning any data
				log.Infof("[Cfored<->Cattach]Cfored not connected to CraneCtld")
				break CforedCattachStateMachineLoop
			} else {
				cattachPid = cattachRequest.GetPayloadTaskConnectReq().CattachPid
				taskId = cattachRequest.GetPayloadTaskConnectReq().GetTaskId()
				stepId = cattachRequest.GetPayloadTaskConnectReq().GetStepdId()

				gVars.ctldReplyChannelMapMtx.Lock()
				gVars.ctldReplyChannelMapByPid[cattachPid] = ctldReplyChannel
				gVars.ctldReplyChannelMapMtx.Unlock()

				cforedRequest := &protos.StreamCforedRequest{
					Type: protos.StreamCforedRequest_TASK_META_REQUEST,
					Payload: &protos.StreamCforedRequest_PayloadTaskMetaReq{
						PayloadTaskMetaReq: &protos.StreamCforedRequest_TaskMetaReq{
							Uid:        uid,
							TaskId:     taskId,
							CattachPid: cattachPid,
						},
					},
				}

				gVars.cforedRequestCtldChannel <- cforedRequest

				state = CattachWaitTaskMeta
			}
		case CattachWaitTaskMeta:
			log.Infof("[Cfored<->Cattach][Pid #%d] Enter State WAIT_TASK_META", cattachPid)
			select {
			case item := <-RequestChannel:
				cattachRequest, err := item.message, item.err
				if err != nil {
					log.Debug("[Cattach->Cfored] Connection to cattach was broken.")
				} else if cattachRequest != nil || err == nil {
					log.Fatal("[Cattach->Cfored] Expect only nil (cattach connection broken) here!")
				}
				state = End

			case ctldReply := <-ctldReplyChannel:
				switch ctldReply.Type {
				case protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY:
					log.Debugf("[Ctld->Cfored->Cattach][Step #%d.%d] Receive COMPLETION_ACK_REPLY", taskId, stepId)
					state = DeadCattach
				case protos.StreamCtldReply_TASK_META_REPLY:
					Ok := ctldReply.GetPayloadTaskMetaReply().Ok
					log.Tracef("[Ctld->Cfored->Cattach][Pid#%d] Receive TaskMeta, Ok: %v", cattachPid, Ok)

					gVars.ctldReplyChannelMapMtx.Lock()
					delete(gVars.ctldReplyChannelMapByPid, cattachPid)
					var task *protos.TaskToCtld
					if Ok {
						// node[03-04]
						execCranedIds, ok := util.ParseHostList(ctldReply.GetPayloadTaskMetaReply().Task.GetNodelist())
						if !ok {
							state = End
							break
						}
						task = ctldReply.GetPayloadTaskMetaReply().Task
						cattachPty = ctldReply.GetPayloadTaskMetaReply().Task.GetInteractiveMeta().Pty
						if cattachPty {
							// For crun with pty, only execute on first node
							execCranedIds = []string{execCranedIds[0]}
						}
						if gVars.ctldReplyChannelMapForCattachByTaskId[taskId] == nil {
							gVars.ctldReplyChannelMapForCattachByTaskId[taskId] = make(map[int32]chan *protos.StreamCtldReply)
						}
						gVars.ctldReplyChannelMapForCattachByTaskId[taskId][cattachPid] = ctldReplyChannel
						gVars.pidStepMapMtx.Lock()
						gVars.pidStepMap[cattachPid] = StepIdentifier{JobId: taskId, StepId: stepId}
						gVars.pidStepMapMtx.Unlock()
						gSupervisorChanKeeper.setRemoteIoToCrunChannel(cattachPid, taskId, stepId, TaskIoRequestChannel)
					}

					gVars.ctldReplyChannelMapMtx.Unlock()

					reply = &protos.StreamCattachReply{
						Type: protos.StreamCattachReply_TASK_CONNECT_REPLY,
						Payload: &protos.StreamCattachReply_PayloadTaskConnectReply{
							PayloadTaskConnectReply: &protos.StreamCattachReply_TaskConnectReply{
								Ok:            Ok,
								Task:          task,
								FailureReason: ctldReply.GetPayloadTaskMetaReply().FailureReason,
							},
						},
					}

					if err := toCattachStream.Send(reply); err != nil {
						log.Debugf("[Cfored<->Cattach][Step #%d.%d] Connection to cattach was broken.", taskId, stepId)
						state = End
						break
					}

					if Ok {
						state = CattachWaitIOForward
					} else {
						// Cattach task req failed
						// channel was already removed from gVars.ctldReplyChannelMapByPid
						log.Infof("[Cfored<->Cattach][Pid #%d] Task connect failed", cattachPid)
						break CforedCattachStateMachineLoop
					}
				}
			}
		case CattachWaitIOForward:
			log.Infof("[Cfored<->Cattach][Step #%d.%d] Enter State WAIT_TASK_IO_FORWARD.", taskId, stepId)

			stopWaiting := atomic.Bool{}
			stopWaiting.Store(false)
			readyChannel := make(chan bool, 1)
			go gSupervisorChanKeeper.waitSupervisorChannelsReady(execCranedIds, readyChannel, &stopWaiting, taskId, stepId)

			select {
			case ctldReply := <-ctldReplyChannel:
				if ctldReply.Type != protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY {
					log.Fatalf("[Ctld->Cfored->Cattach][Step #%d.%d] Expect type TASK_COMPLETION_ACK_REPLY but got %s, ignored",
						taskId, stepId, ctldReply.Type)
				} else {
					log.Debugf("[Ctld->Cfored->Cattach][Step #%d.%d] Receive COMPLETION_ACK_REPLY", taskId, stepId)
					state = DeadCattach
				}
				stopWaiting.Store(true)
			case <-readyChannel:
				reply = &protos.StreamCattachReply{
					Type: protos.StreamCattachReply_TASK_IO_FORWARD_READY,
					Payload: &protos.StreamCattachReply_PayloadTaskIoForwardReadyReply{
						PayloadTaskIoForwardReadyReply: &protos.StreamCattachReply_TaskIOForwardReadyReply{
							Ok: true,
						},
					},
				}
				forwardEstablished.Store(true)

				if err := toCattachStream.Send(reply); err != nil {
					log.Debugf("[Cfored<->Cattach][Step #%d.%d] Failed to send TASK_IO_FORWARD_READY to cattach: %s. "+
						"The connection to cattach was broken.", taskId, stepId, err.Error())
					state = End
				} else {
					state = CattachWaitTaskComplete
				}
			}
		case CattachWaitTaskComplete:
			log.Debugf("[Cfored<->Cattach][Job #%d] Enter State Cattach_Wait_Task_Complete", taskId)
			history := gSupervisorChanKeeper.getRemoteHistory(taskId, stepId)
			for _, taskMsg := range history {
				if taskMsg == nil {
					log.Errorf("[Supervisor->Cfored->Cattach][Step #%d.%d] One of Craneds [%v] down. Exit....",
						taskId, stepId, execCranedIds)
					// IO Channel from Craned was shut down unexpectedly.
					state = DeadCattach
					break
				}

				if err := forwardTaskMsgToCattach(taskId, stepId, taskMsg, toCattachStream); err != nil {
					state = End
					break
				}
			}
		forwarding:
			for {
				select {
				case ctldReply := <-ctldReplyChannel:
					if ctldReply.Type != protos.StreamCtldReply_TASK_COMPLETION_ACK_REPLY {
						log.Warningf("[Ctld->Cfored->Cattach][Step #%d.%d] Expect type TASK_COMPLETION_ACK_REPLY but got %s, ignored",
							taskId, stepId, ctldReply.Type)
					} else {
						log.Debugf("[Ctld->Cfored->Cattach][Step #%d.%d] Receive TASK_COMPLETION_ACK_REPLY", taskId, stepId)
						state = DeadCattach
						break forwarding
					}

				case item := <-RequestChannel:
					cattachRequest, err := item.message, item.err
					if err != nil {
						switch err {
						case io.EOF:
							fallthrough
						default:
							log.Debugf("[Cattach->Cfored][Step #%d.%d] Connection to cattach was broken.", taskId, stepId)
							state = End
							break forwarding
						}
					} else {
						switch cattachRequest.Type {
						case protos.StreamCattachRequest_TASK_IO_FORWARD:
							log.Debugf("[Cattach->Cfored->Supervisor][Step #%d.%d] Receive TASK_IO_FORWARD Request to"+
								" task, msg size[%d], EOF [%v]", taskId, stepId,
								len(cattachRequest.GetPayloadTaskIoForwardReq().GetMsg()),
								cattachRequest.GetPayloadTaskIoForwardReq().Eof)
							gSupervisorChanKeeper.forwardCattachRequestToSupervisor(taskId, stepId, cattachRequest)

						case protos.StreamCattachRequest_TASK_X11_FORWARD:
							log.Debugf("[Cattach->Cfored->Supervisor][Step #%d.%d] Receive Local TASK_X11_FORWARD to remote task",
								cattachRequest.GetPayloadTaskX11ForwardReq().GetTaskId(), stepId)
							gSupervisorChanKeeper.forwardCattachRequestToSupervisor(taskId, stepId, cattachRequest)

						case protos.StreamCattachRequest_TASK_COMPLETION_REQUEST:
							log.Debugf("[Cattach->Cfored->Ctld][Step #%d.%d] Receive TaskCompletionRequest", taskId, stepId)
							state = End
							break forwarding
						default:
							log.Fatalf("[Cattach->Cfored][Step #%d.%d] Expect TASK_COMPLETION_REQUEST or TASK_IO_FORWARD",
								taskId, stepId)
							break forwarding
						}
					}

				case taskMsg := <-TaskIoRequestChannel:
					if taskMsg == nil {
						log.Errorf("[Supervisor->Cfored->Cattach][Step #%d.%d] One of Craneds [%v] down. Exit....",
							taskId, stepId, execCranedIds)
						// IO Channel from Craned was shut down unexpectedly.
						state = DeadCattach
						break forwarding
					}

					if err := forwardTaskMsgToCattach(taskId, stepId, taskMsg, toCattachStream); err != nil {
						state = End
						break forwarding
					}
				}
			}
		case DeadCattach:
			log.Infof("[Cfored<->Cattach][Job #%d] Enter State DEAD_CATTACH", taskId)
			reply = &protos.StreamCattachReply{
				Type: protos.StreamCattachReply_TASK_COMPLETION_ACK_REPLY,
				Payload: &protos.StreamCattachReply_PayloadTaskCompletionAckReply{
					PayloadTaskCompletionAckReply: &protos.StreamCattachReply_TaskCompletionAckReply{
						Ok: true,
					},
				},
			}

			if err := toCattachStream.Send(reply); err != nil {
				log.Errorf("[Cfored->Cattach] Failed to send CompletionAck to cattach: %s. "+
					"The connection to cattach was broken.", err.Error())
			} else {
				log.Debug("[Cfored->Cattach] TASK_COMPLETION_ACK_REPLY sent to Cattach")
			}
			state = End
		case End:
			log.Infof("[Cfored<->Cattach][Job #%d] Enter State End", taskId)

			// remove cattach by taskId for ctldReplyChannel
			gVars.ctldReplyChannelMapMtx.Lock()
			if gVars.ctldReplyChannelMapForCattachByTaskId[taskId] != nil {
				delete(gVars.ctldReplyChannelMapForCattachByTaskId[taskId], cattachPid)
			}
			if len(gVars.ctldReplyChannelMapForCattachByTaskId[taskId]) == 0 {
				delete(gVars.ctldReplyChannelMapForCattachByTaskId, taskId)
			}
			// remove cattach by pid for ctldReplyChannel
			gVars.pidStepMapMtx.Lock()
			delete(gVars.pidStepMap, cattachPid)
			gVars.pidStepMapMtx.Unlock()

			gVars.ctldReplyChannelMapMtx.Unlock()

			// remove task IO channel
			gSupervisorChanKeeper.cattachStopAndRemoveChannel(taskId, stepId, cattachPid)

			break CforedCattachStateMachineLoop
		}
	}

	return nil
}

func forwardTaskMsgToCattach(
	taskId, stepId uint32,
	taskMsg *protos.StreamTaskIORequest,
	toCattachStream protos.CraneForeD_CattachStreamServer,
) error {
	var reply *protos.StreamCattachReply

	switch taskMsg.Type {
	case protos.StreamTaskIORequest_TASK_OUTPUT:
		reply = &protos.StreamCattachReply{
			Type: protos.StreamCattachReply_TASK_IO_FORWARD,
			Payload: &protos.StreamCattachReply_PayloadTaskIoForwardReply{
				PayloadTaskIoForwardReply: &protos.StreamCattachReply_TaskIOForwardReply{
					Msg: taskMsg.GetPayloadTaskOutputReq().Msg,
				},
			},
		}
		log.Tracef("[Supervisor->Cfored->Cattach][Step #%d.%d] forwarding msg size[%d]",
			taskId, stepId, len(taskMsg.GetPayloadTaskOutputReq().GetMsg()))
	case protos.StreamTaskIORequest_TASK_X11_OUTPUT:
		reply = &protos.StreamCattachReply{
			Type: protos.StreamCattachReply_TASK_X11_FORWARD,
			Payload: &protos.StreamCattachReply_PayloadTaskX11ForwardReply{
				PayloadTaskX11ForwardReply: &protos.StreamCattachReply_TaskX11ForwardReply{
					Msg: taskMsg.GetPayloadTaskX11OutputReq().Msg,
				},
			},
		}
		log.Tracef("[Supervisor->Cfored->Cattach][Step #%d.%d] forwarding x11 msg size[%d]",
			taskId, stepId, len(taskMsg.GetPayloadTaskX11OutputReq().Msg))
	default:
		log.Fatalf("[Supervisor->Cfored->Cattach][Step #%d.%d] Expect Type TASK_OUTPUT or TASK_X11_OUTPUT.",
			taskId, stepId)
		return errors.New("unexpected taskMsg.Type")
	}

	if err := toCattachStream.Send(reply); err != nil {
		log.Debugf("[Supervisor->Cfored->Cattach][Step #%d.%d] Failed to send reply to cattach: %s. "+
			"The connection to cattach was broken.", taskId, stepId, err.Error())
		return err
	}
	return nil
}
