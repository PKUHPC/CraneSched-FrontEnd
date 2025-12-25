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

package crun

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"errors"
	"os/user"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"

	"github.com/pkg/term/termios"
	"golang.org/x/sys/unix"

	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type StateOfCrun int

const (
	ConnectCfored StateOfCrun = 0
	ReqTaskId     StateOfCrun = 1
	WaitRes       StateOfCrun = 2
	WaitForward   StateOfCrun = 3
	Forwarding    StateOfCrun = 4
	TaskKilling   StateOfCrun = 5
	WaitAck       StateOfCrun = 6
	End           StateOfCrun = 7
)

const (
	FlagIOForwardALL  string = "all"
	FlagIOForwardNONE string = "none"
)

type GlobalVariables struct {
	cwd string

	globalCtx       context.Context
	globalCtxCancel context.CancelFunc

	connectionBroken bool
}

var gVars GlobalVariables

type ReplyReceiveItem struct {
	reply *protos.StreamCrunReply
	err   error
}

type StateMachineOfCrun struct {
	job    *protos.TaskToCtld
	step   *protos.StepToCtld
	jobId  uint32 // This field will be set after ReqTaskId state
	stepId uint32 // This field will be set after ReqTaskId state

	cranedId      []string
	cranedTaskMap map[string][]uint32 // craned to task ids map
	ntasksTotal   uint32

	inputFlag  string // Crun --input flag, used to determine how to read input from stdin
	outputFlag string // Crun --output flag, used to determine how to write output to stdout
	errorFlag  string // Crun --err flag, used to determine how to write error to stderr

	state StateOfCrun
	err   util.ExitCode // Hold the final error of the state machine if any

	// Hold grpc resources and will be freed in Close.
	conn   *grpc.ClientConn
	client protos.CraneForeDClient
	stream grpc.BidiStreamingClient[protos.StreamCrunRequest, protos.StreamCrunReply]

	sigs         chan os.Signal
	savedPtyAttr unix.Termios

	cforedReplyReceiver *CforedReplyReceiver

	// These fields are used under Forwarding State.
	stopStepCtx context.Context
	stopStepCb  context.CancelFunc
	//stop step will stop reading from local stdin/file/x11
	stopReadCtx             context.Context
	stopWriteCtx            context.Context
	chanInputFromLocal      chan []byte
	chanOutputFromRemote    chan []byte
	chanErrOutputFromRemote chan []byte
	X11SessionMgr           *X11SessionMgr
}
type CforedReplyReceiver struct {
	stream       protos.CraneForeD_CrunStreamClient
	replyChannel chan ReplyReceiveItem
}

func (r *CforedReplyReceiver) GetReplyChannel() chan ReplyReceiveItem {
	return r.replyChannel
}

func (r *CforedReplyReceiver) StartReplyReceiveRoutine(stream protos.CraneForeD_CrunStreamClient) {
	r.stream = stream
	r.replyChannel = make(chan ReplyReceiveItem, 8)
	go r.ReplyReceiveRoutine()
}

func (r *CforedReplyReceiver) ReplyReceiveRoutine() {
	for {
		cforedReply, err := r.stream.Recv()
		r.replyChannel <- ReplyReceiveItem{
			reply: cforedReply,
			err:   err,
		}
		if err != nil {
			if err != io.EOF {
				log.Errorf("Failed to receive CforedReply: %s. "+
					"Connection to Cfored is broken. "+
					"ReplyReceiveRoutine is exiting...", err)
			}
			break
		}
	}
}

func (m *StateMachineOfCrun) Init(job *protos.TaskToCtld, step *protos.StepToCtld) {
	m.job = job
	m.step = step
	m.state = ConnectCfored
	m.err = util.ErrorSuccess

	m.sigs = make(chan os.Signal, 1)
	signal.Notify(m.sigs, syscall.SIGINT, syscall.SIGTTOU)
}

func (m *StateMachineOfCrun) Close() {
	err := m.conn.Close()
	if err != nil {
		log.Errorf("Failed to close grpc conn: %s", err)
	}

	if FlagPty {
		err = termios.Tcsetattr(os.Stdin.Fd(), termios.TCSANOW, &m.savedPtyAttr)
		if err != nil {
			log.Errorf("Failed to restore stdin attr: %s", err.Error())
		}
	}
}

func (m *StateMachineOfCrun) StateConnectCfored() {
	config := util.ParseConfig(FlagConfigFilePath)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithKeepaliveParams(util.ClientKeepAliveParams))
	opts = append(opts, grpc.WithConnectParams(util.ClientConnectParams))

	unixSocketPath := "unix:///" + config.CranedCforedSockPath

	var err error
	m.conn, err = grpc.NewClient(unixSocketPath, opts...)
	if err != nil {
		log.Errorf("Failed to connect to local unix socket %s: %s",
			unixSocketPath, err)

		m.err = util.ErrorBackend
		m.state = End
		return
	}

	m.client = protos.NewCraneForeDClient(m.conn)

	log.Trace("Sending Task Req to Cfored")
	m.stream, err = m.client.CrunStream(gVars.globalCtx)
	if err != nil {
		log.Errorf("Failed to create CrunStream: %s.", err)
		m.err = util.ErrorNetwork
		m.state = End
		return
	}

	m.cforedReplyReceiver = new(CforedReplyReceiver)
	m.cforedReplyReceiver.StartReplyReceiveRoutine(m.stream)
	var request *protos.StreamCrunRequest
	if m.job != nil {
		request = &protos.StreamCrunRequest{
			Type: protos.StreamCrunRequest_TASK_REQUEST,
			Payload: &protos.StreamCrunRequest_PayloadTaskReq{
				PayloadTaskReq: &protos.StreamCrunRequest_TaskReq{
					Task:    m.job,
					CrunPid: int32(os.Getpid()),
				},
			},
		}
	} else {
		request = &protos.StreamCrunRequest{
			Type: protos.StreamCrunRequest_STEP_REQUEST,
			Payload: &protos.StreamCrunRequest_PayloadStepReq{
				PayloadStepReq: &protos.StreamCrunRequest_StepReq{
					Step:    m.step,
					CrunPid: int32(os.Getpid()),
				},
			},
		}
	}

	if err := m.stream.Send(request); err != nil {
		log.Errorf("Failed to send Request to CrunStream: %s. "+
			"Connection to Crun is broken", err)
		gVars.connectionBroken = true

		m.state = End
		m.err = util.ErrorNetwork
		return
	}

	m.state = ReqTaskId
}

func (m *StateMachineOfCrun) StateReqTaskId() {
	log.Trace("Waiting TaskId")
	select {
	case item := <-m.cforedReplyReceiver.GetReplyChannel():
		cforedReply, err := item.reply, item.err

		if err != nil {
			switch err {
			case io.EOF:
				fallthrough
			default:
				log.Errorf("Connection to Cfored broken when requesting "+
					"job id: %s. Exiting...", err)
				gVars.connectionBroken = true
				m.state = End
				m.err = util.ErrorNetwork
				return
			}
		}

		if cforedReply.Type != protos.StreamCrunReply_TASK_ID_REPLY {
			log.Errorln("Expect type TASK_ID_REPLY")
			m.state = End
			m.err = util.ErrorBackend
			return
		}
		payload := cforedReply.GetPayloadTaskIdReply()

		if payload.Ok {
			m.jobId = payload.JobId
			m.stepId = payload.StepId
			if m.step == nil {
				fmt.Printf("Task id allocated: %d, waiting resources.\n", m.jobId)
			} else {
				fmt.Printf("Job %d step %d allocated, waiting resources.\n", m.jobId, m.stepId)
			}
			m.state = WaitRes
		} else {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to allocate job id: %s\n", payload.FailureReason)
			m.state = End
			m.err = util.ErrorBackend
			return
		}
	case sig := <-m.sigs:
		if sig == syscall.SIGINT {
			log.Tracef("SIGINT Received. Not allowed to cancel job when ReqTaskId")
		} else {
			log.Tracef("Unhanled sig %s", sig.String())
		}
	}
}

func (m *StateMachineOfCrun) StateWaitRes() {
	log.Trace("Waiting Res Alloc")
	select {
	case item := <-m.cforedReplyReceiver.GetReplyChannel():
		cforedReply, err := item.reply, item.err

		if err != nil { // Failure Edge
			switch err {
			case io.EOF:
				fallthrough
			default:
				log.Errorf("Connection to Cfored broken when waiting "+
					"resource allocated: %s. Exiting...", err)
				gVars.connectionBroken = true
				m.state = End
				m.err = util.ErrorNetwork
				return
			}
		}

		switch cforedReply.Type {
		case protos.StreamCrunReply_TASK_RES_ALLOC_REPLY:
			cforedPayload := cforedReply.GetPayloadTaskAllocReply()
			Ok := cforedPayload.Ok

			if Ok {
				fmt.Printf("Allocated craned nodes: %s\n", cforedPayload.AllocatedCranedRegex)

				m.cranedId = cforedPayload.CranedIds
				m.cranedTaskMap = make(map[string][]uint32)
				for craned, tasks := range cforedPayload.CranedTaskMap {
					m.cranedTaskMap[craned] = tasks.TaskIds
				}
				m.ntasksTotal = cforedPayload.NtasksTotal

				m.state = WaitForward
			} else {
				log.Errorln("Failed to allocate job resource. Exiting...")
				m.state = End
				m.err = util.ErrorBackend
				return
			}
		case protos.StreamCrunReply_TASK_CANCEL_REQUEST:
			log.Tracef("Received Task Cancel Request when wait res")
			m.state = TaskKilling
		}

	case sig := <-m.sigs:
		if sig == syscall.SIGINT {
			log.Tracef("SIGINT Received. Cancelling the job...")
			m.state = TaskKilling
		} else {
			log.Tracef("Unhandled sig %s", sig.String())
			m.state = TaskKilling
		}
	}
}

func (m *StateMachineOfCrun) StateWaitForward() {
	select {
	case item := <-m.cforedReplyReceiver.GetReplyChannel():
		cforedReply, err := item.reply, item.err
		if err != nil { // Failure Edge
			switch err {
			case io.EOF:
				fallthrough
			default:
				log.Errorf("Connection to Cfored broken when waiting for forwarding user i/o "+
					"%s. Exiting...", err)
				gVars.connectionBroken = true
				m.state = End
				m.err = util.ErrorNetwork
				return
			}
		}
		switch cforedReply.Type {
		case protos.StreamCrunReply_TASK_IO_FORWARD_READY:
			cforedPayload := cforedReply.GetPayloadTaskIoForwardReadyReply()
			Ok := cforedPayload.Ok
			if Ok {
				fmt.Println("Task io forward ready, waiting input.")
				m.state = Forwarding
				return
			} else {
				log.Errorln("Failed to wait for job io forward ready. Exiting...")
				m.state = End
				m.err = util.ErrorBackend
				return
			}
		case protos.StreamCrunReply_TASK_CANCEL_REQUEST:
			m.state = TaskKilling
		case protos.StreamCrunReply_TASK_COMPLETION_ACK_REPLY:
			// Task launch failed !
			fmt.Println("Task failed ")
			m.state = End
			m.err = util.ErrorBackend
			return
		default:
			log.Errorf("Received unhandeled msg type %s", cforedReply.Type.String())
			m.state = TaskKilling
		}

	case sig := <-m.sigs:
		if sig == syscall.SIGINT {
			m.state = TaskKilling
		} else {
			log.Tracef("Unhanled sig %s", sig.String())
			m.state = TaskKilling
		}
	}
}

func (m *StateMachineOfCrun) StateForwarding() {
	var request *protos.StreamCrunRequest
	var taskIdWithInput *uint32

	if FlagPty {
		ptyAttr := unix.Termios{}
		err := termios.Tcgetattr(os.Stdin.Fd(), &ptyAttr)
		if err != nil {
			log.Errorf("Failed to get stdin attr: %s,killing", err.Error())
			m.state = TaskKilling
			m.err = util.ErrorSystem
			return
		}
		m.savedPtyAttr = ptyAttr

		termios.Cfmakeraw(&ptyAttr)
		termios.Cfmakecbreak(&ptyAttr)
		err = termios.Tcsetattr(os.Stdin.Fd(), termios.TCSANOW, &ptyAttr)
		if err != nil {
			log.Errorf("Failed to get stdin attr: %s,killing", err.Error())
			m.state = TaskKilling
			m.err = util.ErrorSystem
			return
		}
	}

	m.StartIOForward()
	var x11ReqFromLocal chan *protos.StreamCrunRequest
	if m.X11SessionMgr != nil {
		x11ReqFromLocal = m.X11SessionMgr.X11RequestChan
	} else {
		x11ReqFromLocal = nil
	}

	parsedId, err := strconv.ParseUint(m.inputFlag, 10, 32)
	if err == nil {
		if parsedId < uint64(m.ntasksTotal) {
			taskIdWithInput = new(uint32)
			*taskIdWithInput = uint32(parsedId)
		} else {
			log.Tracef("The task id %d specified in --input is out of range [0, %d), "+
				"consider it a file path, input is broadcasted.", parsedId, m.ntasksTotal)
		}
	}

	// Forward input to Cfored.
	go func() {
		for {
			select {
			case msg, ok := <-m.chanInputFromLocal:
				if !ok {
					msg = nil
				}
				request = &protos.StreamCrunRequest{
					Type: protos.StreamCrunRequest_TASK_IO_FORWARD,
					Payload: &protos.StreamCrunRequest_PayloadTaskIoForwardReq{
						PayloadTaskIoForwardReq: &protos.StreamCrunRequest_TaskIOForwardReq{
							Msg:    msg,
							Eof:    msg == nil,
							TaskId: taskIdWithInput,
						},
					},
				}
				if err := m.stream.Send(request); err != nil {
					log.Errorf("Failed to send Task Request to CrunStream: %s. "+
						"Connection to Crun is broken", err)
					gVars.connectionBroken = true
					return
				}
				if msg == nil {
					return
				}

			case request := <-x11ReqFromLocal:
				if err := m.stream.Send(request); err != nil {
					log.Errorf("Failed to send Task X11 Input to CrunStream: %s. "+
						"Connection to Crun is broken", err)
					gVars.connectionBroken = true
					return
				}

			case <-m.stopReadCtx.Done():
				return
			}
		}
	}()

	for m.state == Forwarding {
		select {
		case <-m.stopStepCtx.Done():
			request = &protos.StreamCrunRequest{
				Type: protos.StreamCrunRequest_TASK_COMPLETION_REQUEST,
				Payload: &protos.StreamCrunRequest_PayloadTaskCompleteReq{
					PayloadTaskCompleteReq: &protos.StreamCrunRequest_TaskCompleteReq{
						Status: protos.TaskStatus_Completed,
					},
				},
			}

			log.Debug("Sending TASK_COMPLETION_REQUEST with COMPLETED state...")
			if err := m.stream.Send(request); err != nil {
				log.Errorf("The connection to Cfored was broken: %s. "+
					"Exiting...", err)
				gVars.connectionBroken = true
				m.state = End
				m.err = util.ErrorNetwork
			} else {
				m.state = WaitAck
			}

		case item := <-m.cforedReplyReceiver.replyChannel:
			cforedReply, err := item.reply, item.err
			if err != nil {
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Errorf("The connection to Cfored was broken: %s. "+
						"Killing job...", err)
					gVars.connectionBroken = true
					m.err = util.ErrorNetwork
					m.state = TaskKilling
				}
			} else {
				switch cforedReply.Type {
				case protos.StreamCrunReply_TASK_IO_FORWARD:
					select {
					case m.chanOutputFromRemote <- cforedReply.GetPayloadTaskIoForwardReply().Msg:
					case <-m.stopWriteCtx.Done():
					}
				case protos.StreamCrunReply_TASK_ERR_OUTPUT_FORWARD:
					select {
					case m.chanErrOutputFromRemote <- cforedReply.GetPayloadTaskIoErrOutputForwardReply().Msg:
					case <-m.stopWriteCtx.Done():
					}

				case protos.StreamCrunReply_TASK_X11_CONN:
					fallthrough
				case protos.StreamCrunReply_TASK_X11_FORWARD:
					fallthrough
				case protos.StreamCrunReply_TASK_X11_EOF:
					m.X11SessionMgr.X11ReplyChan <- cforedReply

				case protos.StreamCrunReply_TASK_EXIT_STATUS:
					exitStatus := cforedReply.GetPayloadTaskExitStatusReply()
					if exitStatus.ExitCode != 0 {
						if exitStatus.Signaled {
							fmt.Fprintf(os.Stderr, "error: task %d: Terminated\n", exitStatus.TaskId)
						} else {
							fmt.Fprintf(os.Stderr, "error: task %d: Exited with exit code %d\n",
								exitStatus.TaskId, exitStatus.ExitCode)
						}
						m.err = int(exitStatus.ExitCode)
					}

				case protos.StreamCrunReply_TASK_CANCEL_REQUEST:
					m.stopStepCb()
					log.Trace("Received TASK_CANCEL_REQUEST")
					m.state = TaskKilling

				case protos.StreamCrunReply_TASK_COMPLETION_ACK_REPLY:
					log.Debug("Task completed.")
					m.state = End
				}
			}
		}
	}

}

func (m *StateMachineOfCrun) StateTaskKilling() {
	request := &protos.StreamCrunRequest{
		Type: protos.StreamCrunRequest_TASK_COMPLETION_REQUEST,
		Payload: &protos.StreamCrunRequest_PayloadTaskCompleteReq{
			PayloadTaskCompleteReq: &protos.StreamCrunRequest_TaskCompleteReq{
				Status: protos.TaskStatus_Cancelled,
			},
		},
	}

	if gVars.connectionBroken {
		log.Errorf("The connection to Cfored was broken. Exiting...")
		m.err = util.ErrorNetwork
		m.state = End
		return
	}

	log.Debug("Sending TASK_COMPLETION_REQUEST with CANCELLED state...")
	if err := m.stream.Send(request); err != nil {
		log.Errorf("The connection to Cfored was broken: %s. Exiting...", err)
		gVars.connectionBroken = true
		m.err = util.ErrorNetwork
		m.state = End
		return
	} else {
		m.state = WaitAck
	}
}

func (m *StateMachineOfCrun) StateWaitAck() {
	log.Debug("Waiting Ctld TASK_COMPLETION_ACK_REPLY")
	item := <-m.cforedReplyReceiver.replyChannel
	cforedReply, err := item.reply, item.err

	if err != nil {
		switch err {
		case io.EOF:
			fallthrough
		default:
			log.Errorf("The connection to Cfored was broken: %s. "+
				"Exiting...", err)
			gVars.connectionBroken = true
			m.err = util.ErrorNetwork
			m.state = End
			return
		}
	}

	switch cforedReply.Type {
	case protos.StreamCrunReply_TASK_IO_FORWARD:
		select {
		case m.chanOutputFromRemote <- cforedReply.GetPayloadTaskIoForwardReply().Msg:
		case <-m.stopWriteCtx.Done():
		}
		return // Still in WaitAck state
	case protos.StreamCrunReply_TASK_ERR_OUTPUT_FORWARD:
		select {
		case m.chanErrOutputFromRemote <- cforedReply.GetPayloadTaskIoErrOutputForwardReply().Msg:
		case <-m.stopWriteCtx.Done():
		}
		return

	case protos.StreamCrunReply_TASK_X11_CONN:
		fallthrough
	case protos.StreamCrunReply_TASK_X11_FORWARD:
		fallthrough
	case protos.StreamCrunReply_TASK_X11_EOF:
		m.X11SessionMgr.X11ReplyChan <- cforedReply
		return // Still in WaitAck state

	case protos.StreamCrunReply_TASK_EXIT_STATUS:
		exitStatus := cforedReply.GetPayloadTaskExitStatusReply()
		if exitStatus.ExitCode != 0 {
			if exitStatus.Signaled {
				fmt.Fprintf(os.Stderr, "error: task %d: Terminated\n", exitStatus.TaskId)
			} else {
				fmt.Fprintf(os.Stderr, "error: task %d: Exited with exit code %d\n",
					exitStatus.TaskId, exitStatus.ExitCode)
			}
			m.err = int(exitStatus.ExitCode)
		}
		return // Still in WaitAck state

	case protos.StreamCrunReply_TASK_CANCEL_REQUEST:
		log.Fatalf("Received TASK_CANCEL_REQUEST in WaitAck state.")

	case protos.StreamCrunReply_TASK_COMPLETION_ACK_REPLY:
		log.Debug("Task completed.")
		m.state = End
	}

	if cforedReply.Type != protos.StreamCrunReply_TASK_COMPLETION_ACK_REPLY {
		log.Errorf("Expect TASK_COMPLETION_ACK_REPLY. bug get %s\n", cforedReply.Type.String())
		m.err = util.ErrorBackend
		m.state = End
		return
	}

	if cforedReply.GetPayloadTaskCompletionAckReply().Ok {
		log.Debug("Task completed.")
	} else {
		log.Errorln("Failed to notify server of job completion")
		m.err = util.ErrorBackend
	}

	m.state = End
}

func (m *StateMachineOfCrun) Run() {
CrunStateMachineLoop:
	for {
		switch m.state {
		case ConnectCfored:
			m.StateConnectCfored()

		case ReqTaskId:
			m.StateReqTaskId()

		case WaitRes:
			m.StateWaitRes()

		case WaitForward:
			m.StateWaitForward()

		case Forwarding:
			m.StateForwarding()

		case TaskKilling:
			m.StateTaskKilling()

		case WaitAck:
			m.StateWaitAck()
		case End:
			{
				break CrunStateMachineLoop
			}
		}
	}
}

func (m *StateMachineOfCrun) forwardingSigHandlerRoutine() {
	signal.Ignore(syscall.SIGTTOU, syscall.SIGTTIN)

	lastSigint := time.Now().Add(-2 * time.Second)
loop:
	for {
		select {
		case sig := <-m.sigs:
			switch sig {
			/*
				multiple sigint will cancel this job
			*/
			case syscall.SIGINT:
				log.Tracef("Recv signal: %v", sig)
				now := time.Now()
				if lastSigint.Add(time.Second).After(now) {
					m.stopStepCb()
					break loop
				} else {
					lastSigint = now
					fmt.Println("Send interrupt once more in 1s to abort.")
				}

			default:
				log.Tracef("Ignored signal: %v", sig)
			}
		}
	}
	log.Tracef("Signal processing goroutine exit.")
}

func (m *StateMachineOfCrun) StdoutWriterRoutine() {
	log.Trace("Starting StdoutWriterRoutine")
	writer := bufio.NewWriter(os.Stdout)

writing:
	for {
		select {

		case msg := <-m.chanOutputFromRemote:
			_, err := writer.Write(msg)

			if err != nil {
				fmt.Printf("Failed to write to fd: %v\n", err)
				break writing
			}
			err = writer.Flush()
			if err != nil {
				fmt.Printf("Failed to flush to fd: %v\n", err)
				break writing
			}

		case <-m.stopWriteCtx.Done():
			break writing
		}
	}
}

func (m *StateMachineOfCrun) StderrWriterRoutine() {
	log.Trace("Starting StderrWriterRoutine")
	writer := bufio.NewWriter(os.Stderr)

writing:
	for {
		select {
		case msg := <-m.chanErrOutputFromRemote:
			_, err := writer.Write(msg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write to stderr: %v\n", err)
				break writing
			}
			if err := writer.Flush(); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to flush to stderr: %v\n", err)
				break writing
			}
		case <-m.stopWriteCtx.Done():
			break writing
		}
	}
}

func (m *StateMachineOfCrun) DiscardRoutine(src <-chan []byte, name string) {
	log.Tracef("Starting DiscardRoutine(%s)", name)
	for {
		select {
		case <-src:
		case <-m.stopWriteCtx.Done():
			return
		}
	}
}

func (m *StateMachineOfCrun) FileWriterRoutine(filePath string, src <-chan []byte) {
	if filePath == "" {
		return
	}

	if dir := filepath.Dir(filePath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Errorf("Failed to create output dir %s: %s", dir, err)
			m.stopStepCb()
			return
		}
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		log.Errorf("Failed to open file %s: %s", filePath, err)
		m.stopStepCb()
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Errorf("Failed to close file %s: %s", filePath, err)
		}
	}()

	writer := bufio.NewWriter(file)

writing:
	for {
		select {
		case msg := <-src:
			if _, err := writer.Write(msg); err != nil {
				log.Errorf("Failed to write to file %s: %s", filePath, err)
				m.stopStepCb()
				break writing
			}
			if err := writer.Flush(); err != nil {
				log.Errorf("Failed to flush file %s: %s", filePath, err)
				m.stopStepCb()
				break writing
			}
		case <-m.stopWriteCtx.Done():
			break writing
		}
	}
}

func (m *StateMachineOfCrun) StdoutFileWriterRoutine(filePattern string) {
	parsedFilePath, isLocalFile, err := m.ParseFilePattern(filePattern)
	if err != nil {
		log.Errorf("Failed to parse file pattern %s: %s", filePattern, err)
		m.stopStepCb()
		return
	}
	if !isLocalFile {
		log.Debugf("Output file pattern is remote-only, skip: %s", filePattern)
		return
	}
	log.Debugf("Writing stdout to file %s", parsedFilePath)
	m.FileWriterRoutine(parsedFilePath, m.chanOutputFromRemote)
}

func (m *StateMachineOfCrun) StderrFileWriterRoutine(filePattern string) {
	parsedFilePath, isLocalFile, err := m.ParseFilePattern(filePattern)
	if err != nil {
		log.Errorf("Failed to parse file pattern %s: %s", filePattern, err)
		m.stopStepCb()
		return
	}
	if !isLocalFile {
		log.Debugf("Error file pattern is remote-only, skip: %s", filePattern)
		return
	}
	log.Debugf("Writing stderr to file %s", parsedFilePath)
	m.FileWriterRoutine(parsedFilePath, m.chanErrOutputFromRemote)
}

func (m *StateMachineOfCrun) StdinReaderRoutine() {

	err := syscall.SetNonblock(int(os.Stdin.Fd()), true)
	if err != nil {
		return
	}

	epfd, err := syscall.EpollCreate1(0)
	if err != nil {
		log.Tracef("EpollCreate1: %v", err)
		return
	}

	event := &syscall.EpollEvent{
		Events: syscall.EPOLLIN,
		Fd:     int32(int(os.Stdin.Fd())),
	}

	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, int(os.Stdin.Fd()), event); err != nil {
		log.Tracef("EpollCtl: %v", err)
		return
	}

	defer syscall.Close(epfd)
	defer close(m.chanInputFromLocal)
	events := make([]syscall.EpollEvent, 10)
	buf := make([]byte, 4096)
reading:
	for {
		if FlagPty {
			ptyAttr := unix.Termios{}
			err := termios.Tcgetattr(os.Stdin.Fd(), &ptyAttr)
			if err != nil {
				log.Errorf("Failed to get stdin attr")
			}
			termios.Cfmakeraw(&ptyAttr)
			err = termios.Tcsetattr(os.Stdin.Fd(), termios.TCSANOW, &ptyAttr)
			if err != nil {
				log.Errorf("Failed to set stdin attr")
			}
		}

		select {
		case <-m.stopReadCtx.Done():
			break reading
		default:
		}

		n, err := syscall.EpollWait(epfd, events, 100) //100MS timeout
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				log.Trace("EpollWait interrupted by signal, retrying")
				continue
			}
			log.Tracef("EpollWait: %v", err)
			return
		}
		for i := 0; i < n; i++ {
			if events[i].Fd == int32(os.Stdin.Fd()) && events[i].Events&syscall.EPOLLIN != 0 {
				nr, err := syscall.Read(int(os.Stdin.Fd()), buf)
				if err != nil {
					if errors.Is(err, syscall.EAGAIN) {
						log.Trace("Read EAGAIN, no data available now")
						continue
					}
					if errors.Is(err, syscall.EINTR) {
						log.Trace("Read interrupted by signal, retrying")
						continue
					}
					if errors.Is(err, syscall.EIO) {
						log.Trace("Read EIO.")
						continue
					}
					return
				}
				if nr == 0 {
					log.Trace("Read 0 bytes (EOF), closing channel and exiting goroutine")
					return
				}
				m.chanInputFromLocal <- buf[:nr]
				log.Tracef("Sent %d bytes to channel", nr)
			}
		}
	}

}

func (m *StateMachineOfCrun) ParseFilePattern(pattern string) (string, bool, error) {
	log.Tracef("Parsefile pattern: %s", pattern)
	if pattern == "" {
		return pattern, true, nil
	}
	var uid uint32
	var name string
	if m.job != nil {
		uid = m.job.Uid
		name = m.job.Name
	} else {
		uid = m.step.Uid
		name = m.step.Name
	}
	currentUser, err := user.LookupId(fmt.Sprintf("%d", uid))
	if err != nil {
		return pattern, true, fmt.Errorf("failed to lookup user by uid %d: %s", uid, err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return pattern, true, fmt.Errorf("failed to get hostname:%s", err)
	}
	nodeId := slices.Index(m.cranedId, hostname)
	if nodeId == -1 {
		return pattern, true, fmt.Errorf("failed to find hostname %s in allocated craned nodes", hostname)
	}
	// User input two backslash , but we will only get one.
	if strings.Contains(pattern, "\\") {
		return strings.ReplaceAll(pattern, "\\", ""), true, nil
	}

	remoteReplacements := map[string]struct{}{
		//short hostname
		"%N": {},
		//Node identifier relative to current job (e.g. "0" is the first node of the running job)
		"%n": {},
		// task id in step
		"%t": {},
	}

	localReplacements := map[string]string{
		"%%": "%",
		//Job array's master job allocation number.
		//"%A": "",
		//Job array ID (index) number.
		//"%a": "",
		//jobid.stepid of the running job (e.g. "128.0")
		"%J": fmt.Sprintf("%d.%d", m.jobId, m.stepId),
		// job id
		"%j": fmt.Sprintf("%d", m.jobId),
		// step id
		"%s": fmt.Sprintf("%d", m.stepId),
		//User name
		"%u": currentUser.Username,
		// Job name
		"%x": name,
	}

	re := regexp.MustCompile(`%%|%(\d*)([AajJsNntuUx])`)

	isLocalFile := true

	result := re.ReplaceAllStringFunc(pattern, func(match string) string {
		parts := re.FindStringSubmatch(match)
		if parts[0] == "%%" {
			return "%"
		}
		if len(parts) < 3 {
			return match // fallback
		}

		padding := parts[1]   // '5' in '%5j'
		specifier := parts[2] // 'j' in '%5j'

		_, foundInRemote := remoteReplacements["%"+specifier]
		if foundInRemote {
			isLocalFile = false
		}

		value, found := localReplacements["%"+specifier]
		if !found {
			return match // fallback
		}

		if specifier == "j" || specifier == "s" {
			if padding == "" {
				return value
			}
			_, err := strconv.Atoi(padding)
			if err != nil {
				return value
			}
			paddedFormat := "%0" + padding + "v"
			return fmt.Sprintf(paddedFormat, value)
		}

		return value
	})
	if !isLocalFile {
		return "", false, nil
	} else {
		return result, true, nil
	}
}

func (m *StateMachineOfCrun) FileReaderRoutine(filePattern string) {
	defer func() {
		// File input producer owns closing the input channel.
		// The forwarder goroutine must handle channel close gracefully.
		close(m.chanInputFromLocal)
	}()

	parsedFilePath, isLocalFile, err := m.ParseFilePattern(filePattern)
	if err != nil {
		log.Errorf("Failed to parse file pattern %s: %s", filePattern, err)
		m.chanInputFromLocal <- nil
		m.stopStepCb()
		return
	}
	if !isLocalFile {
		log.Debugf("Input file is not a local file: %s", filePattern)
		m.chanInputFromLocal <- nil
		m.stopStepCb()
		return
	}
	file, err := os.Open(parsedFilePath)
	if err != nil {
		log.Errorf("Failed to open file %s: %s", parsedFilePath, err)
		m.chanInputFromLocal <- nil
		m.stopStepCb()
		return
	}
	log.Debugf("Reading from file %s", parsedFilePath)
	reader := bufio.NewReader(file)
	buffer := make([]byte, 4096)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf("Failed to close stdin file: %s.", err)
		}
	}(file)
reading:
	for {
		select {
		case <-m.stopReadCtx.Done():
			break reading
		default:
			n, err := reader.Read(buffer)
			if err != nil {
				if err == io.EOF {
					m.chanInputFromLocal <- buffer[:n]
					m.chanInputFromLocal <- nil
					break reading
				}
				log.Errorf("Failed to read from fd: %v", err)
				break reading
			}
			m.chanInputFromLocal <- buffer[:n]
		}
	}
}

func (m *StateMachineOfCrun) StartIOForward() {
	m.stopStepCtx, m.stopStepCb = context.WithCancel(context.Background())
	m.stopReadCtx = m.stopStepCtx
	m.stopWriteCtx = m.stopStepCtx

	m.chanInputFromLocal = make(chan []byte, 100)
	m.chanOutputFromRemote = make(chan []byte, 20)
	m.chanErrOutputFromRemote = make(chan []byte, 20)
	go m.forwardingSigHandlerRoutine()
	if strings.ToLower(m.inputFlag) == FlagIOForwardALL {
		log.Debugf("Input from stdin to all tasks")
		go m.StdinReaderRoutine()
	} else if strings.ToLower(m.inputFlag) == FlagIOForwardNONE {
		log.Debugf("No input forwarding")
	} else {
		taskId, err := strconv.ParseUint(m.inputFlag, 10, 32)
		if err != nil {
			log.Debugf("Input from file %s, filepath is not a number", m.inputFlag)
			go m.FileReaderRoutine(m.inputFlag)
		} else {
			if taskId < uint64(m.ntasksTotal) {
				log.Debugf("Input from stdin to %d", taskId)
				go m.StdinReaderRoutine()
			} else {
				log.Debugf("Input from file %s, num but greater than ntasksTotal %d", m.inputFlag, m.ntasksTotal)
				go m.FileReaderRoutine(m.inputFlag)

			}
		}
	}

	if strings.ToLower(m.outputFlag) == FlagIOForwardALL {
		log.Debugf("Output to stdout")
		go m.StdoutWriterRoutine()
	} else if strings.ToLower(m.outputFlag) == FlagIOForwardNONE {
		log.Debugf("Output discarded")
		go m.DiscardRoutine(m.chanOutputFromRemote, "stdout")
	} else {
		taskId, err := strconv.ParseUint(m.outputFlag, 10, 32)
		if err != nil {
			log.Debugf("Output to file %s", m.outputFlag)
			go m.StdoutFileWriterRoutine(m.outputFlag)
		} else {
			if taskId < uint64(m.ntasksTotal) {
				log.Debugf("Output to stdout (filtered by sender task %d)", taskId)
				go m.StdoutWriterRoutine()
			} else {
				log.Debugf("Output to file %s (task id %d >= ntasksTotal %d)", m.outputFlag, taskId, m.ntasksTotal)
				go m.StdoutFileWriterRoutine(m.outputFlag)
			}
		}
	}

	if strings.ToLower(m.errorFlag) == FlagIOForwardALL {
		log.Debugf("Stderr output to stderr")
		go m.StderrWriterRoutine()
	} else if strings.EqualFold(m.errorFlag, "none") {
		log.Debugf("Stderr output discarded")
		go m.DiscardRoutine(m.chanErrOutputFromRemote, "stderr")
	} else {
		taskId, err := strconv.ParseUint(m.errorFlag, 10, 32)
		if err != nil {
			log.Debugf("Stderr output to file %s", m.errorFlag)
			go m.StderrFileWriterRoutine(m.errorFlag)
		} else {
			if taskId < uint64(m.ntasksTotal) {
				log.Debugf("Stderr output to stderr (filtered by sender task %d)", taskId)
				go m.StderrWriterRoutine()
			} else {
				log.Debugf("Stderr output to file %s (task id %d >= ntasksTotal %d)", m.errorFlag, taskId, m.ntasksTotal)
				go m.StderrFileWriterRoutine(m.errorFlag)
			}
		}
	}

	var iaMeta *protos.InteractiveTaskAdditionalMeta
	if m.job != nil {
		iaMeta = m.job.GetInteractiveMeta()
	} else {
		iaMeta = m.step.GetInteractiveMeta()
	}
	if iaMeta.X11 && iaMeta.GetX11Meta().EnableForwarding {
		m.X11SessionMgr = NewX11SessionMgr(iaMeta.GetX11Meta(), &m.stopReadCtx)
		go m.X11SessionMgr.SessionMgrRoutine()
	}
}

func MainCrun(cmd *cobra.Command, args []string) error {
	util.InitLogger(FlagDebugLevel)

	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())

	var err error
	if gVars.cwd, err = os.Getwd(); err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "Failed to get working directory: %s.", err)
	}

	if len(args) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "Please specify program to run")
	}
	var jobId uint32
	envJobIdString, stepMode := syscall.Getenv("CRANE_JOB_ID")
	jobMode := !stepMode
	if stepMode {
		parsedJobId, err := strconv.ParseUint(envJobIdString, 10, 32)
		if err != nil {
			return util.NewCraneErr(util.ErrorInvalidFormat,
				"Invalid CRANE_JOB_ID")

		}
		jobId = uint32(parsedJobId)
	}

	var job *protos.TaskToCtld
	var step *protos.StepToCtld
	egid := syscall.Getegid()
	groups, err := syscall.Getgroups()
	if err != nil {
		return util.NewCraneErr(util.ErrorSystem, fmt.Sprintf("Failed to get user groups: %s.", err))
	}
	gids := []uint32{uint32(egid)}

	for _, g := range groups {
		if g != egid {
			gids = append(gids, uint32(g))
		}
	}

	if jobMode {
		job = &protos.TaskToCtld{
			Name:          "Interactive",
			TimeLimit:     util.InvalidDuration(),
			PartitionName: "",
			ReqResources: &protos.ResourceView{
				AllocatableRes: &protos.AllocatableResource{
					CpuCoreLimit:       1,
					MemoryLimitBytes:   0,
					MemorySwLimitBytes: 0,
				},
			},
			Type:            protos.TaskType_Interactive,
			Uid:             uint32(os.Getuid()),
			Gid:             gids[0],
			NodeNum:         1,
			NtasksPerNode:   1,
			CpusPerTask:     1,
			RequeueIfFailed: false,
			Payload: &protos.TaskToCtld_InteractiveMeta{
				InteractiveMeta: &protos.InteractiveTaskAdditionalMeta{},
			},
			ShScript: strings.Join(args, " "),
			IoMeta:   &protos.IoMeta{},
			CmdLine:  strings.Join(args, " "),
			Cwd:      gVars.cwd,

			Env: make(map[string]string),
		}
	} else {
		step = &protos.StepToCtld{
			Name:      "InteractiveStep",
			TimeLimit: util.InvalidDuration(),
			JobId:     jobId,
			ReqResourcesPerTask: &protos.ResourceView{
				AllocatableRes: &protos.AllocatableResource{
					CpuCoreLimit:       0,
					MemoryLimitBytes:   0,
					MemorySwLimitBytes: 0,
				},
			},
			Type:            protos.TaskType_Interactive,
			Uid:             uint32(os.Getuid()),
			Gid:             gids,
			NodeNum:         0,
			NtasksPerNode:   0,
			RequeueIfFailed: false,
			Payload: &protos.StepToCtld_InteractiveMeta{
				InteractiveMeta: &protos.InteractiveTaskAdditionalMeta{},
			},
			ShScript: strings.Join(args, " "),
			IoMeta:   &protos.IoMeta{},
			CmdLine:  strings.Join(args, " "),
			Cwd:      gVars.cwd,

			Env: make(map[string]string),
		}
	}

	structExtraFromCli := &util.JobExtraAttrs{}

	if jobMode {
		job.NodeNum = FlagNodes
		job.CpusPerTask = FlagCpuPerTask
		job.NtasksPerNode = FlagNtasksPerNode
		job.Name = util.ExtractExecNameFromArgs(args)
	} else {
		if cmd.Flags().Changed(NodesOptionStr) {
			step.NodeNum = FlagNodes
		}
		if cmd.Flags().Changed(NtasksPerNodeOptionStr) {
			step.NtasksPerNode = FlagNtasksPerNode
		}
		if cmd.Flags().Changed(CpuPerTaskOptionStr) {
			step.ReqResourcesPerTask.AllocatableRes.CpuCoreLimit = FlagCpuPerTask
		}
		step.Name = util.ExtractExecNameFromArgs(args)
	}

	if FlagTime != "" {
		seconds, err := util.ParseDurationStrToSeconds(FlagTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: invalid --time: %s.", err))
		}
		if jobMode {
			job.TimeLimit.Seconds = seconds
		} else {
			step.TimeLimit.Seconds = seconds
		}
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s.", err))
		}
		if jobMode {
			job.ReqResources.AllocatableRes.MemoryLimitBytes = memInByte
			job.ReqResources.AllocatableRes.MemorySwLimitBytes = memInByte
		} else {
			if step.ReqResourcesPerTask == nil {
				step.ReqResourcesPerTask = &protos.ResourceView{
					AllocatableRes: &protos.AllocatableResource{MemoryLimitBytes: memInByte,
						MemorySwLimitBytes: memInByte},
				}
			} else {
				step.ReqResourcesPerTask.AllocatableRes.MemoryLimitBytes = memInByte
				step.ReqResourcesPerTask.AllocatableRes.MemorySwLimitBytes = memInByte
			}
		}
	}
	if FlagGres != "" {
		gresMap, err := util.ParseGres(FlagGres)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: invalid --gres: %s", err))
		}
		if jobMode {
			job.ReqResources.DeviceMap = gresMap
		} else {
			step.ReqResourcesPerTask.DeviceMap = gresMap
		}
	}
	if FlagPartition != "" {
		if jobMode {
			job.PartitionName = FlagPartition
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --partition is not supported in step."))
		}
	}

	if FlagJob != "" {
		if jobMode {
			job.Name = FlagJob
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --job is not supported in step."))

		}
	}

	if FlagQos != "" {
		if jobMode {
			job.Qos = FlagQos
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --qos is not supported in step."))

		}
	}

	if FlagCwd != "" {
		if jobMode {
			job.Cwd = FlagCwd
		} else {
			step.Cwd = FlagCwd
		}
	}
	if FlagAccount != "" {
		if jobMode {
			job.Account = FlagAccount
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --account is not supported in step."))
		}

	}

	if FlagNodelist != "" {
		if jobMode {
			job.Nodelist = FlagNodelist
		} else {
			step.Nodelist = FlagNodelist
		}
	}
	if FlagExcludes != "" {
		if jobMode {
			job.Excludes = FlagExcludes
		} else {
			step.Excludes = FlagExcludes
		}
	}
	if FlagGetUserEnv {
		if jobMode {
			job.GetUserEnv = true
		} else {
			step.GetUserEnv = true
		}
	}
	if FlagExport != "" {
		if jobMode {
			job.Env["CRANE_EXPORT_ENV"] = FlagExport
		} else {
			step.Env["CRANE_EXPORT_ENV"] = FlagExport
		}
	}
	if FlagReservation != "" {
		if jobMode {
			job.Reservation = FlagReservation
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --reservation is not supported in step."))
		}
	}

	if FlagExtraAttr != "" {
		structExtraFromCli.ExtraAttr = FlagExtraAttr
	}
	if FlagMailType != "" {
		structExtraFromCli.MailType = FlagMailType
	}
	if FlagMailUser != "" {
		structExtraFromCli.MailUser = FlagMailUser
	}
	if FlagComment != "" {
		structExtraFromCli.Comment = FlagComment
	}
	if FlagExclusive {
		if jobMode {
			job.Exclusive = true
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --exclusive is not supported in step."))

		}
	}
	if FlagHold {
		if jobMode {
			job.Hold = true
		} else {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: --hold is not supported in step."))
		}

	}

	if FlagLicenses != "" {
		licCount, isLicenseOr, err := util.ParseLicensesString(FlagLicenses)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid argument: %s.", err)
		}
		job.LicensesCount = licCount
		job.IsLicensesOr = isLicenseOr
	}

	// Marshal extra attributes
	if jobMode {
		if err := structExtraFromCli.Marshal(&job.ExtraAttr); err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s.", err))
		}
	} else {
		if err := structExtraFromCli.Marshal(&step.ExtraAttr); err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s.", err))
		}
	}

	// Set total limit of cpu cores
	if jobMode {
		job.ReqResources.AllocatableRes.CpuCoreLimit = job.CpusPerTask * float64(job.NtasksPerNode)
	}

	// Check the validity of the parameters
	if jobMode {
		if err := util.CheckTaskArgs(job); err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s.", err))
		}
	} else {
		if err := util.CheckStepArgs(step); err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid argument: %s.", err))
		}
	}

	if jobMode {
		util.SetPropagatedEnviron(&job.Env, &job.GetUserEnv)
	} else {
		util.SetPropagatedEnviron(&step.Env, &step.GetUserEnv)
	}

	var iaMeta *protos.InteractiveTaskAdditionalMeta
	if jobMode {
		iaMeta = job.GetInteractiveMeta()
	} else {
		iaMeta = step.GetInteractiveMeta()
	}
	iaMeta.Pty = FlagPty

	if FlagX11 {
		target, port, err := util.GetX11DisplayEx(!FlagX11Fwd)
		if err != nil {
			return util.NewCraneErr(util.ErrorSystem, fmt.Sprintf("Error in reading X11 $DISPLAY: %s.", err))
		}

		if !FlagX11Fwd && (target == "" || target == "localhost") {
			if target, err = os.Hostname(); err != nil {
				return util.NewCraneErr(util.ErrorSystem, fmt.Sprintf("failed to get hostname: %s.", err))
			}
			log.Debugf("Host in $DISPLAY (%v) is invalid, using hostname: %s",
				port-util.X11TcpPortOffset, target)
		}

		cookie, err := util.GetX11AuthCookie()
		if err != nil {
			return util.NewCraneErr(util.ErrorSystem, fmt.Sprintf("Error in reading X11 xauth cookies: %s.", err))
		}

		iaMeta.X11 = true
		iaMeta.X11Meta = &protos.X11Meta{
			Cookie:           cookie,
			Target:           target,
			Port:             uint32(port),
			EnableForwarding: FlagX11Fwd,
		}

		log.Debugf("X11 forwarding enabled (%v:%d). ", target, port)
	}

	termEnv, exits := syscall.Getenv("TERM")
	if exits {
		iaMeta.TermEnv = termEnv
	}
	iaMeta.InteractiveType = protos.InteractiveTaskType_Crun

	m := new(StateMachineOfCrun)
	m.inputFlag = FlagInput
	m.outputFlag = FlagOutput
	m.errorFlag = FlagErr

	if FlagPty {
		if cmd.Flags().Changed("input") || cmd.Flags().Changed("output") || cmd.Flags().Changed("err") {
			return util.NewCraneErr(util.ErrorCmdArg, "--input/--output/--err are incompatible with --pty.")
		} else {
			log.Debugf("Crun with pty, set input/output/error to 0/0/none")
			//For pty, we always set inputFlag to "0", only fwd for task 0
			m.inputFlag = "0"
			m.outputFlag = "0"
			m.errorFlag = "none"
		}
	}

	var ioMeta *protos.IoMeta
	if jobMode {
		ioMeta = job.IoMeta
	} else {
		ioMeta = step.IoMeta
	}
	ioMeta.OpenModeAppend = proto.Bool(true)
	ioMeta.InputFilePattern = m.inputFlag
	ioMeta.OutputFilePattern = m.outputFlag
	ioMeta.ErrorFilePattern = m.errorFlag

	m.Init(job, step)
	m.Run()
	defer m.Close()

	if m.err == util.ErrorSuccess {
		return nil
	}
	return &util.CraneError{Code: m.err}
}
