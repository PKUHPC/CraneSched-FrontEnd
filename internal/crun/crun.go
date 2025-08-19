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

import "C"

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"errors"
	"net"
	"os/user"
	"regexp"
	"strconv"

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
	FlagInputALL string = "all"
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
	task   *protos.TaskToCtld
	taskId uint32 // This field will be set after ReqTaskId state

	inputFlag string // Crun --input flag, used to determine how to read input from stdin

	state StateOfCrun
	err   util.CraneCmdError // Hold the final error of the state machine if any

	// Hold grpc resources and will be freed in Close.
	conn   *grpc.ClientConn
	client protos.CraneForeDClient
	stream grpc.BidiStreamingClient[protos.StreamCrunRequest, protos.StreamCrunReply]

	sigs         chan os.Signal
	savedPtyAttr unix.Termios

	cforedReplyReceiver *CforedReplyReceiver

	// These fields are used under Forwarding State.
	taskFinishCtx           context.Context
	taskFinishCb            context.CancelFunc
	taskErrCtx              context.Context
	taskErrCb               context.CancelFunc
	chanInputFromTerm       chan []byte
	chanOutputFromRemote    chan []byte
	chanX11InputFromLocal   chan []byte
	chanX11OutputFromRemote chan []byte
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

func (m *StateMachineOfCrun) Init(task *protos.TaskToCtld) {
	m.task = task
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

	request := &protos.StreamCrunRequest{
		Type: protos.StreamCrunRequest_TASK_REQUEST,
		Payload: &protos.StreamCrunRequest_PayloadTaskReq{
			PayloadTaskReq: &protos.StreamCrunRequest_TaskReq{
				Task:    m.task,
				CrunPid: int32(os.Getpid()),
			},
		},
	}

	if err := m.stream.Send(request); err != nil {
		log.Errorf("Failed to send Task Request to CrunStream: %s. "+
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
					"task id: %s. Exiting...", err)
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
			m.taskId = payload.TaskId
			fmt.Printf("Task id allocated: %d, waiting resources.\n", m.taskId)
			m.state = WaitRes
		} else {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to allocate task id: %s\n", payload.FailureReason)
			m.state = End
			m.err = util.ErrorBackend
			return
		}
	case sig := <-m.sigs:
		if sig == syscall.SIGINT {
			log.Tracef("SIGINT Received. Not allowed to cancel task when ReqTaskId")
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
				m.state = WaitForward
			} else {
				log.Errorln("Failed to allocate task resource. Exiting...")
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
			log.Tracef("SIGINT Received. Cancelling the task...")
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
				log.Errorln("Failed to wait for task io forward ready. Exiting...")
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

	// Forward Terminal input to Cfored.
	go func() {
		for {
			select {
			case msg := <-m.chanInputFromTerm:
				request = &protos.StreamCrunRequest{
					Type: protos.StreamCrunRequest_TASK_IO_FORWARD,
					Payload: &protos.StreamCrunRequest_PayloadTaskIoForwardReq{
						PayloadTaskIoForwardReq: &protos.StreamCrunRequest_TaskIOForwardReq{
							TaskId: m.taskId,
							Msg:    msg,
							Eof:    msg == nil,
						},
					},
				}
				if err := m.stream.Send(request); err != nil {
					log.Errorf("Failed to send Task Request to CrunStream: %s. "+
						"Connection to Crun is broken", err)
					gVars.connectionBroken = true
					return
				}

			case msg := <-m.chanX11InputFromLocal:
				request = &protos.StreamCrunRequest{
					Type: protos.StreamCrunRequest_TASK_X11_FORWARD,
					Payload: &protos.StreamCrunRequest_PayloadTaskX11ForwardReq{
						PayloadTaskX11ForwardReq: &protos.StreamCrunRequest_TaskX11ForwardReq{
							TaskId: m.taskId,
							Msg:    msg,
						},
					},
				}
				if err := m.stream.Send(request); err != nil {
					log.Errorf("Failed to send Task X11 Input to CrunStream: %s. "+
						"Connection to Crun is broken", err)
					gVars.connectionBroken = true
					return
				}

			case <-m.taskFinishCtx.Done():
				return
			}
		}
	}()

	for m.state == Forwarding {
		select {
		case <-m.taskFinishCtx.Done():
			request = &protos.StreamCrunRequest{
				Type: protos.StreamCrunRequest_TASK_COMPLETION_REQUEST,
				Payload: &protos.StreamCrunRequest_PayloadTaskCompleteReq{
					PayloadTaskCompleteReq: &protos.StreamCrunRequest_TaskCompleteReq{
						TaskId: m.taskId,
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

		case <-m.taskErrCtx.Done():
			request = &protos.StreamCrunRequest{
				Type: protos.StreamCrunRequest_TASK_COMPLETION_REQUEST,
				Payload: &protos.StreamCrunRequest_PayloadTaskCompleteReq{
					PayloadTaskCompleteReq: &protos.StreamCrunRequest_TaskCompleteReq{
						TaskId: m.taskId,
						Status: protos.TaskStatus_Cancelled,
					},
				},
			}

			log.Debug("Sending TASK_COMPLETION_REQUEST with Cancelled state...")
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
						"Killing task...", err)
					gVars.connectionBroken = true
					m.err = util.ErrorNetwork
					m.state = TaskKilling
				}
			} else {
				switch cforedReply.Type {
				case protos.StreamCrunReply_TASK_IO_FORWARD:
					m.chanOutputFromRemote <- cforedReply.GetPayloadTaskIoForwardReply().Msg

				case protos.StreamCrunReply_TASK_X11_FORWARD:
					m.chanX11OutputFromRemote <- cforedReply.GetPayloadTaskX11ForwardReply().Msg

				case protos.StreamCrunReply_TASK_CANCEL_REQUEST:
					m.taskFinishCtx.Done()
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
				TaskId: m.taskId,
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

	if cforedReply.Type != protos.StreamCrunReply_TASK_COMPLETION_ACK_REPLY {
		log.Errorf("Expect TASK_COMPLETION_ACK_REPLY. bug get %s\n", cforedReply.Type.String())
		m.err = util.ErrorBackend
		m.state = End
		return
	}

	if cforedReply.GetPayloadTaskCompletionAckReply().Ok {
		log.Debug("Task completed.")
	} else {
		log.Errorln("Failed to notify server of task completion")
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
			break CrunStateMachineLoop
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
					m.taskFinishCb()
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
	file := os.NewFile(os.Stdout.Fd(), "stdout")
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf("Failed to close stdout file: %s.", err)
		}
	}(file)

	log.Trace("Starting StdoutWriterRoutine")
	writer := bufio.NewWriter(file)

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

		case <-m.taskFinishCtx.Done():
			break writing
		}
	}
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
	defer close(m.chanInputFromTerm)
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
		case <-m.taskFinishCtx.Done():
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
				m.chanInputFromTerm <- buf[:nr]
				log.Tracef("Sent %d bytes to channel", nr)
			}
		}
	}

}

func (m *StateMachineOfCrun) ParseFilePattern(pattern string) (string, error) {
	log.Tracef("Parsefile pattern: %s", pattern)
	if pattern == "" {
		return pattern, nil
	}
	// User input two backslash , but we will only get one.
	if strings.Contains(pattern, "\\") {
		return strings.ReplaceAll(pattern, "\\", ""), nil
	}
	currentUser, err := user.LookupId(fmt.Sprintf("%d", m.task.Uid))
	if err != nil {
		return pattern, fmt.Errorf("failed to lookup user by uid %d: %s", m.task.Uid, err)
	}
	replacements := map[string]string{
		"%%": "%",
		//Job array's master job allocation number.
		//"%A": "",
		//Job array ID (index) number.
		//"%a": "",
		//jobid.stepid of the running job (e.g. "128.0")
		//"%J": "111.0",
		// job id
		"%j": fmt.Sprintf("%d", m.taskId),
		// step id
		"%s": "0",
		//short hostname
		//"%N": "node1",
		//Node identifier relative to current job (e.g. "0" is the first node of the running job)
		//"%n": "0",
		//task identifier (rank) relative to current job.
		//"%t": "0",
		//User name
		"%u": currentUser.Username,
		// Job name
		"%x": m.task.Name,
	}

	re := regexp.MustCompile(`%%|%(\d*)([AajJsNntuUx])`)

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

		value, found := replacements["%"+specifier]
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

	return result, nil
}
func (m *StateMachineOfCrun) FileReaderRoutine(filePattern string) {
	parsedFilePath, err := m.ParseFilePattern(filePattern)
	if err != nil {
		log.Errorf("Failed to parse file pattern %s: %s", filePattern, err)
		return
	}
	file, err := os.Open(parsedFilePath)
	if err != nil {
		log.Errorf("Failed to open file %s: %s", parsedFilePath, err)
		m.chanInputFromTerm <- nil
		m.taskErrCb()
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
		case <-m.taskFinishCtx.Done():
			break reading
		default:
			n, err := reader.Read(buffer)
			if err != nil {
				if err == io.EOF {
					m.chanInputFromTerm <- buffer[:n]
					m.chanInputFromTerm <- nil
					break reading
				}
				log.Errorf("Failed to read from fd: %v", err)
				break reading
			}
			m.chanInputFromTerm <- buffer[:n]
		}
	}
}

func (m *StateMachineOfCrun) StartX11ReaderWriterRoutine() {
	var reader *bufio.Reader
	var conn net.Conn
	var err error

	x11meta := m.task.GetInteractiveMeta().GetX11Meta()
	if x11meta.Port == 0 { // Unix Socket
		conn, err = net.Dial("unix", x11meta.Target)
		if err != nil {
			log.Errorf("Failed to connect to X11 display by unix: %v", err)
			return
		}
	} else { // TCP socket
		address := net.JoinHostPort(x11meta.Target, fmt.Sprintf("%d", x11meta.Port))
		conn, err = net.Dial("tcp", address)
		if err != nil {
			log.Errorf("Failed to connect to X11 display by tcp: %v", err)
			return
		}
	}
	defer conn.Close()

	go func() {
		reader = bufio.NewReader(conn)
		buffer := make([]byte, 4096)

		for {
			n, err := reader.Read(buffer)
			if err != nil {
				if err == io.EOF {
					return
				}
				log.Tracef("X11 fd has been closed and stop reading: %v", err)
				return
			}
			data := make([]byte, n)
			copy(data, buffer[:n])
			m.chanX11InputFromLocal <- data
			log.Tracef("Received data from x11 fd (len %d)", len(data))
		}
	}()

	writer := bufio.NewWriter(conn)
loop:
	for {
		select {
		case <-m.taskFinishCtx.Done():
			break loop

		case msg := <-m.chanX11OutputFromRemote:
			log.Tracef("Writing to x11 fd.")
			_, err := writer.Write(msg)
			if err != nil {
				log.Errorf("Failed to write to x11 fd: %v", err)
				break loop
			}

			err = writer.Flush()
			if err != nil {
				log.Errorf("Failed to flush data to x11 fd: %v", err)
				break loop
			}
		}
	}
}

func (m *StateMachineOfCrun) StartIOForward() {
	m.taskFinishCtx, m.taskFinishCb = context.WithCancel(context.Background())
	m.taskErrCtx, m.taskErrCb = context.WithCancel(context.Background())

	m.chanInputFromTerm = make(chan []byte, 100)
	m.chanOutputFromRemote = make(chan []byte, 20)

	m.chanX11InputFromLocal = make(chan []byte, 100)
	m.chanX11OutputFromRemote = make(chan []byte, 20)

	go m.forwardingSigHandlerRoutine()
	if strings.ToLower(FlagInput) == FlagInputALL {
		go m.StdinReaderRoutine()
	} else {
		num, err := strconv.Atoi(FlagInput)
		if err != nil {
			go m.FileReaderRoutine(FlagInput)
		} else {
			if uint32(num) > m.task.NtasksPerNode*m.task.NodeNum {
				go m.FileReaderRoutine(FlagInput)
			} else {
				//TODO: should fwd io to the task with relative id equal to task id instead of file
				go m.FileReaderRoutine(FlagInput)
			}
		}
	}

	go m.StdoutWriterRoutine()

	iaMeta := m.task.GetInteractiveMeta()
	if iaMeta.X11 && iaMeta.GetX11Meta().EnableForwarding {
		go m.StartX11ReaderWriterRoutine()
	}
}

func MainCrun(args []string) error {
	util.InitLogger(FlagDebugLevel)

	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())

	var err error
	if gVars.cwd, err = os.Getwd(); err != nil {
		return &util.CraneError{
			Code:    util.ErrorBackend,
			Message: fmt.Sprintf("Failed to get working directory: %s.", err),
		}
	}

	if len(args) == 0 {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "Please specify program to run",
		}
	}

	task := &protos.TaskToCtld{
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
		Gid:             uint32(os.Getgid()),
		NodeNum:         1,
		NtasksPerNode:   1,
		CpusPerTask:     1,
		RequeueIfFailed: false,
		Payload: &protos.TaskToCtld_InteractiveMeta{
			InteractiveMeta: &protos.InteractiveTaskAdditionalMeta{},
		},
		CmdLine: strings.Join(args, " "),
		Cwd:     gVars.cwd,

		Env: make(map[string]string),
	}

	structExtraFromCli := &util.JobExtraAttrs{}

	task.NodeNum = FlagNodes
	task.CpusPerTask = FlagCpuPerTask
	task.NtasksPerNode = FlagNtasksPerNode
	task.Name = util.ExtractExecNameFromArgs(args)

	if FlagTime != "" {
		seconds, err := util.ParseDurationStrToSeconds(FlagTime)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid argument: invalid --time: %s.", err),
			}
		}
		task.TimeLimit.Seconds = seconds
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid argument: %s.", err),
			}
		}
		task.ReqResources.AllocatableRes.MemoryLimitBytes = memInByte
		task.ReqResources.AllocatableRes.MemorySwLimitBytes = memInByte
	}
	if FlagGres != "" {
		gresMap := util.ParseGres(FlagGres)
		task.ReqResources.DeviceMap = gresMap
	}
	if FlagPartition != "" {
		task.PartitionName = FlagPartition
	}
	if FlagJob != "" {
		task.Name = FlagJob
	}
	if FlagQos != "" {
		task.Qos = FlagQos
	}
	if FlagCwd != "" {
		task.Cwd = FlagCwd
	}
	if FlagAccount != "" {
		task.Account = FlagAccount
	}
	if FlagNodelist != "" {
		task.Nodelist = FlagNodelist
	}
	if FlagExcludes != "" {
		task.Excludes = FlagExcludes
	}
	if FlagGetUserEnv {
		task.GetUserEnv = true
	}
	if FlagExport != "" {
		task.Env["CRANE_EXPORT_ENV"] = FlagExport
	}
	if FlagReservation != "" {
		task.Reservation = FlagReservation
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
		task.Exclusive = true
	}
	if FlagHold {
		task.Hold = true
	}

	// Marshal extra attributes
	if err := structExtraFromCli.Marshal(&task.ExtraAttr); err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid argument: %s.", err),
		}
	}

	// Set total limit of cpu cores
	task.ReqResources.AllocatableRes.CpuCoreLimit = task.CpusPerTask * float64(task.NtasksPerNode)

	// Check the validity of the parameters
	if err := util.CheckTaskArgs(task); err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid argument: %s.", err),
		}
	}

	util.SetPropagatedEnviron(task)

	iaMeta := task.GetInteractiveMeta()
	iaMeta.Pty = FlagPty

	if FlagX11 {
		target, port, err := util.GetX11DisplayEx(!FlagX11Fwd)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorSystem,
				Message: fmt.Sprintf("Error in reading X11 $DISPLAY: %s.", err),
			}
		}

		if !FlagX11Fwd && (target == "" || target == "localhost") {
			if target, err = os.Hostname(); err != nil {
				return &util.CraneError{
					Code:    util.ErrorSystem,
					Message: fmt.Sprintf("failed to get hostname: %s.", err),
				}
			}
			log.Debugf("Host in $DISPLAY (%v) is invalid, using hostname: %s",
				port-util.X11TcpPortOffset, target)
		}

		cookie, err := util.GetX11AuthCookie()
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorSystem,
				Message: fmt.Sprintf("Error in reading X11 xauth cookies: %s.", err),
			}
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

	iaMeta.ShScript = strings.Join(args, " ")
	termEnv, exits := syscall.Getenv("TERM")
	if exits {
		iaMeta.TermEnv = termEnv
	}
	iaMeta.InteractiveType = protos.InteractiveTaskType_Crun

	m := new(StateMachineOfCrun)
	m.inputFlag = FlagInput

	if FlagPty && strings.ToLower(FlagInput) != FlagInputALL {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("--input is incompatible with --pty."),
		}
	}

	m.Init(task)
	m.Run()
	defer m.Close()

	if m.err == util.ErrorSuccess {
		return nil
	}
	return &util.CraneError{Code: m.err}
}
