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
	"net"

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
	chanInputFromTerm       chan string
	chanOutputFromRemote    chan string
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
	signal.Notify(m.sigs, syscall.SIGINT)
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

	unixSocketPath := "unix:///" + config.CranedCforedSockPath

	var err error
	m.conn, err = grpc.Dial(unixSocketPath, opts...)
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

func (m *StateMachineOfCrun) forwardingSigintHandlerRoutine() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)

	lastSigint := time.Now().Add(-2 * time.Second)
loop:
	for {
		select {
		case sig := <-sigs:
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

	writer := bufio.NewWriter(file)

writing:
	for {
		select {
		case msg := <-m.chanOutputFromRemote:
			_, err := writer.WriteString(msg)

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
	file := os.NewFile(os.Stdin.Fd(), "stdin")
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf("Failed to close stdin file: %s.", err)
		}
	}(file)

	reader := bufio.NewReader(file)

reading:
	for {
		select {
		case <-m.taskFinishCtx.Done():
			break reading

		default:
			if FlagPty {
				data, err := reader.ReadByte()
				if err != nil {
					if err == io.EOF {
						break reading
					}
					log.Errorf("Failed to read from fd: %v", err)
					break reading
				}
				m.chanInputFromTerm <- string(data)
			} else {
				data, err := reader.ReadString('\n')
				if err != nil {
					if err == io.EOF {
						break reading
					}
					log.Errorf("Failed to read from fd: %v", err)
					break reading
				}
				m.chanInputFromTerm <- data
			}

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
	} else { // Tcp socket
		address := fmt.Sprintf("%s:%d", x11meta.Target, x11meta.Port)
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
	m.chanInputFromTerm = make(chan string, 100)
	m.chanOutputFromRemote = make(chan string, 20)
	m.taskFinishCtx, m.taskFinishCb = context.WithCancel(context.Background())

	go m.forwardingSigintHandlerRoutine()
	go m.StdinReaderRoutine()
	go m.StdoutWriterRoutine()

	if m.task.GetInteractiveMeta().X11 {
		m.chanX11InputFromLocal = make(chan []byte, 100)
		m.chanX11OutputFromRemote = make(chan []byte, 20)
		go m.StartX11ReaderWriterRoutine()
	}
}

func MainCrun(args []string) util.CraneCmdError {
	util.InitLogger(FlagDebugLevel)

	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())

	var err error
	if gVars.cwd, err = os.Getwd(); err != nil {
		log.Errorf("Failed to get working directory: %s", err.Error())
		return util.ErrorBackend
	}

	if len(args) == 0 {
		log.Errorf("Please specify program to run")
		return util.ErrorCmdArg
	}

	task := &protos.TaskToCtld{
		Name:          "Interactive",
		TimeLimit:     util.InvalidDuration(),
		PartitionName: "",
		Resources: &protos.ResourceView{
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

		// Todo: use --export here!
		Env: make(map[string]string),
	}

	task.NodeNum = FlagNodes
	task.CpusPerTask = FlagCpuPerTask
	task.NtasksPerNode = FlagNtasksPerNode

	if FlagTime != "" {
		seconds, err := util.ParseDurationStrToSeconds(FlagTime)
		if err != nil {
			log.Errorf("Invalid argument: invalid --time: %v", err)
			return util.ErrorCmdArg
		}
		task.TimeLimit.Seconds = seconds
	}
	if FlagMem != "" {
		memInByte, err := util.ParseMemStringAsByte(FlagMem)
		if err != nil {
			log.Errorf("Invalid argument: %v", err)
			return util.ErrorCmdArg
		}
		task.Resources.AllocatableRes.MemoryLimitBytes = memInByte
		task.Resources.AllocatableRes.MemorySwLimitBytes = memInByte
	}
	if FlagGres != "" {
		gresMap := util.ParseGres(FlagGres)
		task.Resources.DeviceMap = gresMap
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

	// Set total limit of cpu cores
	task.Resources.AllocatableRes.CpuCoreLimit = task.CpusPerTask * float64(task.NtasksPerNode)

	// Check the validity of the parameters
	if err := util.CheckTaskArgs(task); err != nil {
		log.Errorf("Invalid argument: %v", err)
		return util.ErrorCmdArg
	}

	util.SetPropagatedEnviron(task)

	iaMeta := task.GetInteractiveMeta()
	iaMeta.Pty = FlagPty

	if FlagX11 {
		target, port, err := util.GetX11Display()
		if err != nil {
			log.Errorf("Error in reading X11 $DISPLAY: %v", err)
			return util.ErrorGeneric
		}

		cookie, err := util.GetX11AuthCookie()
		if err != nil {
			log.Errorf("Error in reading X11 xauth cookies: %v", err)
			return util.ErrorGeneric
		}

		iaMeta.X11 = true
		iaMeta.X11Meta = &protos.X11Meta{
			Cookie: cookie,
			Target: target,
			Port:   uint32(port),
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

	m.Init(task)
	m.Run()
	defer m.Close()

	return m.err
}
