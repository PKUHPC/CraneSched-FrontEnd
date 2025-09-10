package cattach

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/pkg/term/termios"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type StateOfCattach int

const (
	ConnectCfored StateOfCattach = 0
	WaitForward   StateOfCattach = 1
	Forwarding    StateOfCattach = 2
	TaskKilling   StateOfCattach = 3
	WaitAck       StateOfCattach = 4
	End           StateOfCattach = 5
)

type GlobalVariables struct {
	cwd string

	globalCtx       context.Context
	globalCtxCancel context.CancelFunc

	connectionBroken bool
}

var gVars GlobalVariables

type ReplyReceiveItem struct {
	reply *protos.StreamCattachReply
	err   error
}

type CforedReplyReceiver struct {
	stream       protos.CraneForeD_CattachStreamClient
	replyChannel chan ReplyReceiveItem
}

func (r *CforedReplyReceiver) GetReplyChannel() chan ReplyReceiveItem {
	return r.replyChannel
}

func (r *CforedReplyReceiver) StartReplyReceiveRoutine(stream protos.CraneForeD_CattachStreamClient) {
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

type StateMachineOfCattach struct {
	taskId    uint32 // This field will be set after ReqTaskId state
	stepId    uint32
	task      *protos.TaskToCtld
	inputFlag string // Crun --input flag, used to determine how to read input from stdin

	state StateOfCattach
	err   util.CraneCmdError // Hold the final error of the state machine if any

	// Hold grpc resources and will be freed in Close.
	conn   *grpc.ClientConn
	client protos.CraneForeDClient
	stream grpc.BidiStreamingClient[protos.StreamCattachRequest, protos.StreamCattachReply]

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

func (m *StateMachineOfCattach) Init() {
	m.state = ConnectCfored
	m.err = util.ErrorSuccess

	m.sigs = make(chan os.Signal, 1)
	signal.Notify(m.sigs, syscall.SIGINT, syscall.SIGTTOU)
}

func (m *StateMachineOfCattach) Close() {
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

func (m *StateMachineOfCattach) StateConnectCfored() {
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

	log.Trace("Connecting to Cfored....")
	m.stream, err = m.client.CattachStream(gVars.globalCtx)
	if err != nil {
		log.Errorf("Failed to create CattachStream: %s.", err)
		m.err = util.ErrorNetwork
		m.state = End
		return
	}

	m.cforedReplyReceiver = new(CforedReplyReceiver)
	m.cforedReplyReceiver.StartReplyReceiveRoutine(m.stream)

	request := &protos.StreamCattachRequest{
		Type: protos.StreamCattachRequest_TASK_CONNECT_REQUEST,
		Payload: &protos.StreamCattachRequest_PayloadTaskConnectReq{
			PayloadTaskConnectReq: &protos.StreamCattachRequest_TaskConnectReq{
				CattachPid: int32(os.Getpid()),
				TaskId:     m.taskId,
				StepdId:    m.stepId,
				Uid:        uint32(os.Getuid()),
			},
		},
	}

	if err := m.stream.Send(request); err != nil {
		log.Errorf("Failed to send Task Request to CattachStream: %s. "+
			"Connection to Cattach is broken", err)
		gVars.connectionBroken = true

		m.state = End
		m.err = util.ErrorNetwork
		return
	}

	m.state = WaitForward
}

func (m *StateMachineOfCattach) StateWaitForward() {
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
		case protos.StreamCattachReply_TASK_CONNECT_REPLY:
			cforedReply := cforedReply.GetPayloadTaskConnectReply()
			Ok := cforedReply.Ok
			if Ok {
				m.task = cforedReply.Task
			} else {
				log.Errorf("Failed to wait for task connect reply, reason: %s. Exiting...", cforedReply.FailureReason)
				m.state = End
				m.err = util.ErrorBackend
				return
			}
		case protos.StreamCattachReply_TASK_IO_FORWARD_READY:
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
		case protos.StreamCattachReply_TASK_COMPLETION_ACK_REPLY:
			// Task completion !
			m.state = End
			m.err = util.ErrorBackend
			return
		default:
			log.Errorf("Received unhandeled msg type %s", cforedReply.Type.String())
			m.state = End
		}

	case sig := <-m.sigs:
		if sig == syscall.SIGINT {
			m.state = End
		} else {
			log.Tracef("Unhanled sig %s", sig.String())
			m.state = End
		}
	}
}

func (m *StateMachineOfCattach) StateForwarding() {
	var request *protos.StreamCattachRequest

	if m.task.GetInteractiveMeta().Pty {
		ptyAttr := unix.Termios{}
		err := termios.Tcgetattr(os.Stdin.Fd(), &ptyAttr)
		if err != nil {
			log.Errorf("Failed to get stdin attr: %s,killing", err.Error())
			m.state = End
			m.err = util.ErrorSystem
			return
		}
		m.savedPtyAttr = ptyAttr

		termios.Cfmakeraw(&ptyAttr)
		termios.Cfmakecbreak(&ptyAttr)
		err = termios.Tcsetattr(os.Stdin.Fd(), termios.TCSANOW, &ptyAttr)
		if err != nil {
			log.Errorf("Failed to get stdin attr: %s,killing", err.Error())
			m.state = End
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
				request = &protos.StreamCattachRequest{
					Type: protos.StreamCattachRequest_TASK_IO_FORWARD,
					Payload: &protos.StreamCattachRequest_PayloadTaskIoForwardReq{
						PayloadTaskIoForwardReq: &protos.StreamCattachRequest_TaskIOForwardReq{
							TaskId: m.taskId,
							Msg:    msg,
							Eof:    msg == nil,
						},
					},
				}
				if err := m.stream.Send(request); err != nil {
					log.Errorf("Failed to send Task Request to CattachStream: %s. "+
						"Connection to Cattach is broken", err)
					gVars.connectionBroken = true
					return
				}

			case msg := <-m.chanX11InputFromLocal:
				request = &protos.StreamCattachRequest{
					Type: protos.StreamCattachRequest_TASK_X11_FORWARD,
					Payload: &protos.StreamCattachRequest_PayloadTaskX11ForwardReq{
						PayloadTaskX11ForwardReq: &protos.StreamCattachRequest_TaskX11ForwardReq{
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
			m.state = End
			return
		case <-m.taskErrCtx.Done():
			m.state = End
			return
		case item := <-m.cforedReplyReceiver.replyChannel:
			cforedReply, err := item.reply, item.err
			if err != nil {
				switch err {
				case io.EOF:
					fallthrough
				default:
					log.Errorf("The connection to Cfored was broken: %s.", err)
					gVars.connectionBroken = true
					m.err = util.ErrorNetwork
					m.state = End
				}
			} else {
				switch cforedReply.Type {
				case protos.StreamCattachReply_TASK_IO_FORWARD:
					m.chanOutputFromRemote <- cforedReply.GetPayloadTaskIoForwardReply().Msg

				case protos.StreamCattachReply_TASK_X11_FORWARD:
					m.chanX11OutputFromRemote <- cforedReply.GetPayloadTaskX11ForwardReply().Msg
				case protos.StreamCattachReply_TASK_COMPLETION_ACK_REPLY:
					log.Debug("Task completed.")
					m.state = End
					return
				}
			}
		}
	}
}

func (m *StateMachineOfCattach) StateWaitAck() {
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

	if cforedReply.Type != protos.StreamCattachReply_TASK_COMPLETION_ACK_REPLY {
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

func (m *StateMachineOfCattach) Run() {
CrunStateMachineLoop:
	for {
		switch m.state {
		case ConnectCfored:
			m.StateConnectCfored()

		case WaitForward:
			m.StateWaitForward()

		case Forwarding:
			m.StateForwarding()

		case WaitAck:
			m.StateWaitAck()
		case End:
			break CrunStateMachineLoop
		}
	}
}

func (m *StateMachineOfCattach) StartIOForward() {
	m.taskFinishCtx, m.taskFinishCb = context.WithCancel(context.Background())
	m.taskErrCtx, m.taskErrCb = context.WithCancel(context.Background())

	m.chanInputFromTerm = make(chan []byte, 100)
	m.chanOutputFromRemote = make(chan []byte, 20)

	m.chanX11InputFromLocal = make(chan []byte, 100)
	m.chanX11OutputFromRemote = make(chan []byte, 20)

	//go m.forwardingSigHandlerRoutine()
	//if strings.ToLower(FlagInput) == FlagInputALL {
	//	go m.StdinReaderRoutine()
	//} else {
	//	num, err := strconv.Atoi(FlagInput)
	//	if err != nil {
	//		go m.FileReaderRoutine(FlagInput)
	//	} else {
	//		if uint32(num) > m.task.NtasksPerNode*m.task.NodeNum {
	//			go m.FileReaderRoutine(FlagInput)
	//		} else {
	//			//TODO: should fwd io to the task with relative id equal to task id instead of file
	//			go m.FileReaderRoutine(FlagInput)
	//		}
	//	}
	//}

	go m.StdoutWriterRoutine()

	iaMeta := m.task.GetInteractiveMeta()
	if iaMeta.X11 && iaMeta.GetX11Meta().EnableForwarding {
		go m.StartX11ReaderWriterRoutine()
	}
}

func (m *StateMachineOfCattach) StartX11ReaderWriterRoutine() {
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

func (m *StateMachineOfCattach) StdoutWriterRoutine() {
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

func MainCattach(args []string) error {

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
			Message: "Please specify taskid.stepid",
		}
	}

	m := new(StateMachineOfCattach)

	//m.inputFlag = FlagInput

	//if FlagPty && strings.ToLower(FlagInput) != FlagInputALL {
	//	return &util.CraneError{
	//		Code:    util.ErrorCmdArg,
	//		Message: fmt.Sprintf("--input is incompatible with --pty."),
	//	}
	//}
	parts := strings.Split(args[0], ".")
	if len(parts) != 2 {
		return fmt.Errorf("Failed to parse stepid from command line options: %s", args[0])
	}

	if i, err := strconv.Atoi(parts[0]); err != nil {
		return fmt.Errorf("Failed to parse stepid from command line options: %s", args[0])
	} else {
		m.taskId = uint32(i)
	}

	if i, err := strconv.Atoi(parts[1]); err != nil {
		return fmt.Errorf("Failed to parse stepid from command line options: %s", args[0])
	} else {
		m.stepId = uint32(i)
	}

	m.Init()
	m.Run()
	defer m.Close()

	if m.err == util.ErrorSuccess {
		return nil
	}
	return &util.CraneError{Code: m.err}
}
