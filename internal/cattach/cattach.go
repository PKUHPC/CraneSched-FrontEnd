package cattach

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"os/user"
	"regexp"
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
	End           StateOfCattach = 3
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
	jobId  uint32 // This field will be set after ReqTaskId state
	stepId uint32
	step   *protos.StepToCtld

	state StateOfCattach
	err   util.ExitCode // Hold the final error of the state machine if any

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
				TaskId:     m.jobId,
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
				m.step = cforedReply.Step
				FlagPty = m.step.GetInteractiveMeta().Pty
				if FlagLayout {
					m.PrintStepLayout()
					m.state = End
					return
				}
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
			// Task launch COMPLETION
			m.state = End
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

	if m.step.GetInteractiveMeta().Pty {
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
							TaskId: m.jobId,
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
							TaskId: m.jobId,
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

	go m.forwardingSigHandlerRoutine()
	go m.StdinReaderRoutine()
	go m.StdoutWriterRoutine()

	iaMeta := m.step.GetInteractiveMeta()
	if iaMeta != nil && iaMeta.X11 && iaMeta.GetX11Meta().EnableForwarding {
		go m.StartX11ReaderWriterRoutine()
	}
}

func (m *StateMachineOfCattach) StdinReaderRoutine() {

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

				if FlagPty {
					for i := 0; i < nr; i++ {
						if buf[i] == 0x03 { // Ctrl+C
							log.Trace("Local Ctrl+C detected, exiting cattach")
							m.taskFinishCb()
							return
						}
					}
				}

				m.chanInputFromTerm <- buf[:nr]
				log.Tracef("Sent %d bytes to channel", nr)
			}
		}
	}

}

func (m *StateMachineOfCattach) ParseFilePattern(pattern string) (string, error) {
	log.Tracef("Parsefile pattern: %s", pattern)
	if pattern == "" {
		return pattern, nil
	}
	// User input two backslash , but we will only get one.
	if strings.Contains(pattern, "\\") {
		return strings.ReplaceAll(pattern, "\\", ""), nil
	}
	currentUser, err := user.LookupId(fmt.Sprintf("%d", m.step.Uid))
	if err != nil {
		return pattern, fmt.Errorf("failed to lookup user by uid %d: %s", m.step.Uid, err)
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
		"%j": fmt.Sprintf("%d", m.jobId),
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
		"%x": m.step.Name,
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

func (m *StateMachineOfCattach) FileReaderRoutine(filePattern string) {
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

func (m *StateMachineOfCattach) forwardingSigHandlerRoutine() {
	signal.Ignore(syscall.SIGTTOU, syscall.SIGTTIN)

	for {
		select {
		case sig := <-m.sigs:
			switch sig {
			case syscall.SIGINT:
				log.Tracef("Recv signal: %v", sig)
				m.taskFinishCb()
				log.Tracef("Signal processing goroutine exit.")
				return
			default:
				log.Tracef("Ignored signal: %v", sig)
			}
		}
	}
}

func (m *StateMachineOfCattach) StartX11ReaderWriterRoutine() {
	var reader *bufio.Reader
	var conn net.Conn
	var err error

	x11meta := m.step.GetInteractiveMeta().GetX11Meta()
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

func (m *StateMachineOfCattach) PrintStepLayout() {
	fmt.Printf("Job step layout:\n")
	fmt.Printf("        %d tasks, %d nodes (%s)\n\n", 1, m.step.NodeNum, m.step.Nodelist)
	//fmt.Printf("        Node %d (%s), %d task(s):", m.task.Node, layout.NodeName, len(layout.TaskIDs))
	//for _, tid := range layout.TaskIDs {
	//	fmt.Printf(" %d", tid)
	//}
	fmt.Println()
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

	parts := strings.Split(args[0], ".")
	if len(parts) != 2 {
		return fmt.Errorf("Failed to parse stepid from command line options: %s", args[0])
	}

	if i, err := strconv.Atoi(parts[0]); err != nil {
		return fmt.Errorf("Failed to parse stepid from command line options: %s", args[0])
	} else {
		m.jobId = uint32(i)
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
