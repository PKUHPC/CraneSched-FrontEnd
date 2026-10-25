package cfored

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type testCtldStream struct {
	requests   chan *protos.StreamCforedRequest
	replies    chan *protos.StreamCtldReply
	disconnect chan struct{}
}

type testCtld struct {
	protos.UnimplementedCraneCtldForInternalServer
	streams chan *testCtldStream
	exits   chan struct{}
}

func (s *testCtld) CforedStream(stream protos.CraneCtldForInternal_CforedStreamServer) error {
	c := &testCtldStream{
		requests: make(chan *protos.StreamCforedRequest, 32),
		replies:  make(chan *protos.StreamCtldReply, 32), disconnect: make(chan struct{}),
	}
	s.streams <- c
	requests := make(chan *protos.StreamCforedRequest)
	go func() {
		defer close(requests)
		for {
			r, err := stream.Recv()
			if err != nil {
				return
			}
			select {
			case requests <- r:
			case <-stream.Context().Done():
				return
			}
		}
	}()
	for {
		select {
		case <-c.disconnect:
			return status.Error(codes.Unavailable, "test disconnect")
		case <-stream.Context().Done():
			return stream.Context().Err()
		case reply := <-c.replies:
			if err := stream.Send(reply); err != nil {
				return err
			}
		case r, ok := <-requests:
			if !ok {
				return nil
			}
			if r.Type == protos.StreamCforedRequest_CFORED_GRACEFUL_EXIT {
				s.exits <- struct{}{}
				return stream.Send(&protos.StreamCtldReply{
					Type: protos.StreamCtldReply_CFORED_GRACEFUL_EXIT_ACK,
					Payload: &protos.StreamCtldReply_PayloadGracefulExitAck{
						PayloadGracefulExitAck: &protos.StreamCtldReply_CforedGracefulExitAck{Ok: true},
					},
				})
			}
			c.requests <- r
		}
	}
}

func receiveTest[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
		var zero T
		return zero
	}
}

func eventuallyTest(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met")
}

func initCforedTest(t *testing.T) {
	t.Helper()
	gVars = GlobalVariables{}
	gVars.globalCtx, gVars.globalCtxCancel = context.WithCancel(context.Background())
	gVars.hostName = "cfored-test"
	gVars.cforedRequestCtldChannel = make(chan *protos.StreamCforedRequest, 8)
	gVars.ctldReplyChannelMapByPid = make(map[int32]chan *protos.StreamCtldReply)
	gVars.ctldReplyChannelMapForCattachByPid = make(map[int32]chan *protos.StreamCtldReply)
	gVars.ctldReplyChannelMapByStep = make(map[StepIdentifier]chan *protos.StreamCtldReply)
	gVars.ctldReplyChannelMapForCattachByStep = make(map[StepIdentifier]map[int32]chan *protos.StreamCtldReply)
	gVars.pidStepMap = make(map[int32]StepIdentifier)
	gSupervisorChanKeeper = NewCranedChannelKeeper()
	t.Cleanup(gVars.globalCtxCancel)
}

type cforedTest struct {
	ctld  *testCtld
	front protos.CraneForeDClient
	done  chan struct{}
}

func startCforedTest(t *testing.T) *cforedTest {
	t.Helper()
	initCforedTest(t)
	ctldListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctld := &testCtld{streams: make(chan *testCtldStream, 8), exits: make(chan struct{}, 1)}
	ctldServer := grpc.NewServer()
	protos.RegisterCraneCtldForInternalServer(ctldServer, ctld)
	go ctldServer.Serve(ctldListener)
	_, port, _ := net.SplitHostPort(ctldListener.Addr().String())
	client := &GrpcCtldClient{config: &util.Config{
		ControlMachine: "127.0.0.1", CraneCtldForInternalListenPort: port,
	}}
	var wg sync.WaitGroup
	wg.Add(1)
	go client.StartCtldClientStream(&wg)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	frontListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	frontServer := grpc.NewServer()
	protos.RegisterCraneForeDServer(frontServer, &GrpcCforedServer{})
	go frontServer.Serve(frontListener)
	conn, err := grpc.NewClient(frontListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	// Match production: shutdown closes inbound transports as well as waking
	// state-machine selects, so blocked network writes cannot prevent cleanup.
	go func(ctx context.Context) { <-ctx.Done(); frontServer.Stop() }(gVars.globalCtx)
	t.Cleanup(func() {
		gVars.globalCtxCancel()
		frontServer.Stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Cfored client did not exit")
		}
		conn.Close()
		ctldServer.Stop()
	})
	return &cforedTest{ctld: ctld, front: protos.NewCraneForeDClient(conn), done: done}
}

func registerTest(t *testing.T, ctld *testCtld) *testCtldStream {
	t.Helper()
	c := receiveTest(t, ctld.streams)
	if r := receiveTest(t, c.requests); r.Type != protos.StreamCforedRequest_CFORED_REGISTRATION {
		t.Fatalf("expected registration, got %v", r.Type)
	}
	c.replies <- registrationAckTest()
	eventuallyTest(t, gVars.ctldConnected.Load)
	return c
}

func registrationAckTest() *protos.StreamCtldReply {
	return &protos.StreamCtldReply{
		Type: protos.StreamCtldReply_CFORED_REGISTRATION_ACK,
		Payload: &protos.StreamCtldReply_PayloadCforedRegAck{
			PayloadCforedRegAck: &protos.StreamCtldReply_CforedRegistrationAck{Ok: true},
		},
	}
}

func completionAckTest(jobID uint32) *protos.StreamCtldReply {
	return &protos.StreamCtldReply{
		Type: protos.StreamCtldReply_JOB_COMPLETION_ACK_REPLY,
		Payload: &protos.StreamCtldReply_PayloadJobCompletionAck{
			PayloadJobCompletionAck: &protos.StreamCtldReply_JobCompletionAckReply{JobId: jobID, StepId: 1},
		},
	}
}

func jobIDReplyTest(pid int32) *protos.StreamCtldReply {
	return &protos.StreamCtldReply{
		Type: protos.StreamCtldReply_JOB_ID_REPLY,
		Payload: &protos.StreamCtldReply_PayloadJobIdReply{
			PayloadJobIdReply: &protos.StreamCtldReply_JobIdReply{Ok: true, Pid: pid, JobId: uint32(pid), StepId: 1},
		},
	}
}

func crunRequestTest(pid int32) *protos.StreamCrunRequest {
	return &protos.StreamCrunRequest{
		Type: protos.StreamCrunRequest_JOB_REQUEST,
		Payload: &protos.StreamCrunRequest_PayloadJobReq{
			PayloadJobReq: &protos.StreamCrunRequest_JobReq{CrunPid: pid, Job: &protos.JobToCtld{
				Payload: &protos.JobToCtld_InteractiveMeta{InteractiveMeta: &protos.InteractiveJobAdditionalMeta{}},
			}},
		},
	}
}

func startCrunTest(t *testing.T, f *cforedTest, c *testCtldStream, pid int32) (protos.CraneForeD_CrunStreamClient, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	stream, err := f.front.CrunStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(crunRequestTest(pid)); err != nil {
		t.Fatal(err)
	}
	if r := receiveTest(t, c.requests); r.Type != protos.StreamCforedRequest_JOB_REQUEST {
		t.Fatalf("expected job request, got %v", r.Type)
	}
	c.replies <- jobIDReplyTest(pid)
	r, err := stream.Recv()
	if err != nil || !r.GetPayloadStepIdReply().GetOk() {
		t.Fatalf("job ID reply: %v, %v", r, err)
	}
	return stream, cancel
}

func allocateCrunTest(t *testing.T, c *testCtldStream, stream protos.CraneForeD_CrunStreamClient, jobID uint32) {
	t.Helper()
	c.replies <- &protos.StreamCtldReply{
		Type: protos.StreamCtldReply_JOB_RES_ALLOC_REPLY,
		Payload: &protos.StreamCtldReply_PayloadJobResAllocReply{
			PayloadJobResAllocReply: &protos.StreamCtldReply_JobResAllocatedReply{
				JobId: jobID, StepId: 1, Ok: true, CranedIds: []string{"node1"},
			},
		},
	}
	if r, err := stream.Recv(); err != nil || r.Type != protos.StreamCrunReply_STEP_RES_ALLOC_REPLY {
		t.Fatalf("resource reply: %v, %v", r, err)
	}
}

func assertNoSessionsTest(t *testing.T) {
	t.Helper()
	gVars.ctldReplyChannelMapMtx.Lock()
	defer gVars.ctldReplyChannelMapMtx.Unlock()
	if len(gVars.ctldReplyChannelMapByPid)+len(gVars.ctldReplyChannelMapByStep)+
		len(gVars.ctldReplyChannelMapForCattachByPid)+len(gVars.ctldReplyChannelMapForCattachByStep) != 0 {
		t.Fatal("session routing entries remain after shutdown")
	}
	gVars.pidStepMapMtx.RLock()
	defer gVars.pidStepMapMtx.RUnlock()
	if len(gVars.pidStepMap) != 0 {
		t.Fatal("PID entries remain after shutdown")
	}
	gSupervisorChanKeeper.stepIORequestChannelMtx.Lock()
	defer gSupervisorChanKeeper.stepIORequestChannelMtx.Unlock()
	if len(gSupervisorChanKeeper.stepIORequestChannelMap) != 0 {
		t.Fatal("I/O channels remain after shutdown")
	}
}

func TestCtldReconnectPreservesSessionsAwaitingCompletion(t *testing.T) {
	f := startCforedTest(t)
	c := registerTest(t, f.ctld)
	_, deadCancel := startCrunTest(t, f, c, 1001)
	active, _ := startCrunTest(t, f, c, 1002)
	allocateCrunTest(t, c, active, 1002)
	valid := &atomic.Bool{}
	valid.Store(true)
	gSupervisorChanKeeper.supervisorUpAndSetMsgToSupervisorChannel(1002, 1, "node1", nil, nil, valid, false)
	if r, err := active.Recv(); err != nil || r.Type != protos.StreamCrunReply_TASK_IO_FORWARD_READY {
		t.Fatalf("I/O ready reply: %v, %v", r, err)
	}
	for pid := int32(1003); pid <= 1005; pid++ {
		startCrunTest(t, f, c, pid)
	}
	deadCancel()
	if r := receiveTest(t, c.requests); r.Type != protos.StreamCforedRequest_JOB_COMPLETION_REQUEST {
		t.Fatalf("expected dead crun completion, got %v", r.Type)
	}
	// Reproduce the original window: completion was consumed before the
	// stream broke, but its ACK has not yet reached the session.
	close(c.disconnect)
	eventuallyTest(t, func() bool { return !gVars.ctldConnected.Load() })
	c = receiveTest(t, f.ctld.streams)
	receiveTest(t, c.requests) // registration
	gVars.ctldReplyChannelMapMtx.Lock()
	if len(gVars.ctldReplyChannelMapByStep) != 5 {
		t.Error("disconnect removed existing sessions")
	}
	for _, ch := range gVars.ctldReplyChannelMapByStep {
		if len(ch) != 0 {
			t.Error("disconnect injected cancellation or fake ACK")
		}
	}
	gVars.ctldReplyChannelMapMtx.Unlock()
	// Existing supervisor output still reaches crun while Ctld registration
	// is pending; reconnect must not tear down the I/O routing either.
	gSupervisorChanKeeper.forwardRemoteIoToFront(1002, 1, &protos.StreamStepIORequest{
		Type: protos.StreamStepIORequest_TASK_OUTPUT,
		Payload: &protos.StreamStepIORequest_PayloadTaskOutputReq{
			PayloadTaskOutputReq: &protos.StreamStepIORequest_TaskOutputReq{Msg: []byte("still running")},
		},
	})
	if r, err := active.Recv(); err != nil || string(r.GetPayloadTaskIoForwardReply().GetMsg()) != "still running" {
		t.Fatalf("output during reconnect: %v, %v", r, err)
	}
	// Ctld can send an existing callback before registration ACK because it
	// installs the replacement writer first. The dead session must complete.
	c.replies <- completionAckTest(1001)
	c.replies <- registrationAckTest()
	eventuallyTest(t, func() bool {
		gVars.ctldReplyChannelMapMtx.Lock()
		defer gVars.ctldReplyChannelMapMtx.Unlock()
		return gVars.ctldConnected.Load() && len(gVars.ctldReplyChannelMapByStep) == 4
	})
	startCrunTest(t, f, c, 1006)
	gVars.globalCtxCancel()
	receiveTest(t, f.ctld.exits)
	receiveTest(t, f.done)
	assertNoSessionsTest(t)
}

func TestCtldReconnectRetriesUnacknowledgedCompletion(t *testing.T) {
	f := startCforedTest(t)
	c := registerTest(t, f.ctld)
	_, cancel := startCrunTest(t, f, c, 4001)
	cancel()
	original := receiveTest(t, c.requests)
	if original.Type != protos.StreamCforedRequest_JOB_COMPLETION_REQUEST {
		t.Fatalf("expected completion, got %v", original.Type)
	}
	// Lose the ACK twice. Each new registration must retry the same completion,
	// without requiring the exited crun to send another request.
	for range 2 {
		close(c.disconnect)
		c = registerTest(t, f.ctld)
		if retry := receiveTest(t, c.requests); !proto.Equal(original, retry) {
			t.Fatalf("completion changed on retry: %v", retry)
		}
	}
	c.replies <- completionAckTest(4001)
	eventuallyTest(t, func() bool {
		gVars.ctldReplyChannelMapMtx.Lock()
		defer gVars.ctldReplyChannelMapMtx.Unlock()
		return len(gVars.ctldReplyChannelMapByStep) == 0
	})
	close(c.disconnect)
	c = registerTest(t, f.ctld)
	// The next request must be this new submission, not another retry of an
	// acknowledged completion. Old stream receivers must not disrupt it.
	startCrunTest(t, f, c, 4002)
	gVars.globalCtxCancel()
	receiveTest(t, f.done)
	assertNoSessionsTest(t)
}

func TestShutdownCleansWaitingAndUnallocatedSessions(t *testing.T) {
	f := startCforedTest(t)
	c := registerTest(t, f.ctld)
	stream, _ := startCrunTest(t, f, c, 2001)
	if err := stream.Send(&protos.StreamCrunRequest{Type: protos.StreamCrunRequest_STEP_COMPLETION_REQUEST}); err != nil {
		t.Fatal(err)
	}
	receiveTest(t, c.requests) // leave CrunWaitCtldAck without a Ctld ACK
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Calloc has not received its ID yet.
	calloc, err := f.front.CallocStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := calloc.Send(&protos.StreamCallocRequest{
		Type: protos.StreamCallocRequest_JOB_REQUEST,
		Payload: &protos.StreamCallocRequest_PayloadJobReq{PayloadJobReq: &protos.StreamCallocRequest_JobReq{
			CallocPid: 2002, Job: crunRequestTest(2002).GetPayloadJobReq().Job,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	receiveTest(t, c.requests)
	// Cattach has not received metadata yet.
	attach, err := f.front.CattachStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := attach.Send(&protos.StreamCattachRequest{
		Type: protos.StreamCattachRequest_STEP_CONNECT_REQUEST,
		Payload: &protos.StreamCattachRequest_PayloadStepConnectReq{
			PayloadStepConnectReq: &protos.StreamCattachRequest_StepConnectReq{CattachPid: 2003, JobId: 2001, StepId: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}
	receiveTest(t, c.requests)
	gVars.globalCtxCancel()
	receiveTest(t, f.ctld.exits)
	receiveTest(t, f.done)
	assertNoSessionsTest(t)
}

func TestShutdownDuringReconnectWithFullRequestQueue(t *testing.T) {
	f := startCforedTest(t)
	c := registerTest(t, f.ctld)
	var streams []protos.CraneForeD_CrunStreamClient
	for pid := int32(3001); pid <= 3012; pid++ {
		stream, _ := startCrunTest(t, f, c, pid)
		allocateCrunTest(t, c, stream, uint32(pid))
		streams = append(streams, stream)
	}
	close(c.disconnect)
	eventuallyTest(t, func() bool { return !gVars.ctldConnected.Load() })
	// Keep the new connection in WaitReg while more than eight existing
	// sessions publish completion. Some producers must block on the queue.
	c = receiveTest(t, f.ctld.streams)
	receiveTest(t, c.requests)
	for _, stream := range streams {
		// Complete while still waiting for supervisors. A full Ctld queue must
		// not prevent stopping either the handler or its readiness waiter.
		if err := stream.Send(&protos.StreamCrunRequest{Type: protos.StreamCrunRequest_STEP_COMPLETION_REQUEST}); err != nil {
			t.Fatal(err)
		}
	}
	eventuallyTest(t, func() bool { return len(gVars.cforedRequestCtldChannel) == cap(gVars.cforedRequestCtldChannel) })
	gVars.globalCtxCancel()
	receiveTest(t, f.done)
	assertNoSessionsTest(t)
}

func TestFrontEndWaitRequiresCleanupAndRejectsNewAdmissions(t *testing.T) {
	initCforedTest(t)
	if !startFrontEnd() {
		t.Fatal("initial admission failed")
	}
	done := make(chan struct{})
	go func() { waitFrontEnds(); close(done) }()
	eventuallyTest(t, func() bool {
		gVars.ctldReplyChannelMapMtx.Lock()
		defer gVars.ctldReplyChannelMapMtx.Unlock()
		return gVars.frontEndsStopping
	})
	if startFrontEnd() {
		t.Fatal("admitted a session after shutdown began")
	}
	select {
	case <-done:
		t.Fatal("wait returned before the existing session finished cleanup")
	default:
	}
	// Cleanup can take the mapping lock while Wait is blocked.
	if !gVars.ctldReplyChannelMapMtx.TryLock() {
		t.Fatal("Wait holds the mapping lock")
	}
	gVars.ctldReplyChannelMapMtx.Unlock()
	gVars.frontEndWG.Done()
	receiveTest(t, done)
}
