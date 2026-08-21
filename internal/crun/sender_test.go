package crun

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"
)

type recordingCrunRequestStream struct {
	activeSends       atomic.Int32
	maxConcurrentSend atomic.Int32

	mutex    sync.Mutex
	requests []*protos.StreamCrunRequest
}

type blockingFirstCrunRequestStream struct {
	firstSendStarted chan struct{}
	releaseFirst     chan struct{}
	sendCount        atomic.Int32

	mutex    sync.Mutex
	requests []*protos.StreamCrunRequest
}

func newBlockingFirstCrunRequestStream() *blockingFirstCrunRequestStream {
	return &blockingFirstCrunRequestStream{
		firstSendStarted: make(chan struct{}),
		releaseFirst:     make(chan struct{}),
	}
}

func (s *blockingFirstCrunRequestStream) Send(request *protos.StreamCrunRequest) error {
	if s.sendCount.Add(1) == 1 {
		close(s.firstSendStarted)
		<-s.releaseFirst
	}

	s.mutex.Lock()
	s.requests = append(s.requests, request)
	s.mutex.Unlock()
	return nil
}

func (s *recordingCrunRequestStream) Send(request *protos.StreamCrunRequest) error {
	active := s.activeSends.Add(1)
	defer s.activeSends.Add(-1)
	for {
		maximum := s.maxConcurrentSend.Load()
		if active <= maximum || s.maxConcurrentSend.CompareAndSwap(maximum, active) {
			break
		}
	}

	time.Sleep(time.Millisecond)
	s.mutex.Lock()
	s.requests = append(s.requests, request)
	s.mutex.Unlock()
	return nil
}

func TestCrunStreamSenderSerializesConcurrentSends(t *testing.T) {
	stream := &recordingCrunRequestStream{}
	sender := newCrunStreamSender(stream)
	defer sender.Close()

	const requestCount = 32
	var waitGroup sync.WaitGroup
	waitGroup.Add(requestCount)
	for range requestCount {
		go func() {
			defer waitGroup.Done()
			if err := sender.Send(&protos.StreamCrunRequest{
				Type: protos.StreamCrunRequest_TASK_IO_FORWARD,
			}); err != nil {
				t.Errorf("Send() failed: %v", err)
			}
		}()
	}
	waitGroup.Wait()

	if got := stream.maxConcurrentSend.Load(); got != 1 {
		t.Fatalf("maximum concurrent stream sends = %d, want 1", got)
	}
	if got := len(stream.requests); got != requestCount {
		t.Fatalf("sent request count = %d, want %d", got, requestCount)
	}
}

func TestCrunStreamSenderMakesTerminalRequestLast(t *testing.T) {
	stream := &recordingCrunRequestStream{}
	sender := newCrunStreamSender(stream)

	const requestCount = 32
	start := make(chan struct{})
	results := make(chan error, requestCount)
	for range requestCount {
		go func() {
			<-start
			results <- sender.Send(&protos.StreamCrunRequest{
				Type: protos.StreamCrunRequest_TASK_IO_FORWARD,
			})
		}()
	}

	terminalRequest := &protos.StreamCrunRequest{
		Type: protos.StreamCrunRequest_STEP_COMPLETION_REQUEST,
	}
	terminalResult := make(chan error, 1)
	go func() {
		<-start
		terminalResult <- sender.SendTerminal(terminalRequest)
	}()
	close(start)

	if err := <-terminalResult; err != nil {
		t.Fatalf("SendTerminal() failed: %v", err)
	}
	for range requestCount {
		err := <-results
		if err != nil && !errors.Is(err, errCrunStreamSenderClosed) {
			t.Errorf("concurrent Send() returned unexpected error: %v", err)
		}
	}
	if err := sender.Send(&protos.StreamCrunRequest{
		Type: protos.StreamCrunRequest_TASK_IO_FORWARD,
	}); !errors.Is(err, errCrunStreamSenderClosed) {
		t.Fatalf("Send() after terminal request returned %v, want %v", err, errCrunStreamSenderClosed)
	}

	if len(stream.requests) == 0 {
		t.Fatal("stream received no requests")
	}
	if stream.requests[len(stream.requests)-1] != terminalRequest {
		t.Fatal("terminal request was not the last request sent to the stream")
	}
}

func TestCrunStreamSenderPreservesAdmittedRequestsBeforeTerminal(t *testing.T) {
	stream := newBlockingFirstCrunRequestStream()
	sender := newCrunStreamSender(stream)

	firstRequest := &protos.StreamCrunRequest{
		Type: protos.StreamCrunRequest_TASK_IO_FORWARD,
	}
	secondRequest := &protos.StreamCrunRequest{
		Type: protos.StreamCrunRequest_TASK_IO_FORWARD,
	}
	terminalRequest := &protos.StreamCrunRequest{
		Type: protos.StreamCrunRequest_STEP_COMPLETION_REQUEST,
	}

	firstResult := make(chan error, 1)
	go func() { firstResult <- sender.Send(firstRequest) }()
	<-stream.firstSendStarted

	secondResult := make(chan error, 1)
	go func() { secondResult <- sender.Send(secondRequest) }()
	waitForSenderQueueLength(t, sender, 1)

	terminalResult := make(chan error, 1)
	go func() { terminalResult <- sender.SendTerminal(terminalRequest) }()
	waitForSenderQueueLength(t, sender, 2)

	if err := sender.Send(&protos.StreamCrunRequest{
		Type: protos.StreamCrunRequest_TASK_IO_FORWARD,
	}); !errors.Is(err, errCrunStreamSenderClosed) {
		t.Fatalf("Send() after terminal admission returned %v, want %v", err, errCrunStreamSenderClosed)
	}

	close(stream.releaseFirst)
	if err := <-firstResult; err != nil {
		t.Fatalf("first Send() failed: %v", err)
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("second Send() failed: %v", err)
	}
	if err := <-terminalResult; err != nil {
		t.Fatalf("SendTerminal() failed: %v", err)
	}

	stream.mutex.Lock()
	defer stream.mutex.Unlock()
	if len(stream.requests) != 3 {
		t.Fatalf("sent request count = %d, want 3", len(stream.requests))
	}
	for i, want := range []*protos.StreamCrunRequest{firstRequest, secondRequest, terminalRequest} {
		if stream.requests[i] != want {
			t.Fatalf("request %d = %p, want %p", i, stream.requests[i], want)
		}
	}
}

func waitForSenderQueueLength(t *testing.T, sender *crunStreamSender, want int) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		sender.mu.Lock()
		got := len(sender.requestQueue)
		sender.mu.Unlock()
		if got >= want {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("sender queue length = %d, want at least %d", got, want)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestCrunStreamSenderRejectsSendAfterClose(t *testing.T) {
	stream := &recordingCrunRequestStream{}
	sender := newCrunStreamSender(stream)
	sender.Close()

	if err := sender.Send(&protos.StreamCrunRequest{}); !errors.Is(err, errCrunStreamSenderClosed) {
		t.Fatalf("Send() after Close() returned %v, want %v", err, errCrunStreamSenderClosed)
	}
	if len(stream.requests) != 0 {
		t.Fatalf("stream received %d requests after sender close", len(stream.requests))
	}
}
