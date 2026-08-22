package crun

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"
	"golang.org/x/sys/unix"
)

func TestShellJoinArgsPreservesArguments(t *testing.T) {
	testCases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "nested shell command",
			args: []string{"sh", "-c", "printf a"},
			want: "a",
		},
		{
			name: "argument boundaries and shell metacharacters",
			args: []string{
				"printf", "<%s>\\n", "", "two words", "single'quote",
				"$HOME", "$(printf expanded)", "*",
			},
			want: "<>\n<two words>\n<single'quote>\n<$HOME>\n<$(printf expanded)>\n<*>\n",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			output, err := exec.Command("/bin/sh", "-c", shellJoinArgs(testCase.args)).CombinedOutput()
			if err != nil {
				t.Fatalf("execute quoted command: %v: %s", err, output)
			}
			if string(output) != testCase.want {
				t.Fatalf("command output = %q, want %q", output, testCase.want)
			}
		})
	}
}

func TestStartStdinReaderDoesNotChangeSharedFileFlags(t *testing.T) {
	originalStdin := os.Stdin
	pipeReader, pipeWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdin pipe: %v", err)
	}
	defer func() {
		os.Stdin = originalStdin
		_ = pipeReader.Close()
		_ = pipeWriter.Close()
	}()

	observedFd, err := unix.Dup(int(pipeReader.Fd()))
	if err != nil {
		t.Fatalf("duplicate shared fd: %v", err)
	}
	defer unix.Close(observedFd)

	originalFlags, err := unix.FcntlInt(uintptr(observedFd), unix.F_GETFL, 0)
	if err != nil {
		t.Fatalf("read original fd flags: %v", err)
	}

	os.Stdin = pipeReader
	stopReadCtx, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()
	m := &StateMachineOfCrun{
		chanInputFromLocal: make(chan []byte, 1),
		stopReadCtx:        stopReadCtx,
	}
	m.startStdinReader()

	gotFlags, err := unix.FcntlInt(uintptr(observedFd), unix.F_GETFL, 0)
	if err != nil {
		t.Fatalf("read updated fd flags: %v", err)
	}
	if gotFlags != originalFlags {
		t.Errorf("shared fd flags changed from %#x to %#x", originalFlags, gotFlags)
	}
	if m.stdinFlagsSaved {
		t.Error("stdin flags were saved even though the reader did not change them")
	}

	if _, err := pipeWriter.Write([]byte("input")); err != nil {
		t.Fatalf("write stdin input: %v", err)
	}
	if err := pipeWriter.Close(); err != nil {
		t.Fatalf("close stdin writer: %v", err)
	}
	if _, ok := <-m.chanInputFromLocal; !ok {
		t.Fatal("stdin reader closed without forwarding input")
	}
	cancelRead()
	select {
	case _, ok := <-m.chanInputFromLocal:
		if ok {
			t.Fatal("stdin reader forwarded unexpected extra input")
		}
	case <-time.After(time.Second):
		t.Fatal("stdin reader did not stop after cancellation")
	}
}

func TestStateWaitAckHandlesOutputBeforeIOForwardReady(t *testing.T) {
	originalQuiet := FlagQuiet
	FlagQuiet = true
	defer func() { FlagQuiet = originalQuiet }()

	testCases := []struct {
		name  string
		reply *protos.StreamCrunReply
	}{
		{
			name: "stdout",
			reply: &protos.StreamCrunReply{
				Type: protos.StreamCrunReply_TASK_IO_FORWARD,
				Payload: &protos.StreamCrunReply_PayloadTaskIoForwardReply{
					PayloadTaskIoForwardReply: &protos.StreamCrunReply_TaskIOForwardReply{
						Msg: []byte("early stdout"),
					},
				},
			},
		},
		{
			name: "stderr",
			reply: &protos.StreamCrunReply{
				Type: protos.StreamCrunReply_TASK_ERR_OUTPUT_FORWARD,
				Payload: &protos.StreamCrunReply_PayloadTaskIoErrOutputForwardReply{
					PayloadTaskIoErrOutputForwardReply: &protos.StreamCrunReply_TaskIOErrOutputForwardReply{
						Msg: []byte("early stderr"),
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			replies := make(chan ReplyReceiveItem, 3)
			m := &StateMachineOfCrun{
				inputFlag:  FlagIOForwardNONE,
				outputFlag: FlagIOForwardNONE,
				errorFlag:  FlagIOForwardNONE,
			}
			m.Init(nil, &protos.StepToCtld{})
			defer signal.Stop(m.sigs)
			m.cforedReplyReceiver = &CforedReplyReceiver{replyChannel: replies}
			// An unbuffered channel proves a writer is already consuming output;
			// merely allocating a channel would otherwise hide the lifecycle bug.
			m.chanOutputFromRemote = make(chan []byte)
			m.chanErrOutputFromRemote = make(chan []byte)

			replies <- ReplyReceiveItem{reply: &protos.StreamCrunReply{
				Type: protos.StreamCrunReply_STEP_RES_ALLOC_REPLY,
				Payload: &protos.StreamCrunReply_PayloadStepAllocReply{
					PayloadStepAllocReply: &protos.StreamCrunReply_StepResAllocatedReply{
						Ok:          true,
						NtasksTotal: 1,
					},
				},
			}}
			m.state = WaitRes
			m.StateWaitRes()
			if m.state != WaitForward {
				t.Fatalf("state after resource allocation = %d, want WaitForward", m.state)
			}
			if m.stdinFlagsSaved {
				t.Fatal("stdin forwarding started before TASK_IO_FORWARD_READY")
			}
			defer func() {
				m.stopWriteCb()
				m.writerWg.Wait()
			}()

			// Cancel before TASK_IO_FORWARD_READY. This transition must start the
			// output writers without starting any input-side goroutines.
			replies <- ReplyReceiveItem{reply: &protos.StreamCrunReply{
				Type: protos.StreamCrunReply_STEP_CANCEL_REQUEST,
			}}
			m.StateWaitForward()
			if m.state != JobKilling {
				t.Fatalf("state after cancellation = %d, want JobKilling", m.state)
			}

			// StateJobKilling sends the completion request before entering WaitAck;
			// the stream send itself is outside this output-lifecycle regression.
			m.state = WaitAck
			replies <- ReplyReceiveItem{reply: testCase.reply}
			waitAckReturned := make(chan struct{})
			go func() {
				m.StateWaitAck()
				close(waitAckReturned)
			}()
			select {
			case <-waitAckReturned:
			case <-time.After(time.Second):
				t.Fatal("WaitAck blocked because no output writer was running")
			}
			if m.state != WaitAck {
				t.Fatalf("state after forwarded output = %d, want WaitAck", m.state)
			}

			replies <- ReplyReceiveItem{reply: &protos.StreamCrunReply{
				Type: protos.StreamCrunReply_STEP_COMPLETION_ACK_REPLY,
				Payload: &protos.StreamCrunReply_PayloadStepCompletionAckReply{
					PayloadStepCompletionAckReply: &protos.StreamCrunReply_StepCompletionAckReply{Ok: true},
				},
			}}
			m.StateWaitAck()
			if m.state != End {
				t.Fatalf("state after completion acknowledgement = %d, want End", m.state)
			}

			m.stopWriteCb()
			writersDone := make(chan struct{})
			go func() {
				m.writerWg.Wait()
				close(writersDone)
			}()
			select {
			case <-writersDone:
			case <-time.After(time.Second):
				t.Fatal("output writers did not stop")
			}
			m.stopWriteCb = func() {}
		})
	}
}

func TestStateWaitAckHandlesCompletionBeforeIOForwardReady(t *testing.T) {
	replies := make(chan ReplyReceiveItem, 1)
	m := &StateMachineOfCrun{}
	m.Init(nil, &protos.StepToCtld{})
	defer signal.Stop(m.sigs)
	defer m.stopWriteCb()
	m.cforedReplyReceiver = &CforedReplyReceiver{replyChannel: replies}
	m.state = WaitAck

	replies <- ReplyReceiveItem{reply: &protos.StreamCrunReply{
		Type: protos.StreamCrunReply_STEP_COMPLETION_ACK_REPLY,
		Payload: &protos.StreamCrunReply_PayloadStepCompletionAckReply{
			PayloadStepCompletionAckReply: &protos.StreamCrunReply_StepCompletionAckReply{Ok: true},
		},
	}}
	m.StateWaitAck()

	if m.state != End {
		t.Fatalf("state after completion acknowledgement = %d, want End", m.state)
	}
}

func TestStateForwardingPtySetupFailureStartsOutputForwarding(t *testing.T) {
	originalPty := FlagPty
	FlagPty = true
	defer func() { FlagPty = originalPty }()

	originalStdin := os.Stdin
	pipeReader, pipeWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdin pipe: %v", err)
	}
	os.Stdin = pipeReader
	defer func() {
		os.Stdin = originalStdin
		_ = pipeReader.Close()
		_ = pipeWriter.Close()
	}()

	replies := make(chan ReplyReceiveItem, 1)
	m := &StateMachineOfCrun{
		inputFlag:  FlagIOForwardNONE,
		outputFlag: FlagIOForwardNONE,
		errorFlag:  FlagIOForwardNONE,
	}
	m.Init(nil, &protos.StepToCtld{})
	defer signal.Stop(m.sigs)
	m.cforedReplyReceiver = &CforedReplyReceiver{replyChannel: replies}
	m.chanOutputFromRemote = make(chan []byte)
	m.chanErrOutputFromRemote = make(chan []byte)
	m.state = Forwarding

	m.StateForwarding()
	if m.state != JobKilling {
		t.Fatalf("state after PTY setup failure = %d, want JobKilling", m.state)
	}
	defer func() {
		m.stopWriteCb()
		m.writerWg.Wait()
	}()

	m.state = WaitAck
	replies <- ReplyReceiveItem{reply: &protos.StreamCrunReply{
		Type: protos.StreamCrunReply_TASK_IO_FORWARD,
		Payload: &protos.StreamCrunReply_PayloadTaskIoForwardReply{
			PayloadTaskIoForwardReply: &protos.StreamCrunReply_TaskIOForwardReply{
				Msg: []byte("pty setup failure output"),
			},
		},
	}}
	waitAckReturned := make(chan struct{})
	go func() {
		m.StateWaitAck()
		close(waitAckReturned)
	}()
	select {
	case <-waitAckReturned:
	case <-time.After(time.Second):
		t.Fatal("WaitAck blocked because PTY failure did not start an output writer")
	}
}

func TestResourceAllocationDoesNotTruncateOutputFileBeforeIOReady(t *testing.T) {
	originalQuiet := FlagQuiet
	FlagQuiet = true
	defer func() { FlagQuiet = originalQuiet }()

	filePath := filepath.Join(t.TempDir(), "output.log")
	const originalContent = "existing output"
	if err := os.WriteFile(filePath, []byte(originalContent), 0o600); err != nil {
		t.Fatalf("write existing output file: %v", err)
	}

	replies := make(chan ReplyReceiveItem, 1)
	m := &StateMachineOfCrun{
		inputFlag:  FlagIOForwardNONE,
		outputFlag: filePath,
		errorFlag:  FlagIOForwardNONE,
	}
	m.Init(nil, &protos.StepToCtld{})
	defer signal.Stop(m.sigs)
	defer m.stopWriteCb()
	m.cforedReplyReceiver = &CforedReplyReceiver{replyChannel: replies}
	replies <- ReplyReceiveItem{reply: &protos.StreamCrunReply{
		Type: protos.StreamCrunReply_STEP_RES_ALLOC_REPLY,
		Payload: &protos.StreamCrunReply_PayloadStepAllocReply{
			PayloadStepAllocReply: &protos.StreamCrunReply_StepResAllocatedReply{
				Ok:          true,
				NtasksTotal: 1,
			},
		},
	}}
	m.state = WaitRes
	m.StateWaitRes()

	got, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read existing output file: %v", err)
	}
	if string(got) != originalContent {
		t.Fatalf("output file after resource allocation = %q, want %q", got, originalContent)
	}
}

func TestFileWriterDrainsBufferedOutputOnStop(t *testing.T) {
	stopWriteCtx, stopWriteCb := context.WithCancel(context.Background())
	m := &StateMachineOfCrun{
		stopWriteCtx: stopWriteCtx,
		stopStepCb:   func() {},
	}
	output := make(chan []byte, 20)
	for range 20 {
		output <- []byte("tail")
	}
	stopWriteCb()

	filePath := filepath.Join(t.TempDir(), "output.log")
	writerDone := make(chan struct{})
	go func() {
		m.FileWriterRoutine(filePath, output)
		close(writerDone)
	}()
	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("file writer did not stop")
	}

	got, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if want := strings.Repeat("tail", 20); string(got) != want {
		t.Fatalf("file output = %q, want %q", got, want)
	}
}
