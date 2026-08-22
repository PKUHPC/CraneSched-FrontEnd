package crun

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"CraneFrontEnd/generated/protos"
)

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
