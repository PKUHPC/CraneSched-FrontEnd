package crun

import (
	"errors"
	"testing"

	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"golang.org/x/sys/unix"
)

func TestHandleTaskExitStatusRecordsExitCode(t *testing.T) {
	m := &StateMachineOfCrun{}

	m.handleTaskExitStatus(&protos.StreamCrunReply_TaskExitStatusReply{
		TaskId:   0,
		ExitCode: 7,
	})
	if m.taskExitCode != 7 {
		t.Fatalf("task exit code = %d, want 7", m.taskExitCode)
	}

	m.handleTaskExitStatus(&protos.StreamCrunReply_TaskExitStatusReply{TaskId: 1})
	if m.taskExitCode != 7 {
		t.Fatalf("zero exit status changed saved task exit code to %d", m.taskExitCode)
	}
	if m.err != util.ErrorSuccess {
		t.Fatalf("task exit status changed Crane error to %d", m.err)
	}
}

func TestResultErrorKeepsTaskExitCodeSeparateFromCraneErrors(t *testing.T) {
	m := &StateMachineOfCrun{taskExitCode: 2}

	err := m.resultError()
	var commandExitErr *util.CommandExitError
	if !errors.As(err, &commandExitErr) {
		t.Fatalf("result error = %T, want *util.CommandExitError", err)
	}
	if commandExitErr.Code != 2 {
		t.Fatalf("command exit code = %d, want 2", commandExitErr.Code)
	}

	m.err = util.ErrorBackend
	err = m.resultError()
	var craneErr *util.CraneError
	if !errors.As(err, &craneErr) {
		t.Fatalf("result error = %T, want *util.CraneError", err)
	}
	if craneErr.Code != util.ErrorBackend {
		t.Fatalf("Crane error code = %d, want %d", craneErr.Code, util.ErrorBackend)
	}
}

func TestFailedCompletionAckBecomesCraneError(t *testing.T) {
	replies := make(chan ReplyReceiveItem, 1)
	replies <- ReplyReceiveItem{reply: &protos.StreamCrunReply{
		Type: protos.StreamCrunReply_STEP_COMPLETION_ACK_REPLY,
		Payload: &protos.StreamCrunReply_PayloadStepCompletionAckReply{
			PayloadStepCompletionAckReply: &protos.StreamCrunReply_StepCompletionAckReply{Ok: false},
		},
	}}
	m := &StateMachineOfCrun{
		state: End,
		cforedReplyReceiver: &CforedReplyReceiver{
			replyChannel: replies,
		},
	}

	m.StateWaitAck()

	if m.err != util.ErrorBackend {
		t.Fatalf("Crane error = %d, want %d", m.err, util.ErrorBackend)
	}
}

func TestCloseRestoresOriginalStdinFlags(t *testing.T) {
	pipeFds := make([]int, 2)
	if err := unix.Pipe(pipeFds); err != nil {
		t.Fatalf("unix.Pipe() failed: %v", err)
	}
	defer unix.Close(pipeFds[0])
	defer unix.Close(pipeFds[1])

	fd := pipeFds[0]
	originalFlags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		t.Fatalf("failed to read original flags: %v", err)
	}
	savedFlags, err := setFileNonblocking(fd)
	if err != nil {
		t.Fatalf("setFileNonblocking() failed: %v", err)
	}
	if savedFlags != originalFlags {
		t.Fatalf("saved flags = %#x, want %#x", savedFlags, originalFlags)
	}

	nonblockingFlags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		t.Fatalf("failed to read nonblocking flags: %v", err)
	}
	if nonblockingFlags&unix.O_NONBLOCK == 0 {
		t.Fatal("O_NONBLOCK was not set")
	}

	m := &StateMachineOfCrun{
		stdinFd:         fd,
		savedStdinFlags: savedFlags,
		stdinFlagsSaved: true,
	}
	m.Close()

	restoredFlags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		t.Fatalf("failed to read restored flags: %v", err)
	}
	if restoredFlags != originalFlags {
		t.Fatalf("restored flags = %#x, want %#x", restoredFlags, originalFlags)
	}
	if m.stdinFlagsSaved {
		t.Fatal("stdin flags still marked as saved after successful restore")
	}
}
