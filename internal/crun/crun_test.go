package crun

import (
	"context"
	"errors"
	"testing"
	"time"

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

	m.handleTaskExitStatus(&protos.StreamCrunReply_TaskExitStatusReply{
		TaskId: 1,
	})
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

func TestStdinReaderClosesChannelAfterPipeHangup(t *testing.T) {
	pipeFds := make([]int, 2)
	if err := unix.Pipe(pipeFds); err != nil {
		t.Fatalf("unix.Pipe() failed: %v", err)
	}
	defer unix.Close(pipeFds[0])

	if _, err := setFileNonblocking(pipeFds[0]); err != nil {
		t.Fatalf("setFileNonblocking() failed: %v", err)
	}

	want := []byte("stdin-eof")
	if _, err := unix.Write(pipeFds[1], want); err != nil {
		t.Fatalf("unix.Write() failed: %v", err)
	}
	if err := unix.Close(pipeFds[1]); err != nil {
		t.Fatalf("closing pipe writer failed: %v", err)
	}

	m := &StateMachineOfCrun{
		stopReadCtx:        context.Background(),
		chanInputFromLocal: make(chan []byte, 2),
	}
	go m.stdinReaderRoutine(pipeFds[0])

	select {
	case got := <-m.chanInputFromLocal:
		if string(got) != string(want) {
			t.Fatalf("stdin data = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stdin data")
	}

	select {
	case _, ok := <-m.chanInputFromLocal:
		if ok {
			t.Fatal("stdin channel remained open after pipe hangup")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stdin channel close")
	}
}
