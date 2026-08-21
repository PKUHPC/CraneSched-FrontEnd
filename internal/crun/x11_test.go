package crun

import (
	"bytes"
	"sync"
	"testing"

	"CraneFrontEnd/generated/protos"
)

func TestSendEofToSupervisorSetsEofFlag(t *testing.T) {
	requests := make(chan *protos.StreamCrunRequest, 1)
	session := &X11Session{
		X11Id: X11GlobalId{
			CranedId: "node0",
			LocalId:  1,
		},
		X11ToSupervisor: requests,
		eofSent:         &sync.Once{},
	}
	tail := []byte("tail")

	session.SendEofToSupervisor(tail)

	request := <-requests
	payload := request.GetPayloadStepX11ForwardReq()
	if !payload.GetEof() {
		t.Fatal("X11 EOF request did not set eof")
	}
	if !bytes.Equal(payload.GetMsg(), tail) {
		t.Fatalf("X11 EOF payload = %q, want %q", payload.GetMsg(), tail)
	}
}
