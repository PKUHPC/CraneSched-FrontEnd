package cfored

import (
	"testing"

	"CraneFrontEnd/generated/protos"
)

func TestNewTaskIORequestChannelUsesConfiguredCapacity(t *testing.T) {
	const capacity = 8192
	cforedServer := GrpcCforedServer{taskIOChannelCapacity: capacity}

	channel := cforedServer.newTaskIORequestChannel()

	if got := cap(channel); got != capacity {
		t.Fatalf("channel capacity = %d, want %d", got, capacity)
	}
}

func TestDrainReadyStepIOForwardsAllQueuedMessages(t *testing.T) {
	channel := make(chan *protos.StreamStepIORequest, 2)
	first := &protos.StreamStepIORequest{Type: protos.StreamStepIORequest_TASK_OUTPUT}
	second := &protos.StreamStepIORequest{Type: protos.StreamStepIORequest_TASK_EXIT_STATUS}
	channel <- first
	channel <- second

	var forwarded []*protos.StreamStepIORequest
	ok := drainReadyStepIO(channel, func(message *protos.StreamStepIORequest) bool {
		forwarded = append(forwarded, message)
		return true
	})

	if !ok {
		t.Fatal("drain stopped before the channel was empty")
	}
	if len(forwarded) != 2 || forwarded[0] != first || forwarded[1] != second {
		t.Fatalf("forwarded messages = %v, want queued messages in order", forwarded)
	}
}

func TestDrainReadyStepIOPropagatesForwardFailure(t *testing.T) {
	channel := make(chan *protos.StreamStepIORequest, 1)
	message := &protos.StreamStepIORequest{Type: protos.StreamStepIORequest_TASK_OUTPUT}
	channel <- message

	if drainReadyStepIO(channel, func(got *protos.StreamStepIORequest) bool {
		return got != message
	}) {
		t.Fatal("drainReadyStepIO returned success after forward failure")
	}
}
