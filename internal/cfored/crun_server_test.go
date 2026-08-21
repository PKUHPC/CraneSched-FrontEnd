package cfored

import (
	"testing"

	"CraneFrontEnd/generated/protos"
)

func TestCrunTaskIOChannelCapacity(t *testing.T) {
	channel := make(chan *protos.StreamStepIORequest, crunTaskIOChannelCapacity)
	if got := cap(channel); got != 4096 {
		t.Fatalf("Crun task I/O channel capacity = %d, want 4096", got)
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

func TestDrainReadyStepIORejectsNilMessage(t *testing.T) {
	channel := make(chan *protos.StreamStepIORequest, 1)
	channel <- nil

	if drainReadyStepIO(channel, func(message *protos.StreamStepIORequest) bool {
		return message != nil
	}) {
		t.Fatal("drainReadyStepIO returned success for a nil supervisor message")
	}
}
