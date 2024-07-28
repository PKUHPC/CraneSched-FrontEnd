package cplugind

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"net"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type PluginDaemon struct {
	protos.UnimplementedCranePluginDServer
	Server *grpc.Server
}

func NewPluginD(opts []grpc.ServerOption) *PluginDaemon {
	p := &PluginDaemon{
		Server: grpc.NewServer(opts...),
	}

	protos.RegisterCranePluginDServer(p.Server, p)

	return p
}

func (pd *PluginDaemon) Launch(socket net.Listener) error {
	go func() {
		if err := pd.Server.Serve(socket); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	return nil
}

func (pd *PluginDaemon) Stop() {
	pd.Server.Stop()
}

func (pd *PluginDaemon) GracefulStop() {
	pd.Server.GracefulStop()
}

func (pd *PluginDaemon) PreRunHook(context.Context, *protos.PreRunHookRequest) (*protos.PreRunHookReply, error) {
	for _, p := range gPluginList {
		(*p).PreRunHook()
	}

	return &protos.PreRunHookReply{}, nil
}

func (pd *PluginDaemon) PostRunHook(context.Context, *protos.PostRunHookRequest) (*protos.PostRunHookReply, error) {
	for _, p := range gPluginList {
		(*p).PostRunHook()
	}

	return &protos.PostRunHookReply{}, nil
}

func (pd *PluginDaemon) PreCompletionHook(context.Context, *protos.PreCompletionHookRequest) (*protos.PreCompletionHookReply, error) {
	for _, p := range gPluginList {
		(*p).PreCompletionHook()
	}

	return &protos.PreCompletionHookReply{}, nil
}

func (pd *PluginDaemon) PostCompletionHook(context.Context, *protos.PostCompletionHookRequest) (*protos.PostCompletionHookReply, error) {
	for _, p := range gPluginList {
		(*p).PostCompletionHook()
	}

	return &protos.PostCompletionHookReply{}, nil
}
