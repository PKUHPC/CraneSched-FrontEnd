package cplugind

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"net"
	"os"

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
			log.Errorf("Failed to serve: %v", err)
			os.Exit(util.ErrorGeneric)
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

func (pd *PluginDaemon) PreStartHook(context.Context, *protos.PreStartHookRequest) (*protos.PreStartHookReply, error) {
	for _, p := range gPluginList {
		(*p).PreStartHook()
	}

	return &protos.PreStartHookReply{}, nil
}

func (pd *PluginDaemon) PostStartHook(context.Context, *protos.PostStartHookRequest) (*protos.PostStartHookReply, error) {
	for _, p := range gPluginList {
		(*p).PostStartHook()
	}

	return &protos.PostStartHookReply{}, nil
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
