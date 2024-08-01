package cplugind

import (
	"CraneFrontEnd/api"
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

func (pd *PluginDaemon) StartHook(ctx context.Context, req *protos.StartHookRequest) (*protos.StartHookReply, error) {
	reply := &protos.StartHookReply{}
	hs := make([]api.PluginHandler, 0)
	for _, p := range gPluginList {
		hs = append(hs, (*p).StartHook)
	}

	c := api.NewContext(ctx, req, api.StartHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) EndHook(ctx context.Context, req *protos.EndHookRequest) (*protos.EndHookReply, error) {
	reply := &protos.EndHookReply{}
	hs := make([]api.PluginHandler, 0)
	for _, p := range gPluginList {
		hs = append(hs, (*p).EndHook)
	}

	c := api.NewContext(ctx, req, api.EndHook, &hs)
	c.Start()

	return reply, nil
}
