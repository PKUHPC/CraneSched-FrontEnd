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

func (pd *PluginDaemon) PreStartHook(ctx context.Context, req *protos.PreStartHookRequest) (*protos.PreStartHookReply, error) {
	reply := &protos.PreStartHookReply{}

	// TODO: Maybe we should make a slice after loading plugins
	// instead of generating it every time.
	hs := make([]api.PluginHandler, 0)
	for _, p := range gPluginList {
		hs = append(hs, (*p).PreStartHook)
	}

	c := api.NewContext(ctx, req, api.PreStartHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) PostStartHook(ctx context.Context, req *protos.PostStartHookRequest) (*protos.PostStartHookReply, error) {
	reply := &protos.PostStartHookReply{}
	hs := make([]api.PluginHandler, 0)
	for _, p := range gPluginList {
		hs = append(hs, (*p).PostStartHook)
	}

	c := api.NewContext(ctx, req, api.PostStartHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) PreEndHook(ctx context.Context, req *protos.PreEndHookRequest) (*protos.PreEndHookReply, error) {
	reply := &protos.PreEndHookReply{}
	hs := make([]api.PluginHandler, 0)
	for _, p := range gPluginList {
		hs = append(hs, (*p).PreEndHook)
	}

	c := api.NewContext(ctx, req, api.PreEndHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) PostEndHook(ctx context.Context, req *protos.PostEndHookRequest) (*protos.PostEndHookReply, error) {
	reply := &protos.PostEndHookReply{}
	hs := make([]api.PluginHandler, 0)
	for _, p := range gPluginList {
		hs = append(hs, (*p).PostEndHook)
	}

	c := api.NewContext(ctx, req, api.PostEndHook, &hs)
	c.Start()

	return reply, nil
}
