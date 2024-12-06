/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

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
	log.Tracef("StartHook request received: %v", req)
	reply := &protos.StartHookReply{}
	hs := make([]api.PluginHandler, 0)
	for _, p := range gPluginMap {
		hs = append(hs, (*p).StartHook)
	}

	c := api.NewContext(ctx, req, api.StartHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) EndHook(ctx context.Context, req *protos.EndHookRequest) (*protos.EndHookReply, error) {
	log.Tracef("EndHook request received: %v", req)
	reply := &protos.EndHookReply{}
	hs := make([]api.PluginHandler, 0)
	for _, p := range gPluginMap {
		hs = append(hs, (*p).EndHook)
	}

	c := api.NewContext(ctx, req, api.EndHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) CreateCgroupHook(ctx context.Context, req *protos.CreateCgroupHookRequest) (*protos.CreateCgroupHookReply, error) {
	log.Tracef("CreateCgroupHook request received: %v", req)
	reply := &protos.CreateCgroupHookReply{}
	hs := make([]api.PluginHandler, 0)
	for _, p := range gPluginMap {
		hs = append(hs, (*p).CreateCgroupHook)
	}

	c := api.NewContext(ctx, req, api.CreateCgroupHook, &hs)
	c.Start()

	return reply, nil
}

func (pd *PluginDaemon) DestroyCgroupHook(ctx context.Context, req *protos.DestroyCgroupHookRequest) (*protos.DestroyCgroupHookReply, error) {
	log.Tracef("DestroyCgroupHook request received: %v", req)
	reply := &protos.DestroyCgroupHookReply{}
	hs := make([]api.PluginHandler, 0)
	for _, p := range gPluginMap {
		hs = append(hs, (*p).DestroyCgroupHook)
	}

	c := api.NewContext(ctx, req, api.DestroyCgroupHook, &hs)
	c.Start()

	return reply, nil
}
