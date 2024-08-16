/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
