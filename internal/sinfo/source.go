// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package sinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

type snapshotSource interface {
	Load(context.Context, *options) (*snapshot, error)
	Close() error
}

// A source owns its connection through all iterations, including certificate
// renewal. A replacement becomes the connection used by subsequent requests.
type craneSource struct {
	config *util.Config
	conn   *grpc.ClientConn
	uid    uint32
}

func openCraneSource(ctx context.Context, configPath string) (snapshotSource, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read Crane configuration: %w", err)
	}
	config := &util.Config{}
	if err = yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("parse Crane configuration: %w", err)
	}
	if config.ControlMachineConnectAddr() == "" || config.CraneCtldListenPort == "" {
		return nil, fmt.Errorf("Crane configuration must specify ControlMachine and CraneCtldListenPort")
	}
	if config.CraneBaseDir == "" {
		config.CraneBaseDir = util.DefaultCraneBaseDir
	}
	if config.TlsConfig.Enabled && config.TlsConfig.UserTlsCertPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		config.TlsConfig.UserTlsCertPath = filepath.Join(home, util.DefaultUserConfigPrefix)
	}
	s := &craneSource{config: config, uid: uint32(os.Getuid())}
	if err := s.connect(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *craneSource) connect(ctx context.Context) error {
	var transport credentials.TransportCredentials = insecure.NewCredentials()
	if s.config.TlsConfig.Enabled {
		tlsConfig, err := util.UpdateTLSConfigContext(ctx, s.config)
		if err != nil {
			return err
		}
		transport = credentials.NewTLS(tlsConfig)
	}
	conn, err := grpc.NewClient(net.JoinHostPort(s.config.ControlMachineConnectAddr(), s.config.CraneCtldListenPort),
		grpc.WithTransportCredentials(transport), grpc.WithKeepaliveParams(util.ClientKeepAliveParams), grpc.WithConnectParams(util.ClientConnectParams))
	if err != nil {
		return err
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.conn = conn
	return nil
}

func (s *craneSource) Load(ctx context.Context, o *options) (*snapshot, error) {
	query := func() (*protos.QuerySinfoInfoReply, error) {
		return protos.NewCraneCtldClient(s.conn).QuerySinfoInfo(ctx, &protos.QuerySinfoInfoRequest{Uid: s.uid, ReservationsOnly: o.Reservations})
	}
	reply, err := query()
	if err != nil && s.config.TlsConfig.Enabled && (status.Code(err) == codes.Unauthenticated || status.Code(err) == codes.Unavailable && strings.Contains(status.Convert(err).Message(), "certificate")) {
		if err = util.DoSignAndSaveUserCertificateContext(ctx, s.config); err != nil {
			return nil, err
		}
		if err = s.connect(ctx); err != nil {
			return nil, err
		}
		reply, err = query()
	}
	if status.Code(err) == codes.Unimplemented {
		return nil, fmt.Errorf("Crane controller lacks the sinfo query API; upgrade the controller")
	}
	if err != nil {
		return nil, err
	}
	return adaptSnapshot(reply, o)
}

func (s *craneSource) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
