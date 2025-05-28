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

package util

import (
	"CraneFrontEnd/generated/protos"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
	"io/fs"
	"math"
	"net"
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
)

var ServerKeepAliveParams = keepalive.ServerParameters{
	Time:    10 * time.Minute, // GRPC_ARG_KEEPALIVE_TIME_MS
	Timeout: 20 * time.Second, // GRPC_ARG_KEEPALIVE_TIMEOUT_MS
}

var ServerKeepAlivePolicy = keepalive.EnforcementPolicy{
	MinTime:             10 * time.Second, // GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS
	PermitWithoutStream: true,             // GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS
	//No MaxPingStrikes available default is 2
}

var ClientKeepAliveParams = keepalive.ClientParameters{
	Time:                20 * time.Second, // 20s GRPC_ARG_KEEPALIVE_TIME_MS
	Timeout:             10 * time.Second, // 10s GRPC_ARG_KEEPALIVE_TIMEOUT_MS
	PermitWithoutStream: true,             // GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS
}

var ClientConnectParams = grpc.ConnectParams{
	Backoff: backoff.Config{
		BaseDelay: 1 * time.Second,  // 1s GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS
		MaxDelay:  30 * time.Second, // 30s GRPC_ARG_MAX_RECONNECT_BACKOFF_MS
		//No min delay available GRPC_ARG_MIN_RECONNECT_BACKOFF_MS
	},
}

func GetTCPSocket(bindAddr string, config *Config) (net.Listener, error) {
	if config.UseTls {
		CaCertContent, err := os.ReadFile(config.ServerCertFilePath)
		if err != nil {
			return nil, err
		}

		caPool := x509.NewCertPool()
		if ok := caPool.AppendCertsFromPEM(CaCertContent); !ok {
			return nil, err
		}

		cert, err := tls.LoadX509KeyPair(config.ServerCertFilePath, config.ServerKeyFilePath)
		if err != nil {
			return nil, err
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caPool,

			// NextProtos is a list of supported application level protocols, in
			// order of preference.
			// It MUST be filled. Otherwise, when c++ clients try to connect this
			// server, "Cannot check peer: missing selected ALPN property" error
			// will occur!!!!
			NextProtos: []string{"h2"},
		}
		listen, err := tls.Listen("tcp", bindAddr, tlsConfig)
		if err != nil {
			return nil, err
		}
		return listen, nil
	} else {
		listen, err := net.Listen("tcp", bindAddr)
		if err != nil {
			return nil, err
		}
		return listen, nil
	}
}

func GetUnixSocket(path string, mode fs.FileMode) (net.Listener, error) {
	dir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	ok := RemoveFileIfExists(path)
	if !ok {
		return nil, fmt.Errorf("error when removing existing unix socket")
	}

	socket, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}

	// 0600 -> only owner can access, no need to use TLS
	// 0666 -> everyone can access, insecure
	if err = os.Chmod(path, mode); err != nil {
		return nil, err
	}

	return socket, nil
}

// TODO: Refactor this to return ErrCodes instead of exiting.
func GetStubToCtldByConfig(config *Config) protos.CraneCtldClient {
	var serverAddr string
	var stub protos.CraneCtldClient

	if config.UseTls {
		serverAddr = fmt.Sprintf("%s.%s:%s",
			config.ControlMachine, config.DomainSuffix, config.CraneCtldListenPort)

		ServerCertContent, err := os.ReadFile(config.ServerCertFilePath)
		if err != nil {
			log.Errorln("Read server certificate error: " + err.Error())
			os.Exit(ErrorGeneric)
		}

		ServerKeyContent, err := os.ReadFile(config.ServerKeyFilePath)
		if err != nil {
			log.Errorln("Read server key error: " + err.Error())
			os.Exit(ErrorGeneric)
		}

		CaCertContent, err := os.ReadFile(config.CaCertFilePath)
		if err != nil {
			log.Errorln("Read CA certifacate error: " + err.Error())
			os.Exit(ErrorGeneric)
		}

		tlsKeyPair, err := tls.X509KeyPair(ServerCertContent, ServerKeyContent)
		if err != nil {
			log.Errorln("tlsKeyPair error: " + err.Error())
			os.Exit(ErrorGeneric)
		}

		caPool := x509.NewCertPool()
		if ok := caPool.AppendCertsFromPEM(CaCertContent); !ok {
			log.Errorln("AppendCertsFromPEM error: " + err.Error())
			os.Exit(ErrorGeneric)
		}

		creds := credentials.NewTLS(&tls.Config{
			Certificates:       []tls.Certificate{tlsKeyPair},
			RootCAs:            caPool,
			InsecureSkipVerify: false,
			// NextProtos is a list of supported application level protocols, in
			// order of preference.
			NextProtos: []string{"h2"},
		})

		conn, err := grpc.Dial(serverAddr,
			grpc.WithTransportCredentials(creds),
			grpc.WithKeepaliveParams(ClientKeepAliveParams),
			grpc.WithConnectParams(ClientConnectParams),
			grpc.WithIdleTimeout(time.Duration(math.MaxInt64)), // GRPC_ARG_CLIENT_IDLE_TIMEOUT_MS
		)
		if err != nil {
			log.Errorln("Cannot connect to CraneCtld: " + err.Error())
			os.Exit(ErrorBackend)
		}

		stub = protos.NewCraneCtldClient(conn)
	} else {
		serverAddr = fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)

		conn, err := grpc.Dial(serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(ClientKeepAliveParams),
			grpc.WithConnectParams(ClientConnectParams),
			grpc.WithIdleTimeout(time.Duration(math.MaxInt64)), // GRPC_ARG_CLIENT_IDLE_TIMEOUT_MS
		)
		if err != nil {
			log.Errorf("Cannot connect to CraneCtld %s: %s", serverAddr, err.Error())
			os.Exit(ErrorBackend)
		}

		stub = protos.NewCraneCtldClient(conn)
	}

	return stub
}

func GrpcErrorPrintf(err error, format string, a ...any) {
	s := fmt.Sprintf(format, a...)
	if rpcErr, ok := grpcstatus.FromError(err); ok {
		if rpcErr.Code() == grpccodes.Unavailable {
			log.Errorf("%s: Connection to CraneCtld is broken.", s)
		} else {
			log.Errorf("%s: gRPC error code %s.", s, rpcErr.String())
		}
	}
}
