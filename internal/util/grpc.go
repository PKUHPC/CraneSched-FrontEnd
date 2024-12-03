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
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc/credentials"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
)

func GetTCPSocket(bindAddr string, config *Config) (net.Listener, error) {
	if config.UseTls {
		CaCertContent, err := os.ReadFile(config.InternalCaFilePath)
		if err != nil {
			return nil, err
		}

		caPool := x509.NewCertPool()
		if ok := caPool.AppendCertsFromPEM(CaCertContent); !ok {
			return nil, err
		}

		cert, err := tls.LoadX509KeyPair(config.CforedCertFilePath, config.CforedKeyFilePath)
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

type TokeAuth struct {
	Token string
}

func (t *TokeAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"Authorization": t.Token,
	}, nil
}

func (t *TokeAuth) RequireTransportSecurity() bool {
	return false
}

func GetStubToCtldByConfig(config *Config) protos.CraneCtldClient {
	var serverAddr string
	var stub protos.CraneCtldClient

	if config.UseTls {
		serverAddr = fmt.Sprintf("%s.%s:%s",
			config.ControlMachine, config.DomainSuffix, config.CraneCtldListenPort)
		creds, err := credentials.NewClientTLSFromFile(config.ExternalCertFilePath, fmt.Sprintf("*.%s", config.DomainSuffix))
		if err != nil {
			log.Errorln("Failed to create TLS credentials " + err.Error())
			os.Exit(ErrorGeneric)
		}
		conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(creds), grpc.WithPerRPCCredentials(&TokeAuth{Token: "token"}))
		if err != nil {
			log.Errorln("Cannot connect to CraneCtld: " + err.Error())
			os.Exit(ErrorBackend)
		}

		stub = protos.NewCraneCtldClient(conn)
	} else {
		serverAddr = fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)

		conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(&TokeAuth{Token: "token"}))
		if err != nil {
			log.Errorf("Cannot connect to CraneCtld %s: %s", serverAddr, err.Error())
			os.Exit(ErrorBackend)
		}

		stub = protos.NewCraneCtldClient(conn)
	}

	return stub
}

func GetStubToCtldForCfored(config *Config) protos.CraneCtldForCforedClient {
	var serverAddr string
	var stub protos.CraneCtldForCforedClient

	if config.UseTls {
		serverAddr = fmt.Sprintf("%s.%s:%s",
			config.ControlMachine, config.DomainSuffix, config.CraneCtldForCforedListenPort)
		cert, _ := tls.LoadX509KeyPair(config.CforedCertFilePath, config.CforedKeyFilePath)

		CaCertContent, err := os.ReadFile(config.InternalCertFilePath)
		if err != nil {
			log.Errorln("Failed to read InternalCertFile: " + err.Error())
			os.Exit(ErrorGeneric)
		}

		caPool := x509.NewCertPool()
		if ok := caPool.AppendCertsFromPEM(CaCertContent); !ok {
			log.Errorln("Failed to append cert Content.")
			os.Exit(ErrorGeneric)
		}

		creds := credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caPool,
		})
		conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(creds))
		if err != nil {
			log.Errorln("Cannot connect to CraneCtld: " + err.Error())
			os.Exit(ErrorBackend)
		}

		stub = protos.NewCraneCtldForCforedClient(conn)
	} else {
		serverAddr = fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldForCforedListenPort)

		conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Errorf("Cannot connect to CraneCtld %s: %s", serverAddr, err.Error())
			os.Exit(ErrorBackend)
		}

		stub = protos.NewCraneCtldForCforedClient(conn)
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
