/**
 * Copyright (c) 2023 Peking University and Peking University
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

package util

import (
	"CraneFrontEnd/generated/protos"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
)

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

		conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(creds))
		if err != nil {
			log.Errorln("Cannot connect to CraneCtld: " + err.Error())
			os.Exit(ErrorBackend)
		}

		stub = protos.NewCraneCtldClient(conn)
	} else {
		serverAddr = fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)

		conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
