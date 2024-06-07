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
	"net"
	"os"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"
)

func ParseConfig(configFilePath string) *Config {
	confFile, err := os.ReadFile(configFilePath)
	if err != nil {
		log.Fatal(err)
	}
	config := &Config{}

	err = yaml.Unmarshal(confFile, config)
	if err != nil {
		log.Fatal(err)
	}

	return config
}

func GetListenSocketByConfig(config *Config) (net.Listener, error) {
	serverAddr := fmt.Sprintf("%s:%s", DefaultCforedServerListenAddress, DefaultCforedServerListenPort)

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
		listen, err := tls.Listen("tcp", serverAddr, tlsConfig)
		if err != nil {
			return nil, err
		}
		return listen, nil
	} else {
		listen, err := net.Listen("tcp", serverAddr)
		if err != nil {
			return nil, err
		}
		return listen, nil
	}
}

func GetStubToCtldByConfig(config *Config) protos.CraneCtldClient {
	var serverAddr string
	var stub protos.CraneCtldClient

	if config.UseTls {
		serverAddr = fmt.Sprintf("%s.%s:%s",
			config.ControlMachine, config.DomainSuffix, config.CraneCtldListenPort)

		ServerCertContent, err := os.ReadFile(config.ServerCertFilePath)
		if err != nil {
			log.Fatal("Read server certificate error: " + err.Error())
		}

		ServerKeyContent, err := os.ReadFile(config.ServerKeyFilePath)
		if err != nil {
			log.Fatal("Read server key error: " + err.Error())
		}

		CaCertContent, err := os.ReadFile(config.CaCertFilePath)
		if err != nil {
			log.Fatal("Read CA certifacate error: " + err.Error())
		}

		tlsKeyPair, err := tls.X509KeyPair(ServerCertContent, ServerKeyContent)
		if err != nil {
			log.Fatal("tlsKeyPair error: " + err.Error())
		}

		caPool := x509.NewCertPool()
		if ok := caPool.AppendCertsFromPEM(CaCertContent); !ok {
			log.Fatal("AppendCertsFromPEM error: " + err.Error())
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
			log.Fatal("Cannot connect to CraneCtld: " + err.Error())
		}

		stub = protos.NewCraneCtldClient(conn)
	} else {
		serverAddr = fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)

		conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Cannot connect to CraneCtld %s: %s", serverAddr, err.Error())
		}

		stub = protos.NewCraneCtldClient(conn)
	}

	return stub
}
