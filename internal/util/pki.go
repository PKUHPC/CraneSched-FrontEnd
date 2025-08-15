package util

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func SignAndSaveUserCertificate(config *Config) error {

	certPath := fmt.Sprintf("%s/user.pem", config.TlsConfig.UserTlsCertPath)
	keyPath := fmt.Sprintf("%s/user.key", config.TlsConfig.UserTlsCertPath)

	if FileExists(certPath) && FileExists(keyPath) {
		return nil
	}

	return DoSignAndSaveUserCertificate(config)
}

func DoSignAndSaveUserCertificate(config *Config) error {
	var client protos.CraneCtldClient

	uid := uint32(os.Getuid())

	RemoveFileIfExists(fmt.Sprintf("%s/user.pem", config.TlsConfig.UserTlsCertPath))

	privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return fmt.Errorf("failed to generate RSA private key: %w", err)
	}
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	if err := SaveFileWithPermissions(fmt.Sprintf("%s/user.key", config.TlsConfig.UserTlsCertPath), privateKeyPEM, 0600); err != nil {
		return err
	}

	csrTemplate := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName: fmt.Sprintf("%d.%s", uid, config.TlsConfig.DomainSuffix),
		},
		DNSNames:           []string{fmt.Sprintf("*.%s", config.TlsConfig.DomainSuffix), "localhost"},
		SignatureAlgorithm: x509.SHA256WithRSA,
	}

	csrBytes, err := x509.CreateCertificateRequest(rand.Reader, csrTemplate, privateKey)
	if err != nil {
		return err
	}

	csrPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: csrBytes,
	})

	if err := SaveFileWithPermissions(fmt.Sprintf("%s/user.csr", config.TlsConfig.UserTlsCertPath), csrPEM, 0600); err != nil {
		return err
	}

	serverAddr := fmt.Sprintf("%s.%s:%s",
		config.ControlMachine, config.TlsConfig.DomainSuffix, config.CraneCtldListenPort)

	creds, err := credentials.NewClientTLSFromFile(config.TlsConfig.ExternalCaFilePath, "*."+config.TlsConfig.DomainSuffix)
	if err != nil {
		return err
	}

	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(ClientKeepAliveParams),
		grpc.WithConnectParams(ClientConnectParams),
		grpc.WithIdleTimeout(time.Duration(math.MaxInt64)))

	if err != nil {
		return err
	}

	client = protos.NewCraneCtldClient(conn)

	request := &protos.SignUserCertificateRequest{Uid: uid, CsrContent: string(csrPEM), AltNames: fmt.Sprintf("localhost, *.%s", config.TlsConfig.DomainSuffix)}

	response, err := client.SignUserCertificate(context.Background(), request)
	if err != nil {
		return err
	}

	if !response.Ok {
		return fmt.Errorf(ErrMsg(response.Reason))
	}

	if err := SaveFileWithPermissions(fmt.Sprintf("%s/user.pem", config.TlsConfig.UserTlsCertPath), []byte(response.Certificate), 0644); err != nil {
		return err
	}

	return nil
}
