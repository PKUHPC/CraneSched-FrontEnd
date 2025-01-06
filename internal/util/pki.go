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
	"os"

	log "github.com/sirupsen/logrus"
)

func SignAndSaveUserCertificate(config *Config) CraneCmdError {
	var client protos.SignServiceClient

	if FileExists(fmt.Sprintf("%s/user.pem", DefaultUserConfigPath)) {
		return ErrorSuccess
	}

	uid := uint32(os.Getuid())

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return ErrorGeneric
	}
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	if err := SaveFileWithPermissions(fmt.Sprintf("%s/user.key", DefaultUserConfigPath), privateKeyPEM, 0600); err != nil {
		return ErrorGeneric
	}

	csrTemplate := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName: fmt.Sprintf("%d.%s", uid, config.DomainSuffix),
		},
		DNSNames:           []string{fmt.Sprintf("*.%s", config.DomainSuffix), "localhost"},
		SignatureAlgorithm: x509.SHA256WithRSA,
	}

	csrBytes, err := x509.CreateCertificateRequest(rand.Reader, csrTemplate, privateKey)
	if err != nil {
		return ErrorGeneric
	}

	csrPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: csrBytes,
	})

	if err := SaveFileWithPermissions(fmt.Sprintf("%s/user.csr", DefaultUserConfigPath), csrPEM, 0600); err != nil {
		return ErrorGeneric
	}

	client = GetStubToCtldForSign(config)

	request := &protos.SignUserCertificateRequest{Uid: uid, CsrContent: string(csrPEM), AltNames: fmt.Sprintf("localhost, *.%s", config.DomainSuffix)}

	response, err := client.SignUserCertificate(context.Background(), request)
	if err != nil {
		GrpcErrorPrintf(err, "Failed to authenticate user")
		return ErrorNetwork
	}

	if !response.Ok {
		log.Error("Failed to authenticate user: ", ErrMsg(response.Reason))
		return ErrorBackend
	}

	if err := SaveFileWithPermissions(fmt.Sprintf("%s/user.pem", DefaultUserConfigPath), []byte(response.Certificate), 0644); err != nil {
		return ErrorGeneric
	}

	if err := SaveFileWithPermissions(fmt.Sprintf("%s/external.pem", DefaultUserConfigPath), []byte(response.ExternalCertificate), 0644); err != nil {
		return ErrorGeneric
	}

	return ErrorSuccess
}
