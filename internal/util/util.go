package util

import (
	"CraneFrontEnd/generated/protos"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"strings"
)

type Config struct {
	ControlMachine      string `yaml:"ControlMachine"`
	CraneCtldListenPort string `yaml:"CraneCtldListenPort"`

	UseTls             bool   `yaml:"UseTls"`
	ServerCertFilePath string `yaml:"ServerCertFilePath"`
	ServerKeyFilePath  string `yaml:"ServerKeyFilePath"`
	CaCertFilePath     string `yaml:"CaCertFilePath"`
	DomainSuffix       string `yaml:"DomainSuffix"`
}

var (
	DefaultConfigPath string
)

func init() {
	DefaultConfigPath = "/etc/crane/config.yaml"
}

func ParseConfig(configFilePath string) *Config {
	confFile, err := ioutil.ReadFile(configFilePath)
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

func GetStubToCtldByConfig(config *Config) protos.CraneCtldClient {
	var serverAddr string
	var stub protos.CraneCtldClient

	if config.UseTls {
		serverAddr = fmt.Sprintf("%s.%s:%s",
			config.ControlMachine, config.DomainSuffix, config.CraneCtldListenPort)

		ServerCertContent, err := ioutil.ReadFile(config.ServerCertFilePath)
		if err != nil {
			log.Fatal("Read server certificate error: " + err.Error())
		}

		ServerKeyContent, err := ioutil.ReadFile(config.ServerKeyFilePath)
		if err != nil {
			log.Fatal("Read server key error: " + err.Error())
		}

		CaCertContent, err := ioutil.ReadFile(config.CaCertFilePath)
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
			log.Fatal("Cannot connect to CraneCtld: " + err.Error())
		}

		stub = protos.NewCraneCtldClient(conn)
	}

	return stub
}

func SetTableStyle(table *tablewriter.Table) {
	table.SetBorder(false)
	table.SetTablePadding("\t")
	table.SetHeaderLine(false)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetNoWhiteSpace(true)
}

func FormatTable(tableOutputWidth []int, tableHeader []string,
	tableData [][]string) (formatTableHeader []string, formatTableData [][]string) {
	for i, h := range tableHeader {
		if tableOutputWidth[i] != -1 {
			padLength := tableOutputWidth[i] - len(h)
			if padLength >= 0 {
				tableHeader[i] = h + strings.Repeat(" ", padLength)
			} else {
				tableHeader[i] = h[:tableOutputWidth[i]]
			}
		}
	}
	for i, row := range tableData {
		for j, cell := range row {
			if tableOutputWidth[j] != -1 {
				padLength := tableOutputWidth[j] - len(cell)
				if padLength >= 0 {
					tableData[i][j] = cell + strings.Repeat(" ", padLength)
				} else {
					tableData[i][j] = cell[:tableOutputWidth[j]]
				}
			}
		}
	}
	return tableHeader, tableData
}
