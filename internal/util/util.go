package util

import (
	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
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
	DefaultConfigPath                string
	DefaultCforedRuntimeDir          string
	DefaultCforedUnixSocketPath      string
	DefaultCforedServerListenAddress string
	DefaultCforedServerListenPort    string
)

func init() {
	DefaultConfigPath = "/etc/crane/config.yaml"
	DefaultCforedRuntimeDir = "/tmp/crane/cfored"
	DefaultCforedUnixSocketPath = DefaultCforedRuntimeDir + "/cfored.sock"
	DefaultCforedServerListenAddress = "0.0.0.0"
	DefaultCforedServerListenPort = "10012"
}

func SetBorderlessTable(table *tablewriter.Table) {
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

func SetBorderTable(table *tablewriter.Table) {
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetCenterSeparator("|")
	table.SetTablePadding("\t")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
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

func InvalidDuration() *duration.Duration {
	return &duration.Duration{
		Seconds: 315576000000,
		Nanos:   0,
	}
}

func InitLogger() {
	log.SetLevel(log.TraceLevel)
	log.SetReportCaller(true)
	log.SetFormatter(&nested.Formatter{})
}
