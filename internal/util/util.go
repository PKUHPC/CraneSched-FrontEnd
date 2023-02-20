package util

import (
	"CraneFrontEnd/generated/protos"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os/exec"
	"strconv"
	"log"
	"os"
	"strings"
	"syscall"
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
	DefaultConfigPath           string
	DefaultCforedRuntimeDir     string
	DefaultCforedUnixSocketPath string
)

func init() {
	DefaultConfigPath = "/etc/crane/config.yaml"
	DefaultCforedRuntimeDir = "/tmp/crane/cfored"
	DefaultCforedUnixSocketPath = DefaultCforedRuntimeDir + "/cfored.sock"
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

// TcGetpgrp gets the process group ID of the foreground process
// group associated with the terminal referred to by fd.
//
// See POSIX.1 documentation for more details:
// http://pubs.opengroup.org/onlinepubs/009695399/functions/tcgetpgrp.html
func TcGetpgrp(fd int) (pgrp int, err error) {
	return unix.IoctlGetInt(fd, unix.TIOCGPGRP)
}

// TcSetpgrp sets the foreground process group ID associated with the
// terminal referred to by fd to pgrp.
//
// See POSIX.1 documentation for more details:
// https://pubs.opengroup.org/onlinepubs/9699919799/functions/tcsetpgrp.html
func TcSetpgrp(fd int, pgrp int) (err error) {
	return unix.IoctlSetPointerInt(fd, unix.TIOCSPGRP, pgrp)
}

// IsForeground returns true if the calling process is a foreground process.
//
// Note that the foreground/background status of a process can change
// at any moment if the user utilizes the shell job control commands (fg/bg).
//
// Example use for command line tools: suppress extra output if a
// process is running in background, provide verbose output when
// running on foreground.
func IsForeground() bool {
	pgrp1, err := TcGetpgrp(int(os.Stdin.Fd()))
	if err != nil {
		return false
	}
	pgrp2 := unix.Getpgrp()
	return pgrp1 == pgrp2
}

func GetPidFromPort(port uint16) (int, error) {
	// 1. Find inode number for the port
	portHex := fmt.Sprintf("%04X", port)
	tcpFilePath := "/proc/net/tcp"
	tcpFileContent, err := ioutil.ReadFile(tcpFilePath)
	if err != nil {
		return -1, err
	}
	tcpLines := strings.Split(string(tcpFileContent), "\n")[1:] // Skip header line
	var inode uint64
	inodeFound := false
	for _, line := range tcpLines {
		fields := strings.Fields(line)
		if len(fields) >= 10 {
			localAddr := fields[1]
			if strings.HasSuffix(localAddr, ":"+portHex) {
				inode, _ = strconv.ParseUint(fields[9], 10, 64)
				inodeFound = true
				break
			}
		}
	}
	if !inodeFound {
		return -1, fmt.Errorf("No inode found for port %d", port)
	}

	// 2. Find PID that is using the inode
	pid := -1
	procPath := "/proc"
	procDirs, _ := ioutil.ReadDir(procPath)
	for _, dir := range procDirs {
		if dir.IsDir() {
			pidStr := dir.Name()
			fdPath := fmt.Sprintf("%s/%s/fd", procPath, pidStr)
			_, err := ioutil.ReadDir(fdPath)
			if err != nil {
				continue
			}
			fdLinks, _ := ioutil.ReadDir(fdPath)
			for _, fdLink := range fdLinks {
				fdPath := fmt.Sprintf("%s/%s/fd/%s", procPath, pidStr, fdLink.Name())
				stat, err := os.Stat(fdPath)
				if err != nil {
					continue
				}
				sysStat := stat.Sys().(*syscall.Stat_t)

				if (sysStat.Mode&syscall.S_IFMT) == syscall.S_IFSOCK && sysStat.Ino == inode {
					pid, _ = strconv.Atoi(pidStr)
					break
				}

			}
		}
		if pid != -1 {
			break
		}
	}
	if pid == -1 {
		return -1, fmt.Errorf("No process found for port %d", port)
	}
	return pid, nil
}

func getParentProcessID(pid int) (int, error) {
	// Construct the path to the procfs entry for the process
	procfsPath := fmt.Sprintf("/proc/%d/stat", pid)

	// Read the stat file for the process
	statBytes, err := ioutil.ReadFile(procfsPath)
	if err != nil {
		return 0, err
	}

	// Split the stat file content into fields
	statFields := strings.Fields(string(statBytes))

	// Parse the parent process ID from the fields
	ppid, err := strconv.Atoi(statFields[3])
	if err != nil {
		return 0, err
	}

	return ppid, nil
}

func InvalidDuration() *duration.Duration {
	return &duration.Duration{
		Seconds: 630720000000,
		Nanos:   0,
	}
}

func InitLogger() {
	log.SetLevel(log.TraceLevel)
	log.SetReportCaller(true)
	log.SetFormatter(&nested.Formatter{})
}

func NixShell(uid string) (string, error) {
	out, err := exec.Command("getent", "passwd", uid).Output()
	if err != nil {
		return "", err
	}

	ent := strings.Split(strings.TrimSuffix(string(out), "\n"), ":")
	return ent[6], nil
}

func SecondTimeFormat(second int64) string {
	timeFormat := ""
	dd := second / 24 / 3600
	second %= 24 * 3600
	hh := second / 3600
	second %= 3600
	mm := second / 60
	ss := second % 60
	if dd > 0 {
		timeFormat = fmt.Sprintf("%d-%02d:%02d:%02d", dd, hh, mm, ss)
	} else {
		timeFormat = fmt.Sprintf("%02d:%02d:%02d", hh, mm, ss)
	}
	return timeFormat
}

func Error(inf string, args ...interface{}) {
	out := fmt.Sprintf(inf, args...)
	fmt.Println(out)
	os.Exit(1)
}
