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
	"errors"
	"github.com/spf13/cobra"
	"strings"

	nested "github.com/antonfisher/nested-logrus-formatter"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	ControlMachine      string `yaml:"ControlMachine"`
	CraneCtldListenPort string `yaml:"CraneCtldListenPort"`

	UseTls             bool   `yaml:"UseTls"`
	ServerCertFilePath string `yaml:"ServerCertFilePath"`
	ServerKeyFilePath  string `yaml:"ServerKeyFilePath"`
	CaCertFilePath     string `yaml:"CaCertFilePath"`
	DomainSuffix       string `yaml:"DomainSuffix"`

	CraneBaseDir         string `yaml:"CraneBaseDir"`
	CranedGoUnixSockPath string `yaml:"CranedGoUnixSockPath"`
}

var (
	DefaultConfigPath                string
	DefaultCforedServerListenAddress string
	DefaultCforedServerListenPort    string
)

func init() {
	DefaultConfigPath = "/etc/crane/config.yaml"
	DefaultCforedServerListenAddress = "0.0.0.0"
	DefaultCforedServerListenPort = "10012"
}

func InitLogger(level log.Level) {
	log.SetLevel(level)
	log.SetReportCaller(true)
	log.SetFormatter(&nested.Formatter{})
}

func RunEWrapperForLeafCommand(cmd *cobra.Command) {
	for _, c := range cmd.Commands() {
		RunEWrapperForLeafCommand(c) // bfs
	}

	if len(cmd.Commands()) == 0 {
		originalRunE := cmd.RunE
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			if originalRunE != nil {
				err := originalRunE(cmd, args)
				var craneErr *CraneError
				if errors.As(err, &craneErr) {
					if craneErr != nil {
						if craneErr.Error() == "" {
							cmd.SilenceErrors = true
						}
						if craneErr.Code != ErrorCmdArg {
							cmd.SilenceUsage = true // Silence usage info output
						}
					}
				}
				return err
			}
			return nil
		}
	}
}
