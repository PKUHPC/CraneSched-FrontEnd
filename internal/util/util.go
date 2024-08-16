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

type Config struct {
	ControlMachine      string `yaml:"ControlMachine"`
	CraneCtldListenPort string `yaml:"CraneCtldListenPort"`

	UseTls             bool   `yaml:"UseTls"`
	ServerCertFilePath string `yaml:"ServerCertFilePath"`
	ServerKeyFilePath  string `yaml:"ServerKeyFilePath"`
	CaCertFilePath     string `yaml:"CaCertFilePath"`
	DomainSuffix       string `yaml:"DomainSuffix"`

	CraneBaseDir         string `yaml:"CraneBaseDir"`
	CranedCforedSockPath string `yaml:"CranedCforedSockPath"`
}

// Path = BaseDir + Dir + Name
const (
	DefaultConfigPath   = "/etc/crane/config.yaml"
	DefaultCraneBaseDir = "/var/crane/"

	DefaultPlugindSocketPath = "cplugind/cplugind.sock"

	DefaultCforedSocketPath          = "craned/cfored.sock"
	DefaultCforedServerListenAddress = "0.0.0.0"
	DefaultCforedServerListenPort    = "10012"
)
