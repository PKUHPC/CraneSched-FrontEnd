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
	"CraneFrontEnd/api"
)

type Config struct {
	ClusterName                    string            `yaml:"ClusterName"`
	ControlMachine                 string            `yaml:"ControlMachine"`
	CraneCtldListenPort            string            `yaml:"CraneCtldListenPort"`
	CraneCtldForInternalListenPort string            `yaml:"CraneCtldForInternalListenPort"`
	CranedNodeList                 []ConfigNodesList `yaml:"Nodes"`

	TlsConfig TLSConfig `yaml:"TLS"`

	CraneBaseDir         string       `yaml:"CraneBaseDir"`
	CforedLogDir         string       `yaml:"CforedLogDir"`
	CforedDebugLevel     *string      `yaml:"CforedDebugLevel"`
	CranedCforedSockPath string       `yaml:"CranedCforedSockPath"`
	Plugin               PluginConfig `yaml:"Plugin"`
}

type TLSConfig struct {
	Enabled              bool   `yaml:"Enabled"`
	InternalCertFilePath string `yaml:"InternalCertFilePath"`
	InternalKeyFilePath  string `yaml:"InternalKeyFilePath"`
	ExternalCertFilePath string `yaml:"ExternalCertFilePath"`
	CaFilePath           string `yaml:"CaFilePath"`
	DomainSuffix         string `yaml:"DomainSuffix"`
	UserTlsCertPath      string `yaml:"UserTlsCertPath"`
}

type PluginConfig struct {
	Enabled       bool             `yaml:"Enabled"`
	SockPath      string           `yaml:"PlugindSockPath"`
	ListenAddress string           `yaml:"PlugindListenAddress"`
	ListenPort    string           `yaml:"PlugindListenPort"`
	LogLevel      string           `yaml:"PlugindDebugLevel"`
	Plugins       []api.PluginMeta `yaml:"Plugins"`
}

// InfluxDB Config represents the structure of the database configuration
type InfluxDbConfig struct {
	Username    string `yaml:"Username"`
	Bucket      string `yaml:"Bucket"`
	Org         string `yaml:"Org"`
	Token       string `yaml:"Token"`
	Measurement string `yaml:"Measurement"`
	Url         string `yaml:"Url"`
}

type ConfigNodesList struct {
	Name   string `yaml:"name"`
	CPU    int    `yaml:"cpu"`
	Memory string `yaml:"memory"`
}

// Path = BaseDir + Dir + Name
const (
	DefaultConfigPath   = "/etc/crane/config.yaml"
	DefaultCraneBaseDir = "/var/crane/"
	DefaultCforedLogDir = "cfored/"

	DefaultPlugindSocketPath = "cplugind/cplugind.sock"

	DefaultUserConfigPrefix = ".config/crane"

	DefaultCforedSocketPath          = "craned/cfored.sock"
	DefaultCforedServerListenAddress = "0.0.0.0"
	DefaultCforedServerListenPort    = "10012"

	MaxJobNameLength               = 60
	MaxJobFileNameLength           = 127
	MaxJobFilePathLengthForWindows = 260 - MaxJobFileNameLength
	MaxJobFilePathLengthForUnix    = 4096 - MaxJobFileNameLength

	MaxJobTimeLimit = 315576000000 // 10000 years
	MaxJobTimeStamp = 253402300799 // 9999-12-31 23:59:59

	MaxEntityNameLength = 30
)

// Param Options
const (
	OpenModeAppend   = "append"
	OpenModeTruncate = "truncate"
)
