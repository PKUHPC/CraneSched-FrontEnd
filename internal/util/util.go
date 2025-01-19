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
	"CraneFrontEnd/generated/protos"
	"os"
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

	CraneBaseDir         string       `yaml:"CraneBaseDir"`
	CranedCforedSockPath string       `yaml:"CranedCforedSockPath"`
	Plugin               PluginConfig `yaml:"Plugin"`
}

type PluginConfig struct {
	Enabled  bool             `yaml:"Enabled"`
	SockPath string           `yaml:"PlugindSockPath"`
	LogLevel string           `yaml:"PlugindDebugLevel"`
	Plugins  []api.PluginMeta `yaml:"Plugins"`
}

// Path = BaseDir + Dir + Name
const (
	DefaultConfigPath   = "/etc/crane/config.yaml"
	DefaultCraneBaseDir = "/var/crane/"

	DefaultPlugindSocketPath = "cplugind/cplugind.sock"

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

func SplitEnvironEntry(env *string) (string, string, bool) {
	eq := strings.IndexByte(*env, '=')
	if eq == -1 {
		return *env, "", false
	} else {
		return (*env)[:eq], (*env)[eq+1:], true
	}
}

func SetPropagatedEnviron(task *protos.TaskToCtld) {
	systemEnv := make(map[string]string)
	for _, str := range os.Environ() {
		name, value, _ := SplitEnvironEntry(&str)
		systemEnv[name] = value

		// The CRANE_* environment variables are loaded anyway.
		if strings.HasPrefix(name, "CRANE_") {
			task.Env[name] = value
		}
	}

	// This value is used only to carry the value of --export flag.
	// Delete it once we get it.
	valueOfExportFlag, haveExportFlag := task.Env["CRANE_EXPORT_ENV"]
	if haveExportFlag {
		delete(task.Env, "CRANE_EXPORT_ENV")
	} else {
		// Default mode is ALL
		valueOfExportFlag = "ALL"
	}

	switch valueOfExportFlag {
	case "NIL":
	case "NONE":
		task.GetUserEnv = true
	case "ALL":
		task.Env = systemEnv

	default:
		// The case like "ALL,A=a,B=b", "NIL,C=c"
		task.GetUserEnv = true
		splitValueOfExportFlag := strings.Split(valueOfExportFlag, ",")
		for _, exportValue := range splitValueOfExportFlag {
			if exportValue == "ALL" {
				for k, v := range systemEnv {
					task.Env[k] = v
				}
			} else {
				k, v, ok := SplitEnvironEntry(&exportValue)
				// If user-specified value is empty, use system value instead.
				if ok {
					task.Env[k] = v
				} else {
					systemEnvValue, envExist := systemEnv[k]
					if envExist {
						task.Env[k] = systemEnvValue
					}
				}
			}
		}
	}
}
