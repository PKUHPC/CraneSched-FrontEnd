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

package ceff

import (
	"CraneFrontEnd/internal/util"
	"os"

	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath string
	FlagJson           bool
	FlagClearData      bool

	RootCmd = &cobra.Command{
		Use:     "ceff [flags][job_id,...]",
		Short:   "Display the detail info of job",
		Long:    "",
		Version: util.Version(),
		Args:    cobra.MaximumNArgs(1),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
			dataConfig = GetInfluxdbPara(FlagConfigFilePath)
		},
		Run: func(cmd *cobra.Command, args []string) {
			var err util.CraneCmdError
			if FlagClearData {
				err = ClearInfluxDBData()
				os.Exit(err)
			}
			jobIds := ""
			if len(args) == 0 {
				jobIds = ""
			} else {
				jobIds = args[0]
			}
			err = QueryTasksInfoByIds(jobIds)
			if err != util.ErrorSuccess {
				os.Exit(err)
			}

		},
	}
)

func ParseCmdArgs() {
	RootCmd.CompletionOptions.DisableDefaultCmd = true
	if err := RootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
	}
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.PersistentFlags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
	RootCmd.PersistentFlags().BoolVar(&FlagClearData, "clear", false, "Clear influxdb data (please operate with caution)")
}
