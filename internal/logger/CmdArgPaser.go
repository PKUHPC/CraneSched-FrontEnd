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

package logger

import (
	"CraneFrontEnd/internal/util"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath string
	FlagLogLevel       string
	FlagJson           bool

	RootCmd = &cobra.Command{
		Use:     "logger [flags]",
		Short:   "Set logger log level",
		Long:    "",
		Version: util.Version(),
		Args:    cobra.MaximumNArgs(1),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
		},
	}

	SetCranedKeeperCmd = &cobra.Command{
		Use:   "cranedkeeper [flags]",
		Short: "Set cranedkeeper logger log level",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if FlagLogLevel == "" {
				log.Errorf("Please input Flag")
				os.Exit(util.ErrorCmdArg)
			}
			if err := SetLogLevel("CranedKeeper", FlagLogLevel); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}

	SetTaskSchedulerCmd = &cobra.Command{
		Use:   "taskscheduler [flags]",
		Short: "Set taskscheduler logger log level",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if FlagLogLevel == "" {
				log.Errorf("Please input Flag")
				os.Exit(util.ErrorCmdArg)
			}
			if err := SetLogLevel("TaskScheduler", FlagLogLevel); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}

	OtherModeCmd = &cobra.Command{
		Use:   "other [flags]",
		Short: "Set other logger log level",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if FlagLogLevel == "" {
				log.Errorf("Please input Flag")
				os.Exit(util.ErrorCmdArg)
			}
			if err := SetLogLevel("Other", FlagLogLevel); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}

	AllModeCmd = &cobra.Command{
		Use:   "all [flags]",
		Short: "Set all logger log level",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if FlagLogLevel == "" {
				log.Errorf("Please input Flag")
				os.Exit(util.ErrorCmdArg)
			}
			if err := SetLogLevel("All", FlagLogLevel); err != util.ErrorSuccess {
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

	RootCmd.AddCommand(SetCranedKeeperCmd)
	{
		SetCranedKeeperCmd.Flags().StringVarP(&FlagLogLevel, "log-level", "l",
			"info", "Available log level: trace, debug, info, warn, error")
	}
	RootCmd.AddCommand(SetTaskSchedulerCmd)
	{
		SetTaskSchedulerCmd.Flags().StringVarP(&FlagLogLevel, "log-level", "l",
			"info", "Available log level: trace, debug, info, warn, error")
	}
	RootCmd.AddCommand(OtherModeCmd)
	{
		OtherModeCmd.Flags().StringVarP(&FlagLogLevel, "log-level", "l",
			"info", "Available log level: trace, debug, info, warn, error")
	}
	RootCmd.AddCommand(AllModeCmd)
	{
		AllModeCmd.Flags().StringVarP(&FlagLogLevel, "log-level", "l",
			"", "Available log level: trace, debug, info, warn, error")
	}

}
