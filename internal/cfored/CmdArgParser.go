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

package cfored

import (
	"CraneFrontEnd/internal/util"
	"os"

	"github.com/spf13/cobra"
)

var (
	FlagConfigFilePath string
	FlagDebugLevel     string
)

func ParseCmdArgs() {
	rootCmd := &cobra.Command{
		Use:     "cfored",
		Short:   "Daemon for interactive job management",
		Version: util.Version(),
		Run: func(cmd *cobra.Command, args []string) {
			StartCfored()
		},
	}

	rootCmd.SetVersionTemplate(util.VersionTemplate())
	rootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	rootCmd.PersistentFlags().StringVarP(&FlagDebugLevel, "debug-level", "D",
		"info", "Available debug level: trace,debug,info")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
	}
}
