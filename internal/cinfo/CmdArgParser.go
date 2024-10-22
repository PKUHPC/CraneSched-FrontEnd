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

package cinfo

import (
	"CraneFrontEnd/internal/util"
	"os"

	"github.com/spf13/cobra"
)

var (
	FlagFilterDownOnly       bool
	FlagFilterRespondingOnly bool
	FlagFilterPartitions     []string
	FlagFilterNodes          []string
	FlagFilterCranedStates   []string
	FlagIterate              uint64
	FlagConfigFilePath       string
	FlagNoHeader             bool
	FlagJson                 bool
	// FlagSummarize            bool
	// FlagFormat               string
	// FlagListReason           bool

	RootCmd = &cobra.Command{
		Use:     "cinfo [flags]",
		Short:   "Display the state of partitions and nodes",
		Long:    "",
		Version: util.Version(),
		Args:    cobra.ExactArgs(0),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
		},
		Run: func(cmd *cobra.Command, args []string) {
			var err util.CraneCmdError
			if FlagIterate != 0 {
				err = loopedQuery(FlagIterate)
			} else {
				err = Query()
			}
			if err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
)

func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
	}
}

func init() {
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.Flags().BoolVarP(&FlagFilterDownOnly, "dead", "d", false,
		"Display non-responding nodes only")
	RootCmd.Flags().StringSliceVarP(&FlagFilterPartitions, "partition", "p",
		nil, "Display nodes in the specified partition only")
	RootCmd.Flags().StringSliceVarP(&FlagFilterNodes, "nodes", "n", nil,
		"Display the specified nodes only")
	RootCmd.Flags().StringSliceVarP(&FlagFilterCranedStates, "states", "t", nil,
		"Display nodes with the specified states only. \n"+
			"The state can take IDLE, MIX, ALLOC and DOWN (case-insensitive). \n"+
			"Example: \n"+
			"\t -t idle,mix \n"+
			"\t -t=alloc \n")
	RootCmd.Flags().BoolVarP(&FlagFilterRespondingOnly, "responding", "r", false,
		"Display responding nodes only")
	RootCmd.Flags().Uint64VarP(&FlagIterate, "iterate", "i", 0,
		"Display at specified intervals (seconds)")
	RootCmd.Flags().BoolVarP(&FlagNoHeader, "noheader", "N", false,
		"Do not print header line in the output")
	RootCmd.Flags().BoolVar(&FlagJson, "json", false, "Output in JSON format")
	//RootCmd.Flags().BoolVarP(&FlagSummarize, "summarize", "s", false,
	//	"Display state summary only")
	//RootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "",
	//	"Format specification")
	//RootCmd.Flags().BoolVarP(&FlagListReason, "list-reasons", "R", false,
	//	"Display reasons if nodes are down or drained")

	RootCmd.MarkFlagsMutuallyExclusive("states", "responding", "dead")
}
