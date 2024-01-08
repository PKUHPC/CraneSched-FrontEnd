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

package cinfo

import (
	"CraneFrontEnd/internal/util"
	"github.com/spf13/cobra"
	"os"
)

var (
	FlagFilterDownOnly       bool
	FlagFilterRespondingOnly bool
	FlagFilterPartitions     []string
	FlagFilterNodes          []string
	FlagFilterCranedStates   []string
	FlagSummarize            bool
	FlagFormat               string
	FlagIterate              uint64
	FlagConfigFilePath       string
	FlagListReason           bool

	RootCmd = &cobra.Command{
		Use:   "cinfo",
		Short: "display the status of all partitions and nodes",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if FlagIterate != 0 {
				loopedQuery(FlagIterate)
			} else {
				cinfoFunc()
			}
		},
	}
)

func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C",
		util.DefaultConfigPath, "Path to configuration file")
	RootCmd.Flags().BoolVarP(&FlagFilterDownOnly, "dead", "d", false,
		"show only non-responding nodes")
	RootCmd.Flags().StringSliceVarP(&FlagFilterPartitions, "partition", "p",
		nil, "report on specific partition")
	RootCmd.Flags().StringSliceVarP(&FlagFilterNodes, "nodes", "n", nil,
		"report on specific node(s)")
	RootCmd.Flags().StringSliceVarP(&FlagFilterCranedStates, "states", "t", nil,
		"Include craned nodes only with certain states. \n"+
			"The state can take IDLE, MIX, ALLOC and DOWN and is case-insensitive. \n"+
			"Example: \n"+
			"\t -t idle,mix \n"+
			"\t -t=alloc \n")
	RootCmd.Flags().BoolVarP(&FlagFilterRespondingOnly, "responding", "r", false,
		"report only responding nodes")
	RootCmd.Flags().Uint64VarP(&FlagIterate, "iterate", "i", 0,
		"specify an interval in seconds")
	RootCmd.Flags().BoolVarP(&FlagSummarize, "summarize", "s", false,
		"report state summary only")
	RootCmd.Flags().StringVarP(&FlagFormat, "format", "o", "",
		"format specification")
	RootCmd.Flags().BoolVarP(&FlagListReason, "list-reasons", "R", false,
		"list reason nodes are down or drained")

	RootCmd.MarkFlagsMutuallyExclusive("states", "responding", "dead")
}
