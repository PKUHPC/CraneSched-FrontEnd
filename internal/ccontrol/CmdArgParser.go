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

package ccontrol

import (
	"CraneFrontEnd/internal/util"
	"github.com/spf13/cobra"
	"os"
	"strconv"
)

var (
	FlagNodeName      string
	FlagState         string
	FlagReason        string
	FlagPartitionName string
	FlagTaskId        uint32
	FlagQueryAll      bool
	FlagTimeLimit     string

	FlagConfigFilePath string

	rootCmd = &cobra.Command{
		Use:   "ccontrol",
		Short: "display the state of partitions and nodes",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
		},
	}
	showCmd = &cobra.Command{
		Use:   "show",
		Short: "display state of identified entity, default is all records",
		Long:  "",
	}
	showNodeCmd = &cobra.Command{
		Use:   "node",
		Short: "display state of the specified node, default is all records",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				FlagNodeName = ""
				FlagQueryAll = true
			} else {
				FlagNodeName = args[0]
				FlagQueryAll = false
			}
			ShowNodes(FlagNodeName, FlagQueryAll)
		},
	}
	showPartitionCmd = &cobra.Command{
		Use:   "partition",
		Short: "display state of the specified partition, default is all records",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				FlagPartitionName = ""
				FlagQueryAll = true
			} else {
				FlagPartitionName = args[0]
				FlagQueryAll = false
			}
			ShowPartitions(FlagPartitionName, FlagQueryAll)
		},
	}
	showTaskCmd = &cobra.Command{
		Use:   "job",
		Short: "display the state of a specified job or all jobs",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				FlagQueryAll = true
			} else {
				id, _ := strconv.Atoi(args[0])
				FlagTaskId = uint32(id)
				FlagQueryAll = false
			}
			ShowTasks(FlagTaskId, FlagQueryAll)
		},
	}
	updateCmd = &cobra.Command{
		Use:   "update",
		Short: "Modify job information",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			ChangeTaskTimeLimit(FlagTaskId, FlagTimeLimit)
		},
	}
	updateNodeCmd = &cobra.Command{
		Use:   "node",
		Short: "Modify node information",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			ChangeNodeState(FlagNodeName, FlagState, FlagReason)
		},
	}
)

// ParseCmdArgs executes the root command.
func ParseCmdArgs() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(showCmd)
	rootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath,
		"Path to configuration file")
	showCmd.AddCommand(showNodeCmd)
	showCmd.AddCommand(showPartitionCmd)
	showCmd.AddCommand(showTaskCmd)
	rootCmd.AddCommand(updateCmd)
	updateCmd.AddCommand(updateNodeCmd)
	updateNodeCmd.Flags().StringVarP(&FlagNodeName, "name", "n", "", "specify a node name")
	updateNodeCmd.Flags().StringVarP(&FlagState, "state", "t", "", "specify the state")
	updateNodeCmd.Flags().StringVarP(&FlagReason, "reason", "r", "", "set reason")
	updateCmd.Flags().Uint32VarP(&FlagTaskId, "job", "J", 0, "Job id")
	updateCmd.Flags().StringVarP(&FlagTimeLimit, "time-limit", "T", "", "time limit")
	err := updateCmd.MarkFlagRequired("job")
	if err != nil {
		return
	}
}
