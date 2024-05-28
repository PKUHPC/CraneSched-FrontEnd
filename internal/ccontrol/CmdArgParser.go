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
	FlagPartitionName string
	FlagTaskId        uint32
	FlagQueryAll      bool
	FlagTimeLimit     string
	FlagPriority	  uint32
	FlagConfigFilePath string

	ConfigPath string

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
	showConfigCmd = &cobra.Command{
		Use:   "config",
		Short: "display the default configuration file",
		Long:  "",
		Args:  cobra.MaximumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			ShowConfig(util.DefaultConfigPath)
		},
	}
	updateCmd = &cobra.Command{
		Use:   "update",
		Short: "Modify job information",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if len(FlagTimeLimit) != 0 {
				ChangeTaskTimeLimit(FlagTaskId, FlagTimeLimit)
			}
			if FlagPriority != 0 {
				ChangeTaskPriority(FlagTaskId, FlagPriority)
			}
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
	showCmd.AddCommand(showConfigCmd)
	rootCmd.AddCommand(updateCmd)
	updateCmd.Flags().Uint32VarP(&FlagTaskId, "job", "J", 0, "Job id")
	updateCmd.Flags().StringVarP(&FlagTimeLimit, "time-limit", "T", "", "time limit")
	updateCmd.Flags().Uint32VarP(&FlagPriority, "priority", "P", 0, "Job priority")
	err := updateCmd.MarkFlagRequired("job")
	if err != nil {
		return
	}
}
