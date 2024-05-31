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
	"os"
	"strconv"

	"github.com/spf13/cobra"
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

	RootCmd = &cobra.Command{
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
			if err := ShowNodes(FlagNodeName, FlagQueryAll); err != util.ErrorSuccess {
				os.Exit(err)
			}
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
			if err := ShowPartitions(FlagPartitionName, FlagQueryAll); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	showJobCmd = &cobra.Command{
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
			if err := ShowTasks(FlagTaskId, FlagQueryAll); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	updateCmd = &cobra.Command{
		Use:   "update",
		Short: "Modify job information",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if err := ChangeTaskTimeLimit(FlagTaskId, FlagTimeLimit); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	updateNodeCmd = &cobra.Command{
		Use:   "node",
		Short: "Modify node information",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if err := ChangeNodeState(FlagNodeName, FlagState, FlagReason); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
)

// ParseCmdArgs executes the root command.
func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(util.ErrorExecuteFailed)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath,
		"Path to configuration file")

	RootCmd.AddCommand(showCmd)
	{
		showCmd.AddCommand(showNodeCmd)
		showCmd.AddCommand(showPartitionCmd)
		showCmd.AddCommand(showJobCmd)
	}

	RootCmd.AddCommand(updateCmd)
	{
		updateCmd.Flags().StringVarP(&FlagTimeLimit, "time-limit", "T", "", "time limit")
		updateCmd.Flags().Uint32VarP(&FlagTaskId, "job", "J", 0, "Job id")

		updateCmd.AddCommand(updateNodeCmd)
		{
			updateNodeCmd.Flags().StringVarP(&FlagNodeName, "name", "n", "", "specify a node name")
			updateNodeCmd.Flags().StringVarP(&FlagState, "state", "t", "", "specify the state")
			updateNodeCmd.Flags().StringVarP(&FlagReason, "reason", "r", "", "set reason")
		}

		err := updateCmd.MarkFlagRequired("job")
		if err != nil {
			return
		}
	}
}
