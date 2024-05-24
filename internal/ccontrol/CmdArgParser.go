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

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	FlagName          string
	FlagCpus          float64
	FlagMem           string
	FlagPartitions    []string
	FlagNodeStr       string
	FlagPriority      int64
	FlagAllowAccounts []string
	FlagDenyAccounts  []string
	FlagState         string
	FlagReason        string
	FlagJobId         uint32
	FlagQueryAll      bool
	FlagTimeLimit     string

	FlagConfigFilePath string

	RootCmd = &cobra.Command{
		Use:   "ccontrol",
		Short: "Display and modify the specified entity",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
		},
	}
	showCmd = &cobra.Command{
		Use:   "show",
		Short: "Display details of the specified entity",
		Long:  "",
	}
	showNodeCmd = &cobra.Command{
		Use:   "node [flags] [node_name]",
		Short: "Display details of the nodes, default is all",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				FlagName = ""
				FlagQueryAll = true
			} else {
				FlagName = args[0]
				FlagQueryAll = false
			}
			if err := ShowNodes(FlagName, FlagQueryAll); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	showPartitionCmd = &cobra.Command{
		Use:   "partition [flags] [partition_name]",
		Short: "Display details of the partitions, default is all",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				FlagName = ""
				FlagQueryAll = true
			} else {
				FlagName = args[0]
				FlagQueryAll = false
			}
			if err := ShowPartitions(FlagName, FlagQueryAll); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	showJobCmd = &cobra.Command{
		Use:   "job [flags] [job_id]",
		Short: "Display details of the jobs, default is all",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				FlagQueryAll = true
			} else {
				id, _ := strconv.Atoi(args[0])
				FlagJobId = uint32(id)
				FlagQueryAll = false
			}
			if err := ShowTasks(FlagJobId, FlagQueryAll); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	addCmd = &cobra.Command{
		Use:   "add",
		Short: "Add a partition or node",
		Long:  "",
	}
	addNodeCmd = &cobra.Command{
		Use:   "node",
		Short: "Add a new node",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			AddNode(FlagName, FlagCpus, FlagMem, FlagPartitions)
		},
	}
	addPartitionCmd = &cobra.Command{
		Use:   "partition",
		Short: "Add a new partition",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			AddPartition(FlagName, FlagNodeStr, FlagPriority, FlagAllowAccounts, FlagDenyAccounts)
		},
	}
	deleteCmd = &cobra.Command{
		Use:     "delete",
		Aliases: []string{"remove"},
		Short:   "Delete a partition or node",
		Long:    "",
	}
	deleteNodeCmd = &cobra.Command{
		Use:   "node",
		Short: "Delete an existing node",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			DeleteNode(FlagName)
		},
	}
	deletePartitionCmd = &cobra.Command{
		Use:   "partition",
		Short: "Delete an existing partition",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			DeletePartition(FlagName)
		},
	}
	showConfigCmd = &cobra.Command{
		Use:   "config",
		Short: "Display the configuration file in key-value format",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := ShowConfig(FlagConfigFilePath); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	updateCmd = &cobra.Command{
		Use:     "update",
		Aliases: []string{"modify"},
		Short:   "Modify attributes of the specified entity",
		Long:    "",
	}
	updateJobCmd = &cobra.Command{
		Use:   "job [flags]",
		Short: "Modify job attributes",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if !cmd.Flags().Changed("time-limit") && !cmd.Flags().Changed("priority") {
				log.Error("No attribute to modify")
				os.Exit(util.ErrorCmdArg)
			}

			if len(FlagTimeLimit) != 0 {
				if err := ChangeTaskTimeLimit(FlagJobId, FlagTimeLimit); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("priority") {
				if err := ChangeTaskPriority(FlagJobId, FlagPriority); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
		},
	}
	updateNodeCmd = &cobra.Command{
		Use:   "node [flags]",
		Short: "Modify node attributes",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			if err := ChangeNodeState(FlagName, FlagState, FlagReason); err != util.ErrorSuccess {
				os.Exit(err)
			}
			UpdateNode(FlagName, FlagCpus, FlagMem)
		},
	}
	updatePartitionCmd = &cobra.Command{
		Use:   "partition",
		Short: "Modify partition information",
		Long:  "",
		Run: func(cmd *cobra.Command, args []string) {
			UpdatePartition(FlagName, FlagNodeStr, FlagPriority, FlagAllowAccounts, FlagDenyAccounts)
		},
	}
)

// ParseCmdArgs executes the root command.
func ParseCmdArgs() {
	if err := RootCmd.Execute(); err != nil {
		os.Exit(util.ErrorGeneric)
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
		showCmd.AddCommand(showConfigCmd)
	}

	RootCmd.AddCommand(addCmd)
	{
		addCmd.AddCommand(addNodeCmd)
		{
			addNodeCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Host name")
			addNodeCmd.Flags().Float64Var(&FlagCpus, "cpu", 0.0, "Number of CPU cores")
			addNodeCmd.Flags().StringVarP(&FlagMem, "memory", "M", "", "Memory size, in units of G/M/K/B")
			addNodeCmd.Flags().StringSliceVarP(&FlagPartitions, "partition", "P", nil, "The partition name to which the node belongs")
			err := addNodeCmd.MarkFlagRequired("name")
			if err != nil {
				return
			}
			err = addNodeCmd.MarkFlagRequired("cpu")
			if err != nil {
				return
			}
			err = addNodeCmd.MarkFlagRequired("memory")
			if err != nil {
				return
			}
		}

		addCmd.AddCommand(addPartitionCmd)
		{
			addPartitionCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Partition name")
			addPartitionCmd.Flags().StringVar(&FlagNodeStr, "nodes", "", "The included nodes can be written individually, abbreviated or mixed, please write them in a string")
			addPartitionCmd.Flags().Int64Var(&FlagPriority, "priority", -1, "Partition priority")
			addPartitionCmd.Flags().StringSliceVar(&FlagAllowAccounts, "allowlist", nil, "List of accounts allowed to use this partition")
			addPartitionCmd.Flags().StringSliceVar(&FlagDenyAccounts, "denylist", nil, "Prohibit the use of the account list in this partition. The --denylist and the --allowlist parameter can only be selected as either")
			addPartitionCmd.MarkFlagsMutuallyExclusive("allowlist", "denylist")
		}
	}

	RootCmd.AddCommand(deleteCmd)
	{
		deleteCmd.AddCommand(deleteNodeCmd)
		deleteNodeCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Host name")

		deleteCmd.AddCommand(deletePartitionCmd)
		deletePartitionCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Partition name")
	}

	updateCmd.AddCommand(updateNodeCmd)

	updateNodeCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Host name")
	updateNodeCmd.Flags().Float64Var(&FlagCpus, "cpu", 0.0, "Number of CPU cores")
	updateNodeCmd.Flags().StringVarP(&FlagMem, "memory", "M", "", "Memory size, in units of G/M/K/B")
	//updateNodeCmd.Flags().StringSliceVarP(&FlagPartitions, "partition", "P", nil, "The partition name to which the node belongs")
	err = updateNodeCmd.MarkFlagRequired("name")
	if err != nil {
		return
	}

	updateCmd.AddCommand(updatePartitionCmd)
	updatePartitionCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Partition name")
	updatePartitionCmd.Flags().StringVar(&FlagNodeStr, "nodes", "", "The included nodes can be written individually, abbreviated or mixed, please write them in a string")
	updatePartitionCmd.Flags().Int64Var(&FlagPriority, "priority", -1, "Partition priority")
	updatePartitionCmd.Flags().StringSliceVar(&FlagAllowAccounts, "allowlist", nil, "List of accounts allowed to use this partition")
	updatePartitionCmd.Flags().StringSliceVar(&FlagDenyAccounts, "denylist", nil, "Prohibit the use of the account list in this partition. The --denylist and the --allowlist parameter can only be selected as either")
	updatePartitionCmd.MarkFlagsMutuallyExclusive("allowlist", "denylist")

	updateCmd.AddCommand(updateJobCmd)
	updateJobCmd.Flags().Uint32VarP(&FlagJobId, "job", "J", 0, "Job id")
	updateJobCmd.Flags().StringVarP(&FlagTimeLimit, "time-limit", "T", "", "time limit")
	err = updateJobCmd.MarkFlagRequired("job")
	if err != nil {
		return
	}

	RootCmd.AddCommand(updateCmd)
	{
		updateCmd.AddCommand(updateNodeCmd)
		{
			updateNodeCmd.Flags().StringVarP(&FlagName, "name", "n", "", "Specify name of the node to be modified")
			updateNodeCmd.Flags().StringVarP(&FlagState, "state", "t", "", "Set the node state")
			updateNodeCmd.Flags().StringVarP(&FlagReason, "reason", "r", "", "Set the reason of this state change")
		}

		updateCmd.AddCommand(updateJobCmd)
		{
			updateJobCmd.Flags().Uint32VarP(&FlagTaskId, "job", "J", 0, "Specify job id of the job to be modified")
			updateJobCmd.Flags().StringVarP(&FlagTimeLimit, "time-limit", "T", "", "Set time limit of the job")
			updateJobCmd.Flags().Float64VarP(&FlagPriority, "priority", "P", 0, "Set the priority of the job")

			err := updateJobCmd.MarkFlagRequired("job")
			if err != nil {
				return
			}
		}
	}
}
