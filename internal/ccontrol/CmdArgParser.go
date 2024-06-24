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
	"errors"
	"github.com/spf13/cobra"
	"os"
	"strconv"
)

var (
	FlagName              string
	FlagCpus              float64
	FlagMem               string
	FlagPartitions        []string
	FlagNodeStr           string
	FlagJobPriority       float64
	FlagPartitionPriority int64
	FlagAllowAccounts     []string
	FlagDenyAccounts      []string
	FlagState             string
	FlagReason            string
	FlagJobId             uint32
	FlagQueryAll          bool
	FlagTimeLimit         string

	FlagConfigFilePath string

	RootCmd = &cobra.Command{
		Use:   "ccontrol",
		Short: "Display and modify the specified entity",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			//cmd.SilenceUsage = true	// If no error occurred here, this flag should not be set because it will silence the flag requirement check for all subcommands.
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
		},
	}

	/* ---------------------------------------------------- show ---------------------------------------------------- */
	showCmd = &cobra.Command{
		Use:     "show",
		Aliases: []string{"find"},
		Short:   "Display details of the specified entity",
		Long:    "",
	}
	showNodeCmd = &cobra.Command{
		Use:   "node [flags] [node_name]",
		Short: "Display details of the nodes, default is all",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				FlagName = ""
				FlagQueryAll = true
			} else {
				FlagName = args[0]
				FlagQueryAll = false
			}
			return ShowNodes(FlagName, FlagQueryAll)
		},
	}
	showPartitionCmd = &cobra.Command{
		Use:   "partition [flags] [partition_name]",
		Short: "Display details of the partitions, default is all",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				FlagName = ""
				FlagQueryAll = true
			} else {
				FlagName = args[0]
				FlagQueryAll = false
			}
			return ShowPartitions(FlagName, FlagQueryAll)
		},
	}
	showJobCmd = &cobra.Command{
		Use:   "job [flags] [job_id]",
		Short: "Display details of the jobs, default is all",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				FlagQueryAll = true
			} else {
				id, _ := strconv.Atoi(args[0])
				FlagJobId = uint32(id)
				FlagQueryAll = false
			}
			return ShowTasks(FlagJobId, FlagQueryAll)
		},
	}
	showConfigCmd = &cobra.Command{
		Use:   "config",
		Short: "Display the configuration file in key-value format",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return ShowConfig(FlagConfigFilePath)
		},
	}

	/* ---------------------------------------------------- add  ---------------------------------------------------- */
	addCmd = &cobra.Command{
		Use:   "add",
		Short: "Add a partition or node",
		Long:  "",
	}
	addNodeCmd = &cobra.Command{
		Use:   "node [flags]",
		Short: "Add a new node",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return AddNode(FlagName, FlagCpus, FlagMem, FlagPartitions)
		},
	}
	addPartitionCmd = &cobra.Command{
		Use:   "partition [flags]",
		Short: "Add a new partition",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("priority") && FlagPartitionPriority < 0 {
				return &util.CraneError{
					Code:    util.ErrorCmdArg,
					Message: "--priority must be greater than or equal to 0",
				}
			}
			return AddPartition(FlagName, FlagNodeStr, FlagPartitionPriority, FlagAllowAccounts, FlagDenyAccounts)
		},
	}

	/* --------------------------------------------------- delete --------------------------------------------------- */
	deleteCmd = &cobra.Command{
		Use:     "delete",
		Aliases: []string{"remove"},
		Short:   "Delete a partition or node",
		Long:    "",
	}
	deleteNodeCmd = &cobra.Command{
		Use:   "node [node_name]",
		Short: "Delete an existing node",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return DeleteNode(args[0])
		},
	}
	deletePartitionCmd = &cobra.Command{
		Use:   "partition [partition_name]",
		Short: "Delete an existing partition",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return DeletePartition(args[0])
		},
	}

	/* --------------------------------------------------- update  -------------------------------------------------- */
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
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Changed("time-limit") && !cmd.Flags().Changed("priority") {
				return &util.CraneError{
					Code:    util.ErrorCmdArg,
					Message: "No attribute to modify",
				}
			}

			if len(FlagTimeLimit) != 0 {
				if err := ChangeTaskTimeLimit(FlagJobId, FlagTimeLimit); err != nil {
					return err
				}
			}
			if cmd.Flags().Changed("priority") {
				if err := ChangeTaskPriority(FlagJobId, FlagJobPriority); err != nil {
					return err
				}
			}
			return nil
		},
	}
	updateNodeCmd = &cobra.Command{
		Use:   "node [flags]",
		Short: "Modify node attributes",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			flagStateSet := cmd.Flags().Changed("state")
			flagReasonSet := cmd.Flags().Changed("reason")
			flagMemorySet := cmd.Flags().Changed("memory")
			flagCpuSet := cmd.Flags().Changed("cpu")

			if flagStateSet && flagReasonSet && !flagMemorySet && !flagCpuSet {
				return ChangeNodeState(FlagName, FlagState, FlagReason)
			} else if !flagStateSet && !flagReasonSet && (flagMemorySet || flagCpuSet) {
				return UpdateNode(FlagName, FlagCpus, FlagMem)
			} else {
				return &util.CraneError{
					Code:    util.ErrorCmdArg,
					Message: "Either --state&&--reason or --cpu or --mem should be set",
				}
			}
		},
	}
	updatePartitionCmd = &cobra.Command{
		Use:   "partition [flags]",
		Short: "Modify partition information",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("priority") && FlagPartitionPriority < 0 {
				return &util.CraneError{
					Code:    util.ErrorCmdArg,
					Message: "--priority must be greater than or equal to 0",
				}
			}
			return UpdatePartition(FlagName, FlagNodeStr, FlagPartitionPriority, FlagAllowAccounts, FlagDenyAccounts)
		},
	}
)

// ParseCmdArgs executes the root command.
func ParseCmdArgs() {
	// Silence usage info output when RunE() returns a non-nil error
	util.RunEWrapperForLeafCommand(RootCmd)

	if err := RootCmd.Execute(); err != nil {
		var craneErr *util.CraneError
		if errors.As(err, &craneErr) {
			os.Exit(craneErr.Code)
		} else {
			os.Exit(util.ErrorGeneric)
		}
	}
	os.Exit(util.ErrorSuccess)
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
			addNodeCmd.Flags().Float64VarP(&FlagCpus, "cpu", "c", 0.0, "Number of CPU cores")
			addNodeCmd.Flags().StringVarP(&FlagMem, "memory", "M", "", "Memory size, in units of G/M/K/B, default MB")
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
			addPartitionCmd.Flags().Int64VarP(&FlagPartitionPriority, "priority", "P", -1, "Partition priority")
			addPartitionCmd.Flags().StringSliceVarP(&FlagAllowAccounts, "allowlist", "A", nil, "List of accounts allowed to use this partition")
			addPartitionCmd.Flags().StringSliceVarP(&FlagDenyAccounts, "denylist", "D", nil, "Prohibit the use of the account list in this partition. The --denylist and the --allowlist parameter can only be selected as either")
			addPartitionCmd.MarkFlagsMutuallyExclusive("allowlist", "denylist")
			err := addPartitionCmd.MarkFlagRequired("name")
			if err != nil {
				return
			}
			err = addPartitionCmd.MarkFlagRequired("nodes")
			if err != nil {
				return
			}
		}
	}

	RootCmd.AddCommand(deleteCmd)
	{
		deleteCmd.AddCommand(deleteNodeCmd)
		deleteCmd.AddCommand(deletePartitionCmd)
	}

	RootCmd.AddCommand(updateCmd)
	{
		updateCmd.AddCommand(updateNodeCmd)
		{
			updateNodeCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Specify name of the node to be modified")
			updateNodeCmd.Flags().StringVarP(&FlagState, "state", "S", "", "Set the node state")
			updateNodeCmd.Flags().StringVarP(&FlagReason, "reason", "R", "", "Set the reason of this state change")

			updateNodeCmd.Flags().Float64VarP(&FlagCpus, "cpu", "c", 0.0, "Number of CPU cores")
			updateNodeCmd.Flags().StringVarP(&FlagMem, "memory", "M", "", "Memory size, in units of G/M/K/B")
			//updateNodeCmd.Flags().StringSliceVarP(&FlagPartitions, "partition", "P", nil, "The partition name to which the node belongs")
			err := updateNodeCmd.MarkFlagRequired("name")
			if err != nil {
				return
			}
		}

		updateCmd.AddCommand(updateJobCmd)
		{
			updateJobCmd.Flags().Uint32VarP(&FlagJobId, "job", "J", 0, "Specify job id of the job to be modified")
			updateJobCmd.Flags().StringVarP(&FlagTimeLimit, "time-limit", "T", "", "Set time limit of the job")
			updateJobCmd.Flags().Float64VarP(&FlagJobPriority, "priority", "P", 0, "Set the priority of the job")

			err := updateJobCmd.MarkFlagRequired("job")
			if err != nil {
				return
			}
		}

		updateCmd.AddCommand(updatePartitionCmd)
		{
			updatePartitionCmd.Flags().StringVarP(&FlagName, "name", "N", "", "Partition name")
			updatePartitionCmd.Flags().StringVar(&FlagNodeStr, "nodes", "", "The included nodes can be written individually, abbreviated or mixed, please write them in a string")
			updatePartitionCmd.Flags().Int64VarP(&FlagPartitionPriority, "priority", "P", -1, "Partition priority")
			updatePartitionCmd.Flags().StringSliceVarP(&FlagAllowAccounts, "allowlist", "A", nil, "List of accounts allowed to use this partition")
			updatePartitionCmd.Flags().StringSliceVarP(&FlagDenyAccounts, "denylist", "D", nil, "Prohibit the use of the account list in this partition. The --denylist and the --allowlist parameter can only be selected as either")
			updatePartitionCmd.MarkFlagsMutuallyExclusive("allowlist", "denylist")
		}
	}
}
