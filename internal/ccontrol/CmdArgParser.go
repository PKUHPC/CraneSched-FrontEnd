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
	"regexp"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	FlagNodeName       string
	FlagState          string
	FlagReason         string
	FlagPartitionName  string
	FlagTaskId         uint32
	FlagQueryAll       bool
	FlagTimeLimit      string
	FlagPriority       float64
	FlagHoldTime       string
	FlagConfigFilePath string

	RootCmd = &cobra.Command{
		Use:     "ccontrol",
		Short:   "Display and modify the specified entity",
		Long:    "",
		Version: util.Version(),
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
		Use:   "partition [flags] [partition_name]",
		Short: "Display details of the partitions, default is all",
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
		Use:   "job [flags] [job_id]",
		Short: "Display details of the jobs, default is all",
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
				if err := ChangeTaskTimeLimit(FlagTaskId, FlagTimeLimit); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("priority") {
				if err := ChangeTaskPriority(FlagTaskId, FlagPriority); err != util.ErrorSuccess {
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
			if err := ChangeNodeState(FlagNodeName, FlagState, FlagReason); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	holdCmd = &cobra.Command{
		Use:   "hold [flags] job_id[,job_id...]",
		Short: "prevent specified job from starting. ",
		Long:  "",
		Args: func(cmd *cobra.Command, args []string) error {
			err := cobra.ExactArgs(1)(cmd, args)
			if err != nil {
				return err
			}
			matched, _ := regexp.MatchString(`^([1-9][0-9]*)(,[1-9][0-9]*)*$`, args[0])
			if !matched {
				log.Error("job id list must follow the format " +
					"<job_id> or '<job_id>,<job_id>,<job_id>...'")
				os.Exit(util.ErrorCmdArg)
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if err := HoldReleaseJobs(args[0], true); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	releaseCmd = &cobra.Command{
		Use:   "release [flags] job_id[,job_id...]",
		Short: "permit specified job to start. ",
		Long:  "",
		Args: func(cmd *cobra.Command, args []string) error {
			err := cobra.ExactArgs(1)(cmd, args)
			if err != nil {
				return err
			}
			matched, _ := regexp.MatchString(`^([1-9][0-9]*)(,[1-9][0-9]*)*$`, args[0])
			if !matched {
				log.Error("job id list must follow the format " +
					"<job_id> or '<job_id>,<job_id>,<job_id>...'")
				os.Exit(util.ErrorCmdArg)
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if err := HoldReleaseJobs(args[0], false); err != util.ErrorSuccess {
				os.Exit(err)
			}
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
	RootCmd.SetVersionTemplate(util.VersionTemplate())
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath,
		"Path to configuration file")

	RootCmd.AddCommand(showCmd)
	{
		showCmd.AddCommand(showNodeCmd)
		showCmd.AddCommand(showPartitionCmd)
		showCmd.AddCommand(showJobCmd)
		showCmd.AddCommand(showConfigCmd)
	}

	RootCmd.AddCommand(updateCmd)
	{
		updateCmd.AddCommand(updateNodeCmd)
		{
			updateNodeCmd.Flags().StringVarP(&FlagNodeName, "name", "n", "", "Specify name of the node to be modified")
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
	RootCmd.AddCommand(holdCmd)
	{
		holdCmd.Flags().StringVarP(&FlagHoldTime, "time", "t", "", "Specify the duration for a job which will be prevented from starting.")
	}
	RootCmd.AddCommand(releaseCmd)
}
