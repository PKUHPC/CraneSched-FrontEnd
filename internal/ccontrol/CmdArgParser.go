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

package ccontrol

import (
	"CraneFrontEnd/internal/util"
	"os"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	FlagNodeName       string
	FlagState          string
	FlagReason         string
	FlagPartitionName  string
	FlagTaskId         uint32
	FlagTaskIds        string
	FlagQueryAll       bool
	FlagTimeLimit      string
	FlagPriority       float64
	FlagHoldTime       string
	FlagConfigFilePath string
	FlagJson           bool

	RootCmd = &cobra.Command{
		Use:     "ccontrol",
		Short:   "Display and modify the specified entity",
		Long:    "",
		Version: util.Version(),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
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
		Use:   "job [flags] [job_id,...]",
		Short: "Display details of the jobs, default is all",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			jobIds := ""
			if len(args) == 0 {
				FlagQueryAll = true
				jobIds = ""
			} else {
				FlagQueryAll = false
				jobIds = args[0]
			}
			if err := ShowJobs(jobIds, FlagQueryAll); err != util.ErrorSuccess {
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
				if err := ChangeTaskTimeLimit(FlagTaskIds, FlagTimeLimit); err != util.ErrorSuccess {
					os.Exit(err)
				}
			}
			if cmd.Flags().Changed("priority") {
				if err := ChangeTaskPriority(FlagTaskIds, FlagPriority); err != util.ErrorSuccess {
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
	sleepCmd = &cobra.Command{
		Use:   "sleep [flags] node_name[,node_name...]",
		Short: "Put nodes into sleep state",
		Long:  "Put the specified nodes into sleep state to save power",
		Args: func(cmd *cobra.Command, args []string) error {
			err := cobra.ExactArgs(1)(cmd, args)
			if err != nil {
				return err
			}
			matched, _ := regexp.MatchString(`^([a-zA-Z0-9_-]+)(,[a-zA-Z0-9_-]+)*$`, args[0])
			if !matched {
				log.Error("node name list must follow the format " +
					"<node_name> or '<node_name>,<node_name>,<node_name>...'")
				os.Exit(util.ErrorCmdArg)
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			nodeNames := strings.Split(args[0], ",")
			for _, nodeName := range nodeNames {
				if err := SleepNode(nodeName); err != util.ErrorSuccess {
					log.Errorf("Failed to put node %s to sleep", nodeName)
					os.Exit(err)
				}
			}
		},
	}
	wakeupCmd = &cobra.Command{
		Use:   "wakeup [flags] node_name[,node_name...]",
		Short: "Wake up sleeping nodes",
		Long:  "Wake up nodes that are in sleep state",
		Args: func(cmd *cobra.Command, args []string) error {
			err := cobra.ExactArgs(1)(cmd, args)
			if err != nil {
				return err
			}
			matched, _ := regexp.MatchString(`^([a-zA-Z0-9_-]+)(,[a-zA-Z0-9_-]+)*$`, args[0])
			if !matched {
				log.Error("node name list must follow the format " +
					"<node_name> or '<node_name>,<node_name>,<node_name>...'")
				os.Exit(util.ErrorCmdArg)
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			nodeNames := strings.Split(args[0], ",")
			for _, nodeName := range nodeNames {
				if err := WakeupNode(nodeName); err != util.ErrorSuccess {
					log.Errorf("Failed to wake up node %s", nodeName)
					os.Exit(err)
				}
			}
		},
	}
	shutdownCmd = &cobra.Command{
		Use:   "shutdown [flags] node_name[,node_name...]",
		Short: "Shutdown nodes",
		Long:  "Completely shutdown the specified nodes",
		Args: func(cmd *cobra.Command, args []string) error {
			err := cobra.ExactArgs(1)(cmd, args)
			if err != nil {
				return err
			}
			matched, _ := regexp.MatchString(`^([a-zA-Z0-9_-]+)(,[a-zA-Z0-9_-]+)*$`, args[0])
			if !matched {
				log.Error("node name list must follow the format " +
					"<node_name> or '<node_name>,<node_name>,<node_name>...'")
				os.Exit(util.ErrorCmdArg)
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			nodeNames := strings.Split(args[0], ",")
			for _, nodeName := range nodeNames {
				if err := ShutdownNode(nodeName); err != util.ErrorSuccess {
					log.Errorf("Failed to shutdown node %s", nodeName)
					os.Exit(err)
				}
			}
		},
	}
	poweronCmd = &cobra.Command{
		Use:   "poweron [flags] node_name[,node_name...]",
		Short: "Power on nodes",
		Long:  "Power on nodes that are completely shutdown",
		Args: func(cmd *cobra.Command, args []string) error {
			err := cobra.ExactArgs(1)(cmd, args)
			if err != nil {
				return err
			}
			matched, _ := regexp.MatchString(`^([a-zA-Z0-9_-]+)(,[a-zA-Z0-9_-]+)*$`, args[0])
			if !matched {
				log.Error("node name list must follow the format " +
					"<node_name> or '<node_name>,<node_name>,<node_name>...'")
				os.Exit(util.ErrorCmdArg)
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			nodeNames := strings.Split(args[0], ",")
			for _, nodeName := range nodeNames {
				if err := PowerOnNode(nodeName); err != util.ErrorSuccess {
					log.Errorf("Failed to power on node %s", nodeName)
					os.Exit(err)
				}
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
	RootCmd.PersistentFlags().BoolVar(&FlagJson, "json", false, "Output in JSON format")

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
			updateNodeCmd.Flags().StringVarP(&FlagNodeName, "name", "n", "", "Specify names of the node to be modified (comma seperated list)")
			updateNodeCmd.Flags().StringVarP(&FlagState, "state", "t", "", "Set the node state")
			updateNodeCmd.Flags().StringVarP(&FlagReason, "reason", "r", "", "Set the reason of this state change")
		}

		updateCmd.AddCommand(updateJobCmd)
		{
			updateJobCmd.Flags().StringVarP(&FlagTaskIds, "job", "J", "", "Specify job ids of the job to be modified (comma seperated list)")
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
		holdCmd.Flags().StringVarP(&FlagHoldTime, "time", "t", "", "Specify the duration the job will be prevented from starting")
	}
	RootCmd.AddCommand(releaseCmd)

	RootCmd.AddCommand(sleepCmd)
	RootCmd.AddCommand(wakeupCmd)
	RootCmd.AddCommand(shutdownCmd)
	RootCmd.AddCommand(poweronCmd)
}
