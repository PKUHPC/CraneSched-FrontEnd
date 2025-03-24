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
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
	"regexp"
	"strconv"
)

var (
	FlagNodeName        string
	FlagState           string
	FlagReason          string
	FlagPartitionName   string
	FlagAllowedAccounts string
	FlagDeniedAccounts  string
	FlagTaskId          uint32
	FlagTaskIds         string
	FlagQueryAll        bool
	FlagTimeLimit       string
	FlagPriority        float64
	FlagHoldTime        string
	FlagConfigFilePath  string
	FlagJson            bool
	FlagServerHostName  string
	FlagRaftPort        uint32

	RootCmd = &cobra.Command{
		Use:     "ccontrol",
		Short:   "Display and modify the specified entity",
		Long:    "",
		Version: util.Version(),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			util.DetectNetworkProxy()
			config := util.ParseConfig(FlagConfigFilePath)
			stub = util.GetStubToCtldByConfig(config)
			userUid = uint32(os.Getuid())
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
	showServerClusterCmd = &cobra.Command{
		Use:   "serverCluster",
		Short: "Display information about the Raft service node cluster",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := ShowServerCluster(); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	showServerNodeCmd = &cobra.Command{
		Use:   "serverNode [server_id]",
		Short: "Display status of the current leader service node",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := ShowServerNode(); err != util.ErrorSuccess {
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
	updatePartitionCmd = &cobra.Command{
		Use:   "partition [flags] partition_name",
		Short: "Modify partition partition attributes",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if cmd.Flags().Changed("allowed-accounts") {
				if err := ModifyPartitionAcl(args[0], true, FlagAllowedAccounts); err != util.ErrorSuccess {
					os.Exit(err)
				}
			} else if cmd.Flags().Changed("denied-accounts") {
				if err := ModifyPartitionAcl(args[0], false, FlagDeniedAccounts); err != util.ErrorSuccess {
					os.Exit(err)
				}
				log.Warning("Hint: When using AllowedAccounts, DeniedAccounts will not take effect.")
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
	addCmd = &cobra.Command{
		Use:   "add",
		Short: "Add some config",
		Long:  "",
	}
	addServerNodeCmd = &cobra.Command{
		Use:   "serverNode [flags]",
		Short: "Add a server node",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := AddServerNode(FlagServerHostName, FlagRaftPort); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	removeCmd = &cobra.Command{
		Use:   "remove",
		Short: "remove some config",
		Long:  "",
	}
	removeServerNodeCmd = &cobra.Command{
		Use:   "serverNode [server_id]",
		Short: "remove a server node",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			id, err := strconv.Atoi(args[0])
			if err != nil {
				log.Errorf("Failed to convert server id to int: %s", err.Error())
				os.Exit(util.ErrorCmdArg)
			}
			if err := RemoveServerNode(id); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
	yieldLeadershipCmd = &cobra.Command{
		Use:   "yieldLeadership [next_server_id]",
		Short: "Yield leadership to other server node, id = [next_server_id], empty parameter 'id' represents priority determination",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var id = -1
			var err error
			if len(args) > 0 {
				id, err = strconv.Atoi(args[0])
				if err != nil {
					log.Errorf("Failed to convert server id to int: %s", err.Error())
					os.Exit(util.ErrorCmdArg)
				}
			}

			if err := YieldLeadership(id); err != util.ErrorSuccess {
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
	RootCmd.PersistentFlags().StringVarP(&FlagConfigFilePath, "config", "C", util.DefaultConfigPath, "Path to configuration file")
	RootCmd.PersistentFlags().BoolVar(&FlagJson, "json", false, "Output in JSON format")

	RootCmd.AddCommand(showCmd)
	{
		showCmd.AddCommand(showNodeCmd)
		showCmd.AddCommand(showPartitionCmd)
		showCmd.AddCommand(showJobCmd)
		showCmd.AddCommand(showConfigCmd)
		showCmd.AddCommand(showServerClusterCmd)
		showCmd.AddCommand(showServerNodeCmd)
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

		updateCmd.AddCommand(updatePartitionCmd)
		{
			updatePartitionCmd.Flags().StringVarP(&FlagAllowedAccounts, "allowed-accounts", "A", "", "Set the allow account list for the partition")
			updatePartitionCmd.Flags().StringVarP(&FlagDeniedAccounts, "denied-accounts", "D", "", "Set the denied account list for the partition")

			updatePartitionCmd.MarkFlagsMutuallyExclusive("allowed-accounts", "denied-accounts")
			updatePartitionCmd.MarkFlagsOneRequired("allowed-accounts", "denied-accounts")
		}
	}
	RootCmd.AddCommand(holdCmd)
	{
		holdCmd.Flags().StringVarP(&FlagHoldTime, "time", "t", "", "Specify the duration the job will be prevented from starting")
	}
	RootCmd.AddCommand(releaseCmd)
	RootCmd.AddCommand(addCmd)
	{
		addCmd.AddCommand(addServerNodeCmd)
		{
			addServerNodeCmd.Flags().StringVarP(&FlagServerHostName, "host_name", "n", "", "Specify the node name")
			addServerNodeCmd.Flags().Uint32VarP(&FlagRaftPort, "port", "r", 10009, "Raft port is used for communication within the Raft protocol itself")

			err := addServerNodeCmd.MarkFlagRequired("host_name")
			if err != nil {
				return
			}
		}
	}
	RootCmd.AddCommand(removeCmd)
	{
		removeCmd.AddCommand(removeServerNodeCmd)
	}
	RootCmd.AddCommand(yieldLeadershipCmd)
}
