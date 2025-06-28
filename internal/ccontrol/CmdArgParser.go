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

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	FlagNodeName        string
	FlagState           string
	FlagReason          string
	FlagPowerEnable     string
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
	FlagReservationName string
	FlagStartTime       string
	FlagDuration        string
	FlagNodes           string
	FlagAccount         string
	FlagUser            string
	FlagHistoryDays     int

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
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				FlagNodeName = ""
				FlagQueryAll = true
			} else {
				FlagNodeName = args[0]
				FlagQueryAll = false
			}
			return ShowNodes(FlagNodeName, FlagQueryAll)
		},
	}
	showPartitionCmd = &cobra.Command{
		Use:   "partition [flags] [partition_name]",
		Short: "Display details of the partitions, default is all",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				FlagPartitionName = ""
				FlagQueryAll = true
			} else {
				FlagPartitionName = args[0]
				FlagQueryAll = false
			}
			return ShowPartitions(FlagPartitionName, FlagQueryAll)
		},
	}
	showReservationsCmd = &cobra.Command{
		Use:   "reservation [flags] [reservation_name]",
		Short: "Display details of the reservations, default is all",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				FlagReservationName = ""
				FlagQueryAll = true
			} else {
				FlagReservationName = args[0]
				FlagQueryAll = false
			}
			return ShowReservations(FlagReservationName, FlagQueryAll)
		},
	}
	showJobCmd = &cobra.Command{
		Use:   "job [flags] [job_id,...]",
		Short: "Display details of the jobs, default is all",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			jobIds := ""
			if len(args) == 0 {
				FlagQueryAll = true
				jobIds = ""
			} else {
				FlagQueryAll = false
				jobIds = args[0]
			}
			return ShowJobs(jobIds, FlagQueryAll)
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
	showEnergyCmd = &cobra.Command{
		Use:   "energy [flags] [node_name[,node_name...]]",
		Short: "Display energy consumption and power management details of nodes",
		Long:  "Display energy consumption and power management details of nodes. Supports querying multiple nodes by separating node names with commas.",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return nil
			}
			err := cobra.MaximumNArgs(1)(cmd, args)
			if err != nil {
				return err
			}
			_, ok := util.ParseHostList(args[0])
			if !ok {
				return &util.CraneError{
					Code:    util.ErrorCmdArg,
					Message: "invalid node name list format. Examples: 'node1', 'node1,node2', 'node[1-5]', 'node[1,3,5]'",
				}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				FlagNodeName = ""
				FlagQueryAll = true
			} else {
				FlagNodeName = args[0]
				FlagQueryAll = false
			}
			return ShowEnergy(FlagNodeName, FlagQueryAll, FlagHistoryDays)
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
		RunE: func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Changed("time-limit") && !cmd.Flags().Changed("priority") {
				return &util.CraneError{
					Code:    util.ErrorCmdArg,
					Message: "No attribute to modify",
				}
			}

			if len(FlagTimeLimit) != 0 {
				return ChangeTaskTimeLimit(FlagTaskIds, FlagTimeLimit)
			}
			if cmd.Flags().Changed("priority") {
				return ChangeTaskPriority(FlagTaskIds, FlagPriority)
			}
			return nil
		},
	}
	updateNodeCmd = &cobra.Command{
		Use:   "node [flags]",
		Short: "Modify node attributes",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			if !cmd.Flags().Changed("state") && !cmd.Flags().Changed("power-enable") {
				return &util.CraneError{
					Code:    util.ErrorCmdArg,
					Message: "No attribute to modify. Please specify --state or --power-enable",
				}
			}

			if cmd.Flags().Changed("state") {
				if err := ChangeNodeState(FlagNodeName, FlagState, FlagReason); err != nil {
					return err
				}
			}

			if cmd.Flags().Changed("power-enable") {
				if err := EnableAutoPowerControl(FlagNodeName, FlagPowerEnable); err != nil {
					return err
				}
			}

			return nil
		},
	}
	updatePartitionCmd = &cobra.Command{
		Use:   "partition [flags] partition_name",
		Short: "Modify partition partition attributes",
		Long:  "",
		Args: func(cmd *cobra.Command, args []string) error {
			err := cobra.ExactArgs(1)(cmd, args)
			if err != nil {
				return err
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("allowed-accounts") {
				return ModifyPartitionAcl(args[0], true, FlagAllowedAccounts)
			} else if cmd.Flags().Changed("denied-accounts") {
				if err := ModifyPartitionAcl(args[0], false, FlagDeniedAccounts); err != nil {
					return err
				}
				log.Warning("Hint: When using AllowedAccounts, DeniedAccounts will not take effect.")
			}
			return nil
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
		RunE: func(cmd *cobra.Command, args []string) error {
			return HoldReleaseJobs(args[0], true)
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
		RunE: func(cmd *cobra.Command, args []string) error {
			return HoldReleaseJobs(args[0], false)
		},
	}
	createCmd = &cobra.Command{
		Use:   "create",
		Short: "Create a new entity",
		Long:  "",
	}
	createReservationCmd = &cobra.Command{
		Use:   "reservation [flags]",
		Short: "Create a new reservation",
		Long:  "",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return CreateReservation()
		},
	}
	deleteCmd = &cobra.Command{
		Use:   "delete",
		Short: "Delete the specified entity",
		Long:  "",
	}
	deleteReservationCmd = &cobra.Command{
		Use:   "reservation reservation_name",
		Short: "Delete the specified reservation",
		Long:  "",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return DeleteReservation(args[0])
		},
	}
)

// ParseCmdArgs executes the root command.
func ParseCmdArgs() {
	util.RunEWrapperForLeafCommand(RootCmd)
	util.RunAndHandleExit(RootCmd)
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
		showCmd.AddCommand(showReservationsCmd)
		showCmd.AddCommand(showEnergyCmd)
		{
			showEnergyCmd.Flags().IntVarP(&FlagHistoryDays, "history-days", "d", 30, "Specify the number of days to query node power state history data (default 30 days)")
		}
	}

	RootCmd.AddCommand(updateCmd)
	{
		updateCmd.AddCommand(updateNodeCmd)
		{
			updateNodeCmd.Flags().StringVarP(&FlagNodeName, "name", "n", "", "Specify names of the node to be modified (comma seperated list)")
			updateNodeCmd.Flags().StringVarP(&FlagState, "state", "t", "", "Set the node state")
			updateNodeCmd.Flags().StringVarP(&FlagReason, "reason", "r", "", "Set the reason of this state change")
			updateNodeCmd.Flags().StringVarP(&FlagPowerEnable, "power-enable", "p", "", "Enable/disable auto power control for node (true/false, yes/no, 1/0, on/off, enable/disable)")
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

	RootCmd.AddCommand(createCmd)
	{
		createCmd.AddCommand(createReservationCmd)
		{
			createReservationCmd.Flags().StringVarP(&FlagReservationName, "name", "n", "", "Specify the name of the reservation")
			createReservationCmd.Flags().StringVarP(&FlagStartTime, "start-time", "s", "", "Specify the start time of the reservation")
			createReservationCmd.Flags().StringVarP(&FlagDuration, "duration", "d", "", "Specify the duration of the reservation")
			createReservationCmd.Flags().StringVarP(&FlagPartitionName, "partition", "p", "", "Specify the partition of the reservation")
			createReservationCmd.Flags().StringVarP(&FlagNodes, "nodes", "N", "", "Specify the nodes of the reservation")
			createReservationCmd.Flags().StringVarP(&FlagAccount, "account", "a", "", "Specify the account of the reservation")
			createReservationCmd.Flags().StringVarP(&FlagUser, "user", "u", "", "Specify the user of the reservation")

			err := createReservationCmd.MarkFlagRequired("name")
			if err != nil {
				return
			}
			err = createReservationCmd.MarkFlagRequired("start-time")
			if err != nil {
				return
			}
			err = createReservationCmd.MarkFlagRequired("duration")
			if err != nil {
				return
			}
			err = createReservationCmd.MarkFlagRequired("account")
			if err != nil {
				return
			}
		}
	}

	RootCmd.AddCommand(deleteCmd)
	{
		deleteCmd.AddCommand(deleteReservationCmd)
	}
}
