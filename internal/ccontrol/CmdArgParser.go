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
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
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
	FlagReservationName string
	FlagStartTime       string
	FlagDuration        string
	FlagNodes           string
	FlagAccount         string
	FlagUser            string

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
	showReservationsCmd = &cobra.Command{
		Use:   "reservation [flags] [reservation_name]",
		Short: "Display details of the reservations, default is all",
		Long:  "",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				FlagReservationName = ""
				FlagQueryAll = true
			} else {
				FlagReservationName = args[0]
				FlagQueryAll = false
			}
			if err := ShowReservations(FlagReservationName, FlagQueryAll); err != util.ErrorSuccess {
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
		Run: func(cmd *cobra.Command, args []string) {
			if err := CreateReservation(); err != util.ErrorSuccess {
				os.Exit(err)
			}
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
		Run: func(cmd *cobra.Command, args []string) {
			if err := DeleteReservation(args[0]); err != util.ErrorSuccess {
				os.Exit(err)
			}
		},
	}
)

// ParseCmdArgs
func ParseCmdArgs() {
	args := os.Args

	// detect cobra special flags
	for _, arg := range args {
		if arg == "--help" || arg == "-h" || strings.Contains(arg, "completion") || arg == "--version" || arg == "-v" {
			if err := RootCmd.Execute(); err != nil {
				os.Exit(util.ErrorGeneric)
			}
			return
		}
	}

	if len(args) < 2 {
		RootCmd.Help()
		return
	}

	result := parseAndExecuteWithCustomParser(args)

	if result == util.ErrorSuccess {
		return
	} else {
		if err := RootCmd.Execute(); err != nil {
			os.Exit(util.ErrorGeneric)
		}
	}

}

// parseAndExecuteWithCustomParser
func parseAndExecuteWithCustomParser(args []string) int {
	cmdStr := strings.Join(args[1:], " ")

	command, err := ParseCControlCommand(cmdStr)
	if err != nil {
		log.Debugf("error parse command: %v", err)
		return util.ErrorCmdArg
	}

	if len(command.Flags) > 0 {
		for i, flag := range command.Flags {
			println(i, ":", flag.Name, "=", flag.Value)
		}
	}

	if !command.IsValid() {
		log.Debug("invalid command format")
		return util.ErrorCmdArg
	}

	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	userUid = uint32(os.Getuid())

	processGlobalFlags(command)

	action := command.GetAction()

	switch action {
	case "show":
		return executeShowCommand(command)
	case "update":
		return executeUpdateCommand(command)
	case "hold":
		return executeHoldCommand(command)
	case "release":
		return executeReleaseCommand(command)
	default:
		log.Debugf("unknown action type: %s", action)
		return util.ErrorCmdArg
	}
}

// processGlobalFlags
func processGlobalFlags(command *CControlCommand) {
	jsonFlag, hasJson := command.GetFlag("json")
	if hasJson && jsonFlag != "" {
		FlagJson = true
	}

	configPath, hasConfig := command.GetFlag("config")
	if hasConfig && configPath != "" {
		FlagConfigFilePath = configPath
	}
}

// executeShowCommand
func executeShowCommand(command *CControlCommand) int {
	resource := command.GetResource()

	switch resource {
	case "node":
		return executeShowNodeCommand(command)
	case "partition":
		return executeShowPartitionCommand(command)
	case "job":
		return executeShowJobCommand(command)
	case "config":
		return executeShowConfigCommand()
	case "reservation":
		return executeShowReservationCommand(command)
	default:
		log.Debugf("unknown resource type: %s", resource)
		return util.ErrorCmdArg
	}
}

// executeShowNodeCommand
func executeShowNodeCommand(command *CControlCommand) int {
	nodeName, hasNodeName := command.GetFirstArg()
	if hasNodeName {
		FlagNodeName = nodeName
		FlagQueryAll = false
	} else {
		FlagNodeName = ""
		FlagQueryAll = true
	}

	if err := ShowNodes(FlagNodeName, FlagQueryAll); err != util.ErrorSuccess {
		os.Exit(err)
	}

	return util.ErrorSuccess
}

// executeShowPartitionCommand
func executeShowPartitionCommand(command *CControlCommand) int {
	partitionName, hasPartitionName := command.GetFirstArg()
	if hasPartitionName {
		FlagPartitionName = partitionName
		FlagQueryAll = false
	} else {
		FlagPartitionName = ""
		FlagQueryAll = true
	}

	if err := ShowPartitions(FlagPartitionName, FlagQueryAll); err != util.ErrorSuccess {
		os.Exit(err)
	}

	return util.ErrorSuccess
}

// executeShowJobCommand
func executeShowJobCommand(command *CControlCommand) int {
	jobIds, hasJobIds := command.GetFirstArg()
	if hasJobIds {
		FlagQueryAll = false
	} else {
		FlagQueryAll = true
	}

	if err := ShowJobs(jobIds, FlagQueryAll); err != util.ErrorSuccess {
		os.Exit(err)
	}

	return util.ErrorSuccess
}

// executeShowConfigCommand
func executeShowConfigCommand() int {
	if err := ShowConfig(FlagConfigFilePath); err != util.ErrorSuccess {
		os.Exit(err)
	}

	return util.ErrorSuccess
}

// executeShowReservationCommand
func executeShowReservationCommand(command *CControlCommand) int {
	reservationName, hasReservationName := command.GetFirstArg()
	if hasReservationName {
		FlagReservationName = reservationName
		FlagQueryAll = false
	} else {
		FlagReservationName = ""
		FlagQueryAll = true
	}

	if err := ShowReservations(FlagReservationName, FlagQueryAll); err != util.ErrorSuccess {
		os.Exit(err)
	}

	return util.ErrorSuccess
}

// executeUpdateCommand
func executeUpdateCommand(command *CControlCommand) int {
	resource := command.GetResource()

	println("resource: ", resource)
	switch resource {
	case "node":
		return executeUpdateNodeCommand(command)
	case "job":
		return executeUpdateJobCommand(command)
	case "partition":
		return executeUpdatePartitionCommand(command)
	default:
		log.Debugf("unknown resource type: %s", resource)
		return util.ErrorCmdArg
	}
}

// executeUpdateNodeCommand
func executeUpdateNodeCommand(command *CControlCommand) int {

	nodeName, hasNodeName := command.GetFirstArg()
	if hasNodeName {
		FlagNodeName = nodeName
	} else {
		nameFlag, hasName := command.GetFlag("name")
		if hasName {
			FlagNodeName = nameFlag
		} else {
			nameShort, hasNameShort := command.GetFlag("n")
			if hasNameShort {
				FlagNodeName = nameShort
			}
		}
	}

	if FlagNodeName == "" {
		log.Debug("no node name specified")
		return util.ErrorCmdArg
	}

	stateFlag, hasState := command.GetFlag("state")
	if !hasState {
		stateShort, hasStateShort := command.GetFlag("t")
		if hasStateShort {
			stateFlag = stateShort
			hasState = true
		}
	}

	reasonFlag, hasReason := command.GetFlag("reason")
	if !hasReason {
		reasonShort, hasReasonShort := command.GetFlag("r")
		if hasReasonShort {
			reasonFlag = reasonShort
			hasReason = true
		}
	}

	if !hasState {
		log.Debug("no state specified")
		return util.ErrorCmdArg
	}

	FlagState = stateFlag
	if hasReason {
		FlagReason = reasonFlag
	}

	if err := ChangeNodeState(FlagNodeName, FlagState, FlagReason); err != util.ErrorSuccess {
		os.Exit(err)
	}

	return util.ErrorSuccess
}

// executeUpdateJobCommand
func executeUpdateJobCommand(command *CControlCommand) int {

	jobFlagLong, hasJobLong := command.GetFlag("job")
	if hasJobLong && jobFlagLong != "" {
		FlagTaskIds = jobFlagLong
		println("Found job ID from --job flag:", FlagTaskIds)
	} else {
		jobFlagShort, hasJobShort := command.GetFlag("J")
		if hasJobShort && jobFlagShort != "" {
			FlagTaskIds = jobFlagShort
			println("Found job ID from -J flag:", FlagTaskIds)
		}
	}

	if FlagTaskIds == "" {
		log.Debug("no job id specified")
		return util.ErrorCmdArg
	}

	timeLimitFlag, hasTimeLimit := command.GetFlag("time-limit")
	if !hasTimeLimit {
		timeLimitShort, hasTimeLimitShort := command.GetFlag("T")
		if hasTimeLimitShort {
			timeLimitFlag = timeLimitShort
			hasTimeLimit = true
		}
	}

	priorityFlag, hasPriority := command.GetFlag("priority")
	if !hasPriority {
		priorityShort, hasPriorityShort := command.GetFlag("P")
		if hasPriorityShort {
			priorityFlag = priorityShort
			hasPriority = true
		}
	}

	if !hasTimeLimit && !hasPriority {
		log.Debug("there is no attribute to be modified")
		return util.ErrorCmdArg
	}

	if hasTimeLimit {
		FlagTimeLimit = timeLimitFlag
		if err := ChangeTaskTimeLimit(FlagTaskIds, FlagTimeLimit); err != util.ErrorSuccess {
			os.Exit(err)
		}
	}

	if hasPriority {
		priority, _ := strconv.ParseFloat(priorityFlag, 64)
		FlagPriority = priority
		if err := ChangeTaskPriority(FlagTaskIds, FlagPriority); err != util.ErrorSuccess {
			os.Exit(err)
		}
	}

	return util.ErrorSuccess
}

// executeUpdatePartitionCommand
func executeUpdatePartitionCommand(command *CControlCommand) int {

	partitionName, hasPartitionName := command.GetFirstArg()
	if !hasPartitionName {
		log.Debug("no partition name specified")
		return util.ErrorCmdArg
	}

	allowedAccounts, hasAllowedAccounts := command.GetFlag("allowed-accounts")
	if !hasAllowedAccounts {
		allowedShort, hasAllowedShort := command.GetFlag("A")
		if hasAllowedShort {
			allowedAccounts = allowedShort
			hasAllowedAccounts = true
		}
	}

	deniedAccounts, hasDeniedAccounts := command.GetFlag("denied-accounts")
	if !hasDeniedAccounts {
		deniedShort, hasDeniedShort := command.GetFlag("D")
		if hasDeniedShort {
			deniedAccounts = deniedShort
			hasDeniedAccounts = true
		}
	}

	if hasAllowedAccounts {
		FlagAllowedAccounts = allowedAccounts
		if err := ModifyPartitionAcl(partitionName, true, FlagAllowedAccounts); err != util.ErrorSuccess {
			os.Exit(err)
		}
	} else if hasDeniedAccounts {
		FlagDeniedAccounts = deniedAccounts
		if err := ModifyPartitionAcl(partitionName, false, FlagDeniedAccounts); err != util.ErrorSuccess {
			os.Exit(err)
		}
		log.Warning("Hint: When using AllowedAccounts, DeniedAccounts will not take effect.")
	} else {
		log.Debug("has no allowed-accounts or denied-accounts")
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

// executeHoldCommand
func executeHoldCommand(command *CControlCommand) int {
	if len(command.Flags2) > 0 {
		println("flags count:", len(command.Flags2))
		for i, flag := range command.Flags2 {
			println("flag", i, ":", flag.Name, "=", flag.Value)
		}
	}

	jobIds := command.GetHoldOrReleaseID()
	if jobIds == "" {
		log.Debug("no job id specified")
		return util.ErrorCmdArg
	}

	timeFlag, hasTime := command.GetFlag2("time-limit")
	if !hasTime {
		timeShort, hasTimeShort := command.GetFlag2("t")
		if hasTimeShort {
			timeFlag = timeShort
			hasTime = true
		}
	}

	if hasTime && timeFlag != "" {
		FlagHoldTime = timeFlag
	}

	if err := HoldReleaseJobs(jobIds, true); err != util.ErrorSuccess {
		os.Exit(err)
	}

	return util.ErrorSuccess
}

// executeReleaseCommand
func executeReleaseCommand(command *CControlCommand) int {
	jobIds := command.GetHoldOrReleaseID()
	if jobIds == "" {
		log.Debug("no job id specified")
		return util.ErrorCmdArg
	}

	if err := HoldReleaseJobs(jobIds, false); err != util.ErrorSuccess {
		os.Exit(err)
	}

	return util.ErrorSuccess
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
