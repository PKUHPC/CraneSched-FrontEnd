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
	"fmt"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
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
	FlagConfigFilePath  string = util.DefaultConfigPath
	FlagJson            bool
	FlagReservationName string
	FlagStartTime       string
	FlagDuration        string
	FlagNodes           string
	FlagAccount         string
	FlagUser            string
)

// ParseCmdArgs
func ParseCmdArgs(args []string) {
	cmdStr := strings.Join(args[1:], " ")
	command, err := ParseCControlCommand(cmdStr)

	_, hasHelp, _ := getGlobalFlag(command, "help", "h")
	if err != nil || hasHelp {
		if handleHelp(command, err) {
			return
		}
	}

	if command.GetAction() != "" && strings.Contains(command.GetAction(), "completion") {
		fmt.Println("Command completion feature is not supported")
		os.Exit(1)
		return
	}

	processGlobalFlags(command)

	if !command.IsValid() {
		log.Debug("invalid command format")
		fmt.Printf("error: command format is incorrect\n\n")
		showHelp()
		os.Exit(util.ErrorCmdArg)
		return
	}

	result := executeCommand(command)
	if result != util.ErrorSuccess {
		switch result {
		case util.ErrorCmdArg:
			fmt.Printf("error: command argument error\n\n")
			if command.GetAction() != "" && command.GetResource() != "" {
				showSubCommandHelp(command.GetAction(), command.GetResource())
			} else if command.GetAction() != "" {
				showCommandHelp(command.GetAction())
			} else {
				showHelp()
			}
		default:
			fmt.Printf("error: command execution failed (error code: %d)\n", result)
		}
		os.Exit(result)
	}
}

// executeCommand
func executeCommand(command *CControlCommand) int {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	userUid = uint32(os.Getuid())

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
	case "create":
		return executeCreateCommand(command)
	case "delete":
		return executeDeleteCommand(command)
	default:
		log.Debugf("unknown operation type: %s", action)
		return util.ErrorCmdArg
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
		return util.ErrorCmdArg
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
		return util.ErrorCmdArg
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
		return util.ErrorCmdArg
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
		nameFlag, hasName, _ := getGlobalFlag(command, "name", "n")
		if hasName {
			FlagNodeName = nameFlag
		}
	}

	if FlagNodeName == "" {
		log.Debug("no node name specified")
		return util.ErrorCmdArg
	}

	stateFlag, hasState, _ := getGlobalFlag(command, "state", "t")
	reasonFlag, hasReason, _ := getGlobalFlag(command, "reason", "r")

	if !hasState {
		log.Debug("no state specified")
		return util.ErrorCmdArg
	}

	FlagState = stateFlag
	if hasReason {
		FlagReason = reasonFlag
	}

	if err := ChangeNodeState(FlagNodeName, FlagState, FlagReason); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

// executeUpdateJobCommand
func executeUpdateJobCommand(command *CControlCommand) int {

	jobFlagLong, hasJobLong, _ := getGlobalFlag(command, "job", "J")
	if hasJobLong && jobFlagLong != "" {
		FlagTaskIds = jobFlagLong
	}

	if FlagTaskIds == "" {
		log.Debug("no job id specified")
		return util.ErrorCmdArg
	}

	timeLimitFlag, hasTimeLimit, _ := getGlobalFlag(command, "time-limit", "T")
	priorityFlag, hasPriority, _ := getGlobalFlag(command, "priority", "P")

	if !hasTimeLimit && !hasPriority {
		log.Debug("there is no attribute to be modified")
		return util.ErrorCmdArg
	}

	if hasTimeLimit {
		FlagTimeLimit = timeLimitFlag
		if err := ChangeTaskTimeLimit(FlagTaskIds, FlagTimeLimit); err != util.ErrorSuccess {
			return util.ErrorCmdArg
		}
	}

	if hasPriority {
		priority, err := strconv.ParseFloat(priorityFlag, 64)
		if err != nil {
			log.Debugf("invalid priority value: %s", priorityFlag)
			return util.ErrorCmdArg
		}
		FlagPriority = priority
		if err := ChangeTaskPriority(FlagTaskIds, FlagPriority); err != util.ErrorSuccess {
			return util.ErrorCmdArg
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

	allowedAccounts, hasAllowedAccounts, _ := getGlobalFlag(command, "allowed-accounts", "A")
	deniedAccounts, hasDeniedAccounts, _ := getGlobalFlag(command, "denied-accounts", "D")

	if hasAllowedAccounts {
		FlagAllowedAccounts = allowedAccounts
		if err := ModifyPartitionAcl(partitionName, true, FlagAllowedAccounts); err != util.ErrorSuccess {
			return util.ErrorCmdArg
		}
	} else if hasDeniedAccounts {
		FlagDeniedAccounts = deniedAccounts
		if err := ModifyPartitionAcl(partitionName, false, FlagDeniedAccounts); err != util.ErrorSuccess {
			return util.ErrorCmdArg
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

	jobIds := command.GetHoldOrReleaseID()
	if jobIds == "" {
		log.Debug("no job id specified")
		return util.ErrorCmdArg
	}

	timeFlag, hasTime, _ := getGlobalFlag(command, "time-limit", "t")

	if hasTime && timeFlag != "" {
		FlagHoldTime = timeFlag
	}

	if err := HoldReleaseJobs(jobIds, true); err != util.ErrorSuccess {
		return util.ErrorCmdArg
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
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

// executeCreateCommand
func executeCreateCommand(command *CControlCommand) int {
	resource := command.GetResource()

	switch resource {
	case "reservation":
		return executeCreateReservationCommand(command)
	default:
		log.Debugf("unknown resource type: %s", resource)
		return util.ErrorCmdArg
	}
}

// executeCreateReservationCommand
func executeCreateReservationCommand(command *CControlCommand) int {
	nameValue, hasName, _ := getGlobalFlag(command, "name", "N")
	if !hasName || nameValue == "" {
		log.Debug("no reservation name specified")
		return util.ErrorCmdArg
	}
	FlagReservationName = nameValue

	startValue, hasStart, _ := getGlobalFlag(command, "start-time", "S")
	if !hasStart || startValue == "" {
		log.Debug("no start time specified")
		return util.ErrorCmdArg
	}
	FlagStartTime = startValue

	durationValue, hasDuration, _ := getGlobalFlag(command, "duration", "D")
	if !hasDuration || durationValue == "" {
		log.Debug("no duration specified")
		return util.ErrorCmdArg
	}
	FlagDuration = durationValue

	nodesValue, hasNodes, _ := getGlobalFlag(command, "nodes", "n")
	if !hasNodes || nodesValue == "" {
		log.Debug("no nodes specified")
		return util.ErrorCmdArg
	}
	FlagNodes = nodesValue

	accountValue, hasAccount, _ := getGlobalFlag(command, "account", "A")
	if hasAccount && accountValue != "" {
		FlagAccount = accountValue
	}

	userValue, hasUser, _ := getGlobalFlag(command, "user", "u")
	if hasUser && userValue != "" {
		FlagUser = userValue
	}

	if err := CreateReservation(); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

// executeDeleteCommand
func executeDeleteCommand(command *CControlCommand) int {
	resource := command.GetResource()

	switch resource {
	case "reservation":
		return executeDeleteReservationCommand(command)
	default:
		log.Debugf("unknown resource type: %s", resource)
		return util.ErrorCmdArg
	}
}

// executeDeleteReservationCommand
func executeDeleteReservationCommand(command *CControlCommand) int {
	reservationName, hasReservationName := command.GetFirstArg()
	if !hasReservationName {
		log.Debug("no reservation name specified")
		return util.ErrorCmdArg
	}

	if err := DeleteReservation(reservationName); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}
