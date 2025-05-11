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

func ParseCmdArgs(args []string) {
	cmdStr := strings.Join(args[1:], " ")
	command, err := ParseCControlCommand(cmdStr)
	if err != nil {
		log.Debugf("invalid command format: %s", err)
		log.Error("error: command format is incorrect")
		showHelp()
		os.Exit(util.ErrorCmdArg)
	}

	processGlobalFlags(command)

	result := executeCommand(command)
	if result != util.ErrorSuccess {
		switch result {
		case util.ErrorCmdArg:
			log.Error("error: command execution failed")
			os.Exit(result)
		default:
			log.Errorf("error: command execution failed (error code: %d)", result)
		}
	}
}

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

func executeShowCommand(command *CControlCommand) int {
	resource := command.GetResource()

	switch resource {
	case "node":
		return executeShowNodeCommand(command)
	case "partition":
		return executeShowPartitionCommand(command)
	case "job":
		return executeShowJobCommand(command)
	case "reservation":
		return executeShowReservationCommand(command)
	default:
		log.Debugf("unknown resource type: %s", resource)
		return util.ErrorCmdArg
	}
}

func executeShowNodeCommand(command *CControlCommand) int {
	name := command.GetKVParamValue("name")
	if len(name) == 0 {
		FlagQueryAll = true
	}

	if err := ShowNodes(name, FlagQueryAll); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

func executeShowPartitionCommand(command *CControlCommand) int {
	name := command.GetKVParamValue("name")
	if len(name) == 0 {
		FlagQueryAll = true
	}

	if err := ShowPartitions(name, FlagQueryAll); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

func executeShowJobCommand(command *CControlCommand) int {
	name := command.GetKVParamValue("name")
	if len(name) == 0 {
		FlagQueryAll = true
	}

	if err := ShowJobs(name, FlagQueryAll); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

func executeShowReservationCommand(command *CControlCommand) int {
	name := command.GetKVParamValue("name")
	if len(name) == 0 {
		FlagQueryAll = true
	}

	if err := ShowReservations(name, FlagQueryAll); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

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

func executeUpdateNodeCommand(command *CControlCommand) int {
	kvParams := command.GetKVMaps()
	if len(kvParams) == 0 {
		log.Debug("no attribute to be modified")
		return util.ErrorCmdArg
	}

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "name":
			FlagNodeName = value
		case "state":
			FlagState = value
		case "reason":
			FlagReason = value
		default:
			log.Errorf("unknown attribute to modify: %s", key)
			return util.ErrorCmdArg
		}
	}

	if err := ChangeNodeState(FlagNodeName, FlagState, FlagReason); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

func executeUpdateJobCommand(command *CControlCommand) int {
	kvParams := command.GetKVMaps()
	if len(kvParams) == 0 {
		log.Debug("no attribute to be modified")
		return util.ErrorCmdArg
	}

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "priority":
			priority, err := strconv.ParseFloat(value, 64)
			if err != nil {
				log.Debugf("invalid priority value: %s", value)
				return util.ErrorCmdArg
			}
			FlagPriority = priority
		case "timelimit":
			FlagTimeLimit = value
		case "name":
			FlagTaskIds = value
		default:
			log.Errorf("unknown attribute to modify: %s", key)
			return util.ErrorCmdArg
		}
	}

	if err := ChangeTaskPriority(FlagTaskIds, FlagPriority); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

func executeUpdatePartitionCommand(command *CControlCommand) int {
	kvParams := command.GetKVMaps()
	if len(kvParams) == 0 {
		log.Debug("no attribute to be modified")
		return util.ErrorCmdArg
	}

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "allowed-accounts":
			FlagAllowedAccounts = value
		case "denied-accounts":
			FlagDeniedAccounts = value
		default:
			log.Errorf("unknown attribute to modify: %s", key)
			return util.ErrorCmdArg
		}
	}

	if err := ModifyPartitionAcl(FlagPartitionName, false, FlagDeniedAccounts); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

func executeHoldCommand(command *CControlCommand) int {
	jobIds := command.GetKVParamValue("name")

	timeLimit := command.GetKVParamValue("time-limit")
	if len(timeLimit) == 0 {
		log.Debug("no time limit specified")
		return util.ErrorCmdArg
	}

	FlagHoldTime = timeLimit

	if jobIds == "" {
		log.Debug("no job id specified")
		return util.ErrorCmdArg
	}

	FlagHoldTime = timeLimit

	if err := HoldReleaseJobs(jobIds, true); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

func executeReleaseCommand(command *CControlCommand) int {
	jobIds := command.GetKVParamValue("name")
	if jobIds == "" {
		log.Debug("no job id specified")
		return util.ErrorCmdArg
	}

	if err := HoldReleaseJobs(jobIds, false); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

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

func executeCreateReservationCommand(command *CControlCommand) int {
	kvParams := command.GetKVMaps()
	if len(kvParams) == 0 {
		log.Debug("no attribute to be modified")
		return util.ErrorCmdArg
	}

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "name":
			FlagReservationName = value
		case "start-time":
			FlagStartTime = value
		case "duration":
			FlagDuration = value
		case "nodes":
			FlagNodes = value
		case "account":
			FlagAccount = value
		case "user":
			FlagUser = value
		default:
			log.Errorf("unknown attribute to modify: %s", key)
			return util.ErrorCmdArg
		}
	}

	if err := CreateReservation(); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

func executeCreateJobCommand(command *CControlCommand) int {
	return util.ErrorSuccess
}

func executeCreatePartitionCommand(command *CControlCommand) int {
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

func executeDeleteReservationCommand(command *CControlCommand) int {
	reservationName := command.GetKVParamValue("name")

	if len(reservationName) == 0 {
		log.Debug("no reservation name specified")
		return util.ErrorCmdArg
	}

	if err := DeleteReservation(reservationName); err != util.ErrorSuccess {
		return util.ErrorCmdArg
	}

	return util.ErrorSuccess
}

func executeDeleteJobCommand(command *CControlCommand) int {
	return util.ErrorSuccess
}

func executeDeletePartitionCommand(command *CControlCommand) int {
	return util.ErrorSuccess
}
