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
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
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
	commandArgs := preParseGlobalFlags(args[1:])

	if len(commandArgs) == 0 {
		showHelp()
		os.Exit(0)
	}

	var processedArgs []string
	for _, arg := range commandArgs {
		if strings.Contains(arg, " ") {
			processedArgs = append(processedArgs, strconv.Quote(arg))
		} else {
			processedArgs = append(processedArgs, arg)
		}
	}
	cmdStr := strings.Join(processedArgs, " ")
	command, err := ParseCControlCommand(cmdStr)

	if err != nil {
		log.Error("error: command format is incorrect")
		os.Exit(util.ErrorCmdArg)
	}

	result := executeCommand(command)
	if result != util.ErrorSuccess {
		log.Errorf("command execution failed")
	}
	os.Exit(result)
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
	entity := command.GetEntity()

	switch entity {
	case "node":
		return executeShowNodeCommand(command)
	case "partition":
		return executeShowPartitionCommand(command)
	case "job":
		return executeShowJobCommand(command)
	case "reservation":
		return executeShowReservationCommand(command)
	default:
		log.Debugf("unknown entity type: %s", entity)
		return util.ErrorCmdArg

	}
}

func executeShowNodeCommand(command *CControlCommand) int {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
		name = ""
	}

	err := ShowNodes(name, FlagQueryAll)
	if err != nil {
		log.Errorf("show nodes failed: %s", err)
		return util.ErrorGeneric
	}
	return util.ErrorSuccess
}

func executeShowPartitionCommand(command *CControlCommand) int {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true

	}

	err := ShowPartitions(name, FlagQueryAll)
	if err != nil {
		log.Errorf("show partitions failed: %s", err)
		return util.ErrorGeneric
	}
	return util.ErrorSuccess
}

func executeShowJobCommand(command *CControlCommand) int {
	name := command.GetID()

	if len(name) == 0 {
		FlagQueryAll = true

	}

	err := ShowJobs(name, FlagQueryAll)
	if err != nil {
		log.Errorf("show jobs failed: %s", err)
		return util.ErrorGeneric
	}
	return util.ErrorSuccess
}

func executeShowReservationCommand(command *CControlCommand) int {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
		name = " "
	}

	err := ShowReservations(name, FlagQueryAll)
	if err != nil {
		log.Errorf("show reservations failed: %s", err)
		return util.ErrorGeneric
	}
	return util.ErrorSuccess
}

func executeUpdateCommand(command *CControlCommand) int {
	kvParams := command.GetKVMaps()
	if len(kvParams) == 0 {
		log.Debug("no attribute to be modified")
		return util.ErrorCmdArg

	}

	for key := range kvParams {
		lowerKey := strings.ToLower(key)
		if lowerKey == "node" || lowerKey == "nodename" {
			FlagNodeName = kvParams[key]
			return executeUpdateNodeCommand(command)
		}
	}

	for key := range kvParams {
		lowerKey := strings.ToLower(key)
		if lowerKey == "job" || lowerKey == "jobid" {
			FlagTaskIds = kvParams[key]
			return executeUpdateJobCommand(command)
		}
	}

	for key := range kvParams {
		lowerKey := strings.ToLower(key)
		if lowerKey == "partition" || lowerKey == "partitionname" {
			FlagPartitionName = kvParams[key]
			return executeUpdatePartitionCommand(command)
		}
	}

	log.Debugf("unknown attribute to modify")
	return util.ErrorCmdArg
}

func executeUpdateNodeCommand(command *CControlCommand) int {
	kvParams := command.GetKVMaps()

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "state":
			FlagState = value
		case "reason":
			FlagReason = value
		case "nodename", "node":
			continue
		default:
			log.Errorf("unknown attribute to modify: %s", key)
			return util.ErrorCmdArg
		}
	}
	err := ChangeNodeState(FlagNodeName, FlagState, FlagReason)
	if err != nil {
		log.Errorf("change node state failed: %s", err)
		return util.ErrorGeneric
	}
	return util.ErrorSuccess
}

func executeUpdateJobCommand(command *CControlCommand) int {
	kvParams := command.GetKVMaps()

	var lastErr int = util.ErrorCmdArg
	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "priority":
			priority, err := strconv.ParseFloat(value, 64)
			if err != nil {
				lastErr = util.ErrorCmdArg
			}
			FlagPriority = priority
			err = ChangeTaskPriority(FlagTaskIds, FlagPriority)
			if err != nil {
				log.Errorf("change task priority failed: %s", err)
				lastErr = util.ErrorGeneric
			}
		case "timelimit":
			FlagTimeLimit = value
			err := ChangeTaskTimeLimit(FlagTaskIds, FlagTimeLimit)
			if err != nil {
				log.Errorf("change task time limit failed: %s", err)
				lastErr = util.ErrorGeneric
			}
		case "jobid", "job":
			continue
		default:
			log.Errorf("unknown attribute to modify: %s", key)
			lastErr = util.ErrorCmdArg
		}
	}

	return lastErr
}

func executeUpdatePartitionCommand(command *CControlCommand) int {
	kvParams := command.GetKVMaps()

	var lastErr int = util.ErrorCmdArg
	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "accounts", "allowedaccounts":
			FlagAllowedAccounts = value
			err := ModifyPartitionAcl(FlagPartitionName, true, FlagAllowedAccounts)
			if err != nil {
				log.Errorf("modify partition acl failed: %s", err)
				lastErr = util.ErrorGeneric
			}
		case "deniedaccounts":
			FlagDeniedAccounts = value
			err := ModifyPartitionAcl(FlagPartitionName, false, FlagDeniedAccounts)
			if err != nil {
				log.Errorf("modify partition acl failed: %s", err)
				lastErr = util.ErrorGeneric
			}
		case "partitionname", "partition":
			continue
		default:
			log.Errorf("unknown attribute to modify: %s", key)
			lastErr = util.ErrorCmdArg
		}
	}

	return lastErr
}

func executeHoldCommand(command *CControlCommand) int {
	jobIds := command.GetID()

	timeLimit := command.GetKVParamValue("timelimit")
	if len(timeLimit) == 0 {
		log.Debug("no time limit specified")
	}

	if jobIds == "" {
		log.Debug("no job id specified")
		return util.ErrorCmdArg
	}

	FlagHoldTime = timeLimit

	err := HoldReleaseJobs(jobIds, true)
	if err != nil {
		log.Errorf("hold jobs failed: %s", err)
		return util.ErrorGeneric
	}
	return util.ErrorSuccess
}

func executeReleaseCommand(command *CControlCommand) int {
	jobIds := command.GetID()
	if jobIds == "" {
		log.Debug("no job id specified")
		return util.ErrorCmdArg
	}

	err := HoldReleaseJobs(jobIds, false)
	if err != nil {
		log.Errorf("release jobs failed: %s", err)
		return util.ErrorGeneric
	}
	return util.ErrorSuccess
}

func executeCreateCommand(command *CControlCommand) int {
	entity := command.GetEntity()

	switch entity {
	case "reservation":
		return executeCreateReservationCommand(command)
	default:
		log.Debugf("unknown entity type: %s", entity)
		return util.ErrorCmdArg
	}
}

func executeCreateReservationCommand(command *CControlCommand) int {
	FlagReservationName = command.GetID()
	if len(FlagReservationName) == 0 {
		log.Debug("no reservation name specified")
		return util.ErrorCmdArg
	}

	kvParams := command.GetKVMaps()

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "starttime":
			FlagStartTime = value
		case "partition":
			FlagPartitionName = value
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

	err := CreateReservation()
	if err != nil {
		log.Errorf("create reservation failed: %s", err)
		return util.ErrorGeneric
	}
	return util.ErrorSuccess
}

func executeDeleteCommand(command *CControlCommand) int {
	entity := command.GetEntity()

	switch entity {
	case "reservation":
		return executeDeleteReservationCommand(command)
	default:
		log.Debugf("unknown entity type: %s", entity)
		return util.ErrorCmdArg
	}
}

func executeDeleteReservationCommand(command *CControlCommand) int {
	name := command.GetID()

	if len(name) == 0 {
		log.Debug("no reservation name specified")
		return util.ErrorCmdArg
	}

	err := DeleteReservation(name)
	if err != nil {
		log.Errorf("delete reservation failed: %s", err)
		return util.ErrorGeneric
	}
	return util.ErrorSuccess
}
