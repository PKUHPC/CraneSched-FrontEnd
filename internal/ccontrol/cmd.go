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
	FlagNodeNum         uint32
)

var actionToExecute = map[string]func(command *CControlCommand) util.CraneError{
	"show":    executeShowCommand,
	"update":  executeUpdateCommand,
	"hold":    executeHoldCommand,
	"release": executeReleaseCommand,
	"create":  executeCreateCommand,
	"delete":  executeDeleteCommand,
}

func ParseCmdArgs(args []string) {
	commandArgs := preParseGlobalFlags(args[1:])
	if len(commandArgs) == 0 {
		showHelp()
		os.Exit(0)
	}

	cmdStr := getCmdStringByArgs(commandArgs)
	command, err := ParseCControlCommand(cmdStr)
	if err != nil {
		log.Errorf("Error: command format is incorrect %v", err)
		os.Exit(util.ErrorCmdArg)
	}

	result := executeCommand(command)
	if result.Message != "" {
		log.Error(result.Message)
	}

	os.Exit(result.Code)
}

func executeCommand(command *CControlCommand) util.CraneError {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	userUid = uint32(os.Getuid())

	action := command.GetAction()
	executeAction, exists := actionToExecute[action]
	if exists {
		return executeAction(command)
	} else {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown operation type: %s\n", action))
	}
}

func executeShowCommand(command *CControlCommand) util.CraneError {
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
	case "lic":
		return executeShowLicenseCommand(command)
	default:
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeShowNodeCommand(command *CControlCommand) util.CraneError {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
		name = ""
	}
	err := ShowNodes(name, FlagQueryAll)
	if err.Code != util.ErrorSuccess {
		return *util.NewCraneErr(util.ErrorGeneric, fmt.Sprintf("show nodes failed: %s\n", err.Message))
	}
	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeShowPartitionCommand(command *CControlCommand) util.CraneError {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
	}
	err := ShowPartitions(name, FlagQueryAll)
	if err.Code != util.ErrorSuccess {
		return *util.NewCraneErr(util.ErrorGeneric, fmt.Sprintf("show partitions failed: %s\n", err.Message))
	}
	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeShowJobCommand(command *CControlCommand) util.CraneError {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
	}

	err := ShowJobs(name, FlagQueryAll)
	if err.Code != util.ErrorSuccess {
		return *util.NewCraneErr(util.ErrorGeneric, fmt.Sprintf("show job failed: %s\n", err.Message))
	}
	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeShowReservationCommand(command *CControlCommand) util.CraneError {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
		name = ""
	}
	err := ShowReservations(name, FlagQueryAll)
	if err.Code != util.ErrorSuccess {
		return *util.NewCraneErr(util.ErrorGeneric, fmt.Sprintf("show reservations failed: %s", err.Message))
	}
	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeShowLicenseCommand(command *CControlCommand) util.CraneError {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
		name = ""
	}
	err := ShowLicenses(name, FlagQueryAll)
	if err.Code != util.ErrorSuccess {
		return *util.NewCraneErr(util.ErrorGeneric, fmt.Sprintf("show licenses failed: %s", err.Message))
	}
	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeUpdateCommand(command *CControlCommand) util.CraneError {
	kvParams := command.GetKVMaps()

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
	return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("unknown attribute to modify"))
}

func executeUpdateNodeCommand(command *CControlCommand) util.CraneError {
	kvParams := command.GetKVMaps()

	if err := checkEmptyKVParams(kvParams, []string{"state", "reason"}); err.Code != util.ErrorSuccess {
		return err
	}

	if FlagNodeName == "" {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("node name not specified"))
	}

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "state":
			FlagState = value
		case "reason":
			FlagReason = value
		case "nodename", "node":
			continue
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown attribute to modify: %s\n", key))
		}
	}
	err := ChangeNodeState(FlagNodeName, FlagState, FlagReason)
	if err.Code != util.ErrorSuccess {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("change node state failed: %s\n", err.Message))
	}
	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeUpdateJobCommand(command *CControlCommand) util.CraneError {
	kvParams := command.GetKVMaps()

	var craneError = *util.NewCraneErr(util.ErrorSuccess, "")

	var jobParamFlags UpdateJobParamFlags
	jobParamValuesMap := make(map[UpdateJobParamFlags]string)
	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "priority":
			jobParamFlags |= PriorityTypeFlag
			jobParamValuesMap[PriorityTypeFlag] = value
		case "timelimit":
			jobParamFlags |= TimelimitTypeFlag
			jobParamValuesMap[TimelimitTypeFlag] = value
		case "comment":
			jobParamFlags |= CommentTypeFlag
			jobParamValuesMap[CommentTypeFlag] = value
		case "mailuser":
			jobParamFlags |= MailUserTypeFlag
			jobParamValuesMap[MailUserTypeFlag] = value
		case "mailtype":
			if !util.CheckMailType(value) {
				return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid mailtype value to modify: %s\n", value))
			}
			jobParamFlags |= MailTypeTypeFlag
			jobParamValuesMap[MailTypeTypeFlag] = value
		case "jobid", "job":
			continue
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown attribute to modify: %s\n", key))
		}
	}

	if jobParamFlags&PriorityTypeFlag != 0 {
		value := jobParamValuesMap[PriorityTypeFlag]
		priority, err := strconv.ParseFloat(value, 64)
		if err != nil {
			craneError.Code = util.ErrorCmdArg
		}
		FlagPriority = priority
		result := ChangeTaskPriority(FlagTaskIds, FlagPriority)
		if result.Code != util.ErrorSuccess {
			craneError.Message += fmt.Sprintf("change task priority failed: %s\n", result.Message)
			craneError.Code = util.ErrorGeneric
		}
	}

	if jobParamFlags&TimelimitTypeFlag != 0 {
		FlagTimeLimit = jobParamValuesMap[TimelimitTypeFlag]
		err := ChangeTaskTimeLimit(FlagTaskIds, FlagTimeLimit)
		if err.Code != util.ErrorSuccess {
			craneError.Message += fmt.Sprintf("change task time limit failed: %s\n", err.Message)
			craneError.Code = util.ErrorGeneric
		}
	}

	if jobParamFlags&(CommentTypeFlag|MailUserTypeFlag|MailTypeTypeFlag) != 0 {
		err := ChangeTaskExtraAttrs(FlagTaskIds, jobParamValuesMap)
		if err.Code != util.ErrorSuccess {
			craneError.Message += fmt.Sprintf("change job ExtraAttrs failed: %s\n", err.Message)
			craneError.Code = util.ErrorGeneric
		}
	}

	return craneError
}

func executeUpdatePartitionCommand(command *CControlCommand) util.CraneError {
	kvParams := command.GetKVMaps()

	if err := checkEmptyKVParams(kvParams, nil); err.Code != util.ErrorSuccess {
		return err
	}

	if FlagPartitionName == "" {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("partition name not specified"))
	}

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "accounts", "allowedaccounts":
			FlagAllowedAccounts = value
			if err := ModifyPartitionAcl(FlagPartitionName, true, FlagAllowedAccounts); err.Code != util.ErrorSuccess {
				return *util.NewCraneErr(util.ErrorGeneric, fmt.Sprintf("%s\n", err.Message))
			}
		case "deniedaccounts":
			FlagDeniedAccounts = value
			if err := ModifyPartitionAcl(FlagPartitionName, false, FlagDeniedAccounts); err.Code != util.ErrorSuccess {
				return *util.NewCraneErr(util.ErrorGeneric, fmt.Sprintf("%s\n", err.Message))
			}
			log.Warning("Hint: When using AllowedAccounts, DeniedAccounts will not take effect.")
		case "partitionname", "partition":
			continue
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown attribute to modify: %s\n", key))
		}
	}

	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeHoldCommand(command *CControlCommand) util.CraneError {
	jobIds := command.GetID()

	timeLimit := command.GetKVParamValue("timelimit")
	if len(timeLimit) == 0 {
		log.Debug("no time limit specified")
	}

	if jobIds == "" {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no job id specified"))
	}

	FlagHoldTime = timeLimit

	err := HoldReleaseJobs(jobIds, true)
	if err.Code != util.ErrorSuccess {
		return *util.NewCraneErr(util.ErrorGeneric, fmt.Sprintf("hold jobs failed: %s\n", err.Message))
	}
	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeReleaseCommand(command *CControlCommand) util.CraneError {
	jobIds := command.GetID()
	if jobIds == "" {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no job id specified"))
	}

	err := HoldReleaseJobs(jobIds, false)
	if err.Code != util.ErrorSuccess {
		return *util.NewCraneErr(util.ErrorGeneric, fmt.Sprintf("release jobs failed: %s\n", err.Message))
	}
	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeCreateCommand(command *CControlCommand) util.CraneError {
	entity := command.GetEntity()
	switch entity {
	case "reservation":
		return executeCreateReservationCommand(command)
	default:
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeCreateReservationCommand(command *CControlCommand) util.CraneError {
	FlagReservationName = command.GetID()
	if len(FlagReservationName) == 0 {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no reservation name specified"))
	}

	kvParams := command.GetKVMaps()

	if err := checkEmptyKVParams(kvParams, []string{"starttime", "duration", "account"}); err.Code != util.ErrorSuccess {
		return err
	}

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
		case "nodecnt":
			nodeNum, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid nodenum value: %s\n", value))
			}
			FlagNodeNum = uint32(nodeNum)
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown attribute to modify: %s\n", key))
		}
	}

	err := CreateReservation()
	if err.Code != util.ErrorSuccess {
		return *util.NewCraneErr(util.ErrorGeneric, err.Message)
	}
	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeDeleteCommand(command *CControlCommand) util.CraneError {
	entity := command.GetEntity()
	switch entity {
	case "reservation":
		return executeDeleteReservationCommand(command)
	default:
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeDeleteReservationCommand(command *CControlCommand) util.CraneError {
	name := command.GetID()
	if len(name) == 0 {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no reservation name specified"))
	}

	err := DeleteReservation(name)
	if err.Code != util.ErrorSuccess {
		return *util.NewCraneErr(util.ErrorGeneric, err.Message)
	}
	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func checkEmptyKVParams(kvParams map[string]string, requiredFields []string) util.CraneError {
	if len(kvParams) == 0 {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no attributes to modify"))
	}

	if len(requiredFields) > 0 {
		missingFields := []string{}
		for _, field := range requiredFields {
			found := false
			for key := range kvParams {
				if strings.ToLower(key) == field {
					found = true
					break
				}
			}
			if !found {
				missingFields = append(missingFields, field)
			}
		}

		if len(missingFields) > 0 {
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("missing required fields: %s\n", strings.Join(missingFields, ", ")))
		}
	}

	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func init() {
	util.InitCraneLogger()
}
