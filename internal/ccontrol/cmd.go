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
	"errors"
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

var actionToExecute = map[string]func(command *CControlCommand) error{
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
	if result != nil {
		var craneError *util.CraneError
		errors.As(result, &craneError)
		if craneError.Message != "" {
			log.Error(craneError.Message)
		}
		os.Exit(craneError.Code)
	} else {
		os.Exit(util.ErrorSuccess)
	}
}

func executeCommand(command *CControlCommand) error {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	userUid = uint32(os.Getuid())

	action := command.GetAction()
	executeAction, exists := actionToExecute[action]
	if exists {
		return executeAction(command)
	} else {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown operation type: %s\n", action))
	}
}

func executeShowCommand(command *CControlCommand) error {
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
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeShowNodeCommand(command *CControlCommand) error {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
		name = ""
	}
	err := ShowNodes(name, FlagQueryAll)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "show nodes failed: %s\n", err)
	}
	return util.NewCraneErr(util.ErrorSuccess, "")
}

func executeShowPartitionCommand(command *CControlCommand) error {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
	}
	err := ShowPartitions(name, FlagQueryAll)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "show partitions failed: %s\n", err)
	}
	return util.NewCraneErr(util.ErrorSuccess, "")
}

func executeShowJobCommand(command *CControlCommand) error {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
	}

	err := ShowJobs(name, FlagQueryAll)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "show job failed: %s\n", err)
	}
	return util.NewCraneErr(util.ErrorSuccess, "")
}

func executeShowReservationCommand(command *CControlCommand) error {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
		name = ""
	}
	err := ShowReservations(name, FlagQueryAll)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "show reservations failed: %s", err)
	}
	return util.NewCraneErr(util.ErrorSuccess, "")
}

func executeShowLicenseCommand(command *CControlCommand) error {
	name := command.GetID()
	if len(name) == 0 {
		FlagQueryAll = true
		name = ""
	}
	err := ShowLicenses(name, FlagQueryAll)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "show licenses failed: %s", err)
	}
	return util.NewCraneErr(util.ErrorSuccess, "")
}

func executeUpdateCommand(command *CControlCommand) error {
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
	return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("unknown attribute to modify"))
}

func executeUpdateNodeCommand(command *CControlCommand) error {
	kvParams := command.GetKVMaps()

	if err := checkEmptyKVParams(kvParams, []string{"state", "reason"}); err != nil {
		return err
	}

	if FlagNodeName == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("node name not specified"))
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
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown attribute to modify: %s\n", key))
		}
	}
	err := ChangeNodeState(FlagNodeName, FlagState, FlagReason)
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "change node state failed: %s\n", err)
	}
	return util.NewCraneErr(util.ErrorSuccess, "")
}

func executeUpdateJobCommand(command *CControlCommand) error {
	kvParams := command.GetKVMaps()

	var craneError = util.NewCraneErr(util.ErrorSuccess, "")

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
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid mailtype value to modify: %s\n", value))
			}
			jobParamFlags |= MailTypeTypeFlag
			jobParamValuesMap[MailTypeTypeFlag] = value
		case "jobid", "job":
			continue
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown attribute to modify: %s\n", key))
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
		if result != nil {
			craneError.Message += fmt.Sprintf("change task priority failed: %s\n", result.Error())
			craneError.Code = util.ErrorGeneric
		}
	}

	if jobParamFlags&TimelimitTypeFlag != 0 {
		FlagTimeLimit = jobParamValuesMap[TimelimitTypeFlag]
		err := ChangeTaskTimeLimit(FlagTaskIds, FlagTimeLimit)
		if err != nil {
			craneError.Message += fmt.Sprintf("change task time limit failed: %s\n", err.Error())
			craneError.Code = util.ErrorGeneric
		}
	}

	if jobParamFlags&(CommentTypeFlag|MailUserTypeFlag|MailTypeTypeFlag) != 0 {
		err := ChangeTaskExtraAttrs(FlagTaskIds, jobParamValuesMap)
		if err != nil {
			craneError.Message += fmt.Sprintf("change job ExtraAttrs failed: %s\n", err.Error())
			craneError.Code = util.ErrorGeneric
		}
	}

	return craneError
}

func executeUpdatePartitionCommand(command *CControlCommand) error {
	kvParams := command.GetKVMaps()

	if err := checkEmptyKVParams(kvParams, nil); err != nil {
		return err
	}

	if FlagPartitionName == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("partition name not specified"))
	}

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "accounts", "allowedaccounts":
			FlagAllowedAccounts = value
			if err := ModifyPartitionAcl(FlagPartitionName, true, FlagAllowedAccounts); err != nil {
				return util.WrapCraneErr(util.ErrorGeneric, "%s\n", err)
			}
		case "deniedaccounts":
			FlagDeniedAccounts = value
			if err := ModifyPartitionAcl(FlagPartitionName, false, FlagDeniedAccounts); err != nil {
				return util.WrapCraneErr(util.ErrorGeneric, "%s\n", err)
			}
			log.Warning("Hint: When using AllowedAccounts, DeniedAccounts will not take effect.")
		case "partitionname", "partition":
			continue
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown attribute to modify: %s\n", key))
		}
	}

	return util.NewCraneErr(util.ErrorSuccess, "")
}

func executeHoldCommand(command *CControlCommand) error {
	jobIds := command.GetID()

	timeLimit := command.GetKVParamValue("timelimit")
	if len(timeLimit) == 0 {
		log.Debug("no time limit specified")
	}

	if jobIds == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no job id specified"))
	}

	FlagHoldTime = timeLimit

	err := HoldReleaseJobs(jobIds, true)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "hold jobs failed: %s\n", err)
	}
	return util.NewCraneErr(util.ErrorSuccess, "")
}

func executeReleaseCommand(command *CControlCommand) error {
	jobIds := command.GetID()
	if jobIds == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no job id specified"))
	}

	err := HoldReleaseJobs(jobIds, false)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "release jobs failed: %s\n", err)
	}
	return util.NewCraneErr(util.ErrorSuccess, "")
}

func executeCreateCommand(command *CControlCommand) error {
	entity := command.GetEntity()
	switch entity {
	case "reservation":
		return executeCreateReservationCommand(command)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeCreateReservationCommand(command *CControlCommand) error {
	FlagReservationName = command.GetID()
	if len(FlagReservationName) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no reservation name specified"))
	}

	kvParams := command.GetKVMaps()

	if err := checkEmptyKVParams(kvParams, []string{"starttime", "duration", "account"}); err != nil {
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
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid nodenum value: %s\n", value))
			}
			FlagNodeNum = uint32(nodeNum)
		default:
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown attribute to modify: %s\n", key))
		}
	}

	err := CreateReservation()
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "%s\n", err)
	}
	return util.NewCraneErr(util.ErrorSuccess, "")
}

func executeDeleteCommand(command *CControlCommand) error {
	entity := command.GetEntity()
	switch entity {
	case "reservation":
		return executeDeleteReservationCommand(command)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeDeleteReservationCommand(command *CControlCommand) error {
	name := command.GetID()
	if len(name) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no reservation name specified"))
	}

	err := DeleteReservation(name)
	if err != nil {
		return util.WrapCraneErr(util.ErrorGeneric, "%s\n", err)
	}
	return util.NewCraneErr(util.ErrorSuccess, "")
}

func checkEmptyKVParams(kvParams map[string]string, requiredFields []string) error {
	if len(kvParams) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("no attributes to modify"))
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
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("missing required fields: %s\n", strings.Join(missingFields, ", ")))
		}
	}

	return nil
}

func init() {
	util.InitCraneLogger()
}
