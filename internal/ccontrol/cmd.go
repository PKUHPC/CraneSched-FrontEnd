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
	FlagNodeNum         uint32
	FlagDeadlineTime    string
)

func ParseCmdArgs(args []string) {
	commandArgs := preParseGlobalFlags(args[1:])

	if len(commandArgs) == 0 {
		showHelp()
		os.Exit(0)
	}

	var processedArgs []string
	for _, arg := range commandArgs {
		if arg == "" {
			processedArgs = append(processedArgs, "\"\"")
			continue
		}

		if strings.Contains(arg, "=") {
			parts := strings.SplitN(arg, "=", 2)
			key := parts[0]
			value := parts[1]

			if value == "" {
				processedArgs = append(processedArgs, key+"=\"\"")
				continue
			}

			if (strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) ||
				(strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) {
				processedArgs = append(processedArgs, arg)
			} else if strings.Contains(value, " ") {
				processedArgs = append(processedArgs, key+"="+strconv.Quote(value))
			} else {
				processedArgs = append(processedArgs, arg)
			}
		} else if strings.Contains(arg, " ") && !strings.HasPrefix(arg, "'") && !strings.HasPrefix(arg, "\"") {
			processedArgs = append(processedArgs, strconv.Quote(arg))
		} else {
			processedArgs = append(processedArgs, arg)
		}
	}
	cmdStr := strings.Join(processedArgs, " ")
	command, err := ParseCControlCommand(cmdStr)

	if err != nil {
		log.Errorf("Error: command format is incorrect %v", err)
		os.Exit(util.ErrorCmdArg)
	}

	result := executeCommand(command)
	if result != util.ErrorSuccess {
		log.Errorf("Command execution failed")
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
		name = ""
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

	err := checkEmptyKVParams(kvParams, []string{"state", "reason"})
	if err != util.ErrorSuccess {
		return err
	}

	if FlagNodeName == "" {
		log.Debug("node name not specified")
		return util.ErrorCmdArg
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
			log.Errorf("unknown attribute to modify: %s", key)
			return util.ErrorCmdArg
		}
	}
	error := ChangeNodeState(FlagNodeName, FlagState, FlagReason)
	if error != nil {
		log.Errorf("change node state failed: %s", error)
		return util.ErrorGeneric
	}
	return util.ErrorSuccess
}

func executeUpdateJobCommand(command *CControlCommand) int {
	kvParams := command.GetKVMaps()

	var lastErr int = util.ErrorSuccess
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
				log.Errorf("Invalid mailtype value to modify: %s", value)
				return util.ErrorCmdArg
			}
			jobParamFlags |= MailTypeTypeFlag
			jobParamValuesMap[MailTypeTypeFlag] = value
		case "deadline":
			jobParamFlags |= DeadlineTypeFlag
			jobParamValuesMap[DeadlineTypeFlag] = value
		case "jobid", "job":
			continue
		default:
			log.Errorf("unknown attribute to modify: %s", key)
			return util.ErrorCmdArg
		}
	}

	if jobParamFlags&PriorityTypeFlag != 0 {
		value := jobParamValuesMap[PriorityTypeFlag]
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
	}

	if jobParamFlags&TimelimitTypeFlag != 0 {
		FlagTimeLimit = jobParamValuesMap[TimelimitTypeFlag]
		err := ChangeTaskTimeLimit(FlagTaskIds, FlagTimeLimit)
		if err != nil {
			log.Errorf("change task time limit failed: %s", err)
			lastErr = util.ErrorGeneric
		}
	}

	if jobParamFlags&(CommentTypeFlag|MailUserTypeFlag|MailTypeTypeFlag) != 0 {
		err := ChangeTaskExtraAttrs(FlagTaskIds, jobParamValuesMap)
		if err != nil {
			log.Errorf("change job ExtraAttrs failed: %s", err)
			lastErr = util.ErrorGeneric
		}
	}

	if jobParamFlags&DeadlineTypeFlag != 0 {
		FlagDeadlineTime = jobParamValuesMap[DeadlineTypeFlag]
		err := ChangeDeadlineTime(FlagTaskIds, FlagDeadlineTime)
		if err != nil {
			log.Errorf("change task deadline failed: %s", err)
			lastErr = util.ErrorGeneric
		}
	}

	return lastErr
}

func executeUpdatePartitionCommand(command *CControlCommand) int {
	kvParams := command.GetKVMaps()

	err := checkEmptyKVParams(kvParams, nil)
	if err != util.ErrorSuccess {
		return err
	}

	if FlagPartitionName == "" {
		log.Debug("partition name not specified")
		return util.ErrorCmdArg
	}

	for key, value := range kvParams {
		switch strings.ToLower(key) {
		case "accounts", "allowedaccounts":
			FlagAllowedAccounts = value
			err := ModifyPartitionAcl(FlagPartitionName, true, FlagAllowedAccounts)
			if err != nil {
				log.Errorf("%s", err)
				return util.ErrorGeneric
			}
		case "deniedaccounts":
			FlagDeniedAccounts = value
			if err := ModifyPartitionAcl(FlagPartitionName, false, FlagDeniedAccounts); err != nil {
				log.Errorf("%s", err)
				return util.ErrorGeneric
			}
			log.Warning("Hint: When using AllowedAccounts, DeniedAccounts will not take effect.")
		case "partitionname", "partition":
			continue
		default:
			log.Errorf("unknown attribute to modify: %s", key)
			return util.ErrorCmdArg
		}
	}

	return util.ErrorSuccess
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

	err := checkEmptyKVParams(kvParams, []string{"starttime", "duration", "account"})
	if err != util.ErrorSuccess {
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
				log.Errorf("invalid nodenum value: %s", value)
				return util.ErrorCmdArg
			}
			FlagNodeNum = uint32(nodeNum)
		default:
			log.Errorf("unknown attribute to modify: %s", key)
			return util.ErrorCmdArg
		}
	}

	error := CreateReservation()
	if error != nil {
		log.Errorf("%s", error)
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
		log.Errorf("%s", err)
		return util.ErrorGeneric
	}
	return util.ErrorSuccess
}

func checkEmptyKVParams(kvParams map[string]string, requiredFields []string) int {
	if len(kvParams) == 0 {
		log.Debug("no attributes to modify")
		return util.ErrorCmdArg
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
			log.Errorf("missing required fields: %s", strings.Join(missingFields, ", "))
			return util.ErrorCmdArg
		}
	}

	return util.ErrorSuccess
}
