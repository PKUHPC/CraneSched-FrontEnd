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

package cacctmgr

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

var (
	FlagAccount protos.AccountInfo
	FlagUser    protos.UserInfo
	FlagQos     = protos.QosInfo{
		MaxJobsPerUser:      math.MaxUint32,
		MaxCpusPerUser:      math.MaxUint32,
		MaxTimeLimitPerTask: util.MaxJobTimeLimit,
	}

	// FlagPartition and FlagSetPartition are different.
	// FlagPartition limits the operation to a specific partition,
	// while the other is the partition to be added or deleted.
	FlagPartition    string
	FlagSetPartition string

	// FlagSetLevel and FlagLevel are different as
	// they have different default values.
	FlagLevel             string = "none"
	FlagSetLevel          string
	FlagSetDefaultAccount string

	// UserInfo does not have these fields (while AccountInfo does),
	// so we use separate flags for them.
	FlagUserCoordinator bool
	FlagUserDefaultQos  string
	FlagUserPartitions  []string
	FlagUserQosList     []string

	FlagForce          bool
	FlagFull           bool
	FlagJson           bool
	FlagConfigFilePath string = util.DefaultConfigPath

	// These flags are implemented,
	// but not added to any cmd!
	FlagNoHeader bool
	FlagNodeList string
	FlagNumLimit uint32

	FlagEntityName          string
	FlagEntityAccount       string
	FlagEntityPartitions    string
	FlagDefaultQos          string
	FlagAllowedQosList      string
	FlagAllowedPartitions   string
	FlagDeleteQosList       string
	FlagDeletePartitionList string
	FlagSetQosList          string
	FlagSetPartitionList    string
	FlagPartitions          string
	FlagQosList             string
	FlagMaxCpu              string
	FlagMaxJob              string
	FlagMaxTimeLimit        string
	FlagPriority            string
	FlagAdminLevel          string
	FlagDescription         string
	FlagFormat              string
)

var actionToExecute = map[string]func(command *CAcctMgrCommand) util.CraneError{
	"add":     executeAddCommand,
	"delete":  executeDeleteCommand,
	"block":   executeBlockCommand,
	"unblock": executeUnblockCommand,
	"modify":  executeModifyCommand,
	"show":    executeShowCommand,
	"reset":   executeResetCommand,
}

func validateUintValue(value string, fieldName string, bitSize int) util.CraneError {
	_, err := strconv.ParseUint(value, 10, bitSize)
	if err != nil {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("invalid argument %s for %s flag: %v\n", value, fieldName, err))
	}
	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func ParseCmdArgs(args []string) {
	commandArgs := preParseGlobalFlags(args[1:])

	if len(commandArgs) == 0 {
		showHelp()
		os.Exit(0)
	}
	cmdStr := getCmdStringByArgs(commandArgs)
	command, err := ParseCAcctMgrCommand(cmdStr)

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

func executeCommand(command *CAcctMgrCommand) util.CraneError {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	userUid = uint32(os.Getuid())

	action := command.GetAction()
	executeAction, exists := actionToExecute[action]
	if exists {
		return executeAction(command)
	} else {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown operation type: %s", action))
	}
}

func executeAddCommand(command *CAcctMgrCommand) util.CraneError {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeAddAccountCommand(command)
	case "user":
		return executeAddUserCommand(command)
	case "qos":
		return executeAddQosCommand(command)
	default:
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeAddAccountCommand(command *CAcctMgrCommand) util.CraneError {
	// Reset FlagAccount to default values
	FlagAccount = protos.AccountInfo{}
	FlagAccount.Name = command.GetID()

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "name":
			FlagAccount.Name = value
		case "description":
			FlagAccount.Description = value
		case "parent":
			FlagAccount.ParentAccount = value
		case "defaultqos":
			FlagAccount.DefaultQos = value
		case "partition":
			FlagAccount.AllowedPartitions = strings.Split(value, ",")
		case "qoslist":
			FlagAccount.AllowedQosList = strings.Split(value, ",")
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return AddAccount(&FlagAccount)
}

func executeAddUserCommand(command *CAcctMgrCommand) util.CraneError {
	FlagUser = protos.UserInfo{}
	FlagUser.Name = command.GetID()
	FlagUserPartitions = []string{}
	FlagLevel = "none"
	FlagUserCoordinator = false

	KVParams := command.GetKVMaps()

	err := checkEmptyKVParams(KVParams, []string{"account"})
	if err.Code != util.ErrorSuccess {
		return err
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagUser.Account = value
		case "coordinator":
			FlagUserCoordinator = value == "true"
		case "level":
			FlagLevel = value
		case "partition":
			FlagUserPartitions = strings.Split(value, ",")
		case "name":
			FlagUser.Name = value
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return AddUser(&FlagUser, FlagUserPartitions, FlagLevel, FlagUserCoordinator)
}

func executeAddQosCommand(command *CAcctMgrCommand) util.CraneError {
	FlagQos = protos.QosInfo{
		MaxJobsPerUser:      math.MaxUint32,
		MaxCpusPerUser:      math.MaxUint32,
		MaxTimeLimitPerTask: util.MaxJobTimeLimit,
	}
	FlagQos.Name = command.GetID()

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "name":
			FlagQos.Name = value
		case "description":
			FlagQos.Description = value
		case "priority":
			if err := validateUintValue(value, "priority", 32); err.Code != util.ErrorSuccess {
				return err
			}
			priority, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.Priority = uint32(priority)
		case "maxjobsperuser":
			if err := validateUintValue(value, "maxJobsPerUser", 32); err.Code != util.ErrorSuccess {
				return err
			}
			maxJobs, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxJobsPerUser = uint32(maxJobs)
		case "maxcpusperuser":
			if err := validateUintValue(value, "maxCpusPerUser", 32); err.Code != util.ErrorSuccess {
				return err
			}
			maxCpus, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxCpusPerUser = uint32(maxCpus)
		case "maxtimelimitpertask":
			if err := validateUintValue(value, "maxTimeLimitPerTask", 64); err.Code != util.ErrorSuccess {
				return err
			}
			maxTimeLimit, _ := strconv.ParseUint(value, 10, 64)
			FlagQos.MaxTimeLimitPerTask = maxTimeLimit
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}
	return AddQos(&FlagQos)
}

func executeDeleteCommand(command *CAcctMgrCommand) util.CraneError {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeDeleteAccountCommand(command)
	case "user":
		return executeDeleteUserCommand(command)
	case "qos":
		return executeDeleteQosCommand(command)
	default:
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeDeleteAccountCommand(command *CAcctMgrCommand) util.CraneError {
	FlagEntityName = command.GetID()

	if FlagEntityName == "" {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: required entity account not set"))
	}

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		}
	}
	return DeleteAccount(FlagEntityName)
}

func executeDeleteUserCommand(command *CAcctMgrCommand) util.CraneError {
	// Reset related flags
	FlagEntityName = command.GetID()
	FlagEntityAccount = ""

	if FlagEntityName == "" {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: required entity user not set"))
	}

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		case "name":
			FlagEntityName = value
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return DeleteUser(FlagEntityName, FlagEntityAccount)
}

func executeDeleteQosCommand(command *CAcctMgrCommand) util.CraneError {
	// Reset FlagEntityName
	FlagEntityName = command.GetID()

	if FlagEntityName == "" {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: required entity qos not set"))
	}

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		}
	}
	return DeleteQos(FlagEntityName)
}

func executeBlockCommand(command *CAcctMgrCommand) util.CraneError {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeBlockAccountCommand(command)
	case "user":
		return executeBlockUserCommand(command)
	default:
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeBlockAccountCommand(command *CAcctMgrCommand) util.CraneError {
	// Reset related flags
	Name := command.GetID()
	FlagEntityAccount = ""

	if Name == "" {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: required entity account not set"))
	}

	KVParams := command.GetKVMaps()

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return BlockAccountOrUser(Name, protos.EntityType_Account, FlagEntityAccount)
}

func executeBlockUserCommand(command *CAcctMgrCommand) util.CraneError {
	// Reset related flags
	FlagEntityName = command.GetID()
	FlagEntityAccount = ""

	if FlagEntityName == "" {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: required entity user not set"))
	}

	KVParams := command.GetKVMaps()

	err := checkEmptyKVParams(KVParams, []string{"account"})
	if err.Code != util.ErrorSuccess {
		return err
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return BlockAccountOrUser(FlagEntityName, protos.EntityType_User, FlagEntityAccount)
}

func executeUnblockCommand(command *CAcctMgrCommand) util.CraneError {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeUnblockAccountCommand(command)
	case "user":
		return executeUnblockUserCommand(command)
	default:
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeUnblockAccountCommand(command *CAcctMgrCommand) util.CraneError {
	// Reset related flags
	FlagEntityName = command.GetID()
	FlagEntityAccount = ""

	if FlagEntityName == "" {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: required entity account not set"))
	}

	KVParams := command.GetKVMaps()

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return UnblockAccountOrUser(FlagEntityName, protos.EntityType_Account, FlagEntityAccount)
}

func executeUnblockUserCommand(command *CAcctMgrCommand) util.CraneError {
	// Reset related flags
	FlagEntityName = command.GetID()
	FlagEntityAccount = ""

	if FlagEntityName == "" {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: required entity user not set"))
	}

	KVParams := command.GetKVMaps()

	err := checkEmptyKVParams(KVParams, []string{"account"})
	if err.Code != util.ErrorSuccess {
		return err
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown flag: %s\n", key))
		}
	}

	return UnblockAccountOrUser(FlagEntityName, protos.EntityType_User, FlagEntityAccount)
}

func executeModifyCommand(command *CAcctMgrCommand) util.CraneError {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeModifyAccountCommand(command)
	case "user":
		return executeModifyUserCommand(command)
	case "qos":
		return executeModifyQosCommand(command)
	default:
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeModifyAccountCommand(command *CAcctMgrCommand) util.CraneError {
	// Reset related flags
	FlagEntityName = ""
	FlagDescription = ""
	FlagDefaultQos = ""
	FlagSetPartitionList = ""
	FlagSetQosList = ""
	FlagSetDefaultAccount = ""
	FlagAllowedPartitions = ""
	FlagAllowedQosList = ""
	FlagDeletePartitionList = ""
	FlagDeleteQosList = ""

	WhereParams := command.GetWhereParams()
	SetParams, AddParams, DeleteParams := command.GetSetParams()

	if len(WhereParams) == 0 {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: modify account command requires 'where' clause to specify which account to modify"))
	}

	err := checkEmptyKVParams(WhereParams, []string{"name"})
	if err.Code != util.ErrorSuccess {
		return err
	}

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: unknown where parameter '%s' for account modification\n", key))
		}
	}

	if len(SetParams) == 0 && len(AddParams) == 0 && len(DeleteParams) == 0 {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: modify account command requires 'set' clause to specify what to modify"))
	}

	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "description":
			FlagDescription = value
			if err := ModifyAccount(protos.ModifyField_Description, FlagDescription, FlagEntityName, protos.OperationType_Overwrite); err.Code != util.ErrorSuccess {
				return err
			}
		case "defaultqos":
			FlagDefaultQos = value
			if err := ModifyAccount(protos.ModifyField_DefaultQos, FlagDefaultQos, FlagEntityName, protos.OperationType_Overwrite); err.Code != util.ErrorSuccess {
				return err
			}
		case "allowedpartition":
			FlagSetPartitionList = value
			if err := ModifyAccount(protos.ModifyField_Partition, FlagSetPartitionList, FlagEntityName, protos.OperationType_Overwrite); err.Code != util.ErrorSuccess {
				return err
			}
		case "allowedqos":
			FlagSetQosList = value
			if err := ModifyAccount(protos.ModifyField_Qos, FlagSetQosList, FlagEntityName, protos.OperationType_Overwrite); err.Code != util.ErrorSuccess {
				return err
			}
		case "defaultaccount":
			FlagSetDefaultAccount = value
			if err := ModifyAccount(protos.ModifyField_DefaultAccount, FlagSetDefaultAccount, FlagEntityName, protos.OperationType_Overwrite); err.Code != util.ErrorSuccess {
				return err
			}
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: unknown set parameter '%s' for account modification\n", key))
		}
	}

	for key, value := range AddParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagAllowedPartitions = value
			if err := ModifyAccount(protos.ModifyField_Partition, FlagAllowedPartitions, FlagEntityName, protos.OperationType_Add); err.Code != util.ErrorSuccess {
				return err
			}
		case "allowedqos":
			FlagAllowedQosList = value
			if err := ModifyAccount(protos.ModifyField_Qos, FlagAllowedQosList, FlagEntityName, protos.OperationType_Add); err.Code != util.ErrorSuccess {
				return err
			}
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: unknown add parameter '%s' for account modification\n", key))
		}
	}

	for key, value := range DeleteParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagDeletePartitionList = value
			if err := ModifyAccount(protos.ModifyField_Partition, FlagDeletePartitionList, FlagEntityName, protos.OperationType_Delete); err.Code != util.ErrorSuccess {
				return err
			}
		case "allowedqos":
			FlagDeleteQosList = value
			if err := ModifyAccount(protos.ModifyField_Qos, FlagDeleteQosList, FlagEntityName, protos.OperationType_Delete); err.Code != util.ErrorSuccess {
				return err
			}
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: unknown delete parameter '%s' for account modification\n", key))
		}
	}

	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeModifyUserCommand(command *CAcctMgrCommand) util.CraneError {
	FlagEntityName = ""
	FlagEntityAccount = ""
	FlagEntityPartitions = ""
	FlagSetPartitionList = ""
	FlagSetQosList = ""
	FlagSetDefaultAccount = ""
	FlagAdminLevel = ""
	FlagAllowedPartitions = ""
	FlagAllowedQosList = ""
	FlagDeletePartitionList = ""
	FlagDeleteQosList = ""

	WhereParams := command.GetWhereParams()
	SetParams, AddParams, DeleteParams := command.GetSetParams()

	if len(WhereParams) == 0 {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: modify user command requires 'where' clause to specify which user to modify"))
	}

	err := checkEmptyKVParams(WhereParams, []string{"name"})
	if err.Code != util.ErrorSuccess {
		return err
	}

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		case "account":
			FlagEntityAccount = value
		case "partition":
			FlagEntityPartitions = value
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: unknown where parameter '%s' for user modification\n", key))
		}
	}

	if len(SetParams) == 0 && len(AddParams) == 0 && len(DeleteParams) == 0 {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: modify user command requires 'set' clause to specify what to modify"))
	}

	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagSetPartitionList = value
			if err := ModifyUser(protos.ModifyField_Partition, FlagSetPartitionList, FlagEntityName, FlagEntityAccount, FlagEntityPartitions, protos.OperationType_Overwrite); err.Code != util.ErrorSuccess {
				return err
			}
		case "allowedqos":
			FlagSetQosList = value
			if err := ModifyUser(protos.ModifyField_Qos, FlagSetQosList, FlagEntityName, FlagEntityAccount, FlagEntityPartitions, protos.OperationType_Overwrite); err.Code != util.ErrorSuccess {
				return err
			}
		case "defaultaccount":
			FlagSetDefaultAccount = value
			if err := ModifyUser(protos.ModifyField_DefaultAccount, FlagSetDefaultAccount, FlagEntityName, FlagEntityAccount, FlagEntityPartitions, protos.OperationType_Overwrite); err.Code != util.ErrorSuccess {
				return err
			}
		case "defaultqos":
			FlagUserDefaultQos = value
			if err := ModifyUser(protos.ModifyField_DefaultQos, FlagUserDefaultQos, FlagEntityName, FlagEntityAccount, FlagEntityPartitions, protos.OperationType_Overwrite); err.Code != util.ErrorSuccess {
				return err
			}
		case "adminlevel":
			FlagAdminLevel = value
			if err := ModifyUser(protos.ModifyField_AdminLevel, FlagAdminLevel, FlagEntityName, FlagEntityAccount, FlagEntityPartitions, protos.OperationType_Overwrite); err.Code != util.ErrorSuccess {
				return err
			}
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: unknown set parameter '%s' for user modification\n", key))
		}
	}

	for key, value := range AddParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagAllowedPartitions = value
			if err := ModifyUser(protos.ModifyField_Partition, FlagAllowedPartitions, FlagEntityName, FlagEntityAccount, FlagEntityPartitions, protos.OperationType_Add); err.Code != util.ErrorSuccess {
				return err
			}
		case "allowedqos":
			FlagAllowedQosList = value
			if err := ModifyUser(protos.ModifyField_Qos, FlagAllowedQosList, FlagEntityName, FlagEntityAccount, FlagEntityPartitions, protos.OperationType_Add); err.Code != util.ErrorSuccess {
				return err
			}
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: unknown add parameter '%s' for user modification\n", key))
		}
	}

	for key, value := range DeleteParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagDeletePartitionList = value
			if err := ModifyUser(protos.ModifyField_Partition, FlagDeletePartitionList, FlagEntityName, FlagEntityAccount, FlagEntityPartitions, protos.OperationType_Delete); err.Code != util.ErrorSuccess {
				return err
			}
		case "allowedqos":
			FlagDeleteQosList = value
			if err := ModifyUser(protos.ModifyField_Qos, FlagDeleteQosList, FlagEntityName, FlagEntityAccount, FlagEntityPartitions, protos.OperationType_Delete); err.Code != util.ErrorSuccess {
				return err
			}
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: unknown delete parameter '%s' for user modification\n", key))
		}
	}

	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeModifyQosCommand(command *CAcctMgrCommand) util.CraneError {
	FlagEntityName = ""
	FlagMaxCpu = ""
	FlagMaxJob = ""
	FlagMaxTimeLimit = ""
	FlagPriority = ""
	FlagDescription = ""

	WhereParams := command.GetWhereParams()
	SetParams, _, _ := command.GetSetParams()

	if len(WhereParams) == 0 {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: modify qos command requires 'where' clause to specify which qos to modify"))
	}

	err := checkEmptyKVParams(WhereParams, []string{"name"})
	if err.Code != util.ErrorSuccess {
		return err
	}

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: unknown where parameter '%s' for qos modification\n", key))
		}
	}

	if len(SetParams) == 0 {
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Error: modify qos command requires 'set' clause to specify what to modify"))
	}

	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "maxcpusperuser":
			if err := validateUintValue(value, "maxCpusPerUser", 32); err.Code != util.ErrorSuccess {
				return err
			}
			FlagMaxCpu = value
			if err := ModifyQos(protos.ModifyField_MaxCpusPerUser, FlagMaxCpu, FlagEntityName); err.Code != util.ErrorSuccess {
				return err
			}
		case "maxjobsperuser":
			if err := validateUintValue(value, "maxJobsPerUser", 32); err.Code != util.ErrorSuccess {
				return err
			}
			FlagMaxJob = value
			if err := ModifyQos(protos.ModifyField_MaxJobsPerUser, FlagMaxJob, FlagEntityName); err.Code != util.ErrorSuccess {
				return err
			}
		case "maxtimelimitpertask":
			if err := validateUintValue(value, "maxTimeLimitPerTask", 64); err.Code != util.ErrorSuccess {
				return err
			}
			FlagMaxTimeLimit = value
			if err := ModifyQos(protos.ModifyField_MaxTimeLimitPerTask, FlagMaxTimeLimit, FlagEntityName); err.Code != util.ErrorSuccess {
				return err
			}
		case "priority":
			if err := validateUintValue(value, "priority", 32); err.Code != util.ErrorSuccess {
				return err
			}
			FlagPriority = value
			if err := ModifyQos(protos.ModifyField_Priority, FlagPriority, FlagEntityName); err.Code != util.ErrorSuccess {
				return err
			}
		case "description":
			FlagDescription = value
			if err := ModifyQos(protos.ModifyField_Description, FlagDescription, FlagEntityName); err.Code != util.ErrorSuccess {
				return err
			}
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: unknown set parameter '%s' for qos modification\n", key))
		}
	}
	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func executeShowCommand(command *CAcctMgrCommand) util.CraneError {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeShowAccountCommand(command)
	case "user":
		return executeShowUserCommand(command)
	case "qos":
		return executeShowQosCommand(command)
	case "transaction":
		return executeShowTxnLogCommand(command)
	default:
		return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("unknown entity type: %s\n", entity))
	}
}

func executeShowAccountCommand(command *CAcctMgrCommand) util.CraneError {
	name := command.GetID()
	if nameParam := command.GetKVParamValue("name"); nameParam != "" {
		name = nameParam
	}
	if name == "" {
		return ShowAccounts()
	}

	return FindAccount(name)
}

func executeShowUserCommand(command *CAcctMgrCommand) util.CraneError {
	name := command.GetID()
	if nameParam := command.GetKVParamValue("name"); nameParam != "" {
		name = nameParam
	}

	account := command.GetKVParamValue("accounts")

	return ShowUser(name, account)
}

func executeShowQosCommand(command *CAcctMgrCommand) util.CraneError {
	name := command.GetID()
	if nameParam := command.GetKVParamValue("name"); nameParam != "" {
		name = nameParam
	}

	return ShowQos(name)
}

// Reset cert
func executeResetCommand(command *CAcctMgrCommand) util.CraneError {
	name := command.GetID()
	if nameParam := command.GetKVParamValue("name"); nameParam != "" {
		name = nameParam
	}

	return ResetUserCredential(name)
}

func executeShowTxnLogCommand(command *CAcctMgrCommand) util.CraneError {
	FlagActor := ""
	FlagTarget := ""
	FlagAction := ""
	FlagInfo := ""
	FlagStartTime := ""

	WhereParams := command.GetWhereParams()

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "actor":
			FlagActor = value
		case "target":
			FlagTarget = value
		case "action":
			FlagAction = value
		case "info":
			FlagInfo = value
		case "starttime":
			FlagStartTime = value
		default:
			return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: unknown where parameter '%s' for show transaction\n", key))
		}
	}

	return ShowTxn(FlagActor, FlagTarget, FlagAction, FlagInfo, FlagStartTime)
}

func checkEmptyKVParams(kvParams map[string]string, requiredFields []string) util.CraneError {
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
			if len(missingFields) == 1 {
				return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: required argument %s not set\n", missingFields[0]))
			} else {
				return *util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Error: required arguments %s not set\n", strings.Join(missingFields, "\", \"")))
			}
		}
	}

	return *util.NewCraneErr(util.ErrorSuccess, "")
}

func init() {
	util.InitCraneLogger()
}
