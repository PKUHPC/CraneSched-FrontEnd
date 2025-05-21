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
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

var (
	FlagAccount protos.AccountInfo
	FlagUser    protos.UserInfo
	FlagQos     protos.QosInfo

	// FlagPartition and FlagSetPartition are different.
	// FlagPartition limits the operation to a specific partition,
	// while the other is the partition to be added or deleted.
	FlagPartition    string
	FlagSetPartition string

	// FlagSetLevel and FlagLevel are different as
	// they have different default values.
	FlagLevel             string
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
	FlagFormat   string
	FlagNodeList string
	FlagNumLimit uint32

	FlagResourceName        string
	FlagResourceAccount     string
	FlagResourcePartitions  string
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
)

func resetFlags() {
	FlagAccount = protos.AccountInfo{}
	FlagUser = protos.UserInfo{}
	FlagQos = protos.QosInfo{}
	FlagPartition = ""
	FlagSetPartition = ""
	FlagLevel = ""
	FlagSetLevel = ""
	FlagSetDefaultAccount = ""
	FlagUserCoordinator = false
	FlagUserDefaultQos = ""
	FlagUserPartitions = []string{}
	FlagUserQosList = []string{}
	FlagForce = false
	FlagFull = false
	FlagJson = false
	FlagConfigFilePath = util.DefaultConfigPath
	FlagNoHeader = false
	FlagFormat = ""
	FlagNodeList = ""
	FlagNumLimit = 0
	FlagResourceName = ""
	FlagResourceAccount = ""
	FlagResourcePartitions = ""
	FlagDefaultQos = ""
	FlagAllowedQosList = ""
	FlagAllowedPartitions = ""
	FlagDeleteQosList = ""
	FlagDeletePartitionList = ""
	FlagSetQosList = ""
	FlagSetPartitionList = ""
	FlagPartitions = ""
	FlagQosList = ""
	FlagMaxCpu = ""
	FlagMaxJob = ""
	FlagMaxTimeLimit = ""
	FlagPriority = ""
	FlagAdminLevel = ""
	FlagDescription = ""
}

func ParseCmdArgs(args []string) {
	resetFlags()
	cmdStr := strings.Join(args[1:], " ")
	command, err := ParseCAcctMgrCommand(cmdStr)
	if err != nil {
		log.Error("error: command format is incorrect")
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

func executeCommand(command *CAcctMgrCommand) int {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	userUid = uint32(os.Getuid())

	action := command.GetAction()

	switch action {
	case "add":
		return executeAddCommand(command)
	case "delete":
		return executeDeleteCommand(command)
	case "block":
		return executeBlockCommand(command)
	case "unblock":
		return executeUnblockCommand(command)
	case "modify":
		return executeModifyCommand(command)
	case "show":
		return executeShowCommand(command)
	default:
		log.Debugf("unknown operation type: %s", action)
		return util.ErrorCmdArg
	}
}

func executeAddCommand(command *CAcctMgrCommand) int {
	resource := command.GetResource()

	switch resource {
	case "account":
		return executeAddAccountCommand(command)
	case "user":
		return executeAddUserCommand(command)
	case "qos":
		return executeAddQosCommand(command)
	default:
		log.Debugf("unknown resource type: %s", resource)
		return util.ErrorCmdArg
	}
}

func executeAddAccountCommand(command *CAcctMgrCommand) int {
	FlagAccount.Name = command.GetID()
	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch key {
		case "names":
			FlagAccount.Name += value
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
			log.Debugf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return AddAccount(&FlagAccount)
}

func executeAddUserCommand(command *CAcctMgrCommand) int {
	FlagUser.Name = command.GetID()
	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch key {
		case "account":
			FlagUser.Account = value
		case "coordinator":
			FlagUserCoordinator = value == "true"
		case "level":
			FlagLevel = value
		case "partition":
			FlagUserPartitions = strings.Split(value, ",")
		case "names":
			FlagUser.Name += value
		default:
			log.Debugf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return AddUser(&FlagUser, FlagUserPartitions, FlagLevel, FlagUserCoordinator)
}

func executeAddQosCommand(command *CAcctMgrCommand) int {
	FlagQos.Name = command.GetID()

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch key {
		case "names":
			FlagQos.Name += value
		case "description":
			FlagQos.Description = value
		case "priority":
			priority, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				log.Debugf("invalid priority value: %s", value)
				return util.ErrorCmdArg
			}
			FlagQos.Priority = uint32(priority)
		case "maxjobsperuser":
			maxJobs, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				log.Debugf("invalid max-jobs-per-user value: %s", value)
				return util.ErrorCmdArg
			}
			FlagQos.MaxJobsPerUser = uint32(maxJobs)
		case "maxcpusperuser":
			maxCpus, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				log.Debugf("invalid max-cpus-per-user value: %s", value)
				return util.ErrorCmdArg
			}
			FlagQos.MaxCpusPerUser = uint32(maxCpus)
		case "maxtimelimitpertask":
			maxTimeLimit, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				log.Debugf("invalid max-time-limit-per-task value: %s", value)
				return util.ErrorCmdArg
			}
			FlagQos.MaxTimeLimitPerTask = maxTimeLimit
		default:
			log.Debugf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}
	return AddQos(&FlagQos)
}

func executeDeleteCommand(command *CAcctMgrCommand) int {
	resource := command.GetResource()

	switch resource {
	case "account":
		return executeDeleteAccountCommand(command)
	case "user":
		return executeDeleteUserCommand(command)
	case "qos":
		return executeDeleteQosCommand(command)
	default:
		log.Debugf("unknown resource type: %s", resource)
		return util.ErrorCmdArg
	}
}

func executeDeleteAccountCommand(command *CAcctMgrCommand) int {
	FlagResourceName = command.GetID()

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch key {
		case "names":
			FlagResourceName += value
		}
	}
	return DeleteAccount(FlagResourceName)
}

func executeDeleteUserCommand(command *CAcctMgrCommand) int {
	FlagResourceName = command.GetID()

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch key {
		case "account":
			FlagResourceAccount = value
		case "names":
			FlagResourceName += value
		default:
			log.Debugf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return DeleteUser(FlagResourceName, FlagResourceAccount)
}

func executeDeleteQosCommand(command *CAcctMgrCommand) int {
	FlagResourceName = command.GetID()

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch key {
		case "names":
			FlagResourceName += value
		}
	}
	return DeleteQos(FlagResourceName)
}

func executeBlockCommand(command *CAcctMgrCommand) int {
	resource := command.GetResource()

	switch resource {
	case "account":
		return executeBlockAccountCommand(command)
	case "user":
		return executeBlockUserCommand(command)
	default:
		log.Debugf("unknown resource type: %s", resource)
		return util.ErrorCmdArg
	}
}

func executeBlockAccountCommand(command *CAcctMgrCommand) int {
	Name := command.GetID()

	KVParams := command.GetKVMaps()

	for key, value := range KVParams {
		switch key {
		case "account":
			FlagResourceAccount = value
		default:
			log.Debugf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return BlockAccountOrUser(Name, protos.EntityType_Account, FlagResourceAccount)
}

func executeBlockUserCommand(command *CAcctMgrCommand) int {
	FlagResourceName := command.GetID()

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch key {
		case "account":
			FlagResourceAccount = value
		default:
			log.Debugf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return BlockAccountOrUser(FlagResourceName, protos.EntityType_User, FlagResourceAccount)
}

func executeUnblockCommand(command *CAcctMgrCommand) int {
	resource := command.GetResource()

	switch resource {
	case "account":
		return executeUnblockAccountCommand(command)
	case "user":
		return executeUnblockUserCommand(command)
	default:
		log.Debugf("unknown resource type: %s", resource)
		return util.ErrorCmdArg
	}
}

func executeUnblockAccountCommand(command *CAcctMgrCommand) int {
	FlagResourceName := command.GetID()

	KVParams := command.GetKVMaps()

	for key, value := range KVParams {
		switch key {
		case "account":
			FlagResourceAccount = value
		default:
			log.Debugf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return UnblockAccountOrUser(FlagResourceName, protos.EntityType_Account, FlagResourceAccount)
}

func executeUnblockUserCommand(command *CAcctMgrCommand) int {
	FlagResourceName := command.GetID()

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch key {
		case "account":
			FlagResourceAccount = value
		default:
			log.Debugf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return UnblockAccountOrUser(FlagResourceName, protos.EntityType_User, FlagResourceAccount)
}

func executeModifyCommand(command *CAcctMgrCommand) int {
	resource := command.GetResource()

	switch resource {
	case "account":
		return executeModifyAccountCommand(command)
	case "user":
		return executeModifyUserCommand(command)
	case "qos":
		return executeModifyQosCommand(command)
	default:
		return util.ErrorCmdArg
	}
}

func executeModifyAccountCommand(command *CAcctMgrCommand) int {
	WhereParams := command.GetWhereParams()
	SetParams := command.GetSetParams()

	for key, value := range WhereParams {
		switch key {
		case "name":
			FlagResourceName = value
		default:
			return util.ErrorCmdArg
		}
	}

	for key, value := range SetParams {
		switch key {
		case "defaultqos":
			FlagDefaultQos = value
			ModifyAccount(protos.ModifyField_DefaultQos, FlagDefaultQos, FlagResourceName, protos.OperationType_Add)
		case "addallowedpartition":
			FlagAllowedPartitions = value
			ModifyAccount(protos.ModifyField_Partition, FlagAllowedPartitions, FlagResourceName, protos.OperationType_Add)
		case "addallowedqos":
			FlagAllowedQosList = value
			ModifyAccount(protos.ModifyField_Qos, FlagAllowedQosList, FlagResourceName, protos.OperationType_Add)
		case "deleteallowedpartition":
			FlagDeletePartitionList = value
			ModifyAccount(protos.ModifyField_Partition, FlagDeletePartitionList, FlagResourceName, protos.OperationType_Delete)
		case "deleteallowedqos":
			FlagDeleteQosList = value
			ModifyAccount(protos.ModifyField_Qos, FlagDeleteQosList, FlagResourceName, protos.OperationType_Delete)
		case "allowedpartition":
			FlagSetPartitionList = value
			ModifyAccount(protos.ModifyField_Partition, FlagSetPartitionList, FlagResourceName, protos.OperationType_Add)
		case "allowedqos":
			FlagSetQosList = value
			ModifyAccount(protos.ModifyField_Qos, FlagSetQosList, FlagResourceName, protos.OperationType_Add)
		case "defaultaccount":
			FlagSetDefaultAccount = value
			ModifyAccount(protos.ModifyField_DefaultAccount, FlagSetDefaultAccount, FlagResourceName, protos.OperationType_Add)
		default:
			return util.ErrorCmdArg
		}
	}
	return util.ErrorSuccess
}

func executeModifyUserCommand(command *CAcctMgrCommand) int {
	WhereParams := command.GetWhereParams()
	SetParams := command.GetSetParams()

	for key, value := range WhereParams {
		switch key {
		case "name":
			FlagResourceName = value
		case "account":
			FlagResourceAccount = value
		case "partition":
			FlagResourcePartitions = value
		default:
			return util.ErrorCmdArg
		}
	}

	for key, value := range SetParams {
		switch key {
		case "addallowedpartition":
			FlagAllowedPartitions = value
			ModifyUser(protos.ModifyField_Partition, FlagAllowedPartitions, FlagResourceName, FlagResourceAccount, FlagResourcePartitions, protos.OperationType_Add)
		case "addallowedqos":
			FlagAllowedQosList = value
			ModifyUser(protos.ModifyField_Qos, FlagAllowedQosList, FlagResourceName, FlagResourceAccount, FlagResourcePartitions, protos.OperationType_Add)
		case "deleteallowedpartition":
			FlagDeletePartitionList = value
			ModifyUser(protos.ModifyField_Partition, FlagDeletePartitionList, FlagResourceName, FlagResourceAccount, FlagResourcePartitions, protos.OperationType_Delete)
		case "deleteallowedqos":
			FlagDeleteQosList = value
			ModifyUser(protos.ModifyField_Qos, FlagDeleteQosList, FlagResourceName, FlagResourceAccount, FlagResourcePartitions, protos.OperationType_Delete)
		case "allowedpartition":
			FlagSetPartitionList = value
			ModifyUser(protos.ModifyField_Partition, FlagSetPartitionList, FlagResourceName, FlagResourceAccount, FlagResourcePartitions, protos.OperationType_Add)
		case "allowedqos":
			FlagSetQosList = value
			ModifyUser(protos.ModifyField_Qos, FlagSetQosList, FlagResourceName, FlagResourceAccount, FlagResourcePartitions, protos.OperationType_Add)
		case "defaultaccount":
			FlagSetDefaultAccount = value
			ModifyUser(protos.ModifyField_DefaultAccount, FlagSetDefaultAccount, FlagResourceName, FlagResourceAccount, FlagResourcePartitions, protos.OperationType_Add)
		case "adminlevel":
			FlagAdminLevel = value
			ModifyUser(protos.ModifyField_AdminLevel, FlagAdminLevel, FlagResourceName, FlagResourceAccount, FlagResourcePartitions, protos.OperationType_Add)
		default:
			return util.ErrorCmdArg
		}
	}
	return util.ErrorSuccess
}

func executeModifyQosCommand(command *CAcctMgrCommand) int {
	WhereParams := command.GetWhereParams()
	SetParams := command.GetSetParams()

	for key, value := range WhereParams {
		switch key {
		case "name":
			FlagResourceName = value
		default:
			return util.ErrorCmdArg
		}
	}

	for key, value := range SetParams {
		switch key {
		case "maxcpuperuser":
			FlagMaxCpu = value
			ModifyQos(protos.ModifyField_MaxCpusPerUser, FlagMaxCpu, FlagResourceName)
		case "maxsubmitjobsperuser":
			FlagMaxJob = value
			ModifyQos(protos.ModifyField_MaxJobsPerUser, FlagMaxJob, FlagResourceName)
		case "maxtimelimitpertask":
			FlagMaxTimeLimit = value
			ModifyQos(protos.ModifyField_MaxTimeLimitPerTask, FlagMaxTimeLimit, FlagResourceName)
		case "priority":
			FlagPriority = value
			ModifyQos(protos.ModifyField_Priority, FlagPriority, FlagResourceName)
		case "description":
			FlagDescription = value
			ModifyQos(protos.ModifyField_Description, FlagDescription, FlagResourceName)
		default:
			return util.ErrorCmdArg
		}
	}
	return util.ErrorSuccess
}

func executeShowCommand(command *CAcctMgrCommand) int {
	resource := command.GetResource()

	switch resource {
	case "account":
		return executeShowAccountCommand(command)
	case "user":
		return executeShowUserCommand(command)
	case "qos":
		return executeShowQosCommand(command)
	default:
		log.Debugf("unknown resource type: %s", resource)
		return util.ErrorCmdArg
	}
}

func executeShowAccountCommand(command *CAcctMgrCommand) int {
	FlagResourceName := command.GetID()
	FlagResourceName += command.GetKVParamValue("names")

	if FlagResourceName == "" {
		return ShowAccounts()
	}

	return FindAccount(FlagResourceName)
}

func executeShowUserCommand(command *CAcctMgrCommand) int {
	FlagResourceName := command.GetID()
	FlagResourceName += command.GetKVParamValue("names")
	account := command.GetKVParamValue("accounts")

	return ShowUser(FlagResourceName, account)
}

func executeShowQosCommand(command *CAcctMgrCommand) int {
	FlagResourceName := command.GetID()
	FlagResourceName += command.GetKVParamValue("names")

	return ShowQos(FlagResourceName)
}
