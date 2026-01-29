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
	FlagWckey protos.WckeyInfo

	FlagResourceName string
	FlagServerName   string
	FlagClusters     string

	FlagQosFlags uint32

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
	FlagMaxTimeLimit        string
	FlagPriority            string
	FlagAdminLevel          string
	FlagDescription         string
	FlagFormat              string
)

var actionToExecute = map[string]func(command *CAcctMgrCommand) int{
	"add":     executeAddCommand,
	"delete":  executeDeleteCommand,
	"block":   executeBlockCommand,
	"unblock": executeUnblockCommand,
	"modify":  executeModifyCommand,
	"show":    executeShowCommand,
	"reset":   executeResetCommand,
	"update":  executeModifyCommand,
}

type ModifyParam struct {
	ModifyField protos.ModifyField
	NewValue    string
	RequestType protos.OperationType
}

func validateUintValue(value string, fieldName string, bitSize int) error {
	_, err := strconv.ParseUint(value, 10, bitSize)
	if err != nil {
		log.Errorf("invalid argument %s for %s flag: %v", value, fieldName, err)
		return err
	}
	return nil
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
	os.Exit(result)
}

func executeCommand(command *CAcctMgrCommand) int {
	config = util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	userUid = uint32(os.Getuid())

	action := command.GetAction()
	executeAction, exists := actionToExecute[action]
	if exists {
		return executeAction(command)
	} else {
		log.Errorf("unknown operation type: %s", action)
		return util.ErrorCmdArg
	}
}

func executeAddCommand(command *CAcctMgrCommand) int {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeAddAccountCommand(command)
	case "user":
		return executeAddUserCommand(command)
	case "qos":
		return executeAddQosCommand(command)
	case "wckey":
		return executeAddWckeyCommand(command)
	case "resource":
		return executeAddResourceCommand(command)
	default:
		log.Errorf("unknown entity type: %s", entity)
		return util.ErrorCmdArg
	}
}

func executeAddAccountCommand(command *CAcctMgrCommand) int {
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
			log.Errorf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return AddAccount(&FlagAccount)
}

func executeAddUserCommand(command *CAcctMgrCommand) int {
	FlagUser = protos.UserInfo{}
	FlagUser.Name = command.GetID()
	FlagUserPartitions = []string{}
	FlagLevel = "none"
	FlagUserCoordinator = false

	KVParams := command.GetKVMaps()

	err := checkEmptyKVParams(KVParams, []string{"account"})
	if err != util.ErrorSuccess {
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
			log.Errorf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return AddUser(&FlagUser, FlagUserPartitions, FlagLevel, FlagUserCoordinator)
}

func executeAddQosCommand(command *CAcctMgrCommand) int {
	FlagQos = protos.QosInfo{
		MaxJobsPerUser:          math.MaxUint32,
		MaxCpusPerUser:          math.MaxUint32,
		MaxSubmitJobsPerUser:    math.MaxUint32,
		MaxSubmitJobsPerAccount: math.MaxUint32,
		MaxJobsPerAccount:       math.MaxUint32,
		MaxTresPerUser:          util.ParseTres(""),
		MaxTresPerAccount:       util.ParseTres(""),
		MaxJobs:                 math.MaxUint32,
		MaxSubmitJobs:           math.MaxUint32,
		MaxTres:                 util.ParseTres(""),
		MaxWall:                 util.MaxJobTimeLimit,
		MaxTimeLimitPerTask:     util.MaxJobTimeLimit,
		Flags:                   0,
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
			if err := validateUintValue(value, "priority", 32); err != nil {
				return util.ErrorCmdArg
			}
			priority, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.Priority = uint32(priority)
		case "maxjobsperuser":
			if err := validateUintValue(value, "maxJobsPerUser", 32); err != nil {
				return util.ErrorCmdArg
			}
			maxJobs, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxJobsPerUser = uint32(maxJobs)
		case "maxcpusperuser":
			if err := validateUintValue(value, "maxCpusPerUser", 32); err != nil {
				return util.ErrorCmdArg
			}
			maxCpus, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxCpusPerUser = uint32(maxCpus)
		case "maxtimelimitpertask":
			if err := validateUintValue(value, "maxTimeLimitPerTask", 64); err != nil {
				return util.ErrorCmdArg
			}
			maxTimeLimit, _ := strconv.ParseUint(value, 10, 64)
			FlagQos.MaxTimeLimitPerTask = maxTimeLimit
		case "maxsubmitjobsperuser":
			if err := validateUintValue(value, "maxSubmitJobsPerUser", 32); err != nil {
				return util.ErrorCmdArg
			}
			maxSubmitJobsPerUser, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxSubmitJobsPerUser = uint32(maxSubmitJobsPerUser)
		case "maxsubmitjobsperaccount":
			if err := validateUintValue(value, "maxSubmitJobsPerAccount", 32); err != nil {
				return util.ErrorCmdArg
			}
			maxSubmitJobsPerAccount, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxSubmitJobsPerAccount = uint32(maxSubmitJobsPerAccount)
		case "maxjobsperaccount":
			if err := validateUintValue(value, "maxJobsPerAccount", 32); err != nil {
				return util.ErrorCmdArg
			}
			maxJobsPerAccount, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxJobsPerAccount = uint32(maxJobsPerAccount)
		case "maxtresperuser":
			FlagQos.MaxTresPerUser = util.ParseTres(value)
		case "maxtresperaccount":
			FlagQos.MaxTresPerAccount = util.ParseTres(value)
		case "maxtres":
			FlagQos.MaxTres = util.ParseTres(value)
		case "maxjobs":
			if err := validateUintValue(value, "maxjobs", 32); err != nil {
				return util.ErrorCmdArg
			}
			maxJobs, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxJobs = uint32(maxJobs)
		case "maxsubmitjobs":
			if err := validateUintValue(value, "maxsubmitjobs", 32); err != nil {
				return util.ErrorCmdArg
			}
			maxsubmitjobs, _ := strconv.ParseUint(value, 10, 32)
			FlagQos.MaxSubmitJobs = uint32(maxsubmitjobs)
		case "maxwall":
			if err := validateUintValue(value, "maxWall", 64); err != nil {
				return util.ErrorCmdArg
			}
			maxWall, _ := strconv.ParseUint(value, 10, 64)
			FlagQos.MaxWall = maxWall
		case "flags":
			var err error
			if FlagQosFlags, err = util.ParseFlags(value); err != nil {
				log.Errorf("invalid argument %s ,err: %v", value, err)
				return util.ErrorCmdArg
			}
			FlagQos.Flags = FlagQosFlags
		default:
			log.Errorf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}
	return AddQos(&FlagQos)
}

func executeAddWckeyCommand(command *CAcctMgrCommand) int {
	FlagWckey = protos.WckeyInfo{}
	KVParams := command.GetKVMaps()

	FlagWckey.Name = command.GetID()
	if FlagWckey.Name == "" {
		log.Errorf("Error: required entity wckey not set")
		return util.ErrorCmdArg
	}

	err := checkEmptyKVParams(KVParams, []string{"user"})
	if err != util.ErrorSuccess {
		return err
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "user":
			FlagWckey.UserName = value
		default:
			log.Errorf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return AddWckey(&FlagWckey)
}

func executeAddResourceCommand(command *CAcctMgrCommand) int {
	FlagOperators := make(map[protos.LicenseResource_Field]string, 0)

	FlagResourceName = command.GetID()
	KVParams := command.GetKVMaps()

	err := checkEmptyKVParams(KVParams, []string{"server"})
	if err != util.ErrorSuccess {
		return err
	}

	hasCluster := false
	hasAllowed := false

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "name":
			FlagResourceName = value
		case "server":
			FlagServerName = value
		case "count":
			if err := validateUintValue(value, "count", 32); err != nil {
				return util.ErrorCmdArg
			}
			FlagOperators[protos.LicenseResource_Count] = value
			if value == "0" {
				log.Warning("The total count of the license you entered is 0.")
			}
		case "description":
			FlagOperators[protos.LicenseResource_Description] = value
		case "lastconsumed":
			if err := validateUintValue(value, "lastConsumed", 32); err != nil {
				return util.ErrorCmdArg
			}
			FlagOperators[protos.LicenseResource_LastConsumed] = value
		case "servertype":
			FlagOperators[protos.LicenseResource_ServerType] = value
		case "type":
			FlagOperators[protos.LicenseResource_ResourceType] = value
		case "allowed":
			if err := validateUintValue(value, "allowed", 32); err != nil {
				return util.ErrorCmdArg
			}
			FlagOperators[protos.LicenseResource_Allowed] = value
			hasAllowed = true
		case "cluster":
			FlagClusters = value
			hasCluster = true
		case "flags":
			FlagOperators[protos.LicenseResource_Flags] = value
		default:
			log.Errorf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	if hasCluster != hasAllowed {
		log.Errorf("'allowed' and 'cluster' must be specified together")
		return util.ErrorCmdArg
	}

	return AddLicenseResource(FlagResourceName, FlagServerName, FlagClusters, FlagOperators)
}

func executeDeleteCommand(command *CAcctMgrCommand) int {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeDeleteAccountCommand(command)
	case "user":
		return executeDeleteUserCommand(command)
	case "qos":
		return executeDeleteQosCommand(command)
	case "wckey":
		return executeDeleteWckeyCommand(command)
	case "resource":
		return executeDeleteResourceCommand(command)
	default:
		log.Errorf("unknown entity type: %s", entity)
		return util.ErrorCmdArg
	}
}

func executeDeleteAccountCommand(command *CAcctMgrCommand) int {
	FlagEntityName = command.GetID()

	if FlagEntityName == "" {
		log.Errorf("Error: required entity account not set")
		return util.ErrorCmdArg
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

func executeDeleteUserCommand(command *CAcctMgrCommand) int {
	// Reset related flags
	FlagEntityName = command.GetID()
	FlagEntityAccount = ""

	if FlagEntityName == "" {
		log.Errorf("Error: required entity user not set")
		return util.ErrorCmdArg
	}

	KVParams := command.GetKVMaps()
	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		case "name":
			FlagEntityName = value
		default:
			log.Errorf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return DeleteUser(FlagEntityName, FlagEntityAccount)
}

func executeDeleteQosCommand(command *CAcctMgrCommand) int {
	// Reset FlagEntityName
	FlagEntityName = command.GetID()

	if FlagEntityName == "" {
		log.Errorf("Error: required entity qos not set")
		return util.ErrorCmdArg
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

func executeDeleteWckeyCommand(command *CAcctMgrCommand) int {
	FlagWckey = protos.WckeyInfo{}
	KVParams := command.GetKVMaps()

	FlagWckey.Name = command.GetID()
	if FlagWckey.Name == "" {
		log.Errorf("Error: required entity wckey not set")
		return util.ErrorCmdArg
	}

	err := checkEmptyKVParams(KVParams, []string{"user"})
	if err != util.ErrorSuccess {
		return err
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "user":
			FlagWckey.UserName = value
		default:
			log.Errorf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}
	return DeleteWckey(FlagWckey.Name, FlagWckey.UserName)
}

func executeDeleteResourceCommand(command *CAcctMgrCommand) int {
	FlagEntityName = command.GetID()
	FlagServer := ""
	if FlagEntityName == "" {
		log.Errorf("Error: required entity resource not set")
		return util.ErrorCmdArg
	}
	KVParams := command.GetKVMaps()

	err := checkEmptyKVParams(KVParams, []string{"server"})
	if err != util.ErrorSuccess {
		return err
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		case "server":
			FlagServer = value
		case "cluster":
			FlagClusters = value
		}
	}
	return DeleteLicenseResource(FlagEntityName, FlagServer, FlagClusters)
}

func executeBlockCommand(command *CAcctMgrCommand) int {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeBlockAccountCommand(command)
	case "user":
		return executeBlockUserCommand(command)
	default:
		log.Errorf("unknown entity type: %s", entity)
		return util.ErrorCmdArg
	}
}

func executeBlockAccountCommand(command *CAcctMgrCommand) int {
	// Reset related flags
	Name := command.GetID()
	FlagEntityAccount = ""

	if Name == "" {
		log.Errorf("Error: required entity account not set")
		return util.ErrorCmdArg
	}

	KVParams := command.GetKVMaps()

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		default:
			log.Errorf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return BlockAccountOrUser(Name, protos.EntityType_Account, FlagEntityAccount)
}

func executeBlockUserCommand(command *CAcctMgrCommand) int {
	// Reset related flags
	FlagEntityName = command.GetID()
	FlagEntityAccount = ""

	if FlagEntityName == "" {
		log.Errorf("Error: required entity user not set")
		return util.ErrorCmdArg
	}

	KVParams := command.GetKVMaps()

	err := checkEmptyKVParams(KVParams, []string{"account"})
	if err != util.ErrorSuccess {
		return err
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		default:
			log.Errorf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return BlockAccountOrUser(FlagEntityName, protos.EntityType_User, FlagEntityAccount)
}

func executeUnblockCommand(command *CAcctMgrCommand) int {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeUnblockAccountCommand(command)
	case "user":
		return executeUnblockUserCommand(command)
	default:
		log.Errorf("unknown entity type: %s", entity)
		return util.ErrorCmdArg
	}
}

func executeUnblockAccountCommand(command *CAcctMgrCommand) int {
	// Reset related flags
	FlagEntityName = command.GetID()
	FlagEntityAccount = ""

	if FlagEntityName == "" {
		log.Errorf("Error: required entity account not set")
		return util.ErrorCmdArg
	}

	KVParams := command.GetKVMaps()

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		default:
			log.Errorf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return UnblockAccountOrUser(FlagEntityName, protos.EntityType_Account, FlagEntityAccount)
}

func executeUnblockUserCommand(command *CAcctMgrCommand) int {
	// Reset related flags
	FlagEntityName = command.GetID()
	FlagEntityAccount = ""

	if FlagEntityName == "" {
		log.Errorf("Error: required entity user not set")
		return util.ErrorCmdArg
	}

	KVParams := command.GetKVMaps()

	err := checkEmptyKVParams(KVParams, []string{"account"})
	if err != util.ErrorSuccess {
		return err
	}

	for key, value := range KVParams {
		switch strings.ToLower(key) {
		case "account":
			FlagEntityAccount = value
		default:
			log.Errorf("unknown flag: %s", key)
			return util.ErrorCmdArg
		}
	}

	return UnblockAccountOrUser(FlagEntityName, protos.EntityType_User, FlagEntityAccount)
}

func executeModifyCommand(command *CAcctMgrCommand) int {
	entity := command.GetEntity()

	switch entity {
	case "account":
		return executeModifyAccountCommand(command)
	case "user":
		return executeModifyUserCommand(command)
	case "qos":
		return executeModifyQosCommand(command)
	case "wckey":
		return executeModifyWckeyCommand(command)
	case "resource":
		return executeModifyResourceCommand(command)
	default:
		log.Errorf("unknown entity type: %s", entity)
		return util.ErrorCmdArg
	}
}

func executeModifyAccountCommand(command *CAcctMgrCommand) int {
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
		log.Errorf("Error: modify account command requires 'where' clause to specify which account to modify")
		return util.ErrorCmdArg
	}

	err := checkEmptyKVParams(WhereParams, []string{"name"})
	if err != util.ErrorSuccess {
		return err
	}

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		default:
			log.Errorf("Error: unknown where parameter '%s' for account modification", key)
			return util.ErrorCmdArg
		}
	}

	if len(SetParams) == 0 && len(AddParams) == 0 && len(DeleteParams) == 0 {
		log.Errorf("Error: modify account command requires 'set' clause to specify what to modify")
		return util.ErrorCmdArg
	}

	var params []ModifyParam

	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "description":
			FlagDescription = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Description,
				NewValue:    FlagDescription,
				RequestType: protos.OperationType_Overwrite,
			})
		case "defaultqos":
			FlagDefaultQos = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_DefaultQos,
				NewValue:    FlagDefaultQos,
				RequestType: protos.OperationType_Overwrite,
			})
		case "allowedpartition":
			FlagSetPartitionList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Partition,
				NewValue:    FlagSetPartitionList,
				RequestType: protos.OperationType_Overwrite,
			})
		case "allowedqos":
			FlagSetQosList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Qos,
				NewValue:    FlagSetQosList,
				RequestType: protos.OperationType_Overwrite,
			})
		case "defaultaccount":
			FlagSetDefaultAccount = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_DefaultAccount,
				NewValue:    FlagSetDefaultAccount,
				RequestType: protos.OperationType_Overwrite,
			})
		default:
			log.Errorf("Error: unknown set parameter '%s' for account modification", key)
			return util.ErrorCmdArg
		}
	}

	for key, value := range AddParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagAllowedPartitions = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Partition,
				NewValue:    FlagAllowedPartitions,
				RequestType: protos.OperationType_Add,
			})
		case "allowedqos":
			FlagAllowedQosList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Qos,
				NewValue:    FlagAllowedQosList,
				RequestType: protos.OperationType_Add,
			})
		default:
			log.Errorf("Error: unknown add parameter '%s' for account modification", key)
			return util.ErrorCmdArg
		}
	}

	for key, value := range DeleteParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagDeletePartitionList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Partition,
				NewValue:    FlagDeletePartitionList,
				RequestType: protos.OperationType_Delete,
			})
		case "allowedqos":
			FlagDeleteQosList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Qos,
				NewValue:    FlagDeleteQosList,
				RequestType: protos.OperationType_Delete,
			})
		default:
			log.Errorf("Error: unknown delete parameter '%s' for account modification", key)
			return util.ErrorCmdArg
		}
	}

	return ModifyAccount(params, FlagEntityName)
}

func executeModifyUserCommand(command *CAcctMgrCommand) int {
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
		log.Errorf("Error: modify user command requires 'where' clause to specify which user to modify")
		return util.ErrorCmdArg
	}

	err := checkEmptyKVParams(WhereParams, []string{"name"})
	if err != util.ErrorSuccess {
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
			log.Errorf("Error: unknown where parameter '%s' for user modification", key)
			return util.ErrorCmdArg
		}
	}

	if len(SetParams) == 0 && len(AddParams) == 0 && len(DeleteParams) == 0 {
		log.Errorf("Error: modify user command requires 'set' clause to specify what to modify")
		return util.ErrorCmdArg
	}

	var params []ModifyParam
	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagSetPartitionList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Partition,
				NewValue:    FlagSetPartitionList,
				RequestType: protos.OperationType_Overwrite,
			})
		case "allowedqos":
			FlagSetQosList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Qos,
				NewValue:    FlagSetQosList,
				RequestType: protos.OperationType_Overwrite,
			})
		case "defaultaccount":
			FlagSetDefaultAccount = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_DefaultAccount,
				NewValue:    FlagSetDefaultAccount,
				RequestType: protos.OperationType_Overwrite,
			})
		case "defaultqos":
			FlagUserDefaultQos = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_DefaultQos,
				NewValue:    FlagUserDefaultQos,
				RequestType: protos.OperationType_Overwrite,
			})
		case "adminlevel":
			FlagAdminLevel = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_AdminLevel,
				NewValue:    FlagAdminLevel,
				RequestType: protos.OperationType_Overwrite,
			})
		default:
			log.Errorf("Error: unknown set parameter '%s' for user modification", key)
			return util.ErrorCmdArg
		}
	}

	for key, value := range AddParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagAllowedPartitions = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Partition,
				NewValue:    FlagAllowedPartitions,
				RequestType: protos.OperationType_Add,
			})
		case "allowedqos":
			FlagAllowedQosList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Qos,
				NewValue:    FlagAllowedQosList,
				RequestType: protos.OperationType_Add,
			})
		default:
			log.Errorf("Error: unknown add parameter '%s' for user modification", key)
			return util.ErrorCmdArg
		}
	}

	for key, value := range DeleteParams {
		switch strings.ToLower(key) {
		case "allowedpartition":
			FlagDeletePartitionList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Partition,
				NewValue:    FlagDeletePartitionList,
				RequestType: protos.OperationType_Delete,
			})
		case "allowedqos":
			FlagDeleteQosList = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Qos,
				NewValue:    FlagDeleteQosList,
				RequestType: protos.OperationType_Delete,
			})
		default:
			log.Errorf("Error: unknown delete parameter '%s' for user modification", key)
			return util.ErrorCmdArg
		}
	}

	return ModifyUser(params, FlagEntityName, FlagEntityAccount, FlagEntityPartitions)
}

func executeModifyWckeyCommand(command *CAcctMgrCommand) int {
	FlagWckey = protos.WckeyInfo{}

	WhereParams := command.GetWhereParams()
	SetParams, AddParams, DeleteParams := command.GetSetParams()

	if len(WhereParams) == 0 {
		log.Errorf("Error: modify wckey command requires 'where' clause to specify which user to modify")
		return util.ErrorCmdArg
	}

	err := checkEmptyKVParams(WhereParams, []string{"user"})
	if err != util.ErrorSuccess {
		return err
	}

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "user":
			FlagWckey.UserName = value
		default:
			log.Errorf("Error: unknown where parameter '%s' for wckey modification", key)
			return util.ErrorCmdArg
		}
	}

	if len(SetParams) == 0 || len(AddParams) != 0 || len(DeleteParams) != 0 {
		log.Errorf("Error: modify wckey command requires only 'set' clause (add/delete not supported)")
		return util.ErrorCmdArg
	}

	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "defaultwckey":
			FlagWckey.Name = value
		default:
			log.Errorf("Error: unknown set parameter '%s' for wckey modification", key)
			return util.ErrorCmdArg
		}
	}
	if FlagWckey.Name == "" {
		log.Errorf("Error: modify wckey command requires non-empty 'defaultwckey'")
		return util.ErrorCmdArg
	}
	return ModifyDefaultWckey(FlagWckey.Name, FlagWckey.UserName)
}

func executeModifyQosCommand(command *CAcctMgrCommand) int {
	FlagEntityName = ""
	FlagMaxCpu = ""
	FlagMaxTimeLimit = ""
	FlagPriority = ""
	FlagDescription = ""

	WhereParams := command.GetWhereParams()
	SetParams, _, _ := command.GetSetParams()

	if len(WhereParams) == 0 {
		log.Errorf("Error: modify qos command requires 'where' clause to specify which qos to modify")
		return util.ErrorCmdArg
	}

	err := checkEmptyKVParams(WhereParams, []string{"name"})
	if err != util.ErrorSuccess {
		return err
	}

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		default:
			log.Errorf("Error: unknown where parameter '%s' for qos modification", key)
			return util.ErrorCmdArg
		}
	}

	if len(SetParams) == 0 {
		log.Errorf("Error: modify qos command requires 'set' clause to specify what to modify")
		return util.ErrorCmdArg
	}
	var params []ModifyParam
	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "maxcpusperuser":
			if err := validateUintValue(value, "maxCpusPerUser", 32); err != nil {
				return util.ErrorCmdArg
			}
			FlagMaxCpu = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxCpusPerUser,
				NewValue:    FlagMaxCpu,
			})
		case "maxjobsperuser":
			if err := validateUintValue(value, "maxJobsPerUser", 32); err != nil {
				return util.ErrorCmdArg
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxJobsPerUser,
				NewValue:    value,
			})
		case "maxsubmitjobsperuser":
			if err := validateUintValue(value, "maxSubmitJobsPerUser", 32); err != nil {
				return util.ErrorCmdArg
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxSubmitJobsPerUser,
				NewValue:    value,
			})
		case "maxjobsperaccount":
			if err := validateUintValue(value, "maxJobsPerAccount", 32); err != nil {
				return util.ErrorCmdArg
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxJobsPerAccount,
				NewValue:    value,
			})
		case "maxsubmitjobsperaccount":
			if err := validateUintValue(value, "maxSubmitJobsPerAccount", 32); err != nil {
				return util.ErrorCmdArg
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxSubmitJobsPerAccount,
				NewValue:    value,
			})
		case "maxtresperuser":
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxTresPerUser,
				NewValue:    value,
			})
		case "maxtresperaccount":
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxTresPerAccount,
				NewValue:    value,
			})
		case "maxtres":
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxTres,
				NewValue:    value,
			})
		case "maxjobs":
			if err := validateUintValue(value, "maxjobs", 32); err != nil {
				return util.ErrorCmdArg
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxJobs,
				NewValue:    value,
			})
		case "maxsubmitjobs":
			if err := validateUintValue(value, "maxsubmitjobs", 32); err != nil {
				return util.ErrorCmdArg
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxSubmitJobs,
				NewValue:    value,
			})
		case "maxwall":
			if err := validateUintValue(value, "maxwall", 64); err != nil {
				return util.ErrorCmdArg
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxWall,
				NewValue:    value,
			})
		case "maxtimelimitpertask":
			if err := validateUintValue(value, "maxTimeLimitPerTask", 64); err != nil {
				return util.ErrorCmdArg
			}
			FlagMaxTimeLimit = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_MaxTimeLimitPerTask,
				NewValue:    FlagMaxTimeLimit,
			})
		case "priority":
			if err := validateUintValue(value, "priority", 32); err != nil {
				return util.ErrorCmdArg
			}
			FlagPriority = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Priority,
				NewValue:    FlagPriority,
			})
		case "description":
			FlagDescription = value
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_Description,
				NewValue:    FlagDescription,
			})
		case "flags":
			var err error
			if FlagQosFlags, err = util.ParseFlags(value); err != nil {
				log.Errorf("invalid argument %s ,err: %v", value, err)
				return util.ErrorCmdArg
			}
			params = append(params, ModifyParam{
				ModifyField: protos.ModifyField_FLags,
				NewValue:    strconv.FormatUint(uint64(FlagQosFlags), 10),
			})
		default:
			log.Errorf("Error: unknown set parameter '%s' for qos modification", key)
			return util.ErrorCmdArg
		}
	}
	return ModifyQos(params, FlagEntityName)
}

func executeModifyResourceCommand(command *CAcctMgrCommand) int {

	FlagOperators := make(map[protos.LicenseResource_Field]string, 0)

	KvParams := command.GetKVMaps()
	WhereParams := command.GetWhereParams()
	SetParams, _, _ := command.GetSetParams()

	if len(KvParams) == 0 && len(WhereParams) == 0 {
		log.Errorf("Error: modify resource command requires 'where' clause to specify which resource to modify")
		return util.ErrorCmdArg
	}

	if len(KvParams) > 0 {
		err := checkEmptyKVParams(KvParams, []string{"name", "server"})
		if err != util.ErrorSuccess {
			return err
		}
	}

	for key, value := range KvParams {
		switch strings.ToLower(key) {
		case "name":
			FlagResourceName = value
		case "server":
			FlagServerName = value
		case "cluster":
			FlagClusters = value
		default:
			log.Errorf("Error: unknown where parameter '%s' for resource modification", key)
			return util.ErrorCmdArg
		}
	}

	if len(WhereParams) > 0 {
		err := checkEmptyKVParams(WhereParams, []string{"name", "server"})
		if err != util.ErrorSuccess {
			return err
		}
	}

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "name":
			FlagResourceName = value
		case "server":
			FlagServerName = value
		case "cluster":
			FlagClusters = value
		default:
			log.Errorf("Error: unknown where parameter '%s' for resource modification", key)
			return util.ErrorCmdArg
		}
	}

	if len(SetParams) == 0 {
		log.Errorf("Error: modify resource command requires 'set' clause to specify what to modify")
		return util.ErrorCmdArg
	}

	for key, value := range SetParams {
		switch strings.ToLower(key) {
		case "count":
			if err := validateUintValue(value, "count", 32); err != nil {
				return util.ErrorCmdArg
			}
			FlagOperators[protos.LicenseResource_Count] = value
		case "lastconsumed":
			if err := validateUintValue(value, "lastConsumed", 32); err != nil {
				return util.ErrorCmdArg
			}
			FlagOperators[protos.LicenseResource_LastConsumed] = value
		case "description":
			FlagOperators[protos.LicenseResource_Description] = value
		case "flags":
			FlagOperators[protos.LicenseResource_Flags] = value
		case "type":
			FlagOperators[protos.LicenseResource_ResourceType] = value
		case "allowed":
			if len(FlagClusters) == 0 {
				log.Errorf("Error: modify 'allowed' requires 'cluster' clause to specify which cluster resource to modify")
				return util.ErrorCmdArg
			}
			if err := validateUintValue(value, "allowed", 32); err != nil {
				return util.ErrorCmdArg
			}
			FlagOperators[protos.LicenseResource_Allowed] = value
		case "servertype":
			FlagOperators[protos.LicenseResource_ServerType] = value
		default:
			log.Errorf("Error: unknown set parameter '%s' for resource modification", key)
			return util.ErrorCmdArg
		}
	}

	return ModifyResource(FlagResourceName, FlagServerName, FlagClusters, FlagOperators)
}

func executeShowCommand(command *CAcctMgrCommand) int {
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
	case "resource":
		return executeShowResourceCommand(command)
	case "event":
		return executeShowEventCommand(command)
	case "wckey":
		return executeShowWckeyCommand(command)
	default:
		log.Errorf("unknown entity type: %s", entity)
		return util.ErrorCmdArg
	}
}

func executeShowAccountCommand(command *CAcctMgrCommand) int {
	name := command.GetID()
	if nameParam := command.GetKVParamValue("name"); nameParam != "" {
		name = nameParam
	}
	if name == "" {
		return ShowAccounts()
	}

	return FindAccount(name)
}

func executeShowWckeyCommand(command *CAcctMgrCommand) int {
	wckeyList := command.GetID()
	return ShowWckey(wckeyList)
}

func executeShowUserCommand(command *CAcctMgrCommand) int {
	name := command.GetID()
	if nameParam := command.GetKVParamValue("name"); nameParam != "" {
		name = nameParam
	}

	account := command.GetKVParamValue("accounts")
	return ShowUser(name, account)
}

func executeShowEventCommand(command *CAcctMgrCommand) int {
	var FlagMaxLines int = 0
	nodesStr := ""
	maxLinesStr := ""
	updateMaxLines := false

	WhereParams := command.GetWhereParams()

	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "maxlines":
			maxLinesStr = value
			updateMaxLines = true
		case "nodes":
			nodesStr = value
		default:
			log.Errorf("Error: unknown where parameter '%s' for show event", key)
			return util.ErrorCmdArg
		}
	}

	if updateMaxLines {
		var err error
		FlagMaxLines, err = strconv.Atoi(maxLinesStr)
		if err != nil || FlagMaxLines <= 0 {
			log.Errorf("Error: invalid maxlines: '%s'", maxLinesStr)
			return util.ErrorCmdArg
		}
	}

	return QueryEventInfoByNodes(nodesStr, FlagMaxLines)
}

func executeShowQosCommand(command *CAcctMgrCommand) int {
	name := command.GetID()
	if nameParam := command.GetKVParamValue("name"); nameParam != "" {
		name = nameParam
	}

	return ShowQos(name)
}

// Reset cert
func executeResetCommand(command *CAcctMgrCommand) int {
	name := command.GetID()
	if nameParam := command.GetKVParamValue("name"); nameParam != "" {
		name = nameParam
	}

	return ResetUserCredential(name)
}

func executeShowTxnLogCommand(command *CAcctMgrCommand) int {
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
			log.Errorf("Error: unknown where parameter '%s' for show transaction", key)
			return util.ErrorCmdArg
		}
	}

	return ShowTxn(FlagActor, FlagTarget, FlagAction, FlagInfo, FlagStartTime)
}

func executeShowResourceCommand(command *CAcctMgrCommand) int {

	FlagWithClusters := command.GetID()

	FlagServer := ""

	WhereParams := command.GetWhereParams()
	for key, value := range WhereParams {
		switch strings.ToLower(key) {
		case "name":
			FlagEntityName = value
		case "server":
			FlagServer = value
		case "cluster":
			FlagClusters = value
		default:
			log.Errorf("Error: unknown where parameter '%s' for show resource", key)
			return util.ErrorCmdArg
		}
	}

	hasWithClusters := false
	if strings.ToLower(FlagWithClusters) == "withclusters" {
		hasWithClusters = true
	}

	return ShowLicenseResources(FlagEntityName, FlagServer, FlagClusters, hasWithClusters)
}

func checkEmptyKVParams(kvParams map[string]string, requiredFields []string) int {
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
				log.Errorf("Error: required argument %s not set", missingFields[0])
			} else {
				log.Errorf("Error: required arguments %s not set", strings.Join(missingFields, "\", \""))
			}
			return util.ErrorCmdArg
		}
	}

	return util.ErrorSuccess
}
