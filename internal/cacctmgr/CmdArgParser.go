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
 )
 
 func ParseCmdArgs(args []string) {
	 cmdStr := strings.Join(args[1:], " ")
	 command, err := ParseCAcctMgrCommand(cmdStr)
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
	 case "find":
		 return executeFindCommand(command)
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
	 KVParams := command.GetKVMaps()
	 for key, value := range KVParams {
		 switch key {
		 case "name":
			 FlagAccount.Name = value
		 case "description":
			 FlagAccount.Description = value
		 case "parent":
			 FlagAccount.ParentAccount = value
		 case "default-qos":
			 FlagAccount.DefaultQos = value
		 case "partition":
			 FlagAccount.AllowedPartitions = strings.Split(value, ",")
		 case "qos-list":
			 FlagAccount.AllowedQosList = strings.Split(value, ",")
		 default:
			 log.Debugf("unknown flag: %s", key)
			 return util.ErrorCmdArg
		 }
	 }
 
	 return AddAccount(&FlagAccount)
 }
 
 func executeAddUserCommand(command *CAcctMgrCommand) int {
	 KVParams := command.GetKVMaps()
	 for key, value := range KVParams {
		 switch key {
		 case "account":
			 FlagUser.Name = value
		 case "coordinator":
			 FlagUserCoordinator = value == "true"
		 case "level":
			 FlagLevel = value
		 case "partition":
			 FlagUserPartitions = strings.Split(value, ",")
		 case "name":
			 FlagUser.Name = value
		 default:
			 log.Debugf("unknown flag: %s", key)
			 return util.ErrorCmdArg
		 }
	 }
 
	 return AddUser(&FlagUser, FlagUserPartitions, FlagLevel, FlagUserCoordinator)
 }
 
 func executeAddQosCommand(command *CAcctMgrCommand) int {
	 KVParams := command.GetKVMaps()
	 for key, value := range KVParams {
		 switch key {
		 case "name":
			 FlagQos.Name = value
		 case "description":
			 FlagQos.Description = value
		 case "priority":
			 priority, err := strconv.ParseUint(value, 10, 32)
			 if err != nil {
				 log.Debugf("invalid priority value: %s", value)
				 return util.ErrorCmdArg
			 }
			 FlagQos.Priority = uint32(priority)
		 case "max-jobs-per-user":
			 maxJobs, err := strconv.ParseUint(value, 10, 32)
			 if err != nil {
				 log.Debugf("invalid max-jobs-per-user value: %s", value)
				 return util.ErrorCmdArg
			 }
			 FlagQos.MaxJobsPerUser = uint32(maxJobs)
		 case "max-cpus-per-user":
			 maxCpus, err := strconv.ParseUint(value, 10, 32)
			 if err != nil {
				 log.Debugf("invalid max-cpus-per-user value: %s", value)
				 return util.ErrorCmdArg
			 }
			 FlagQos.MaxCpusPerUser = uint32(maxCpus)
		 case "max-time-limit-per-task":
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
	 FlagAccount.Name = command.GetKVParamValue("name")
	 if FlagAccount.Name == "" {
		 return util.ErrorCmdArg
	 }
 
	 return DeleteAccount(FlagAccount.Name)
 }
 
 func executeDeleteUserCommand(command *CAcctMgrCommand) int {
	 KVParams := command.GetKVMaps()
	 for key, value := range KVParams {
		 switch key {
		 case "name":
			 FlagUser.Name = value
		 case "account":
			 FlagUser.Account = value
		 default:
			 log.Debugf("unknown flag: %s", key)
			 return util.ErrorCmdArg
		 }
	 }
 
	 return DeleteUser(FlagUser.Name, FlagUser.Account)
 }
 
 func executeDeleteQosCommand(command *CAcctMgrCommand) int {
	 FlagQos.Name = command.GetKVParamValue("name")
	 if FlagQos.Name == "" {
		 log.Debug("未指定QoS名称")
		 return util.ErrorCmdArg
	 }
 
	 return DeleteQos(FlagQos.Name)
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
	 KVParams := command.GetKVMaps()
 
	 account := ""
	 for key, value := range KVParams {
		 switch key {
		 case "name":
			 FlagAccount.Name = value
		 case "account":
			 account = value
		 default:
			 log.Debugf("unknown flag: %s", key)
			 return util.ErrorCmdArg
		 }
	 }
 
	 return BlockAccountOrUser(FlagAccount.Name, protos.EntityType_Account, account)
 }
 
 func executeBlockUserCommand(command *CAcctMgrCommand) int {
	 KVParams := command.GetKVMaps()
	 for key, value := range KVParams {
		 switch key {
		 case "name":
			 FlagUser.Name = value
		 case "account":
			 FlagUser.Account = value
		 }
	 }
 
	 return BlockAccountOrUser(FlagUser.Name, protos.EntityType_User, FlagUser.Account)
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
	 KVParams := command.GetKVMaps()
 
	 account := ""
	 for key, value := range KVParams {
		 switch key {
		 case "name":
			 FlagAccount.Name = value
		 case "account":
			 account = value
		 default:
			 log.Debugf("unknown flag: %s", key)
			 return util.ErrorCmdArg
		 }
	 }
 
	 return UnblockAccountOrUser(FlagAccount.Name, protos.EntityType_Account, account)
 }
 
 func executeUnblockUserCommand(command *CAcctMgrCommand) int {
	 KVParams := command.GetKVMaps()
	 for key, value := range KVParams {
		 switch key {
		 case "name":
			 FlagUser.Name = value
		 case "account":
			 FlagUser.Account = value
		 }
	 }
 
	 return UnblockAccountOrUser(FlagUser.Name, protos.EntityType_User, FlagUser.Account)
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
		 log.Debugf("unknown resource type: %s", resource)
		 return util.ErrorCmdArg
	 }
 }
 
 func executeModifyAccountCommand(command *CAcctMgrCommand) int {
	 KVParams := command.GetKVMaps()
 
	 name := ""
	 defaultQos := ""
	 allowedQosList := ""
	 allowedPartitions := ""
	 deleteQosList := ""
	 deletePartitionList := ""
	 setQosList := ""
	 setPartitionList := ""
 
	 for key, value := range KVParams {
		 switch key {
		 case "name":
			 name = value
		 case "default-qos":
			 defaultQos = value
		 case "force":
			 FlagForce = value == "true"
		 case "add-allowed-qos":
			 allowedQosList = value
		 case "delete-allowed-qos":
			 deleteQosList = value
		 case "set-allowed-qos":
			 setQosList = value
		 case "add-allowed-partition":
			 allowedPartitions = value
		 case "delete-allowed-partition":
			 deletePartitionList = value
		 case "set-allowed-partition":
			 setPartitionList = value
		 }
	 }
 
	 if allowedQosList != "" {
		 if err := ModifyAccount(protos.ModifyField_Qos, allowedQosList, name, protos.OperationType_Overwrite); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if deleteQosList != "" {
		 if err := ModifyAccount(protos.ModifyField_Qos, deleteQosList, name, protos.OperationType_Delete); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if setQosList != "" {
		 if err := ModifyAccount(protos.ModifyField_Qos, setQosList, name, protos.OperationType_Overwrite); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if allowedPartitions != "" {
		 if err := ModifyAccount(protos.ModifyField_Partition, allowedPartitions, name, protos.OperationType_Overwrite); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if deletePartitionList != "" {
		 if err := ModifyAccount(protos.ModifyField_Partition, deletePartitionList, name, protos.OperationType_Delete); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if setPartitionList != "" {
		 if err := ModifyAccount(protos.ModifyField_Partition, setPartitionList, name, protos.OperationType_Overwrite); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if defaultQos != "" {
		 if err := ModifyAccount(protos.ModifyField_DefaultQos, defaultQos, name, protos.OperationType_Overwrite); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 return util.ErrorSuccess
 }
 
 func executeModifyUserCommand(command *CAcctMgrCommand) int {
	 KVParams := command.GetKVMaps()
 
	 name := ""
	 account := ""
	 partitions := ""
	 allowedQosList := ""
	 deleteQosList := ""
	 setQosList := ""
	 allowedPartitions := ""
	 deletePartitionList := ""
	 setPartitionList := ""
	 for key, value := range KVParams {
		 switch key {
		 case "name":
			 name = value
		 case "account":
			 account = value
		 case "partition":
			 partitions = value
		 case "add-allowed-qos":
			 allowedQosList = value
		 case "delete-allowed-qos":
			 deleteQosList = value
		 case "set-allowed-qos":
			 setQosList = value
		 case "add-allowed-partition":
			 allowedPartitions = value
		 case "delete-allowed-partition":
			 deletePartitionList = value
		 case "set-allowed-partition":
			 setPartitionList = value
		 }
	 }
 
	 if allowedQosList != "" {
		 if err := ModifyUser(protos.ModifyField_Qos, allowedQosList, name, account, partitions, protos.OperationType_Overwrite); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if deleteQosList != "" {
		 if err := ModifyUser(protos.ModifyField_Qos, deleteQosList, name, account, partitions, protos.OperationType_Delete); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if setQosList != "" {
		 if err := ModifyUser(protos.ModifyField_Qos, setQosList, name, account, partitions, protos.OperationType_Overwrite); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if allowedPartitions != "" {
		 if err := ModifyUser(protos.ModifyField_Partition, allowedPartitions, name, account, partitions, protos.OperationType_Overwrite); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if deletePartitionList != "" {
		 if err := ModifyUser(protos.ModifyField_Partition, deletePartitionList, name, account, partitions, protos.OperationType_Delete); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if setPartitionList != "" {
		 if err := ModifyUser(protos.ModifyField_Partition, setPartitionList, name, account, partitions, protos.OperationType_Overwrite); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 return util.ErrorSuccess
 }
 
 func executeModifyQosCommand(command *CAcctMgrCommand) int {
	 KVParams := command.GetKVMaps()
 
	 maxCpu := ""
	 maxJob := ""
	 maxTimeLimit := ""
	 priority := ""
	 name := ""
	 for key, value := range KVParams {
		 switch key {
		 case "name":
			 name = value
		 case "max-cpu":
			 maxCpu = value
		 case "max-job":
			 maxJob = value
		 case "max-time-limit":
			 maxTimeLimit = value
		 case "priority":
			 priority = value
		 default:
			 log.Debugf("unknown flag: %s", key)
			 return util.ErrorCmdArg
		 }
	 }
 
	 if maxCpu != "" {
		 if err := ModifyQos(protos.ModifyField_MaxCpusPerUser, maxCpu, name); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if maxJob != "" {
		 if err := ModifyQos(protos.ModifyField_MaxJobsPerUser, maxJob, name); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if maxTimeLimit != "" {
		 if err := ModifyQos(protos.ModifyField_MaxTimeLimitPerTask, maxTimeLimit, name); err != util.ErrorSuccess {
			 return util.ErrorCmdArg
		 }
	 }
 
	 if priority != "" {
		 if err := ModifyQos(protos.ModifyField_Priority, priority, name); err != util.ErrorSuccess {
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
	 default:
		 log.Debugf("unknown resource type: %s", resource)
		 return util.ErrorCmdArg
	 }
 }
 
 func executeShowAccountCommand(command *CAcctMgrCommand) int {
	 return ShowAccounts()
 }
 
 func executeShowUserCommand(command *CAcctMgrCommand) int {
	 KVParamsValue := command.GetKVParamValue("accounts")
 
	 return ShowUser(KVParamsValue, "")
 }
 
 func executeFindCommand(command *CAcctMgrCommand) int {
	 resource := command.GetResource()
 
	 switch resource {
	 case "account":
		 return executeFindAccountCommand(command)
	 case "user":
		 return executeFindUserCommand(command)
	 case "qos":
		 return executeFindQosCommand(command)
	 default:
		 log.Debugf("unknown resource type: %s", resource)
		 return util.ErrorCmdArg
	 }
 }
 
 func executeFindAccountCommand(command *CAcctMgrCommand) int {
	 KVParamsValue := command.GetKVParamValue("account")
 
	 if len(KVParamsValue) == 0 {
		 return util.ErrorCmdArg
	 }
 
	 return FindAccount(KVParamsValue)
 }
 
 func executeFindUserCommand(command *CAcctMgrCommand) int {
	 KVParamsValue := command.GetKVParamValue("user")
 
	 if len(KVParamsValue) == 0 {
		 return util.ErrorCmdArg
	 }
 
	 account := command.GetKVParamValue("account")
	 return ShowUser(KVParamsValue, account)
 }
 
 func executeFindQosCommand(command *CAcctMgrCommand) int {
	 KVParamsValue := command.GetKVParamValue("qos")
 
	 if len(KVParamsValue) == 0 {
		 return util.ErrorCmdArg
	 }
 
	 return ShowQos(KVParamsValue)
 }
 