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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"github.com/xlab/treeprint"
	"gopkg.in/yaml.v3"
)

var (
	userUid  uint32
	stub     protos.CraneCtldClient
	config   *util.Config
	dbConfig *util.InfluxDbConfig

	// unused
	// dbConfigInitOnce sync.Once
)

type ServerAddr struct {
	ControlMachine      string `yaml:"ControlMachine"`
	CraneCtldListenPort string `yaml:"CraneCtldListenPort"`
}

type ResourceUsageRecord struct {
	ClusterName string
	NodeName    string
	Uid         uint64
	StartTime   int64
	EndTime     int64
	State       string
	Reason      string
	Timestamp   time.Time
}

type EventInfoJson struct {
	ClusterName string `json:"cluster_name"`
	NodeName    string `json:"node_name"`
	Uid         uint64 `json:"uid"`
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
	State       string `json:"state"`
	Reason      string `json:"reason"`
}

func PrintAccountTree(parentTreeRoot treeprint.Tree, account string, accountMap map[string]*protos.AccountInfo) {
	if account == "" {
		return
	}

	value, ok := accountMap[account]
	if !ok || len(value.ChildAccounts) == 0 {
		parentTreeRoot.AddNode(account)
	} else {
		branch := parentTreeRoot.AddBranch(account)
		for _, child := range accountMap[account].ChildAccounts {
			PrintAccountTree(branch, child, accountMap)
		}
	}
}

func AddAccount(account *protos.AccountInfo) error {
	if FlagForce {
		log.Warning("The --force flag is ignored for add operations")
	}
	if err := util.CheckEntityName(account.Name); err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to add account: invalid account name: %v\n", err)
	}

	req := new(protos.AddAccountRequest)
	req.Uid = userUid
	req.Account = account
	if account.DefaultQos == "" && len(account.AllowedQosList) > 0 {
		account.DefaultQos = account.AllowedQosList[0]
	}
	if account.DefaultQos != "" {
		find := false
		for _, qos := range account.AllowedQosList {
			if qos == account.DefaultQos {
				find = true
				break
			}
		}
		if !find {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to add account: default QoS %s is not in allowed QoS list\n", account.DefaultQos))
		}
	}

	reply, err := stub.AddAccount(context.Background(), req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to add account: %v\n", err)
	}

	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil

		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
	}
	if reply.GetOk() {
		fmt.Println("Account added successfully.")
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to add account: %s.\n", util.ErrMsg(reply.GetCode())))
	}
}

func AddUser(user *protos.UserInfo, partition []string, level string, coordinator bool) error {
	if FlagForce {
		log.Warning("The --force flag is ignored for add operations")
	}
	var err error
	if err = util.CheckEntityName(user.Name); err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to add user: invalid user name: %v\n", err)
	}

	user.Uid, err = util.GetUidByUserName(user.Name)
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to add user: %v\n", err)
	}

	req := new(protos.AddUserRequest)
	req.Uid = userUid
	req.User = user
	for _, par := range partition {
		user.AllowedPartitionQosList = append(user.AllowedPartitionQosList, &protos.UserInfo_AllowedPartitionQos{PartitionName: par})
	}

	if level == "none" {
		user.AdminLevel = protos.UserInfo_None
	} else if level == "operator" {
		user.AdminLevel = protos.UserInfo_Operator
	} else if level == "admin" {
		user.AdminLevel = protos.UserInfo_Admin
	} else {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Failed to add user: unknown admin level, valid values: none, operator, admin."))
	}

	if coordinator {
		user.CoordinatorAccounts = append(user.CoordinatorAccounts, user.Account)
	}

	reply, err := stub.AddUser(context.Background(), req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to add user: %s\n", err)
	}

	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
	}
	if reply.GetOk() {
		fmt.Println("User added successfully.")
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to add user: %s.\n", util.ErrMsg(reply.GetCode())))
	}
}

func AddQos(qos *protos.QosInfo) error {
	if FlagForce {
		log.Warning("The --force flag is ignored for add operations")
	}
	if err := util.CheckEntityName(qos.Name); err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to add QoS: invalid QoS name: %v\n", err)
	}

	req := new(protos.AddQosRequest)
	req.Uid = userUid
	req.Qos = qos

	reply, err := stub.AddQos(context.Background(), req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to add QoS: %v\n", err)
	}

	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
	}
	if reply.GetOk() {
		fmt.Println("QoS added successfully.")
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Failed to add QoS: %s.\n", util.ErrMsg(reply.GetCode())))
	}
}

func DeleteAccount(value string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for delete operations")
	}

	accountList, err := util.ParseStringParamList(value, ",")
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Invalid user list specified: %v.\n", err)
	}

	req := protos.DeleteAccountRequest{Uid: userUid, AccountList: accountList}

	reply, err := stub.DeleteAccount(context.Background(), &req)
	if err != nil {
		return util.NewCraneErr(util.ErrorNetwork, fmt.Sprintf("Failed to delete account %s: %v\n", value, err))
	}

	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
	}
	if reply.GetOk() {
		fmt.Printf("Successfully deleted account '%s'.\n", value)
		return nil
	} else {
		msg := "Failed to delete account: \n"
		for _, richError := range reply.RichErrorList {
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func DeleteUser(value string, account string) error {
	userList, err := util.ParseStringParamList(value, ",")
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Invalid user list specified: %v.\n", err)
	}

	if slices.Contains(userList, "ALL") {
		if !FlagForce {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("To delete all users in the account, you must set --force."))
		}
		userList = []string{"ALL"}
	}

	req := protos.DeleteUserRequest{Uid: userUid, UserList: userList, Account: account, Force: FlagForce}

	reply, err := stub.DeleteUser(context.Background(), &req)
	if err != nil {
		return util.NewCraneErr(util.ErrorNetwork, fmt.Sprintf("Failed to remove user %s: %v\n", value, err))
	}

	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
	}
	if reply.GetOk() {
		fmt.Printf("Successfully removed user '%s'.\n", value)
		return nil
	} else {
		msg := "Failed to remove user: \n"
		for _, richError := range reply.RichErrorList {
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func DeleteQos(value string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for delete operations")
	}

	qosList, err := util.ParseStringParamList(value, ",")
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Invalid user list specified: %v.\n", err)
	}
	req := protos.DeleteQosRequest{Uid: userUid, QosList: qosList}

	reply, err := stub.DeleteQos(context.Background(), &req)
	if err != nil {
		return util.NewCraneErr(util.ErrorNetwork, fmt.Sprintf("Failed to delete QoS %s: %v\n", value, err))
	}

	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
	}
	if reply.GetOk() {
		fmt.Printf("Successfully deleted QoS '%s'.\n", value)
		return nil
	} else {
		msg := "Failed to delete QoS: \n"
		for _, richError := range reply.RichErrorList {
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func ModifyAccount(modifyField protos.ModifyField, newValue string, name string, requestType protos.OperationType) error {
	var valueList []string
	var err error

	valueList, err = util.ParseStringParamList(newValue, ",")
	if err != nil {
		if modifyField == protos.ModifyField_Qos {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid qos list specified: %v.\n", err)
		} else if modifyField == protos.ModifyField_Partition {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid partition list specified: %v.\n", err)
		} else {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid value list specified: %v.\n", err)
		}
	}

	if modifyField == protos.ModifyField_DefaultQos || modifyField == protos.ModifyField_Description {
		if len(valueList) != 1 {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Invalid value specified! Modify Description and DefaultQos, please provide only one value."))
		}
	}

	req := protos.ModifyAccountRequest{
		Uid:         userUid,
		ModifyField: modifyField,
		ValueList:   valueList,
		Name:        name,
		Type:        requestType,
		Force:       FlagForce,
	}

	reply, err := stub.ModifyAccount(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to modify account information: %v\n", err)
	}

	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
	}

	if reply.GetOk() {
		fmt.Println("Information was successfully modified.")
		return nil
	} else {
		msg := "Failed to modify information:\n"
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				msg += fmt.Sprintf("%s \n", util.ErrMsg(richError.Code))
			} else {
				msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
			}
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func ModifyUser(modifyField protos.ModifyField, newValue string, name string, account string, partition string, requestType protos.OperationType) error {
	if modifyField == protos.ModifyField_AdminLevel {
		if newValue != "none" && newValue != "operator" && newValue != "admin" {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Unknown admin level, valid values: none, operator, admin."))
		}
	}

	var valueList []string
	var err error

	valueList, err = util.ParseStringParamList(newValue, ",")
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Invalid value list specified: %v.\n", err)
	}

	if modifyField == protos.ModifyField_AdminLevel || modifyField == protos.ModifyField_DefaultQos || modifyField == protos.ModifyField_DefaultAccount {
		if len(valueList) != 1 {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Invalid value specified! Modify AdminLevel, DefaultAccount and DefaultQos, please provide only one value."))
		}
	}

	req := protos.ModifyUserRequest{
		Uid:         userUid,
		ModifyField: modifyField,
		ValueList:   valueList,
		Name:        name,
		Partition:   partition,
		Type:        requestType,
		Account:     account,
		Force:       FlagForce,
	}

	reply, err := stub.ModifyUser(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to modify user information: %s\n", err)
	}

	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)

		}
	}
	if reply.GetOk() {
		fmt.Println("Information was successfully modified.")
		return nil
	} else {
		msg := fmt.Sprintln("Modify information failed:")
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				msg += fmt.Sprintf("%s \n", util.ErrMsg(richError.Code))
			} else {
				msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
			}
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func ModifyQos(modifyField protos.ModifyField, newValue string, name string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for QoS modify operations")
	}
	req := protos.ModifyQosRequest{
		Uid:         userUid,
		ModifyField: modifyField,
		Value:       newValue,
		Name:        name,
	}

	reply, err := stub.ModifyQos(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to modify QoS: %v\n", err)
	}

	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
	}

	if reply.GetOk() {
		fmt.Println("Information was successfully modified.")
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Modify information failed: %s.\n", util.ErrMsg(reply.GetCode())))
	}
}

func FindAccount(value string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
	}
	var accountList []string
	if value != "" {
		var err error
		accountList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid account list specified: %v.\n", err)
		}
	}

	req := protos.QueryAccountInfoRequest{Uid: userUid, AccountList: accountList}
	reply, err := stub.QueryAccountInfo(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to find account: %v\n", err)
	}

	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)

		}
	}

	if !reply.GetOk() {
		msg := ""
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				msg += fmt.Sprintln(util.ErrMsg(richError.Code))
				break
			}
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}

	return PrintAccountList(reply.AccountList)
}

func BlockAccountOrUser(value string, entityType protos.EntityType, account string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for block operations")
	}

	var entityList []string
	if value != "all" {
		var err error
		entityList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid account/user list specified: %v.\n", err)
		}
	}

	req := protos.BlockAccountOrUserRequest{Uid: userUid, Block: true, EntityType: entityType, EntityList: entityList, Account: account}
	reply, err := stub.BlockAccountOrUser(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to block entity: %v\n", err)
	}

	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
	}
	if reply.GetOk() {
		fmt.Printf("Block %s succeeded.\n", value)
		return nil
	} else {
		msg := ""
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				msg += fmt.Sprintln(util.ErrMsg(richError.Code))
				break
			}
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

func UnblockAccountOrUser(value string, entityType protos.EntityType, account string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for unblock operations")
	}

	var entityList []string
	if value != "all" {
		var err error
		entityList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid account/user list specified: %v.\n", err)
		}
	}

	req := protos.BlockAccountOrUserRequest{Uid: userUid, Block: false, EntityType: entityType, EntityList: entityList, Account: account}
	reply, err := stub.BlockAccountOrUser(context.Background(), &req)
	if err != nil {
		return util.WrapCraneErr(util.ErrorNetwork, "Failed to unblock entity: %v\n", err)
	}

	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
	}
	if reply.GetOk() {
		fmt.Printf("Unblock %s succeeded.\n", value)
		return nil
	} else {
		msg := ""
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				msg += fmt.Sprintln(util.ErrMsg(richError.Code))
				break
			}
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
}

// Extracts the EventPlugin InfluxDB configuration from the specified YAML configuration files
func GetEventPluginConfig(config *util.Config) (*util.InfluxDbConfig, error) {
	if !config.Plugin.Enabled {
		return nil, util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Plugin is not enabled"))
	}

	var eventConfigPath string
	for _, plugin := range config.Plugin.Plugins {
		if plugin.Name == "event" {
			eventConfigPath = plugin.Config
			break
		}
	}

	if eventConfigPath == "" {
		return nil, util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("event plugin not found"))
	}

	confFile, err := os.ReadFile(eventConfigPath)
	if err != nil {
		return nil, util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to read config file %s: %v.\n", eventConfigPath, err))
	}

	dbConf := &struct {
		Database *util.InfluxDbConfig `yaml:"Database"`
	}{}
	if err := yaml.Unmarshal(confFile, dbConf); err != nil {
		return nil, util.WrapCraneErr(util.ErrorCmdArg, "Failed to parse YAML config file: %v\n", err)
	}
	if dbConf.Database == nil {
		return nil, util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("Database section not found in YAML"))
	}

	return dbConf.Database, nil
}

func MissingElements(ConfigNodesList []util.ConfigNodesList, nodes []string) ([]string, error) {
	nodeNameSet := make(map[string]struct{})
	nodeNameList, err := util.GetValidNodeList(config.CranedNodeList)
	if err != nil {
		return nil, err
	}

	for _, name := range nodeNameList {
		nodeNameSet[name] = struct{}{}
	}

	missing := []string{}
	for _, node := range nodes {
		if _, exists := nodeNameSet[node]; !exists {
			missing = append(missing, node)
		}
	}

	return missing, nil
}

func QueryInfluxDbDataByTags(eventConfig *util.InfluxDbConfig, clusterName string, nodes []string) ([]*ResourceUsageRecord, error) {
	client := influxdb2.NewClient(eventConfig.Url, eventConfig.Token)
	defer client.Close()

	ctx := context.Background()
	if pong, err := client.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping InfluxDB: %v", err)
	} else if !pong {
		return nil, fmt.Errorf("failed to ping InfluxDB: not pong")
	}

	nodeNameFilters := make([]string, len(nodes))
	for i, nodeName := range nodes {
		nodeNameFilters[i] = fmt.Sprintf(`r["node_name"] == "%s"`, nodeName)
	}
	nodeNameCondition := strings.Join(nodeNameFilters, " or ")

	clusterNameCondition := fmt.Sprintf(`r["cluster_name"] == "%s"`, clusterName)

	fluxQuery := fmt.Sprintf(`
	from(bucket: "%s")
	|> range(start: 0)
	|> filter(fn: (r) => 
	    r["_measurement"] == "%s" and 
		(r["_field"] == "state" or r["_field"] == "uid" or 
		r["_field"] == "reason" or r["_field"] == "start_time") and
		(%s) and (%s))`, eventConfig.Bucket,
		eventConfig.Measurement, nodeNameCondition, clusterNameCondition)

	queryAPI := client.QueryAPI(eventConfig.Org)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("execute query failed: %w", err)
	}

	// Parse and aggregate the query results
	dataMap := make(map[string]*ResourceUsageRecord)
	for result.Next() {
		record := result.Record()

		clusterName := fmt.Sprintf("%v", record.ValueByKey("cluster_name"))
		nodeName := fmt.Sprintf("%v", record.ValueByKey("node_name"))
		field := fmt.Sprintf("%v", record.Field())
		timestamp := record.Time()

		key := fmt.Sprintf("%s:%s:%s", clusterName, nodeName, timestamp)
		if _, exists := dataMap[key]; !exists {
			dataMap[key] = &ResourceUsageRecord{
				ClusterName: clusterName,
				NodeName:    nodeName,
				Timestamp:   timestamp,
			}
		}

		switch field {
		case "uid":
			if uid, ok := record.Value().(uint64); ok {
				dataMap[key].Uid = uid
			}
		case "start_time":
			if startTime, ok := record.Value().(int64); ok {
				dataMap[key].StartTime = startTime
			}
		case "state":
			if state, ok := record.Value().(int64); ok {
				dataMap[key].State = util.StateToString(state)
			}
		case "reason":
			if reason, ok := record.Value().(string); ok {
				dataMap[key].Reason = reason
			}
		}
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query parsing error: %w", result.Err())
	}

	var records []*ResourceUsageRecord
	for _, record := range dataMap {
		records = append(records, record)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("no matching data available")
	}

	sort.SliceStable(records, func(i, j int) bool {
		return records[i].Timestamp.Before(records[j].Timestamp)
	})

	return records, nil
}

func QueryEventInfoByNodes(nodeRegex string) error {
	if FlagForce {
		log.Warning("--force flag is ignored for query operations")
	}
	nodeNames := []string{}
	var ok bool
	if len(nodeRegex) != 0 {
		nodeNames, ok = util.ParseHostList(nodeRegex)
		if !ok {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid node pattern: %s.\n", nodeRegex))
		}
	}

	if len(nodeNames) > 0 {
		missingList, err := MissingElements(config.CranedNodeList, nodeNames)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid input for nodes: %v\n", err)
		}
		if len(missingList) > 0 {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid input nodes: %v\n", missingList))
		}
	} else {
		var err error
		nodeNames, err = util.GetValidNodeList(config.CranedNodeList)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid input for nodes: %v\n", err)
		}
	}

	if len(config.ClusterName) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("ClusterName empty"))
	}

	// Query Resource Usage Records in InfluxDB
	result, err := QueryInfluxDbDataByTags(dbConfig, config.ClusterName, nodeNames)
	if err != nil {
		return util.WrapCraneErr(util.ErrorBackend, "Failed to query job info from InfluxDB: %v\n", err)
	}

	filteredRecords, err := SortRecords(result)
	if err != nil {
		return util.WrapCraneErr(util.ErrorCmdArg, "Failed to sort records: %v\n", err)
	}

	if FlagJson {
		eventJsonList := []*EventInfoJson{}
		for _, record := range filteredRecords {
			startTime := FormatNanoTime(record.StartTime)
			endTime := FormatNanoTime(record.EndTime)
			eventJson := &EventInfoJson{
				ClusterName: record.ClusterName,
				NodeName:    record.NodeName,
				Uid:         record.Uid,
				StartTime:   startTime,
				EndTime:     endTime,
				State:       record.State,
				Reason:      record.Reason,
			}
			eventJsonList = append(eventJsonList, eventJson)
		}
		jsonData, err := json.MarshalIndent(eventJsonList, "", "  ")
		if err != nil {
			return util.WrapCraneErr(util.ErrorBackend, "Failed to marshal data to JSON: %v\n", err)
		}
		fmt.Println(string(jsonData))
		return nil
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(true)
	table.SetHeader([]string{"Node", "StartTime", "EndTime", "State", "Reason", "Uid"})

	for _, record := range filteredRecords {
		startTime := FormatNanoTime(record.StartTime)
		endTime := FormatNanoTime(record.EndTime)
		table.Append([]string{
			record.NodeName,
			startTime,
			endTime,
			record.State,
			record.Reason,
			strconv.FormatUint(record.Uid, 10),
		})
	}

	table.Render()
	return nil
}

func FormatNanoTime(ns int64) string {
	if ns == 0 || time.Unix(0, int64(ns)).Year() == 1970 {
		return "Unknown"
	}
	return time.Unix(0, int64(ns)).In(time.Local).Format("2006-01-02 15:04:05")
}

func SortRecords(records []*ResourceUsageRecord) ([]*ResourceUsageRecord, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("records list is empty")
	}

	// Sort the records by NodeName in ascending order
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].NodeName < records[j].NodeName
	})

	drainMap := make(map[string]*ResourceUsageRecord)
	var filteredRecords []*ResourceUsageRecord
	for _, currentRecord := range records {
		if currentRecord.State == "Resume" {
			if previousRecord, exists := drainMap[currentRecord.NodeName]; exists {
				previousRecord.EndTime = currentRecord.StartTime
				continue
			}
		} else if currentRecord.State == "Drain" {
			drainMap[currentRecord.NodeName] = currentRecord
		}

		filteredRecords = append(filteredRecords, currentRecord)
	}

	// Sort the filteredRecords by StartTime
	sort.SliceStable(filteredRecords, func(i, j int) bool {
		return filteredRecords[i].StartTime > filteredRecords[j].StartTime
	})

	if FlagNumLimit > 0 && len(filteredRecords) > int(FlagNumLimit) {
		filteredRecords = filteredRecords[:FlagNumLimit]
	}

	return filteredRecords, nil
}

func ResetUserCredential(value string) error {
	var userList []string

	if value == "" {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintln("User is empty"))
	}

	if value != "all" {
		var err error
		userList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid user list specified: %v.\n", err)
		}
	}

	req := protos.ResetUserCredentialRequest{Uid: userUid, UserList: userList}
	reply, err := stub.ResetUserCredential(context.Background(), &req)
	if err != nil {
		return util.NewCraneErrFromGrpc(util.ErrorNetwork, err, "Failed to reset user credential")
	}
	if FlagJson {
		msg := fmt.Sprintln(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			fmt.Print(msg)
			return nil
		} else {
			return util.NewCraneErr(util.ErrorBackend, msg)
		}
	}

	if !reply.GetOk() {
		msg := ""
		for _, richError := range reply.RichErrorList {
			msg += fmt.Sprintf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.NewCraneErr(util.ErrorBackend, msg)
	}
	fmt.Printf("reset user %s credential succeeded.\n", value)
	return nil
}
