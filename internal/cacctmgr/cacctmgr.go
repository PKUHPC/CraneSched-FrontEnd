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
	"math"
	"net"
	"os"
	"os/user"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"github.com/xlab/treeprint"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	userUid uint32
	stub    protos.CraneCtldClient
	config  *util.Config
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

func AddAccount(account *protos.AccountInfo) util.ExitCode {
	if FlagForce {
		log.Warning("The --force flag is ignored for add operations")
	}
	if err := util.CheckEntityName(account.Name); err != nil {
		log.Errorf("Failed to add account: invalid account name: %v", err)
		return util.ErrorCmdArg
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
			log.Errorf("Failed to add account: default QoS %s is not in allowed QoS list", account.DefaultQos)
			return util.ErrorCmdArg
		}
	}

	reply, err := stub.AddAccount(context.Background(), req)
	if err != nil {
		log.Errorf("Failed to add account: %v", err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	if reply.GetOk() {
		fmt.Println("Account added successfully.")
		return util.ErrorSuccess
	} else {
		fmt.Printf("Failed to add account: %s.\n", util.ErrMsg(reply.GetCode()))
		return util.ErrorBackend
	}
}

func AddUser(user *protos.UserInfo, partition []string, level string, coordinator bool) util.ExitCode {
	if FlagForce {
		log.Warning("The --force flag is ignored for add operations")
	}
	var err error
	if err = util.CheckEntityName(user.Name); err != nil {
		log.Errorf("Failed to add user: invalid user name: %v", err)
		return util.ErrorCmdArg
	}

	user.Uid, err = util.GetUidByUserName(user.Name)
	if err != nil {
		log.Errorf("Failed to add user: %v", err)
		return util.ErrorCmdArg
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
		log.Errorf("Failed to add user: unknown admin level, valid values: none, operator, admin.")
		return util.ErrorCmdArg
	}

	if coordinator {
		user.CoordinatorAccounts = append(user.CoordinatorAccounts, user.Account)
	}

	reply, err := stub.AddUser(context.Background(), req)
	if err != nil {
		log.Errorf("Failed to add user: %v", err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	if reply.GetOk() {
		fmt.Println("User added successfully.")
		return util.ErrorSuccess
	} else {
		fmt.Printf("Failed to add user: %s.\n", util.ErrMsg(reply.GetCode()))
		return util.ErrorBackend
	}
}

func AddQos(qos *protos.QosInfo) util.ExitCode {
	if FlagForce {
		log.Warning("The --force flag is ignored for add operations")
	}
	if err := util.CheckEntityName(qos.Name); err != nil {
		log.Errorf("Failed to add QoS: invalid QoS name: %v", err)
		return util.ErrorCmdArg
	}

	req := new(protos.AddQosRequest)
	req.Uid = userUid
	req.Qos = qos

	reply, err := stub.AddQos(context.Background(), req)
	if err != nil {
		log.Errorf("Failed to add QoS: %v", err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	if reply.GetOk() {
		fmt.Println("QoS added successfully.")
		return util.ErrorSuccess
	} else {
		fmt.Printf("Failed to add QoS: %s.\n", util.ErrMsg(reply.GetCode()))
		return util.ErrorBackend
	}
}

func DeleteAccount(value string) util.ExitCode {
	if FlagForce {
		log.Warning("--force flag is ignored for delete operations")
	}

	accountList, err := util.ParseStringParamList(value, ",")
	if err != nil {
		log.Errorf("Invalid user list specified: %v.\n", err)
		return util.ErrorCmdArg
	}

	req := protos.DeleteAccountRequest{Uid: userUid, AccountList: accountList}

	reply, err := stub.DeleteAccount(context.Background(), &req)
	if err != nil {
		log.Errorf("Failed to delete account %s: %v", value, err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	if reply.GetOk() {
		fmt.Printf("Successfully deleted account '%s'.\n", value)
		return util.ErrorSuccess
	} else {
		fmt.Printf("Failed to delete account: \n")
		for _, richError := range reply.RichErrorList {
			fmt.Printf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.ErrorBackend
	}
}

func DeleteUser(value string, account string) util.ExitCode {
	userList, err := util.ParseStringParamList(value, ",")
	if err != nil {
		log.Errorf("Invalid user list specified: %v.\n", err)
		return util.ErrorCmdArg
	}

	if slices.Contains(userList, "ALL") {
		if !FlagForce {
			log.Errorf("To delete all users in the account, you must set --force.")
			return util.ErrorCmdArg
		}
		userList = []string{"ALL"}
	}

	req := protos.DeleteUserRequest{Uid: userUid, UserList: userList, Account: account, Force: FlagForce}

	reply, err := stub.DeleteUser(context.Background(), &req)
	if err != nil {
		log.Errorf("Failed to remove user %s: %v", value, err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	if reply.GetOk() {
		fmt.Printf("Successfully removed user '%s'.\n", value)
		return util.ErrorSuccess
	} else {
		fmt.Printf("Failed to remove user: \n")
		for _, richError := range reply.RichErrorList {
			fmt.Printf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.ErrorBackend
	}
}

func DeleteQos(value string) util.ExitCode {
	if FlagForce {
		log.Warning("--force flag is ignored for delete operations")
	}

	qosList, err := util.ParseStringParamList(value, ",")
	if err != nil {
		log.Errorf("Invalid user list specified: %v.\n", err)
		return util.ErrorCmdArg
	}
	req := protos.DeleteQosRequest{Uid: userUid, QosList: qosList}

	reply, err := stub.DeleteQos(context.Background(), &req)
	if err != nil {
		log.Errorf("Failed to delete QoS %s: %v", value, err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	if reply.GetOk() {
		fmt.Printf("Successfully deleted QoS '%s'.\n", value)
		return util.ErrorSuccess
	} else {
		fmt.Printf("Failed to delete QoS: \n")
		for _, richError := range reply.RichErrorList {
			fmt.Printf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.ErrorBackend
	}
}

func ModifyAccount(modifyField protos.ModifyField, newValue string, name string, requestType protos.OperationType) util.ExitCode {
	var valueList []string
	var err error

	valueList, err = util.ParseStringParamList(newValue, ",")
	if err != nil {
		if modifyField == protos.ModifyField_Qos {
			log.Errorf("Invalid qos list specified: %v.\n", err)
		} else if modifyField == protos.ModifyField_Partition {
			log.Errorf("Invalid partition list specified: %v.\n", err)
		} else {
			log.Errorf("Invalid value list specified: %v.\n", err)
		}
		return util.ErrorCmdArg
	}

	if modifyField == protos.ModifyField_DefaultQos || modifyField == protos.ModifyField_Description {
		if len(valueList) != 1 {
			log.Errorf("Invalid value specified! Modify Description and DefaultQos, please provide only one value.")
			return util.ErrorCmdArg
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
		log.Errorf("Failed to modify account information: %v", err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	if reply.GetOk() {
		fmt.Println("Information was successfully modified.")
		return util.ErrorSuccess
	} else {
		fmt.Printf("Failed to modify information:\n")
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				fmt.Printf("%s \n", util.ErrMsg(richError.Code))
			} else {
				fmt.Printf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
			}
		}
		return util.ErrorBackend
	}
}

func ModifyUser(modifyField protos.ModifyField, newValue string, name string, account string, partition string, requestType protos.OperationType) util.ExitCode {
	if modifyField == protos.ModifyField_AdminLevel {
		if newValue != "none" && newValue != "operator" && newValue != "admin" {
			log.Errorf("Unknown admin level, valid values: none, operator, admin.")
			return util.ErrorCmdArg
		}
	}

	var valueList []string
	var err error

	valueList, err = util.ParseStringParamList(newValue, ",")
	if err != nil {
		log.Errorf("Invalid value list specified: %v.\n", err)
		return util.ErrorCmdArg
	}

	if modifyField == protos.ModifyField_AdminLevel || modifyField == protos.ModifyField_DefaultQos || modifyField == protos.ModifyField_DefaultAccount {
		if len(valueList) != 1 {
			log.Errorf("Invalid value specified! Modify AdminLevel, DefaultAccount and DefaultQos, please provide only one value.")
			return util.ErrorCmdArg
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
		log.Errorf("Failed to modify user information: %v", err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	if reply.GetOk() {
		fmt.Println("Modify information succeeded.")
		return util.ErrorSuccess
	} else {
		fmt.Printf("Modify information failed: \n")
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				fmt.Printf("%s \n", util.ErrMsg(richError.Code))
			} else {
				fmt.Printf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
			}
		}
		return util.ErrorBackend
	}
}

func ModifyQos(modifyField protos.ModifyField, newValue string, name string) util.ExitCode {
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
		log.Errorf("Failed to modify QoS: %v", err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	if reply.GetOk() {
		fmt.Println("Modify information succeeded.")
		return util.ErrorSuccess
	} else {
		fmt.Printf("Modify information failed: %s.\n", util.ErrMsg(reply.GetCode()))
		return util.ErrorBackend
	}
}

func FindAccount(value string) util.ExitCode {
	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
	}
	var accountList []string
	if value != "" {
		var err error
		accountList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			log.Errorf("Invalid account list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
	}

	req := protos.QueryAccountInfoRequest{Uid: userUid, AccountList: accountList}
	reply, err := stub.QueryAccountInfo(context.Background(), &req)
	if err != nil {
		log.Errorf("Failed to find account: %v", err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}

	if !reply.GetOk() {
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				fmt.Println(util.ErrMsg(richError.Code))
				break
			}
			fmt.Printf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
	}

	PrintAccountList(reply.AccountList)
	return util.ErrorSuccess
}

func BlockAccountOrUser(value string, entityType protos.EntityType, account string) util.ExitCode {
	if FlagForce {
		log.Warning("--force flag is ignored for block operations")
	}

	var entityList []string
	if value != "all" {
		var err error
		entityList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			log.Errorf("Invalid account/user list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
	}

	req := protos.BlockAccountOrUserRequest{Uid: userUid, Block: true, EntityType: entityType, EntityList: entityList, Account: account}
	reply, err := stub.BlockAccountOrUser(context.Background(), &req)
	if err != nil {
		log.Errorf("Failed to block entity: %v", err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	if reply.GetOk() {
		fmt.Printf("Block %s succeeded.\n", value)
		return util.ErrorSuccess
	} else {
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				fmt.Printf("%s \n", util.ErrMsg(richError.Code))
				break
			}
			fmt.Printf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.ErrorBackend
	}
}

func UnblockAccountOrUser(value string, entityType protos.EntityType, account string) util.ExitCode {
	if FlagForce {
		log.Warning("--force flag is ignored for unblock operations")
	}

	var entityList []string
	if value != "all" {
		var err error
		entityList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			log.Errorf("Invalid account/user list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
	}

	req := protos.BlockAccountOrUserRequest{Uid: userUid, Block: false, EntityType: entityType, EntityList: entityList, Account: account}
	reply, err := stub.BlockAccountOrUser(context.Background(), &req)
	if err != nil {
		log.Errorf("Failed to unblock entity: %v", err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}
	if reply.GetOk() {
		fmt.Printf("Unblock %s succeeded.\n", value)
		return util.ErrorSuccess
	} else {
		for _, richError := range reply.RichErrorList {
			if richError.Description == "" {
				fmt.Printf("%s \n", util.ErrMsg(richError.Code))
				break
			}
			fmt.Printf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.ErrorBackend
	}
}

// GetPlugindClient connects to cplugind for querying event data via RPC
func GetPlugindClient(config *util.Config) (protos.PluginQueryServiceClient, *grpc.ClientConn, error) {
	if !config.Plugin.Enabled {
		return nil, nil, util.NewCraneErr(util.ErrorCmdArg, "Plugin is not enabled")
	}

	addr := config.Plugin.ListenAddress
	port := config.Plugin.ListenPort
	if addr == "" || port == "" {
		return nil, nil, util.NewCraneErr(util.ErrorCmdArg,
			"PlugindListenAddress and PlugindListenPort must be configured")
	}

	endpoint := net.JoinHostPort(addr, port)
	var creds credentials.TransportCredentials
	if config.TlsConfig.Enabled {
		certPath := config.TlsConfig.CaFilePath
		if certPath == "" {
			return nil, nil, util.NewCraneErr(util.ErrorCmdArg,
				"TLS is enabled for plugin client but no certificate file is configured")
		}
		var err error
		creds, err = credentials.NewClientTLSFromFile(certPath, "")
		if err != nil {
			return nil, nil, util.NewCraneErr(util.ErrorCmdArg,
				fmt.Sprintf("Failed to load TLS credentials: %v", err))
		}
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(util.ClientKeepAliveParams),
		grpc.WithConnectParams(util.ClientConnectParams),
		grpc.WithIdleTimeout(time.Duration(math.MaxInt64)),
	)
	if err != nil {
		return nil, nil, util.NewCraneErr(util.ErrorNetwork,
			fmt.Sprintf("Failed to connect to cplugind at %s: %v", endpoint, err))
	}

	return protos.NewPluginQueryServiceClient(conn), conn, nil
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

func QueryEventInfoByNodes(nodeRegex string) util.ExitCode {
	if FlagForce {
		log.Warning("--force flag is ignored for query operations")
	}

	// Parse node names if provided
	nodeNames := []string{}
	var ok bool
	if len(nodeRegex) != 0 {
		nodeNames, ok = util.ParseHostList(nodeRegex)
		if !ok {
			log.Errorf("Invalid node pattern: %s.\n", nodeRegex)
			return util.ErrorCmdArg
		}

		// Validate nodes exist in configuration
		missingList, err := MissingElements(config.CranedNodeList, nodeNames)
		if err != nil {
			log.Errorf("Invalid input for nodes: %v", err)
			return util.ErrorCmdArg
		}
		if len(missingList) > 0 {
			log.Errorf("Invalid input nodes: %v", missingList)
			return util.ErrorCmdArg
		}
	}
	// If no nodes specified, nodeNames will be empty and query all nodes

	// Connect to cplugind
	pluginClient, pluginConn, err := GetPlugindClient(config)
	if err != nil {
		log.Errorf("Failed to connect to cplugind: %v", err)
		return util.ErrorNetwork
	}
	defer pluginConn.Close()

	// Get current user for authorization
	currentUser, err := user.Current()
	if err != nil {
		log.Errorf("Failed to get current user: %v", err)
		return util.ErrorGeneric
	}

	uid, err := strconv.ParseUint(currentUser.Uid, 10, 32)
	if err != nil {
		log.Errorf("Failed to parse user ID: %v", err)
		return util.ErrorGeneric
	}

	// Query events via RPC
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req := &protos.QueryNodeEventsRequest{
		NodeNames: nodeNames,
		Uid:       uint32(uid),
	}

	reply, err := pluginClient.QueryNodeEvents(ctx, req)
	if err != nil {
		log.Errorf("Failed to query node events: %v", err)
		return util.ErrorBackend
	}

	if !reply.Ok {
		log.Errorf("Query node events failed: %s", reply.ErrorMessage)
		return util.ErrorBackend
	}

	eventInfoList := reply.EventInfoList
	if len(eventInfoList) == 0 {
		log.Info("No event data found")
		return util.ErrorSuccess
	}

	// Sort events by start time
	sort.SliceStable(eventInfoList, func(i, j int) bool {
		return eventInfoList[i].StartTime < eventInfoList[j].StartTime
	})

	if FlagJson {
		eventJsonList := []*EventInfoJson{}
		for _, event := range eventInfoList {
			startTime := FormatNanoTime(event.StartTime)
			endTime := FormatNanoTime(event.EndTime)
			eventJson := &EventInfoJson{
				ClusterName: event.ClusterName,
				NodeName:    event.NodeName,
				Uid:         event.Uid,
				StartTime:   startTime,
				EndTime:     endTime,
				State:       event.State,
				Reason:      event.Reason,
			}
			eventJsonList = append(eventJsonList, eventJson)
		}
		jsonData, err := json.MarshalIndent(eventJsonList, "", "  ")
		if err != nil {
			log.Errorf("Failed to marshal data to JSON: %v", err)
			return util.ErrorBackend
		}
		fmt.Println(string(jsonData))
		return util.ErrorSuccess
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(true)
	table.SetHeader([]string{"Node", "StartTime", "EndTime", "State", "Reason", "Uid"})

	for _, event := range eventInfoList {
		startTime := FormatNanoTime(event.StartTime)
		endTime := FormatNanoTime(event.EndTime)
		table.Append([]string{
			event.NodeName,
			startTime,
			endTime,
			event.State,
			event.Reason,
			strconv.FormatUint(event.Uid, 10),
		})
	}

	table.Render()
	return util.ErrorSuccess
}

func FormatNanoTime(ns int64) string {
	if ns == 0 || time.Unix(0, int64(ns)).Year() == 1970 {
		return "Unknown"
	}
	return time.Unix(0, int64(ns)).In(time.Local).Format("2006-01-02 15:04:05")
}

func ResetUserCredential(value string) util.ExitCode {
	var userList []string

	if value == "" {
		log.Errorf("User is empty")
		return util.ErrorCmdArg
	}

	if value != "all" {
		var err error
		userList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			log.Errorf("Invalid user list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
	}

	req := protos.ResetUserCredentialRequest{Uid: userUid, UserList: userList}
	reply, err := stub.ResetUserCredential(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to reset user credential")
		return util.ErrorNetwork
	}
	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}

	if !reply.GetOk() {
		for _, richError := range reply.RichErrorList {
			fmt.Printf("%s: %s \n", richError.Description, util.ErrMsg(richError.Code))
		}
		return util.ErrorBackend
	}

	fmt.Printf("reset user %s credential succeeded.\n", value)
	return util.ErrorSuccess
}
