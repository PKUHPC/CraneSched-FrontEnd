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
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"github.com/xlab/treeprint"
	"gopkg.in/yaml.v3"
)

var (
	userUid          uint32
	stub             protos.CraneCtldClient
	config           *util.Config
	dbConfig         *util.InfluxDbConfig
	dbConfigInitOnce sync.Once
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

func PrintUserList(userList []*protos.UserInfo) {
	if len(userList) == 0 {
		return
	}

	sort.Slice(userList, func(i, j int) bool {
		return userList[i].Uid < userList[j].Uid
	})

	// Slice to map
	userMap := make(map[string][]*protos.UserInfo)
	for _, userInfo := range userList {
		key := ""
		if userInfo.Account[len(userInfo.Account)-1] == '*' {
			key = userInfo.Account[:len(userInfo.Account)-1]
		} else {
			key = userInfo.Account
		}
		if list, ok := userMap[key]; ok {
			userMap[key] = append(list, userInfo)
		} else {
			var list = []*protos.UserInfo{userInfo}
			userMap[key] = list
		}
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader([]string{"Account", "UserName", "Uid", "AllowedPartition", "AllowedQosList", "DefaultQos", "Coordinated", "AdminLevel", "Blocked"})
	tableData := make([][]string, len(userMap))
	for _, value := range userMap {
		for _, userInfo := range value {
			if len(userInfo.AllowedPartitionQosList) == 0 {
				tableData = append(tableData, []string{
					userInfo.Account,
					userInfo.Name,
					strconv.FormatUint(uint64(userInfo.Uid), 10),
					"",
					"",
					"",
					"",
					fmt.Sprintf("%v", userInfo.AdminLevel),
					strconv.FormatBool(userInfo.Blocked),
				})
			}
			for _, allowedPartitionQos := range userInfo.AllowedPartitionQosList {
				tableData = append(tableData, []string{
					userInfo.Account,
					userInfo.Name,
					strconv.FormatUint(uint64(userInfo.Uid), 10),
					allowedPartitionQos.PartitionName,
					strings.Join(allowedPartitionQos.QosList, ", "),
					allowedPartitionQos.DefaultQos,
					strings.Join(userInfo.CoordinatorAccounts, ", "),
					fmt.Sprintf("%v", userInfo.AdminLevel),
					strconv.FormatBool(userInfo.Blocked),
				})
			}
		}
	}

	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableData)
	}

	table.AppendBulk(tableData)
	table.Render()
}

func PrintTxnLogList(txnLogList []*protos.QueryTxnLogReply_Txn) {
	if len(txnLogList) == 0 {
		return
	}

	var TxnActionToString = []string{
		"Add Account", "Modify Account", "Delete Account",
		"Add User", "Modify User", "Delete User",
		"Add QoS", "Modify QoS", "Delete QoS",
	}

	sort.Slice(txnLogList, func(i, j int) bool { return txnLogList[i].CreationTime < txnLogList[j].CreationTime })

	// Table format control
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"CreationTime", "Actor", "Action", "Target", "Info"})
	tableData := make([][]string, 0, len(txnLogList))
	for _, txnLog := range txnLogList {

		CreationTime := time.Unix(txnLog.CreationTime, 0)

		tableData = append(tableData, []string{
			CreationTime.Format("2006-01-02 15:04:05"),
			txnLog.Actor,
			TxnActionToString[txnLog.Action],
			txnLog.Target,
			txnLog.Info})
	}

	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableData)
	}

	table.AppendBulk(tableData)
	table.Render()
}

func PrintQosList(qosList []*protos.QosInfo) {
	if len(qosList) == 0 {
		return
	}

	sort.Slice(qosList, func(i, j int) bool {
		return qosList[i].Name < qosList[j].Name
	})

	// Table format control
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader([]string{"Name", "Description", "Priority", "MaxJobsPerUser", "MaxCpusPerUser", "MaxTimeLimitPerTask"})
	tableData := make([][]string, len(qosList))
	for _, info := range qosList {
		var timeLimitStr string
		if info.MaxTimeLimitPerTask >= util.MaxJobTimeLimit {
			timeLimitStr = "unlimited"
		} else {
			timeLimitStr = util.SecondTimeFormat(int64(info.MaxTimeLimitPerTask))
		}
		var jobsPerUserStr string
		if info.MaxJobsPerUser == math.MaxUint32 {
			jobsPerUserStr = "unlimited"
		} else {
			jobsPerUserStr = strconv.FormatUint(uint64(info.MaxJobsPerUser), 10)
		}
		var cpusPerUserStr string
		if info.MaxCpusPerUser == math.MaxUint32 {
			cpusPerUserStr = "unlimited"
		} else {
			cpusPerUserStr = strconv.FormatUint(uint64(info.MaxCpusPerUser), 10)
		}
		tableData = append(tableData, []string{
			info.Name,
			info.Description,
			fmt.Sprint(info.Priority),
			fmt.Sprint(jobsPerUserStr),
			fmt.Sprint(cpusPerUserStr),
			fmt.Sprint(timeLimitStr)})
	}

	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableData)
	}

	table.AppendBulk(tableData)
	table.Render()
}

func PrintAccountList(accountList []*protos.AccountInfo) {
	if len(accountList) == 0 {
		return
	}

	sort.Slice(accountList, func(i, j int) bool {
		return accountList[i].Name < accountList[j].Name
	})

	// Slice to map and find the root account
	accountMap := make(map[string]*protos.AccountInfo)
	rootAccount := make([]string, 0)
	for _, accountInfo := range accountList {
		accountMap[accountInfo.Name] = accountInfo
	}
	for _, accountInfo := range accountList {
		if accountInfo.ParentAccount == "" || func() bool {
			_, ok := accountMap[accountInfo.ParentAccount]
			return !ok
		}() {
			rootAccount = append(rootAccount, accountInfo.Name)
		}
	}

	// Print account tree
	tree := treeprint.NewWithRoot("AccountTree")
	for _, account := range rootAccount {
		PrintAccountTree(tree, account, accountMap)
	}
	fmt.Println(tree.String())

	// Print account table
	PrintAccountTable(accountList)
}

func PrintAccountTable(accountList []*protos.AccountInfo) {
	table := tablewriter.NewWriter(os.Stdout) //table format control
	header := []string{"Name", "Description", "AllowedPartition", "Users", "DefaultQos", "AllowedQosList", "Coordinators", "Blocked"}
	util.SetBorderTable(table)
	tableData := make([][]string, len(accountList))
	for _, accountInfo := range accountList {
		tableData = append(tableData, []string{
			accountInfo.Name,
			accountInfo.Description,
			strings.Join(accountInfo.AllowedPartitions, ", "),
			strings.Join(accountInfo.Users, ", "),
			accountInfo.DefaultQos,
			strings.Join(accountInfo.AllowedQosList, ", "),
			strings.Join(accountInfo.Coordinators, ", "),
			strconv.FormatBool(accountInfo.Blocked),
		})
	}
	if FlagFormat != "" {
		formatTableData := make([][]string, len(accountList))
		formatReq := strings.Split(FlagFormat, " ")
		tableOutputWidth := make([]int, len(formatReq))
		tableOutputHeader := make([]string, len(formatReq))
		for i := 0; i < len(formatReq); i++ {
			if formatReq[i][0] != '%' || len(formatReq[i]) < 2 {
				log.Errorln("Invalid format.")
				os.Exit(util.ErrorInvalidFormat)
			}
			if formatReq[i][1] == '.' {
				if len(formatReq[i]) < 4 {
					log.Errorln("Invalid format.")
					os.Exit(util.ErrorInvalidFormat)
				}
				width, err := strconv.ParseUint(formatReq[i][2:len(formatReq[i])-1], 10, 32)
				if err != nil {
					log.Errorln("Invalid format.")
					os.Exit(util.ErrorInvalidFormat)
				}
				tableOutputWidth[i] = int(width)
			} else {
				tableOutputWidth[i] = -1
			}
			tableOutputHeader[i] = formatReq[i][len(formatReq[i])-1:]
			//"Name", "Description", "AllowedPartition", "DefaultQos", "AllowedQosList, "Coordinators"
			switch tableOutputHeader[i] {
			case "n":
				tableOutputHeader[i] = "Name"
				for j := 0; j < len(accountList); j++ {
					formatTableData[j] = append(formatTableData[j], accountList[j].Name)
				}
			case "d":
				tableOutputHeader[i] = "Description"
				for j := 0; j < len(accountList); j++ {
					formatTableData[j] = append(formatTableData[j], accountList[j].Description)
				}
			case "P":
				tableOutputHeader[i] = "AllowedPartition"
				for j := 0; j < len(accountList); j++ {
					formatTableData[j] = append(formatTableData[j], strings.Join(accountList[j].AllowedPartitions, ", "))
				}
			case "Q":
				tableOutputHeader[i] = "DefaultQos"
				for j := 0; j < len(accountList); j++ {
					formatTableData[j] = append(formatTableData[j], accountList[j].DefaultQos)
				}
			case "q":
				tableOutputHeader[i] = "AllowedQosList"
				for j := 0; j < len(accountList); j++ {
					formatTableData[j] = append(formatTableData[j], strings.Join(accountList[j].AllowedQosList, ", "))
				}
			default:
				log.Errorln("Invalid format.")
				os.Exit(util.ErrorInvalidFormat)
			}
		}
		header, tableData = util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
	}

	if !FlagNoHeader {
		table.SetHeader(header)
	}

	if !FlagFull && FlagFormat == "" {
		// The data in the fifth column is AllowedQosList, which is not trim
		util.TrimTableExcept(&tableData, 5)
	}

	table.AppendBulk(tableData)
	table.Render()
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

func AddAccount(account *protos.AccountInfo) util.CraneCmdError {
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

func AddUser(user *protos.UserInfo, partition []string, level string, coordinator bool) util.CraneCmdError {
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

func AddQos(qos *protos.QosInfo) util.CraneCmdError {
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

func DeleteAccount(value string) util.CraneCmdError {

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

func DeleteUser(value string, account string) util.CraneCmdError {

	var userList []string
	if value != "all" {
		var err error
		userList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			log.Errorf("Invalid user list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
	} else if !FlagForce {
		log.Errorf("To delete all users in the account, you must set --force.")
		return util.ErrorCmdArg
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

func DeleteQos(value string) util.CraneCmdError {

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

func ModifyAccount(modifyField protos.ModifyField, newValue string, name string, requestType protos.OperationType) util.CraneCmdError {
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

func ModifyUser(modifyField protos.ModifyField, newValue string, name string, account string, partition string, requestType protos.OperationType) util.CraneCmdError {
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

func ModifyQos(modifyField protos.ModifyField, newValue string, name string) util.CraneCmdError {
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

func ShowAccounts() util.CraneCmdError {
	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
	}
	req := protos.QueryAccountInfoRequest{Uid: userUid}
	reply, err := stub.QueryAccountInfo(context.Background(), &req)
	if err != nil {
		log.Errorf("Failed to show accounts: %v", err)
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

func ShowUser(value string, account string) util.CraneCmdError {

	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
	}

	var userList []string
	if value != "" {
		var err error
		userList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			log.Errorf("Invalid user list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
	}

	req := protos.QueryUserInfoRequest{Uid: userUid, UserList: userList, Account: account}
	reply, err := stub.QueryUserInfo(context.Background(), &req)
	if err != nil {
		log.Errorf("Failed to show user: %v", err)
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

	PrintUserList(reply.UserList)

	return util.ErrorSuccess
}

func ShowQos(value string) util.CraneCmdError {
	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
	}
	var qosList []string
	if value != "" {
		var err error
		qosList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			log.Errorf("Invalid user list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
	}
	req := protos.QueryQosInfoRequest{Uid: userUid, QosList: qosList}
	reply, err := stub.QueryQosInfo(context.Background(), &req)
	if err != nil {
		log.Errorf("Failed to show QoS: %v", err)
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

	PrintQosList(reply.QosList)

	return util.ErrorSuccess
}

func FindAccount(value string) util.CraneCmdError {
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

func BlockAccountOrUser(value string, entityType protos.EntityType, account string) util.CraneCmdError {

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

func UnblockAccountOrUser(value string, entityType protos.EntityType, account string) util.CraneCmdError {

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

func ShowTxn(actor string, target string, actionValue string, info string, startTimeValue string) util.CraneCmdError {
	if FlagForce {
		log.Warning("--force flag is ignored for unblock operations")
	}

	var action string
	if actionValue != "" {
		value, ok := util.StringToTxnAction(actionValue)
		if !ok {
			log.Errorf("Invalid action specified: %v.\n", actionValue)
			return util.ErrorCmdArg
		}

		action = strconv.Itoa(int(value))
	}

	req := protos.QueryTxnLogRequest{
		Uid:    userUid,
		Actor:  actor,
		Target: target,
		Action: action,
		Info:   info,
	}

	if startTimeValue != "" {
		req.TimeInterval = &protos.TimeInterval{}
		err := util.ParseInterval(startTimeValue, req.TimeInterval)
		if err != nil {
			log.Error(err)
			return util.ErrorCmdArg
		}
	}

	reply, err := stub.QueryTxnLog(context.Background(), &req)
	if err != nil {
		log.Errorf("Failed to show txn: %v", err)
		return util.ErrorNetwork
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		}
		return util.ErrorBackend
	}

	if !reply.GetOk() {
		fmt.Println(util.ErrMsg(reply.GetCode()))
		return util.ErrorBackend
	}

	PrintTxnLogList(reply.TxnLogList)

	return util.ErrorSuccess
}

// Extracts the EventPlugin InfluxDB configuration from the specified YAML configuration files
func GetEventPluginConfig(config *util.Config) (*util.InfluxDbConfig, util.CraneCmdError) {
	if !config.Plugin.Enabled {
		log.Errorf("Plugin is not enabled")
		return nil, util.ErrorCmdArg
	}

	var eventConfigPath string
	for _, plugin := range config.Plugin.Plugins {
		if plugin.Name == "event" {
			eventConfigPath = plugin.Config
			break
		}
	}

	if eventConfigPath == "" {
		log.Errorf("event plugin not found")
		return nil, util.ErrorCmdArg
	}

	confFile, err := os.ReadFile(eventConfigPath)
	if err != nil {
		log.Errorf("Failed to read config file %s: %v.", eventConfigPath, err)
		return nil, util.ErrorCmdArg
	}

	dbConf := &struct {
		Database *util.InfluxDbConfig `yaml:"Database"`
	}{}
	if err := yaml.Unmarshal(confFile, dbConf); err != nil {
		log.Errorf("Failed to parse YAML config file: %v", err)
		return nil, util.ErrorCmdArg
	}
	if dbConf.Database == nil {
		log.Errorf("Database section not found in YAML")
		return nil, util.ErrorCmdArg
	}

	return dbConf.Database, util.ErrorSuccess
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

func QueryEventInfoByNodes(nodeRegex string) util.CraneCmdError {
	if FlagForce {
		log.Warning("--force flag is ignored for query operations")
	}
	nodeNames := []string{}
	var ok bool
	if len(nodeRegex) != 0 {
		nodeNames, ok = util.ParseHostList(nodeRegex)
		if !ok {
			log.Errorf("Invalid node pattern: %s.\n", nodeRegex)
			return util.ErrorCmdArg
		}
	}

	if len(nodeNames) > 0 {
		missingList, err := MissingElements(config.CranedNodeList, nodeNames)
		if err != nil {
			log.Errorf("Invalid input for nodes: %v", err)
			return util.ErrorCmdArg
		}
		if len(missingList) > 0 {
			log.Errorf("Invalid input nodes: %v", missingList)
			return util.ErrorCmdArg
		}
	} else {
		var err error
		nodeNames, err = util.GetValidNodeList(config.CranedNodeList)
		if err != nil {
			log.Errorf("Invalid input for nodes: %v", err)
			return util.ErrorCmdArg
		}
	}

	if len(config.ClusterName) == 0 {
		log.Errorf("ClusterName empty")
		return util.ErrorCmdArg
	}

	// Query Resource Usage Records in InfluxDB
	result, err := QueryInfluxDbDataByTags(dbConfig, config.ClusterName, nodeNames)
	if err != nil {
		log.Errorf("Failed to query job info from InfluxDB: %v", err)
		return util.ErrorBackend
	}

	filteredRecords, err := SortRecords(result)
	if err != nil {
		log.Errorf("Failed to sort records: %v", err)
		return util.ErrorCmdArg
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
			log.Errorf("Failed to marshal data to JSON: %v", err)
			return util.ErrorBackend
		}
		fmt.Println(string(jsonData))
		return util.ErrorSuccess
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
	return util.ErrorSuccess
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

func ResetUserCredential(value string) util.CraneCmdError {
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
