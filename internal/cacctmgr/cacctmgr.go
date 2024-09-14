/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package cacctmgr

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"math"
	"os"
	OSUser "os/user"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"github.com/xlab/treeprint"
)

var (
	userUid uint32
	stub    protos.CraneCtldClient
)

type ServerAddr struct {
	ControlMachine      string `yaml:"ControlMachine"`
	CraneCtldListenPort string `yaml:"CraneCtldListenPort"`
}

func PrintAllUsers(userList []*protos.UserInfo) {
	if len(userList) == 0 {
		fmt.Println("There is no user in crane.")
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
	table.SetHeader([]string{"Account", "UserName", "Uid", "AllowedPartition", "AllowedQosList", "DefaultQos", "AdminLevel", "Blocked"})
	tableData := make([][]string, len(userMap))
	for key, value := range userMap {
		tableData = append(tableData, []string{key})
		for _, userInfo := range value {
			if len(userInfo.AllowedPartitionQosList) == 0 {
				tableData = append(tableData, []string{
					userInfo.Account,
					userInfo.Name,
					strconv.FormatUint(uint64(userInfo.Uid), 10),
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

func PrintAllQos(qosList []*protos.QosInfo) {
	if len(qosList) == 0 {
		fmt.Println("There is no QoS in crane")
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
		if info.MaxTimeLimitPerTask >= uint64(util.InvalidDuration().Seconds) {
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

func PrintAllAccount(accountList []*protos.AccountInfo) {
	if len(accountList) == 0 {
		fmt.Println("There is no account in crane")
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
	header := []string{"Name", "Description", "AllowedPartition", "Users", "DefaultQos", "AllowedQosList", "Blocked"}
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
			//"Name", "Description", "AllowedPartition", "DefaultQos", "AllowedQosList"
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
	if len(accountMap[account].ChildAccounts) == 0 {
		parentTreeRoot.AddNode(account)
	} else {
		branch := parentTreeRoot.AddBranch(account)
		for _, child := range accountMap[account].ChildAccounts {
			PrintAccountTree(branch, child, accountMap)
		}
	}
}

func AddAccount(account *protos.AccountInfo) util.CraneCmdError {
	// FIXME: Move name validation to the backend?
	// FIXME: Seperate this to Args of cobra package?
	if account.Name == "=" {
		log.Errorf("Account name empty")
		return util.ErrorCmdArg
	}
	if len(account.Name) > 30 {
		log.Errorf("Name is too long (up to 30)")
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
			log.Errorf("Default QoS %s not contain in allowed QoS list", account.DefaultQos)
			return util.ErrorCmdArg
		}
	}

	reply, err := stub.AddAccount(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to add the account")
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
		fmt.Println("Add account succeeded.")
		return util.ErrorSuccess
	} else {
		fmt.Printf("Add account failed: %s.\n", reply.GetReason())
		return util.ErrorBackend
	}
}

func AddUser(user *protos.UserInfo, partition []string, level string, coordinator bool) util.CraneCmdError {
	if user.Name == "=" {
		log.Errorf("User name empty")
		return util.ErrorCmdArg
	}
	if len(user.Name) > 30 {
		log.Errorf("Name is too long (up to 30)")
		return util.ErrorCmdArg
	}

	lu, err := OSUser.Lookup(user.Name)
	if err != nil {
		log.Error(err)
		return util.ErrorCmdArg
	}

	req := new(protos.AddUserRequest)
	req.Uid = userUid
	req.User = user
	for _, par := range partition {
		user.AllowedPartitionQosList = append(user.AllowedPartitionQosList, &protos.UserInfo_AllowedPartitionQos{PartitionName: par})
	}

	i64, err := strconv.ParseInt(lu.Uid, 10, 64)
	if err == nil {
		user.Uid = uint32(i64)
	}

	if level == "none" {
		user.AdminLevel = protos.UserInfo_None
	} else if level == "operator" {
		user.AdminLevel = protos.UserInfo_Operator
	} else if level == "admin" {
		user.AdminLevel = protos.UserInfo_Admin
	} else {
		log.Errorf("Unknown admin level, valid values: none, operator, admin.")
		return util.ErrorCmdArg
	}

	if coordinator {
		user.CoordinatorAccounts = append(user.CoordinatorAccounts, user.Account)
	}

	reply, err := stub.AddUser(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to add the user")
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
		fmt.Println("Add user succeeded.")
		return util.ErrorSuccess
	} else {
		fmt.Printf("Add user failed: %s.\n", reply.GetReason())
		return util.ErrorBackend
	}
}

func AddQos(qos *protos.QosInfo) util.CraneCmdError {
	if qos.Name == "=" {
		log.Errorln("QoS name empty.")
		return util.ErrorCmdArg
	}
	if len(qos.Name) > 30 {
		log.Errorf("Name is too long (up to 30).")
		return util.ErrorCmdArg
	}

	req := new(protos.AddQosRequest)
	req.Uid = userUid
	req.Qos = qos

	reply, err := stub.AddQos(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to add the QoS")
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
		fmt.Println("Add QoS succeeded.")
		return util.ErrorSuccess
	} else {
		fmt.Printf("Add QoS failed: %s.\n", reply.GetReason())
		return util.ErrorBackend
	}
}

func DeleteAccount(name string) util.CraneCmdError {
	req := protos.DeleteAccountRequest{Uid: userUid, Name: name}

	reply, err := stub.DeleteAccount(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to delete account %s", name)
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
		fmt.Printf("Delete account %s succeeded.\n", name)
		return util.ErrorSuccess
	} else {
		fmt.Printf("Delete account %s failed: %s.\n", name, reply.GetReason())
		return util.ErrorBackend
	}
}

func DeleteUser(name string, account string) util.CraneCmdError {
	req := protos.DeleteUserRequest{Uid: userUid, Name: name, Account: account}

	reply, err := stub.DeleteUser(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to remove user %s", name)
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
		fmt.Printf("Remove user %s succeeded.\n", name)
		return util.ErrorSuccess
	} else {
		fmt.Printf("Remove user %s failed: %s.\n", name, reply.GetReason())
		return util.ErrorBackend
	}
}

func DeleteQos(name string) util.CraneCmdError {
	req := protos.DeleteQosRequest{Uid: userUid, Name: name}

	reply, err := stub.DeleteQos(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to delete QoS %s", name)
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
		fmt.Printf("Delete QoS %s succeeded.\n", name)
		return util.ErrorSuccess
	} else {
		fmt.Printf("Delete QoS %s failed: %s.\n", name, reply.GetReason())
		return util.ErrorBackend
	}
}

func ModifyAccount(modify_field protos.ModifyField, new_value string, name string, requestType protos.OperatorType) util.CraneCmdError {
	req := protos.ModifyAccountRequest{
		Uid:         userUid,
		ModifyField: modify_field,
		Value:       new_value,
		Name:        name,
		Type:        requestType,
		Force:       FlagForce,
	}

	reply, err := stub.ModifyAccount(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Modify information")
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
		fmt.Printf("Modify information failed: %s.\n", reply.GetReason())
		return util.ErrorBackend
	}
}

func ModifyUser(modify_field protos.ModifyField, new_value string, name string, account string, partition string, requestType protos.OperatorType) util.CraneCmdError {
	if modify_field == protos.ModifyField_AdminLevel {
		if new_value != "none" && new_value != "operator" && new_value != "admin" {
			log.Errorf("Unknown admin level, valid values: none, operator, admin.")
			return util.ErrorCmdArg
		}
	}

	req := protos.ModifyUserRequest{
		Uid:         userUid,
		ModifyField: modify_field,
		Value:       new_value,
		Name:        name,
		Partition:   partition,
		Type:        requestType,
		Account:     account,
		Force:       FlagForce,
	}

	reply, err := stub.ModifyUser(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to modify the uesr information")
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
		fmt.Printf("Modify information failed: %s.\n", reply.GetReason())
		return util.ErrorBackend
	}
}

func ModifyQos(modify_field protos.ModifyField, new_value string, name string) util.CraneCmdError {
	req := protos.ModifyQosRequest{
		Uid:         userUid,
		ModifyField: modify_field,
		Value:       new_value,
		Name:        name,
	}

	reply, err := stub.ModifyQos(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to modify the QoS")
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
		fmt.Printf("Modify information failed: %s.\n", reply.GetReason())
		return util.ErrorBackend
	}
}

func ShowAccounts() util.CraneCmdError {
	req := protos.QueryAccountInfoRequest{Uid: userUid}
	reply, err := stub.QueryAccountInfo(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show accounts")
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
		PrintAllAccount(reply.AccountList)
		return util.ErrorSuccess
	} else {
		fmt.Println(reply.Reason)
		return util.ErrorBackend
	}
}

func ShowUser(name string, account string) util.CraneCmdError {
	req := protos.QueryUserInfoRequest{Uid: userUid, Name: name, Account: account}
	reply, err := stub.QueryUserInfo(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show the user")
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
		PrintAllUsers(reply.UserList)
		return util.ErrorSuccess
	} else {
		fmt.Println(reply.Reason)
		return util.ErrorBackend
	}
}

func ShowQos(name string) util.CraneCmdError {
	req := protos.QueryQosInfoRequest{Uid: userUid, Name: name}
	reply, err := stub.QueryQosInfo(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show the QoS")
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
		PrintAllQos(reply.QosList)
		return util.ErrorSuccess
	} else {
		if name == "" {
			fmt.Printf("Can't find any QoS. %s.\n", reply.GetReason())
		} else {
			fmt.Printf("Can't find QoS %s.\n", name)
		}
		return util.ErrorBackend
	}
}

func FindAccount(name string) util.CraneCmdError {
	req := protos.QueryAccountInfoRequest{Uid: userUid, Name: name}
	reply, err := stub.QueryAccountInfo(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to find the account")
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
		PrintAccountTable(reply.AccountList)
		return util.ErrorSuccess
	} else {
		fmt.Println(reply.Reason)
		return util.ErrorBackend
	}
}

func BlockAccountOrUser(name string, entityType protos.EntityType, account string) util.CraneCmdError {
	req := protos.BlockAccountOrUserRequest{Uid: userUid, Block: true, EntityType: entityType, Name: name, Account: account}
	reply, err := stub.BlockAccountOrUser(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to block the entity")
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
		fmt.Printf("Block %s succeeded.\n", name)
		return util.ErrorSuccess
	} else {
		fmt.Println(reply.Reason)
		return util.ErrorBackend
	}
}

func UnblockAccountOrUser(name string, entityType protos.EntityType, account string) util.CraneCmdError {
	req := protos.BlockAccountOrUserRequest{Uid: userUid, Block: false, EntityType: entityType, Name: name, Account: account}
	reply, err := stub.BlockAccountOrUser(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to unblock the entity")
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
		fmt.Printf("Unblock %s succeeded.\n", name)
		return util.ErrorSuccess
	} else {
		fmt.Println(reply.Reason)
		return util.ErrorBackend
	}
}
