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
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"github.com/xlab/treeprint"
	"math"
	"os"
	OSUser "os/user"
	"strconv"
	"strings"
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
		fmt.Println("There is no user in crane")
		return
	}
	//slice to map
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

	table := tablewriter.NewWriter(os.Stdout) //table format control
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetCenterSeparator("|")
	table.SetTablePadding("\t")
	table.SetHeader([]string{"Account", "UserName", "Uid", "AllowedPartition", "AllowedQosList", "DefaultQos", "AdminLevel", "blocked"})
	table.SetAutoFormatHeaders(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	tableData := make([][]string, len(userMap))

	for key, value := range userMap {
		tableData = append(tableData, []string{key})
		for _, userInfo := range value {
			if len(userInfo.AllowedPartitionQosList) == 0 {
				tableData = append(tableData, []string{
					key,
					userInfo.Name,
					strconv.FormatUint(uint64(userInfo.Uid), 10),
					"",
					"",
					"",
					fmt.Sprintf("%v", userInfo.AdminLevel)})
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
					strconv.FormatBool(userInfo.Blocked)})
			}
		}
	}

	table.AppendBulk(tableData)
	table.Render()
}

func PrintAllQos(qosList []*protos.QosInfo) {
	if len(qosList) == 0 {
		fmt.Println("There is no qos in crane")
		return
	}

	table := tablewriter.NewWriter(os.Stdout) //table format control
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetCenterSeparator("|")
	table.SetTablePadding("\t")
	table.SetHeader([]string{"Name", "Description", "Priority", "MaxJobsPerUser", "MaxCpusPerUser", "MaxTimeLimitPerTask"})
	table.SetAutoFormatHeaders(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	tableData := make([][]string, len(qosList))
	for _, info := range qosList {
		var timeLimitStr string
		if info.MaxTimeLimitPerTask >= uint64(util.InvalidDuration().Seconds) {
			timeLimitStr = "unlimited"
		} else {
			timeLimitStr = util.SecondTimeFormat(int64(info.MaxTimeLimitPerTask))
		}
		var jobsPerUserStr string
		if info.MaxJobsPerUser >= math.MaxUint32 {
			jobsPerUserStr = "unlimited"
		} else {
			jobsPerUserStr = strconv.FormatUint(uint64(info.MaxJobsPerUser), 10)
		}
		var cpusPerUserStr string
		if info.MaxCpusPerUser >= math.MaxUint32 {
			cpusPerUserStr = "unlimited"
		} else {
			cpusPerUserStr = strconv.FormatUint(uint64(info.MaxCpusPerUser), 10)
		}
		tableData = append(tableData, []string{
			info.Name,
			info.Description,
			fmt.Sprintf("%d", info.Priority),
			fmt.Sprintf(jobsPerUserStr),
			fmt.Sprintf(cpusPerUserStr),
			fmt.Sprintf(timeLimitStr)})
	}

	table.AppendBulk(tableData)
	table.Render()
}

func PrintAllAccount(accountList []*protos.AccountInfo) {
	if len(accountList) == 0 {
		fmt.Println("There is no account in crane")
		return
	}
	//slice to map and find the root account
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

	//print account tree
	var tree treeprint.Tree
	tree = treeprint.NewWithRoot("AccountTree")
	for _, account := range rootAccount {
		PraseAccountTree(tree, account, accountMap)
	}

	fmt.Println(tree.String())

	//print account table
	PrintAccountTable(accountList)
}

func PrintAccountTable(accountList []*protos.AccountInfo) {
	table := tablewriter.NewWriter(os.Stdout) //table format control
	util.SetBorderTable(table)
	header := []string{"Name", "Description", "AllowedPartition", "Users", "DefaultQos", "AllowedQosList", "blocked"}
	tableData := make([][]string, len(accountList))
	for _, accountInfo := range accountList {
		tableData = append(tableData, []string{
			accountInfo.Name,
			accountInfo.Description,
			strings.Join(accountInfo.AllowedPartitions, ", "),
			strings.Join(accountInfo.Users, ", "),
			accountInfo.DefaultQos,
			strings.Join(accountInfo.AllowedQosList, ", "),
			strconv.FormatBool(accountInfo.Blocked)})
	}

	if FlagFormat != "" {
		formatTableData := make([][]string, len(accountList))
		formatReq := strings.Split(FlagFormat, " ")
		tableOutputWidth := make([]int, len(formatReq))
		tableOutputHeader := make([]string, len(formatReq))
		for i := 0; i < len(formatReq); i++ {
			if formatReq[i][0] != '%' || len(formatReq[i]) < 2 {
				fmt.Println("Invalid format.")
				os.Exit(1)
			}
			if formatReq[i][1] == '.' {
				if len(formatReq[i]) < 4 {
					fmt.Println("Invalid format.")
					os.Exit(1)
				}
				width, err := strconv.ParseUint(formatReq[i][2:len(formatReq[i])-1], 10, 32)
				if err != nil {
					if err != nil {
						fmt.Println("Invalid format.")
						os.Exit(1)
					}
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
				fmt.Println("Invalid format.")
				os.Exit(1)
			}
		}
		header, tableData = util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
	}

	if !FlagNoHeader {
		table.SetHeader(header)
	}
	table.AppendBulk(tableData)
	table.Render()
}

func PraseAccountTree(parentTreeRoot treeprint.Tree, account string, accountMap map[string]*protos.AccountInfo) {
	if account == "" {
		return
	}
	if len(accountMap[account].ChildAccounts) == 0 {
		parentTreeRoot.AddNode(account)
	} else {
		branch := parentTreeRoot.AddBranch(account)
		for _, child := range accountMap[account].ChildAccounts {
			PraseAccountTree(branch, child, accountMap)
		}
	}
}

func AddAccount(account *protos.AccountInfo) {
	if account.Name == "=" {
		log.Fatalf("Parameter error : account name empty")
	}
	if len(account.Name) > 30 {
		log.Fatalf("Parameter error : name is too long(up to 30)")
	}

	var req *protos.AddAccountRequest
	req = new(protos.AddAccountRequest)
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
			log.Fatalf("Parameter error : default qos %s not contain in allowed qos list", account.DefaultQos)
		}
	}
	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.AddAccount(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to add the account")
	}
	if reply.GetOk() {
		fmt.Println("Add account success!")
	} else {
		fmt.Printf("Add account failed: %s\n", reply.GetReason())
	}
}

func AddUser(user *protos.UserInfo, partition []string, level string, coordinate bool) {
	if user.Name == "=" {
		log.Fatalf("Parameter error : user name empty")
	}
	if len(user.Name) > 30 {
		log.Fatalf("Parameter error : name is too long(up to 30)")
	}

	lu, err := OSUser.Lookup(user.Name)
	if err != nil {
		log.Fatal(err)
	}
	var req *protos.AddUserRequest
	req = new(protos.AddUserRequest)
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
	}

	if coordinate {
		user.CoordinatorAccounts = append(user.CoordinatorAccounts, user.Account)
	}

	reply, err := stub.AddUser(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to add the user")
	}
	if reply.GetOk() {
		fmt.Println("Add user success!")
	} else {
		fmt.Printf("Add user failed: %s\n", reply.GetReason())
	}
}

func AddQos(qos *protos.QosInfo) {
	if qos.Name == "=" {
		log.Fatalf("Parameter error : QOS name empty")
	}
	if len(qos.Name) > 30 {
		log.Fatalf("Parameter error : name is too long(up to 30)")
	}

	var req *protos.AddQosRequest
	req = new(protos.AddQosRequest)
	req.Uid = userUid
	req.Qos = qos

	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.AddQos(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to add the QoS")
	}
	if reply.GetOk() {
		fmt.Println("Add qos success!")
	} else {
		fmt.Printf("Add qos failed: %s\n", reply.GetReason())
	}
}

func DeleteAccount(name string) {
	var req *protos.DeleteEntityRequest
	req = &protos.DeleteEntityRequest{Uid: userUid, EntityType: protos.EntityType_Account, Name: name}

	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.DeleteEntity(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to delete account %s", name)
	}
	if reply.GetOk() {
		fmt.Printf("Delete account %s success\n", name)
	} else {
		fmt.Printf("Delete account %s failed: %s\n", name, reply.GetReason())
	}
}

func DeleteUser(name string, account string) {
	var req *protos.DeleteEntityRequest
	req = &protos.DeleteEntityRequest{Uid: userUid, EntityType: protos.EntityType_User, Name: name, Account: account}

	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.DeleteEntity(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to remove user %s", name)
	}
	if reply.GetOk() {
		fmt.Printf("Remove User %s success\n", name)
	} else {
		fmt.Printf("Remove User %s failed: %s\n", name, reply.GetReason())
	}
}

func DeleteQos(name string) {
	var req *protos.DeleteEntityRequest
	req = &protos.DeleteEntityRequest{Uid: userUid, EntityType: protos.EntityType_Qos, Name: name}

	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.DeleteEntity(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to delete QoS %s", name)
	}
	if reply.GetOk() {
		fmt.Printf("Delete Qos %s success\n", name)
	} else {
		fmt.Printf("Delete Qos %s failed: %s\n", name, reply.GetReason())
	}
}

func ModifyAccount(itemLeft string, itemRight string, name string, requestType protos.ModifyEntityRequest_OperatorType) {
	req := protos.ModifyEntityRequest{
		Uid:        userUid,
		Item:       itemLeft,
		Value:      itemRight,
		Name:       name,
		Type:       requestType,
		EntityType: protos.EntityType_Account,
		Force:      FlagForce,
	}

	reply, err := stub.ModifyEntity(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Modify information")
	}
	if reply.GetOk() {
		fmt.Println("Modify information success!")
	} else {
		fmt.Printf("Modify information failed: %s\n", reply.GetReason())
	}
}

func ModifyUser(itemLeft string, itemRight string, name string, account string, partition string, requestType protos.ModifyEntityRequest_OperatorType) {
	if itemLeft == "admin_level" {
		if itemRight != "none" && itemRight != "operator" && itemRight != "admin" {
			log.Fatalf("Unknown admin_level, please enter one of {none, operator, admin}")
		}
	}

	req := protos.ModifyEntityRequest{
		Uid:        userUid,
		Item:       itemLeft,
		Value:      itemRight,
		Name:       name,
		Partition:  partition,
		Type:       requestType,
		EntityType: protos.EntityType_User,
		Account:    account,
		Force:      FlagForce,
	}
	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.ModifyEntity(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to modify the uesr information")
	}
	if reply.GetOk() {
		fmt.Println("Modify information success!")
	} else {
		fmt.Printf("Modify information failed: %s\n", reply.GetReason())
	}
}

func ModifyQos(itemLeft string, itemRight string, name string) {
	req := protos.ModifyEntityRequest{
		Uid:        userUid,
		Item:       itemLeft,
		Value:      itemRight,
		Name:       name,
		Type:       protos.ModifyEntityRequest_Overwrite,
		EntityType: protos.EntityType_Qos,
	}

	//fmt.Printf("Req:\n%v\n\n", req)
	reply, err := stub.ModifyEntity(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to modify the QoS")
	}
	if reply.GetOk() {
		fmt.Println("Modify information success!")
	} else {
		fmt.Printf("Modify information failed: %s\n", reply.GetReason())
	}
}

func ShowAccounts() {
	var req *protos.QueryEntityInfoRequest
	req = &protos.QueryEntityInfoRequest{Uid: userUid, EntityType: protos.EntityType_Account}
	reply, err := stub.QueryEntityInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show accounts")
	}

	if reply.GetOk() {
		PrintAllAccount(reply.AccountList)
	} else {
		fmt.Println(reply.Reason)
	}
}

func ShowUser(name string, account string) {
	var req *protos.QueryEntityInfoRequest
	req = &protos.QueryEntityInfoRequest{Uid: userUid, EntityType: protos.EntityType_User, Name: name, Account: account}

	reply, err := stub.QueryEntityInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show the user")
	}

	if reply.GetOk() {
		PrintAllUsers(reply.UserList)
	} else {
		fmt.Println(reply.Reason)
	}
}

func ShowQos(name string) {
	var req *protos.QueryEntityInfoRequest
	req = &protos.QueryEntityInfoRequest{Uid: userUid, EntityType: protos.EntityType_Qos, Name: name}

	reply, err := stub.QueryEntityInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show the QoS")
	}

	if reply.GetOk() {
		PrintAllQos(reply.QosList)
	} else {
		if name == "" {
			fmt.Printf("Can't find any qos! %s\n", reply.GetReason())
		} else {
			fmt.Printf("Can't find qos %s\n", name)
		}
	}
}

func FindAccount(name string) {
	var req *protos.QueryEntityInfoRequest
	req = &protos.QueryEntityInfoRequest{Uid: userUid, EntityType: protos.EntityType_Account, Name: name}

	reply, err := stub.QueryEntityInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to find the account")
	}

	if reply.GetOk() {
		PrintAccountTable(reply.AccountList)
	} else {
		fmt.Println(reply.Reason)
	}
}

func BlockAccountOrUser(name string, entityType protos.EntityType, account string) {
	var req *protos.BlockAccountOrUserRequest
	req = &protos.BlockAccountOrUserRequest{Uid: userUid, Block: true, EntityType: entityType, Name: name, Account: account}

	reply, err := stub.BlockAccountOrUser(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to block the entity")
	}

	if reply.GetOk() {
		fmt.Printf("Block %s success!\n", name)
	} else {
		fmt.Println(reply.Reason)
	}
}

func UnblockAccountOrUser(name string, entityType protos.EntityType, account string) {
	var req *protos.BlockAccountOrUserRequest
	req = &protos.BlockAccountOrUserRequest{Uid: userUid, Block: false, EntityType: entityType, Name: name, Account: account}

	reply, err := stub.BlockAccountOrUser(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to unblock the entity")
	}

	if reply.GetOk() {
		fmt.Printf("Unblock %s success!\n", name)
	} else {
		fmt.Println(reply.Reason)
	}
}
