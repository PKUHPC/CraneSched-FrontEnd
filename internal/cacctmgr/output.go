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
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"github.com/xlab/treeprint"
)

type Tableoutput struct {
	header    []string
	tableData [][]string
}

// init []int
func createSlice(length int, value int) []int {
	slice := make([]int, length)
	for i := range slice {
		slice[i] = value
	}
	return slice
}

// Txn
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

func ShowTxn(actor string, target string, actionValue string, info string, startTimeValue string) util.ExitCode {
	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
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

// Account
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

// Name Desciption AllowedPartition Users DefaultQos AllowedQosList Coordinators Blocked
func AccountFormatOutput(tableCtx *Tableoutput, accountList []*protos.AccountInfo) {
	formatTableData := make([][]string, len(accountList))
	formatReq := util.SplitString(FlagFormat, []string{" ", ","})
	tableOutputWidth := createSlice(len(formatReq), -1)
	tableOutputHeader := make([]string, len(formatReq))

	for i := 0; i < len(formatReq); i++ {
		switch string(bytes.ToLower([]byte(formatReq[i]))) {
		case "name":
			tableOutputHeader[i] = "Name"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], accountList[j].Name)
			}
		case "description":
			tableOutputHeader[i] = "Description"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], accountList[j].Description)
			}
		case "allowedpartition":
			tableOutputHeader[i] = "AllowedPartition"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], strings.Join(accountList[j].AllowedPartitions, ", "))
			}
		case "users":
			tableOutputHeader[i] = "Users"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], strings.Join(accountList[j].Users, ", "))
			}
		case "defaultqos":
			tableOutputHeader[i] = "DefaultQos"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], accountList[j].DefaultQos)
			}
		case "allowedqoslist":
			tableOutputHeader[i] = "AllowedQosList"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], strings.Join(accountList[j].AllowedQosList, ", "))
			}
		case "coordinators":
			tableOutputHeader[i] = "Coordinators"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], strings.Join(accountList[j].Coordinators, ", "))
			}
		case "Blocked":
			tableOutputHeader[i] = "Blocked"
			for j := 0; j < len(accountList); j++ {
				formatTableData[j] = append(formatTableData[j], strconv.FormatBool(accountList[j].Blocked))
			}
		default:
			log.Errorf("Invalid format.Your enter:%s is error", formatReq[i])
			os.Exit(util.ErrorInvalidFormat)
		}
	}
	tableCtx.header, tableCtx.tableData = util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
}
func AccountDefaultOutput(tableCtx *Tableoutput, accountList []*protos.AccountInfo) {
	tableCtx.header = []string{"Name", "Description", "AllowedPartition", "Users", "DefaultQos", "AllowedQosList", "Coordinators", "Blocked"}
	tableCtx.tableData = make([][]string, 0, len(accountList))
	for _, accountInfo := range accountList {
		tableCtx.tableData = append(tableCtx.tableData, []string{
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
}
func PrintAccountTable(accountList []*protos.AccountInfo) {
	table := tablewriter.NewWriter(os.Stdout) //table format control
	util.SetBorderTable(table)

	var tableCtx Tableoutput // include header and datas
	if FlagFormat != "" {
		AccountFormatOutput(&tableCtx, accountList)
	} else {
		AccountDefaultOutput(&tableCtx, accountList)
	}
	if !FlagNoHeader {
		table.SetHeader(tableCtx.header)
	}
	if !FlagFull && FlagFormat == "" {
		// The data in the fifth column is AllowedQosList, which is not trim
		util.TrimTableExcept(&tableCtx.tableData, 5)
	}

	table.AppendBulk(tableCtx.tableData)
	table.Render()
}
func ShowAccounts() util.ExitCode {
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

// User
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
	PrintUserTable(userMap)
}

func UserDefaultOutput(tableCtx *Tableoutput, userMap map[string][]*protos.UserInfo) {
	tableCtx.header = []string{"Account", "UserName", "Uid", "AllowedPartition", "AllowedQosList", "DefaultQos", "Coordinated", "AdminLevel", "Blocked"}
	for _, value := range userMap {
		for _, userInfo := range value {
			if len(userInfo.AllowedPartitionQosList) == 0 {
				tableCtx.tableData = append(tableCtx.tableData, []string{
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
				tableCtx.tableData = append(tableCtx.tableData, []string{
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
}

func CalcuTotalRows(userMap map[string][]*protos.UserInfo) int {
	totalRows := 0
	for _, users := range userMap {
		for _, userInfo := range users {
			if len(userInfo.AllowedPartitionQosList) == 0 {
				totalRows++
			} else {
				totalRows += len(userInfo.AllowedPartitionQosList)
			}
		}
	}
	return totalRows
}

func UserFormatOutput(tableCtx *Tableoutput, userMap map[string][]*protos.UserInfo) {
	formatReq := util.SplitString(FlagFormat, []string{" ", ","})
	tableOutputWidth := createSlice(len(formatReq), -1)
	tableOutputHeader := make([]string, len(formatReq))

	// calculate the total Rows
	totalRows := CalcuTotalRows(userMap)
	formatTableData := make([][]string, totalRows)
	for i := range formatTableData {
		formatTableData[i] = make([]string, 0, len(formatReq))
	}

	// fill the Datas
	for i := 0; i < len(formatReq); i++ {
		currentRow := 0 // current Row index
		switch strings.ToLower(formatReq[i]) {
		case "account":
			tableOutputHeader[i] = "Account"
			for _, users := range userMap {
				for _, userInfo := range users {
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], userInfo.Account)
						currentRow++
					} else {
						for range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow], userInfo.Account)
							currentRow++
						}
					}
				}
			}
		case "username":
			tableOutputHeader[i] = "UserName"
			for _, users := range userMap {
				for _, userInfo := range users {
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], userInfo.Name)
						currentRow++
					} else {
						for range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow], userInfo.Name)
							currentRow++
						}
					}
				}
			}
		case "uid":
			tableOutputHeader[i] = "Uid"
			for _, users := range userMap {
				for _, userInfo := range users {
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow],
							strconv.FormatUint(uint64(userInfo.Uid), 10))
						currentRow++
					} else {
						for range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow],
								strconv.FormatUint(uint64(userInfo.Uid), 10))
							currentRow++
						}
					}
				}
			}
		case "allowedpartition":
			tableOutputHeader[i] = "AllowedPartition"
			for _, users := range userMap {
				for _, userInfo := range users {
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], "")
						currentRow++
					} else {
						for _, allowedPartitionQos := range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow],
								allowedPartitionQos.PartitionName)
							currentRow++
						}
					}
				}
			}
		case "allowedqoslist":
			tableOutputHeader[i] = "AllowedQosList"
			for _, users := range userMap {
				for _, userInfo := range users {
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], "")
						currentRow++
					} else {
						for _, allowedPartitionQos := range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow],
								strings.Join(allowedPartitionQos.QosList, ", "))
							currentRow++
						}
					}
				}
			}
		case "defaultqos":
			tableOutputHeader[i] = "DefaultQos"
			for _, users := range userMap {
				for _, userInfo := range users {
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], "")
						currentRow++
					} else {
						for _, allowedPartitionQos := range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow],
								allowedPartitionQos.DefaultQos)
							currentRow++
						}
					}
				}
			}
		case "coordinated":
			tableOutputHeader[i] = "Coordinated"
			for _, users := range userMap {
				for _, userInfo := range users {
					coordinators := strings.Join(userInfo.CoordinatorAccounts, ", ")
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], coordinators)
						currentRow++
					} else {
						for range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow], coordinators)
							currentRow++
						}
					}
				}
			}
		case "adminlevel":
			tableOutputHeader[i] = "AdminLevel"
			for _, users := range userMap {
				for _, userInfo := range users {
					adminLevelStr := fmt.Sprintf("%v", userInfo.AdminLevel)
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], adminLevelStr)
						currentRow++
					} else {
						for range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow], adminLevelStr)
							currentRow++
						}
					}
				}
			}
		case "blocked":
			tableOutputHeader[i] = "Blocked"
			for _, users := range userMap {
				for _, userInfo := range users {
					blockedStr := strconv.FormatBool(userInfo.Blocked)
					if len(userInfo.AllowedPartitionQosList) == 0 {
						formatTableData[currentRow] = append(formatTableData[currentRow], blockedStr)
						currentRow++
					} else {
						for range userInfo.AllowedPartitionQosList {
							formatTableData[currentRow] = append(formatTableData[currentRow], blockedStr)
							currentRow++
						}
					}
				}
			}
		default:
			log.Errorf("Invalid format. You entered: '%s'", formatReq[i])
			os.Exit(util.ErrorInvalidFormat)
		}
	}
	tableCtx.header, tableCtx.tableData = util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
}

func PrintUserTable(userMap map[string][]*protos.UserInfo) {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)

	var tableCtx Tableoutput
	if FlagFormat != "" {
		UserFormatOutput(&tableCtx, userMap)
	} else {
		UserDefaultOutput(&tableCtx, userMap)
	}
	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableCtx.tableData)
	}

	table.SetHeader(tableCtx.header)
	table.AppendBulk(tableCtx.tableData)
	table.Render()
}

func ShowUser(value string, account string) util.ExitCode {

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

// Qos
func PrintQosList(qosList []*protos.QosInfo) {
	if len(qosList) == 0 {
		return
	}
	sort.Slice(qosList, func(i, j int) bool {
		return qosList[i].Name < qosList[j].Name
	})

	PrintTable(qosList)
}

func QosFormatOutput(tableCtx *Tableoutput, qosList []*protos.QosInfo) {
	formatTableData := make([][]string, len(qosList))
	formatReq := util.SplitString(FlagFormat, []string{" ", ","})
	tableOutputWidth := createSlice(len(formatReq), -1)
	tableOutputHeader := make([]string, len(formatReq))

	for i := 0; i < len(formatReq); i++ {
		currentRow := 0
		switch strings.ToLower(formatReq[i]) {
		case "name":
			tableOutputHeader[i] = "Name"
			for _, info := range qosList {
				formatTableData[currentRow] = append(formatTableData[currentRow], info.Name)
				currentRow++
			}
		case "description":
			tableOutputHeader[i] = "Description"
			for _, info := range qosList {
				formatTableData[currentRow] = append(formatTableData[currentRow], info.Description)
				currentRow++
			}
		case "priority":
			tableOutputHeader[i] = "Priority"
			for _, info := range qosList {
				formatTableData[currentRow] = append(formatTableData[currentRow], fmt.Sprint(info.Priority))
				currentRow++
			}
		case "maxjobsperuser":
			tableOutputHeader[i] = "MaxJobsPerUser"
			for _, info := range qosList {
				var jobsPerUserStr string
				if info.MaxJobsPerUser == math.MaxUint32 {
					jobsPerUserStr = "unlimited"
				} else {
					jobsPerUserStr = strconv.FormatUint(uint64(info.MaxJobsPerUser), 10)
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], fmt.Sprint(jobsPerUserStr))
				currentRow++
			}
		case "maxcpusperuser":
			tableOutputHeader[i] = "MaxCpusPerUser"
			for _, info := range qosList {
				var cpusPerUserStr string
				if info.MaxCpusPerUser == math.MaxUint32 {
					cpusPerUserStr = "unlimited"
				} else {
					cpusPerUserStr = strconv.FormatUint(uint64(info.MaxCpusPerUser), 10)
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], fmt.Sprint(cpusPerUserStr))
				currentRow++
			}
		case "maxtimelimitpertask":
			tableOutputHeader[i] = "MaxTimeLimitPerTask"
			for _, info := range qosList {
				var timeLimitStr string
				if info.MaxTimeLimitPerTask >= util.MaxJobTimeLimit {
					timeLimitStr = "unlimited"
				} else {
					timeLimitStr = util.SecondTimeFormat(int64(info.MaxTimeLimitPerTask))
				}
				formatTableData[currentRow] = append(formatTableData[currentRow], fmt.Sprint(timeLimitStr))
				currentRow++
			}
		default:
			log.Errorf("Invalid format. You entered: '%s'", formatReq[i])
			os.Exit(util.ErrorInvalidFormat)
		}
	}
	tableCtx.header, tableCtx.tableData = util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
}

func QosDefaultOutput(tableCtx *Tableoutput, qosList []*protos.QosInfo) {
	tableCtx.header = []string{"Name", "Description", "Priority", "MaxJobsPerUser", "MaxCpusPerUser", "MaxTimeLimitPerTask"}
	tableCtx.tableData = make([][]string, 0, len(qosList))
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
		tableCtx.tableData = append(tableCtx.tableData, []string{
			info.Name,
			info.Description,
			fmt.Sprint(info.Priority),
			fmt.Sprint(jobsPerUserStr),
			fmt.Sprint(cpusPerUserStr),
			fmt.Sprint(timeLimitStr)})
	}
}

func PrintTable(qosList []*protos.QosInfo) {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)

	var tableCtx Tableoutput
	if FlagFormat != "" {
		QosFormatOutput(&tableCtx, qosList)
	} else {
		QosDefaultOutput(&tableCtx, qosList)
	}

	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableCtx.tableData)
	}

	table.SetHeader(tableCtx.header)
	table.AppendBulk(tableCtx.tableData)
	table.Render()
}

func ShowQos(value string) util.ExitCode {
	if FlagForce {
		log.Warning("--force flag is ignored for show operations")
	}
	var qosList []string
	if value != "" {
		var err error
		qosList, err = util.ParseStringParamList(value, ",")
		if err != nil {
			log.Errorf("Invalid QoS list specified: %v.\n", err)
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
