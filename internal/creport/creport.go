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

package creport

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"os"

	//"os/user"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	//log "github.com/sirupsen/logrus"

	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	stub protos.CraneCtldClient
)

type CheckStatus int

const (
	CheckAccountUserStatus = iota
	CheckUserAccountStatus
	CheckUserWckeyStatus
	CheckWckeyUserStatus
	CheckAccountQosStatus
	CheckClusterStatus
	CheckAccountCpusStatus
	CheckWckeyCpusStatus
	CheckAccountWckeyCpusStatus
	StatusFailed
)

func QueryUsersTopSummaryItem() error {
	request := &protos.QueryAccountUserSummaryItemRequest{}
	if FlagFilterAccounts != "" {
		filterAccountList, err := util.ParseStringParamList(FlagFilterAccounts, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid account list specified: %s.", err),
			}
		}
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterUsers != "" {
		filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid user list specified: %s.", err),
			}
		}
		request.FilterUsers = filterUserList
	}

	if FlagGroups != "" {
		filterGroupUserList, err := util.GetAllGroupUsers(FlagGroups)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid user list specified: %s.", err),
			}
		}
		request.FilterUsers = util.MergeAndDedupStrings(request.FilterUsers, filterGroupUserList)
	}

	var start_time, end_time time.Time
	var err error
	if FlagFilterStartTime != "" {
		start_time, err = util.ParseTime(FlagFilterStartTime)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to parse the StartTime filter: %s.", err),
			}
		}
		request.FilterStartTime = timestamppb.New(start_time)
	}

	if FlagFilterEndTime != "" {
		end_time, err = util.ParseTime(FlagFilterEndTime)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to parse the EndTime filter: %s.", err),
			}
		}
		request.FilterEndTime = timestamppb.New(end_time)
	}

	if FlagOutType != "" {
		if !util.CheckCreportOutType(FlagOutType) {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid argument: invalid: --time/-t, please input seconds/minutes/hours"),
			}
		}
	}

	rpcStart := time.Now()
	stream, err := stub.QueryAccountUserSummaryItemStream(context.Background(), request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query AccountUserSummary info")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	var accountUserList []*protos.AccountUserSummaryItem

	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			util.GrpcErrorPrintf(err, "Failed to receive item")
			return &util.CraneError{Code: util.ErrorNetwork}
		}
		for _, item := range batch.Items {
			accountUserList = append(accountUserList, item)
		}
	}
	rpcElapsed := time.Since(rpcStart)
	fmt.Printf("[QueryAccountUserSummaryItemStream] QueryAccountUserSummaryItemStream RPC used %d ms, accountUserList size %v\n", rpcElapsed.Milliseconds(), len(accountUserList))

	PrintUsersTopSumList(accountUserList, start_time, end_time)

	return nil
}

func QueryAccountUserSummaryItem(CheckType CheckStatus) error {

	request := &protos.QueryAccountUserSummaryItemRequest{}
	if FlagFilterAccounts != "" {
		filterAccountList, err := util.ParseStringParamList(FlagFilterAccounts, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid account list specified: %s.", err),
			}
		}
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterUsers != "" {
		filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid user list specified: %s.", err),
			}
		}
		request.FilterUsers = filterUserList
	}

	if FlagFilterQoss != "" {
		FlagFilterQosList, err := util.ParseStringParamList(FlagFilterQoss, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid qos list specified: %s.", err),
			}
		}
		request.FilterUsers = FlagFilterQosList
	}

	var start_time, end_time time.Time
	var err error
	if FlagFilterStartTime != "" {
		start_time, err = util.ParseTime(FlagFilterStartTime)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to parse the StartTime filter: %s.", err),
			}
		}
		request.FilterStartTime = timestamppb.New(start_time)
	}

	if FlagFilterEndTime != "" {
		end_time, err = util.ParseTime(FlagFilterEndTime)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to parse the EndTime filter: %s.", err),
			}
		}
		request.FilterEndTime = timestamppb.New(end_time)
	}

	if FlagOutType != "" {
		if !util.CheckCreportOutType(FlagOutType) {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid argument: invalid: --time/-t, please input seconds/minutes/hours"),
			}
		}
	}

	rpcStart := time.Now()
	stream, err := stub.QueryAccountUserSummaryItemStream(context.Background(), request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query AccountUserSummary info")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	var accountUserList []*protos.AccountUserSummaryItem

	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			util.GrpcErrorPrintf(err, "Failed to receive item")
			return &util.CraneError{Code: util.ErrorNetwork}
		}
		for _, item := range batch.Items {
			accountUserList = append(accountUserList, item)
		}
	}

	rpcElapsed := time.Since(rpcStart)
	fmt.Printf("[QueryAccountUserSummaryItemStream] QueryAccountUserSummaryItemStream RPC used %d ms, accountUserList size %v\n", rpcElapsed.Milliseconds(), len(accountUserList))

	if CheckType == CheckAccountUserStatus {
		PrintAccountUserList(accountUserList, start_time, end_time)
	} else if CheckType == CheckUserAccountStatus {
		PrintUserAccountList(accountUserList, start_time, end_time)
	} else if CheckType == CheckClusterStatus {
		PrintClusterList(accountUserList, start_time, end_time)
	} else if CheckType == CheckAccountQosStatus {
		PrintAccountQosList(accountUserList, start_time, end_time)
	}

	return nil
}

func QueryAccountUserWckeySummaryItem(CheckType CheckStatus) error {

	request := &protos.QueryAccountUserWckeySummaryItemRequest{}
	if FlagFilterAccounts != "" {
		filterAccountList, err := util.ParseStringParamList(FlagFilterAccounts, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid account list specified: %s.", err),
			}
		}
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterUsers != "" {
		filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid user list specified: %s.", err),
			}
		}
		request.FilterUsers = filterUserList
	}

	if FlagFilterWckeys != "" {
		filterWckeyList, err := util.ParseStringParamList(FlagFilterWckeys, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid user list specified: %s.", err),
			}
		}
		request.FilterWckeys = filterWckeyList
	}
	var start_time, end_time time.Time
	var err error
	if FlagFilterStartTime != "" {
		start_time, err = util.ParseTime(FlagFilterStartTime)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to parse the StartTime filter: %s.", err),
			}
		}
		request.FilterStartTime = timestamppb.New(start_time)
	}

	if FlagFilterEndTime != "" {
		end_time, err = util.ParseTime(FlagFilterEndTime)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to parse the EndTime filter: %s.", err),
			}
		}
		request.FilterEndTime = timestamppb.New(end_time)
	}

	if FlagOutType != "" {
		if !util.CheckCreportOutType(FlagOutType) {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid argument: invalid: --time/-t, please input seconds/minutes/hours"),
			}
		}
	}

	rpcStart := time.Now()
	stream, err := stub.QueryAccountUserWckeySummaryItemStream(context.Background(), request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query AccountUserSummary info")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	var accountUserWckeyList []*protos.AccountUserWckeySummaryItem

	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			util.GrpcErrorPrintf(err, "Failed to receive item")
			return &util.CraneError{Code: util.ErrorNetwork}
		}
		for _, item := range batch.Items {
			accountUserWckeyList = append(accountUserWckeyList, item)
		}
	}
	rpcElapsed := time.Since(rpcStart)
	fmt.Printf("[QueryAccountUserSummaryItemStream] QueryAccountUserSummaryItemStream RPC used %d ms, accountUserWckeyList size %v\n", rpcElapsed.Milliseconds(), len(accountUserWckeyList))

	if CheckType == CheckUserWckeyStatus {
		PrintUserWckeyList(accountUserWckeyList, start_time, end_time)
	} else if CheckType == CheckWckeyUserStatus {
		PrintWckeyUserList(accountUserWckeyList, start_time, end_time)
	} else if CheckType == CheckAccountCpusStatus {
		PrintAccountCpusList(accountUserWckeyList, start_time, end_time)
	} else if CheckType == CheckWckeyCpusStatus {
		PrintWckeyCpusList(accountUserWckeyList, start_time, end_time)
	} else if CheckType == CheckAccountWckeyCpusStatus {
		PrintAccountWckeyCpusList(accountUserWckeyList, start_time, end_time)
	}
	return nil
}

func PrintUsersTopSumList(accountUserList []*protos.AccountUserSummaryItem, startTime, endTime time.Time) {
	if len(accountUserList) == 0 {
		fmt.Printf("accountUserList empty\n")
		return
	}

	sort.Slice(accountUserList, func(i, j int) bool {
		return accountUserList[i].TotalCpuTime > accountUserList[j].TotalCpuTime
	})

	countMax := FlagTopCount
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Top %v Users %s - %s (%d secs)\n",
			countMax, startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType)
	header := []string{"Cluster", "Login", "Proper_name", "Account", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(accountUserList))

	var notFoundUsers []string
	for count, item := range accountUserList {
		// usr, err := user.Lookup(item.Username)
		// if err != nil {
		// 	notFoundUsers = append(notFoundUsers, fmt.Sprintf("User %s not found: %v", item.Username, err))
		// 	continue
		// }
		if count >= int(countMax) {
			break
		}
		tableData = append(tableData, []string{
			item.Cluster,
			item.Username,
			item.Username, //usr.Name,
			item.Account,
			strconv.FormatFloat(float64(item.TotalCpuTime/divisor), 'f', 1, 32),
			"0",
		})
	}
	table.AppendBulk(tableData)
	table.Render()

	for _, msg := range notFoundUsers {
		fmt.Println(msg)
	}
}

func PrintAccountUserList(accountUserList []*protos.AccountUserSummaryItem, startTime, endTime time.Time) {
	if len(accountUserList) == 0 {
		fmt.Printf("accountUserList empty\n")
		return
	}

	sort.Slice(accountUserList, func(i, j int) bool {
		if accountUserList[i].Account < accountUserList[j].Account {
			return true
		}
		if accountUserList[i].Account > accountUserList[j].Account {
			return false
		}
		return accountUserList[i].Username < accountUserList[j].Username
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/Account/User Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType)

	header := []string{"Cluster", "Account", "User", "Proper_name", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(accountUserList))

	var notFoundUsers []string
	for _, item := range accountUserList {
		// usr, err := user.Lookup(item.Username)
		// if err != nil {
		// 	notFoundUsers = append(notFoundUsers, fmt.Sprintf("User %s not found: %v", item.Username, err))
		// 	continue
		// }
		tableData = append(tableData, []string{
			item.Cluster,
			item.Account,
			item.Username,
			item.Username, //usr.Name,
			strconv.FormatFloat(float64(item.TotalCpuTime/divisor), 'f', 1, 32),
			"0",
		})
	}
	table.AppendBulk(tableData)
	table.Render()

	for _, msg := range notFoundUsers {
		fmt.Println(msg)
	}
}

func PrintUserAccountList(accountUserList []*protos.AccountUserSummaryItem, startTime, endTime time.Time) {
	if len(accountUserList) == 0 {
		fmt.Printf("accountUserList empty\n")
		return
	}

	sort.Slice(accountUserList, func(i, j int) bool {
		if accountUserList[i].Username < accountUserList[j].Username {
			return true
		}
		if accountUserList[i].Username > accountUserList[j].Username {
			return false
		}
		return accountUserList[i].Account < accountUserList[j].Account
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/User/Account Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType)

	header := []string{"Cluster", "User", "Proper_name", "Account", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(accountUserList))

	var notFoundUsers []string
	for _, item := range accountUserList {
		// usr, err := user.Lookup(item.Username)
		// if err != nil {
		// 	notFoundUsers = append(notFoundUsers, fmt.Sprintf("User %s not found: %v", item.Username, err))
		// 	continue
		// }
		tableData = append(tableData, []string{
			item.Cluster,
			item.Username,
			item.Username, //usr.Name,
			item.Account,
			strconv.FormatFloat(float64(item.TotalCpuTime/divisor), 'f', 1, 32),
			"0",
		})
	}
	table.AppendBulk(tableData)
	table.Render()

	for _, msg := range notFoundUsers {
		fmt.Println(msg)
	}
}

func PrintUserWckeyList(accountUserWckeyList []*protos.AccountUserWckeySummaryItem, startTime, endTime time.Time) {

	if len(accountUserWckeyList) == 0 {
		fmt.Printf("accountUserList empty\n")
		return
	}

	sort.Slice(accountUserWckeyList, func(i, j int) bool {
		return accountUserWckeyList[i].TotalCpuTime < accountUserWckeyList[j].TotalCpuTime
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/Account/User Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType)

	header := []string{"Cluster", "User", "Proper_name", "Wckey", "Used"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(accountUserWckeyList))

	var notFoundUsers []string
	for _, item := range accountUserWckeyList {
		// usr, err := user.Lookup(item.Username)
		// if err != nil {
		// 	notFoundUsers = append(notFoundUsers, fmt.Sprintf("User %s not found: %v", item.Username, err))
		// 	continue
		// }
		tableData = append(tableData, []string{
			item.Cluster,
			item.Username,
			item.Username, //usr.Name,
			item.Wckey,
			strconv.FormatFloat(float64(item.TotalCpuTime/divisor), 'f', 1, 32),
		})
	}
	table.AppendBulk(tableData)
	table.Render()

	for _, msg := range notFoundUsers {
		fmt.Println(msg)
	}
}

func PrintWckeyUserList(accountUserWckeyList []*protos.AccountUserWckeySummaryItem, startTime, endTime time.Time) {
	if len(accountUserWckeyList) == 0 {
		fmt.Printf("accountUserList empty\n")
		return
	}

	sort.Slice(accountUserWckeyList, func(i, j int) bool {
		return accountUserWckeyList[i].Wckey < accountUserWckeyList[j].Wckey
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/WCKey/User Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType)

	header := []string{"Cluster", "Wckey", "User", "Proper_name", "Used"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(accountUserWckeyList))

	var notFoundUsers []string
	for _, item := range accountUserWckeyList {
		// usr, err := user.Lookup(item.Username)
		// if err != nil {
		// 	notFoundUsers = append(notFoundUsers, fmt.Sprintf("User %s not found: %v", item.Username, err))
		// 	continue
		// }
		tableData = append(tableData, []string{
			item.Cluster,
			item.Wckey,
			item.Username,
			item.Username, //usr.Name,
			strconv.FormatFloat(float64(item.TotalCpuTime/divisor), 'f', 1, 32),
		})
	}
	table.AppendBulk(tableData)
	table.Render()

	for _, msg := range notFoundUsers {
		fmt.Println(msg)
	}
}

func PrintAccountQosList(accountUserList []*protos.AccountUserSummaryItem, startTime, endTime time.Time) {
	if len(accountUserList) == 0 {
		fmt.Printf("accountUserList empty\n")
		return
	}

	sort.Slice(accountUserList, func(i, j int) bool {
		if accountUserList[i].Account < accountUserList[j].Account {
			return true
		}
		if accountUserList[i].Account > accountUserList[j].Account {
			return false
		}
		return accountUserList[i].Qos < accountUserList[j].Qos
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/Account/Qos Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType)

	header := []string{"Cluster", "Account", "Qos", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(accountUserList))

	for _, item := range accountUserList {
		tableData = append(tableData, []string{
			item.Cluster,
			item.Account,
			item.Qos,
			strconv.FormatFloat(float64(item.TotalCpuTime/divisor), 'f', 1, 32),
			"0",
		})
	}
	table.AppendBulk(tableData)
	table.Render()
}

func PrintClusterList(accountUserList []*protos.AccountUserSummaryItem, startTime, endTime time.Time) {
	if len(accountUserList) == 0 {
		fmt.Printf("accountUserList empty\n")
		return
	}

	clusterMap := make(map[string]float64)
	for _, item := range accountUserList {
		clusterMap[item.Cluster] += item.TotalCpuTime
	}

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType)

	header := []string{"Cluster", "Allocate", "Down", "Planned", "Reported"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(accountUserList))
	for cluster, TotalCpuTime := range clusterMap {
		tableData = append(tableData, []string{
			cluster,
			strconv.FormatFloat(float64(TotalCpuTime/divisor), 'f', 0, 32),
			"-",
			"-",
			"-",
		})
	}
	table.AppendBulk(tableData)
	table.Render()

}

func PrintAccountCpusList(accountUserWckeyList []*protos.AccountUserWckeySummaryItem, startTime, endTime time.Time) {
	if len(accountUserWckeyList) == 0 {
		fmt.Printf("accountUserList empty\n")
		return
	}

	type CpuLevelSummary struct {
		CpuTime [5]float64
	}

	// cluster -> account -> summary
	clusterAccountMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotalCpu := make(map[string]float64)

	for _, item := range accountUserWckeyList {
		if _, ok := clusterAccountMap[item.Cluster]; !ok {
			clusterAccountMap[item.Cluster] = make(map[string]*CpuLevelSummary)
		}
		if _, ok := clusterAccountMap[item.Cluster][item.Account]; !ok {
			clusterAccountMap[item.Cluster][item.Account] = &CpuLevelSummary{}
		}
		summary := clusterAccountMap[item.Cluster][item.Account]
		if item.CpuLevel >= protos.CpuLevel_CPU_LEVEL_0_49 && item.CpuLevel <= protos.CpuLevel_CPU_LEVEL_1000_PLUS {
			summary.CpuTime[item.CpuLevel] += item.TotalCpuTime
			clusterTotalCpu[item.Cluster] += item.TotalCpuTime
		}
	}

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Job Sizes %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType)

	header := []string{
		"Cluster",
		"Account",
		"0-49 CPUs",
		"50-249 CPUs",
		"250-499 CPUs",
		"500-999 CPUs",
		">= 1000 CPUs",
		"Total Cpu Time",
		"% of cluster",
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)

	clusters := make([]string, 0, len(clusterAccountMap))
	for cluster := range clusterAccountMap {
		clusters = append(clusters, cluster)
	}
	sort.Strings(clusters)

	for _, cluster := range clusters {
		accounts := clusterAccountMap[cluster]
		accountNames := make([]string, 0, len(accounts))
		for account := range accounts {
			accountNames = append(accountNames, account)
		}
		sort.Strings(accountNames)
		for _, account := range accountNames {
			summary := accounts[account]
			accountTotalCpu := 0.0
			for _, cpu := range summary.CpuTime {
				accountTotalCpu += cpu
			}
			percent := 0.0
			if clusterTotalCpu[cluster] > 0 {
				percent = accountTotalCpu / clusterTotalCpu[cluster] * 100
			}
			row := []string{
				cluster,
				account,
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_0_49]/divisor),
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_50_249]/divisor),
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_250_499]/divisor),
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_500_999]/divisor),
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_1000_PLUS]/divisor),
				fmt.Sprintf("%.0f", accountTotalCpu/divisor),
				fmt.Sprintf("%.2f%%", percent),
			}
			table.Append(row)
		}
	}
	table.Render()
}

func PrintWckeyCpusList(accountUserWckeyList []*protos.AccountUserWckeySummaryItem, startTime, endTime time.Time) {
	if len(accountUserWckeyList) == 0 {
		fmt.Printf("accountUserList empty\n")
		return
	}

	type CpuLevelSummary struct {
		CpuTime [5]float64
	}

	// cluster -> wckey -> summary
	clusterWckeyMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotalCpu := make(map[string]float64)

	for _, item := range accountUserWckeyList {
		if _, ok := clusterWckeyMap[item.Cluster]; !ok {
			clusterWckeyMap[item.Cluster] = make(map[string]*CpuLevelSummary)
		}
		if _, ok := clusterWckeyMap[item.Cluster][item.Wckey]; !ok {
			clusterWckeyMap[item.Cluster][item.Wckey] = &CpuLevelSummary{}
		}
		summary := clusterWckeyMap[item.Cluster][item.Wckey]
		if item.CpuLevel >= protos.CpuLevel_CPU_LEVEL_0_49 && item.CpuLevel <= protos.CpuLevel_CPU_LEVEL_1000_PLUS {
			summary.CpuTime[item.CpuLevel] += item.TotalCpuTime
			clusterTotalCpu[item.Cluster] += item.TotalCpuTime
		}
	}

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Job Sizes by Wckey %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType)

	header := []string{
		"Cluster",
		"Wckey",
		"0-49 CPUs",
		"50-249 CPUs",
		"250-499 CPUs",
		"500-999 CPUs",
		">= 1000 CPUs",
		"Total Cpu Time",
		"% of cluster",
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)

	clusters := make([]string, 0, len(clusterWckeyMap))
	for cluster := range clusterWckeyMap {
		clusters = append(clusters, cluster)
	}
	sort.Strings(clusters)

	for _, cluster := range clusters {
		wckeyMap := clusterWckeyMap[cluster]
		wckeyNames := make([]string, 0, len(wckeyMap))
		for wckey := range wckeyMap {
			wckeyNames = append(wckeyNames, wckey)
		}
		sort.Strings(wckeyNames)
		for _, wckey := range wckeyNames {
			summary := wckeyMap[wckey]
			wckeyTotalCpu := 0.0
			for _, cpu := range summary.CpuTime {
				wckeyTotalCpu += cpu
			}
			percent := 0.0
			if clusterTotalCpu[cluster] > 0 {
				percent = wckeyTotalCpu / clusterTotalCpu[cluster] * 100
			}
			row := []string{
				cluster,
				wckey,
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_0_49]/divisor),
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_50_249]/divisor),
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_250_499]/divisor),
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_500_999]/divisor),
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_1000_PLUS]/divisor),
				fmt.Sprintf("%.0f", wckeyTotalCpu/divisor),
				fmt.Sprintf("%.2f%%", percent),
			}
			table.Append(row)
		}
	}
	table.Render()
}

func PrintAccountWckeyCpusList(accountUserWckeyList []*protos.AccountUserWckeySummaryItem, startTime, endTime time.Time) {
	if len(accountUserWckeyList) == 0 {
		fmt.Printf("accountUserList empty\n")
		return
	}

	type CpuLevelSummary struct {
		CpuTime [5]float64
	}

	// cluster -> account:wckey -> summary
	clusterAccountWckeyMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotalCpu := make(map[string]float64)

	for _, item := range accountUserWckeyList {
		if _, ok := clusterAccountWckeyMap[item.Cluster]; !ok {
			clusterAccountWckeyMap[item.Cluster] = make(map[string]*CpuLevelSummary)
		}
		accountWckey := fmt.Sprintf("%s:%s", item.Account, item.Wckey)
		if _, ok := clusterAccountWckeyMap[item.Cluster][accountWckey]; !ok {
			clusterAccountWckeyMap[item.Cluster][accountWckey] = &CpuLevelSummary{}
		}
		summary := clusterAccountWckeyMap[item.Cluster][accountWckey]
		if item.CpuLevel >= protos.CpuLevel_CPU_LEVEL_0_49 && item.CpuLevel <= protos.CpuLevel_CPU_LEVEL_1000_PLUS {
			summary.CpuTime[item.CpuLevel] += item.TotalCpuTime
			clusterTotalCpu[item.Cluster] += item.TotalCpuTime
		}
	}

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Job Sizes %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType)

	header := []string{
		"Cluster",
		"Account:Wckey",
		"0-49 CPUs",
		"50-249 CPUs",
		"250-499 CPUs",
		"500-999 CPUs",
		">= 1000 CPUs",
		"Total Cpu Time",
		"% of cluster",
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)

	clusters := make([]string, 0, len(clusterAccountWckeyMap))
	for cluster := range clusterAccountWckeyMap {
		clusters = append(clusters, cluster)
	}
	sort.Strings(clusters)

	for _, cluster := range clusters {
		accountWckeyMap := clusterAccountWckeyMap[cluster]
		accountWckeyNames := make([]string, 0, len(accountWckeyMap))
		for accountWckey := range accountWckeyMap {
			accountWckeyNames = append(accountWckeyNames, accountWckey)
		}
		sort.Strings(accountWckeyNames)
		for _, accountWckey := range accountWckeyNames {
			summary := accountWckeyMap[accountWckey]
			accountWckeyTotalCpu := 0.0
			for _, cpu := range summary.CpuTime {
				accountWckeyTotalCpu += cpu
			}
			percent := 0.0
			if clusterTotalCpu[cluster] > 0 {
				percent = accountWckeyTotalCpu / clusterTotalCpu[cluster] * 100
			}
			row := []string{
				cluster,
				accountWckey,
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_0_49]/divisor),
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_50_249]/divisor),
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_250_499]/divisor),
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_500_999]/divisor),
				fmt.Sprintf("%.0f", summary.CpuTime[protos.CpuLevel_CPU_LEVEL_1000_PLUS]/divisor),
				fmt.Sprintf("%.0f", accountWckeyTotalCpu/divisor),
				fmt.Sprintf("%.2f%%", percent),
			}
			table.Append(row)
		}
	}
	table.Render()
}
