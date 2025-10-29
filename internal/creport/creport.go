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
	"math"
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

func GetDefaultStartTime() string {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	zero := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, yesterday.Location())
	return zero.Format("2006-01-02T15:04:05")
}

func GetDefaultEndTime() string {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	endTime := time.Date(
		yesterday.Year(), yesterday.Month(), yesterday.Day(),
		23, 59, 59, 0, yesterday.Location())
	return endTime.Format("2006-01-02T15:04:05")
}

func QueryUsersTopSummaryItem() error {
	request := &protos.QueryJobSummaryRequest{}
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
		request.FilterUsers = util.MergeAndDedup(request.FilterUsers, filterGroupUserList)
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
		start := request.FilterStartTime.AsTime()
		end := request.FilterEndTime.AsTime()
		if !end.After(start) {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("End_time %s must be after start_time %s", FlagFilterStartTime, FlagFilterEndTime),
			}
		}
	}

	if FlagOutType != "" {
		if !util.CheckCreportOutType(FlagOutType) {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: "Invalid argument: invalid: --time/-t, please input seconds/minutes/hours",
			}
		}
	}

	rpcStart := time.Now()
	stream, err := stub.QueryJobSummary(context.Background(), request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query AccountUserSummary info")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	var JobSumItemList []*protos.JobSummaryItem

	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			util.GrpcErrorPrintf(err, "Failed to receive item")
			return &util.CraneError{Code: util.ErrorNetwork}
		}
		JobSumItemList = append(JobSumItemList, batch.ItemList...)
	}
	rpcElapsed := time.Since(rpcStart)
	fmt.Printf("[QueryJobSummary] QueryJobSummary RPC used %d ms, JobSumItemList size %v\n", rpcElapsed.Milliseconds(), len(JobSumItemList))

	PrintUsersTopSumList(JobSumItemList, start_time, end_time)

	return nil
}

func QueryJobSummary(CheckType CheckStatus) error {
	request := &protos.QueryJobSummaryRequest{}
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

	if FlagFilterGids != "" {
		filterGidList, err := util.GetUsersByGIDs(FlagFilterGids)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid gid list specified: %s.", err),
			}
		}
		request.FilterUsers = util.MergeAndDedup(request.FilterUsers, filterGidList)
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
		start := request.FilterStartTime.AsTime()
		end := request.FilterEndTime.AsTime()
		if !end.After(start) {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("End_time %s must be after start_time %s", FlagFilterStartTime, FlagFilterEndTime),
			}
		}
	}

	if FlagOutType != "" {
		if !util.CheckCreportOutType(FlagOutType) {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: "Invalid argument: invalid: --time/-t, please input seconds/minutes/hours",
			}
		}
	}

	rpcStart := time.Now()
	stream, err := stub.QueryJobSummary(context.Background(), request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query AccountUserSummary info")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	var JobSumItemList []*protos.JobSummaryItem

	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			util.GrpcErrorPrintf(err, "Failed to receive item")
			return &util.CraneError{Code: util.ErrorNetwork}
		}
		JobSumItemList = append(JobSumItemList, batch.ItemList...)
	}

	rpcElapsed := time.Since(rpcStart)
	fmt.Printf("[QueryJobSummary] QueryJobSummary RPC used %d ms, JobSumItemList size %v\n", rpcElapsed.Milliseconds(), len(JobSumItemList))

	switch CheckType {
	case CheckAccountUserStatus:
		PrintAccountUserList(JobSumItemList, start_time, end_time)
	case CheckUserAccountStatus:
		PrintUserAccountList(JobSumItemList, start_time, end_time)
	case CheckClusterStatus:
		PrintClusterList(JobSumItemList, start_time, end_time)
	case CheckAccountQosStatus:
		PrintAccountQosList(JobSumItemList, start_time, end_time)
	case CheckUserWckeyStatus:
		PrintUserWckeyList(JobSumItemList, start_time, end_time)
	case CheckWckeyUserStatus:
		PrintWckeyUserList(JobSumItemList, start_time, end_time)
	default:
		return fmt.Errorf("unsupported cmd")
	}

	return nil
}

func QueryJobSizeSummary(CheckType CheckStatus) error {

	request := &protos.QueryJobSizeSummaryRequest{}
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

	if FlagFilterGids != "" {
		filterGidList, err := util.GetUsersByGIDs(FlagFilterGids)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid gid list specified: %s.", err),
			}
		}
		request.FilterUsers = util.MergeAndDedup(request.FilterUsers, filterGidList)
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
		start := request.FilterStartTime.AsTime()
		end := request.FilterEndTime.AsTime()
		if !end.After(start) {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("End_time %s must be after start_time %s", FlagFilterStartTime, FlagFilterEndTime),
			}
		}
	}

	if FlagOutType != "" {
		if !util.CheckCreportOutType(FlagOutType) {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: "Invalid argument: invalid: --time/-t, please input seconds/minutes/hours",
			}
		}
	}

	if FlagFilterGrouping != "" {
		result, err := util.ParseAndSortUintList(FlagFilterGrouping)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid argument: invalid: --grouping: %s", err),
			}
		}
		request.FilterGroupingList = result
	}

	if FlagFilterJobIDs != "" {
		filterJobIdList, err := util.ParseJobIdList(FlagFilterJobIDs, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid job list specified: %s.", err),
			}
		}
		request.FilterJobIds = filterJobIdList
	}

	if FlagFilterPartitions != "" {
		filterPartitionList, err := util.ParseStringParamList(FlagFilterPartitions, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid partition list specified: %s.", err),
			}
		}
		request.FilterPartitions = filterPartitionList
	}

	if FlagFilterNodesName != "" {
		filterNodeNameList, ok := util.ParseHostList(FlagFilterNodesName)
	if !ok {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid node pattern: %s.", filterNodeNameList),
		}
	}
		request.FilterNodesname = filterNodeNameList
	}

	rpcStart := time.Now()
	stream, err := stub.QueryJobSizeSummary(context.Background(), request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query JobSizeSummary info")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	var JobSumItemList []*protos.JobSizeSummaryItem

	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			util.GrpcErrorPrintf(err, "Failed to receive item")
			return &util.CraneError{Code: util.ErrorNetwork}
		}
		JobSumItemList = append(JobSumItemList, batch.ItemList...)
	}

	rpcElapsed := time.Since(rpcStart)
	fmt.Printf("[QueryJobSummary] QueryJobSummary RPC used %d ms, JobSumItemList size %v\n", rpcElapsed.Milliseconds(), len(JobSumItemList))
	if CheckType == CheckAccountCpusStatus {
		PrintAccountCpusList(JobSumItemList, start_time, end_time, request.FilterGroupingList, FlagPrintJobCount)
	} else if CheckType == CheckWckeyCpusStatus {
		PrintWckeyCpusList(JobSumItemList, start_time, end_time, request.FilterGroupingList, FlagPrintJobCount)
	} else if CheckType == CheckAccountWckeyCpusStatus {
		PrintAccountWckeyCpusList(JobSumItemList, start_time, end_time, request.FilterGroupingList, FlagPrintJobCount)
	}

	return nil
}

func PrintUsersTopSumList(JobSumItemList []*protos.JobSummaryItem, startTime, endTime time.Time) {
	sort.Slice(JobSumItemList, func(i, j int) bool {
		return JobSumItemList[i].TotalCpuTime > JobSumItemList[j].TotalCpuTime
	})

	countMax := FlagTopCount
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Top %v Users %s - %s (%d secs)\n",
			countMax, startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType, false, false)
	header := []string{"Cluster", "Login", "Proper_name", "Account", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(JobSumItemList))

	var notFoundUsers []string
	for count, item := range JobSumItemList {
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

func PrintAccountUserList(JobSumItemList []*protos.JobSummaryItem, startTime, endTime time.Time) {
	sort.Slice(JobSumItemList, func(i, j int) bool {
		if JobSumItemList[i].Account < JobSumItemList[j].Account {
			return true
		}
		if JobSumItemList[i].Account > JobSumItemList[j].Account {
			return false
		}
		return JobSumItemList[i].Username < JobSumItemList[j].Username
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/Account/User Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType, false, false)

	header := []string{"Cluster", "Account", "User", "Proper_name", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(JobSumItemList))

	var notFoundUsers []string
	for _, item := range JobSumItemList {
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

func PrintUserAccountList(JobSumItemList []*protos.JobSummaryItem, startTime, endTime time.Time) {
	sort.Slice(JobSumItemList, func(i, j int) bool {
		if JobSumItemList[i].Username < JobSumItemList[j].Username {
			return true
		}
		if JobSumItemList[i].Username > JobSumItemList[j].Username {
			return false
		}
		return JobSumItemList[i].Account < JobSumItemList[j].Account
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/User/Account Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType, false, false)

	header := []string{"Cluster", "User", "Proper_name", "Account", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(JobSumItemList))

	var notFoundUsers []string
	for _, item := range JobSumItemList {
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

func PrintUserWckeyList(accountUserWckeyList []*protos.JobSummaryItem, startTime, endTime time.Time) {
	sort.Slice(accountUserWckeyList, func(i, j int) bool {
		return accountUserWckeyList[i].TotalCpuTime < accountUserWckeyList[j].TotalCpuTime
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/Account/User Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType, false, false)

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

func PrintWckeyUserList(accountUserWckeyList []*protos.JobSummaryItem, startTime, endTime time.Time) {
	sort.Slice(accountUserWckeyList, func(i, j int) bool {
		return accountUserWckeyList[i].Wckey < accountUserWckeyList[j].Wckey
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/WCKey/User Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType, false, false)

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

func PrintAccountQosList(JobSumItemList []*protos.JobSummaryItem, startTime, endTime time.Time) {
	sort.Slice(JobSumItemList, func(i, j int) bool {
		if JobSumItemList[i].Account < JobSumItemList[j].Account {
			return true
		}
		if JobSumItemList[i].Account > JobSumItemList[j].Account {
			return false
		}
		return JobSumItemList[i].Qos < JobSumItemList[j].Qos
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/Account/Qos Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType, false, false)

	header := []string{"Cluster", "Account", "Qos", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(JobSumItemList))

	for _, item := range JobSumItemList {
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

func PrintClusterList(JobSumItemList []*protos.JobSummaryItem, startTime, endTime time.Time) {
	clusterMap := make(map[string]float64)
	for _, item := range JobSumItemList {
		clusterMap[item.Cluster] += item.TotalCpuTime
	}

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType, false, false)

	header := []string{"Cluster", "Allocate", "Down", "Planned", "Reported"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(JobSumItemList))
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

func GetCpusGroupHeaders(groupList []uint32) []string {
	if len(groupList) == 1 {
		return []string{fmt.Sprintf("%d-%d CPUs", groupList[0], math.MaxUint32)}
	}
	headers := []string{}
	for i := 0; i < len(groupList)-1; i++ {
		headers = append(headers, fmt.Sprintf("%d-%d CPUs", groupList[i], groupList[i+1]-1))
	}
	headers = append(headers, fmt.Sprintf(">= %d CPUs", groupList[len(groupList)-1]))
	return headers
}

func FindCpuGroupIndex(cpuAlloc uint32, groupList []uint32) int {
	if len(groupList) == 1 {
		return 0
	}
	for i := 0; i < len(groupList)-1; i++ {
		if cpuAlloc < groupList[i+1] {
			return i
		}
	}
	return len(groupList) - 1
}

type CpuLevelSummary struct {
	CpuTime  []float64
	JobCount []int64
}

func PrintAccountCpusList(accountUserWckeyList []*protos.JobSizeSummaryItem, startTime, endTime time.Time, groupList []uint32, isPrintCount bool) {
	clusterAccountMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotal := make(map[string]float64) // float64 for cpu, int64 for count

	var actualGroupList []uint32
	var cpuAllocIndexMap map[uint32]int

	if len(groupList) == 0 {
		// Dynamically collect all actual CpuAlloc values
		cpuAllocSet := make(map[uint32]struct{})
		for _, item := range accountUserWckeyList {
			cpuAllocSet[item.CpuAlloc] = struct{}{}
		}
		actualGroupList = make([]uint32, 0, len(cpuAllocSet))
		for k := range cpuAllocSet {
			actualGroupList = append(actualGroupList, k)
		}
		sort.Slice(actualGroupList, func(i, j int) bool { return actualGroupList[i] < actualGroupList[j] })
		// Build a mapping for fast group lookup
		cpuAllocIndexMap = make(map[uint32]int)
		for i, v := range actualGroupList {
			cpuAllocIndexMap[v] = i
		}
	} else {
		// Use the original grouping logic
		actualGroupList = groupList
	}

	for _, item := range accountUserWckeyList {
		if _, ok := clusterAccountMap[item.Cluster]; !ok {
			clusterAccountMap[item.Cluster] = make(map[string]*CpuLevelSummary)
		}
		if _, ok := clusterAccountMap[item.Cluster][item.Account]; !ok {
			clusterAccountMap[item.Cluster][item.Account] = &CpuLevelSummary{
				CpuTime:  make([]float64, len(actualGroupList)),
				JobCount: make([]int64, len(actualGroupList)),
			}
		}
		summary := clusterAccountMap[item.Cluster][item.Account]

		var groupIdx int
		if len(groupList) == 0 {
			// Group by actual cpuAlloc value
			groupIdx = cpuAllocIndexMap[item.CpuAlloc]
		} else {
			// Use original bucketing/grouping logic
			groupIdx = FindCpuGroupIndex(item.CpuAlloc, groupList)
		}

		if isPrintCount {
			summary.JobCount[groupIdx] += item.TotalCount
			clusterTotal[item.Cluster] += float64(item.TotalCount)
		} else {
			summary.CpuTime[groupIdx] += item.TotalCpuTime
			clusterTotal[item.Cluster] += item.TotalCpuTime
		}
	}

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Job Sizes %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType, true, isPrintCount)

	// Generate table header dynamically
	header := []string{"Cluster", "Account"}
	if len(groupList) == 0 {
		for _, cpu := range actualGroupList {
			header = append(header, fmt.Sprintf("%d CPUs", cpu))
		}
	} else {
		header = append(header, GetCpusGroupHeaders(groupList)...)
	}
	if isPrintCount {
		header = append(header, "Total Job Count", "% of cluster")
	} else {
		header = append(header, "Total Cpu Time", "% of cluster")
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
			var accountTotal float64
			row := []string{cluster, account}
			if isPrintCount {
				for _, cnt := range summary.JobCount {
					row = append(row, fmt.Sprintf("%d", cnt))
					accountTotal += float64(cnt)
				}
			} else {
				for _, cpu := range summary.CpuTime {
					row = append(row, fmt.Sprintf("%.1f", cpu/divisor))
					accountTotal += cpu
				}
				accountTotal /= divisor
			}
			percent := 0.0
			if clusterTotal[cluster] > 0 {
				percent = accountTotal / clusterTotal[cluster] * 100
			}
			row = append(row,
				fmt.Sprintf("%.1f", accountTotal),
				fmt.Sprintf("%.2f%%", percent),
			)
			table.Append(row)
		}
	}
	table.Render()
}

func PrintWckeyCpusList(
	accountUserWckeyList []*protos.JobSizeSummaryItem,
	startTime, endTime time.Time,
	groupList []uint32,
	isPrintCount bool,
) {
	// cluster -> wckey -> summary
	clusterWckeyMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotal := make(map[string]float64) // float64 for cpu, int64 for count

	var actualGroupList []uint32
	var cpuAllocIndexMap map[uint32]int

	if len(groupList) == 0 {
		// Dynamically collect all actual CpuAlloc values
		cpuAllocSet := make(map[uint32]struct{})
		for _, item := range accountUserWckeyList {
			cpuAllocSet[item.CpuAlloc] = struct{}{}
		}
		actualGroupList = make([]uint32, 0, len(cpuAllocSet))
		for k := range cpuAllocSet {
			actualGroupList = append(actualGroupList, k)
		}
		sort.Slice(actualGroupList, func(i, j int) bool { return actualGroupList[i] < actualGroupList[j] })
		// Build a mapping for fast group lookup
		cpuAllocIndexMap = make(map[uint32]int)
		for i, v := range actualGroupList {
			cpuAllocIndexMap[v] = i
		}
	} else {
		// Use the original grouping logic
		actualGroupList = groupList
	}

	for _, item := range accountUserWckeyList {
		// Initialize cluster map if not exists
		if _, ok := clusterWckeyMap[item.Cluster]; !ok {
			clusterWckeyMap[item.Cluster] = make(map[string]*CpuLevelSummary)
		}
		// Initialize wckey summary if not exists
		if _, ok := clusterWckeyMap[item.Cluster][item.Wckey]; !ok {
			clusterWckeyMap[item.Cluster][item.Wckey] = &CpuLevelSummary{
				CpuTime:  make([]float64, len(actualGroupList)),
				JobCount: make([]int64, len(actualGroupList)),
			}
		}
		summary := clusterWckeyMap[item.Cluster][item.Wckey]

		var groupIdx int
		if len(groupList) == 0 {
			// Group by actual cpuAlloc value
			groupIdx = cpuAllocIndexMap[item.CpuAlloc]
		} else {
			// Use original bucketing/grouping logic
			groupIdx = FindCpuGroupIndex(item.CpuAlloc, groupList)
		}

		if isPrintCount {
			summary.JobCount[groupIdx] += item.TotalCount
			clusterTotal[item.Cluster] += float64(item.TotalCount)
		} else {
			summary.CpuTime[groupIdx] += item.TotalCpuTime
			clusterTotal[item.Cluster] += item.TotalCpuTime
		}
	}

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Job Sizes by Wckey %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType, true, isPrintCount)

	// Generate table header dynamically
	header := []string{"Cluster", "Wckey"}
	if len(groupList) == 0 {
		for _, cpu := range actualGroupList {
			header = append(header, fmt.Sprintf("%d CPUs", cpu))
		}
	} else {
		header = append(header, GetCpusGroupHeaders(groupList)...)
	}
	if isPrintCount {
		header = append(header, "Total Job Count", "% of cluster")
	} else {
		header = append(header, "Total Cpu Time", "% of cluster")
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)

	// Collect and sort cluster names
	clusters := make([]string, 0, len(clusterWckeyMap))
	for cluster := range clusterWckeyMap {
		clusters = append(clusters, cluster)
	}
	sort.Strings(clusters)

	for _, cluster := range clusters {
		wckeyMap := clusterWckeyMap[cluster]
		// Collect and sort wckey names
		wckeyNames := make([]string, 0, len(wckeyMap))
		for wckey := range wckeyMap {
			wckeyNames = append(wckeyNames, wckey)
		}
		sort.Strings(wckeyNames)
		for _, wckey := range wckeyNames {
			summary := wckeyMap[wckey]
			var wckeyTotal float64
			row := []string{cluster, wckey}
			if isPrintCount {
				for _, cnt := range summary.JobCount {
					row = append(row, fmt.Sprintf("%d", cnt))
					wckeyTotal += float64(cnt)
				}
			} else {
				for _, cpu := range summary.CpuTime {
					row = append(row, fmt.Sprintf("%.1f", cpu/divisor))
					wckeyTotal += cpu
				}
				wckeyTotal /= divisor
			}
			percent := 0.0
			if clusterTotal[cluster] > 0 {
				percent = wckeyTotal / clusterTotal[cluster] * 100
			}
			row = append(row,
				fmt.Sprintf("%.1f", wckeyTotal),
				fmt.Sprintf("%.2f%%", percent),
			)
			table.Append(row)
		}
	}
	table.Render()
}

func PrintAccountWckeyCpusList(accountUserWckeyList []*protos.JobSizeSummaryItem, startTime, endTime time.Time, groupList []uint32, isPrintCount bool) {
	// cluster -> account:wckey -> summary
	clusterAccountWckeyMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotal := make(map[string]float64) // float64 for cpu, int64 for count

	var actualGroupList []uint32
	var cpuAllocIndexMap map[uint32]int

	if len(groupList) == 0 {
		// Dynamically collect all actual CpuAlloc values
		cpuAllocSet := make(map[uint32]struct{})
		for _, item := range accountUserWckeyList {
			cpuAllocSet[item.CpuAlloc] = struct{}{}
		}
		actualGroupList = make([]uint32, 0, len(cpuAllocSet))
		for k := range cpuAllocSet {
			actualGroupList = append(actualGroupList, k)
		}
		sort.Slice(actualGroupList, func(i, j int) bool { return actualGroupList[i] < actualGroupList[j] })
		// Build a mapping for fast group lookup
		cpuAllocIndexMap = make(map[uint32]int)
		for i, v := range actualGroupList {
			cpuAllocIndexMap[v] = i
		}
	} else {
		// Use the original grouping logic
		actualGroupList = groupList
	}

	for _, item := range accountUserWckeyList {
		// Initialize cluster map if not exists
		if _, ok := clusterAccountWckeyMap[item.Cluster]; !ok {
			clusterAccountWckeyMap[item.Cluster] = make(map[string]*CpuLevelSummary)
		}
		accountWckey := fmt.Sprintf("%s:%s", item.Account, item.Wckey)
		// Initialize account:wckey summary if not exists
		if _, ok := clusterAccountWckeyMap[item.Cluster][accountWckey]; !ok {
			clusterAccountWckeyMap[item.Cluster][accountWckey] = &CpuLevelSummary{
				CpuTime:  make([]float64, len(actualGroupList)),
				JobCount: make([]int64, len(actualGroupList)),
			}
		}
		summary := clusterAccountWckeyMap[item.Cluster][accountWckey]

		var groupIdx int
		if len(groupList) == 0 {
			// Group by actual cpuAlloc value
			groupIdx = cpuAllocIndexMap[item.CpuAlloc]
		} else {
			// Use original bucketing/grouping logic
			groupIdx = FindCpuGroupIndex(item.CpuAlloc, groupList)
		}

		if isPrintCount {
			summary.JobCount[groupIdx] += item.TotalCount
			clusterTotal[item.Cluster] += float64(item.TotalCount)
		} else {
			summary.CpuTime[groupIdx] += item.TotalCpuTime
			clusterTotal[item.Cluster] += item.TotalCpuTime
		}
	}

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Job Sizes %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	divisor := util.ReportUsageType(FlagOutType, true, isPrintCount)

	// Generate table header dynamically
	header := []string{"Cluster", "Account:Wckey"}
	if len(groupList) == 0 {
		for _, cpu := range actualGroupList {
			header = append(header, fmt.Sprintf("%d CPUs", cpu))
		}
	} else {
		header = append(header, GetCpusGroupHeaders(groupList)...)
	}
	if isPrintCount {
		header = append(header, "Total Job Count", "% of cluster")
	} else {
		header = append(header, "Total Cpu Time", "% of cluster")
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)

	// Collect and sort cluster names
	clusters := make([]string, 0, len(clusterAccountWckeyMap))
	for cluster := range clusterAccountWckeyMap {
		clusters = append(clusters, cluster)
	}
	sort.Strings(clusters)

	for _, cluster := range clusters {
		accountWckeyMap := clusterAccountWckeyMap[cluster]
		// Collect and sort account:wckey names
		accountWckeyNames := make([]string, 0, len(accountWckeyMap))
		for accountWckey := range accountWckeyMap {
			accountWckeyNames = append(accountWckeyNames, accountWckey)
		}
		sort.Strings(accountWckeyNames)
		for _, accountWckey := range accountWckeyNames {
			summary := accountWckeyMap[accountWckey]
			var accountWckeyTotal float64
			row := []string{cluster, accountWckey}
			if isPrintCount {
				for _, cnt := range summary.JobCount {
					row = append(row, fmt.Sprintf("%d", cnt))
					accountWckeyTotal += float64(cnt)
				}
			} else {
				for _, cpu := range summary.CpuTime {
					row = append(row, fmt.Sprintf("%.1f", cpu/divisor))
					accountWckeyTotal += cpu
				}
				accountWckeyTotal /= divisor
			}
			percent := 0.0
			if clusterTotal[cluster] > 0 {
				percent = accountWckeyTotal / clusterTotal[cluster] * 100
			}
			row = append(row,
				fmt.Sprintf("%.1f", accountWckeyTotal),
				fmt.Sprintf("%.2f%%", percent),
			)
			table.Append(row)
		}
	}
	table.Render()
}
