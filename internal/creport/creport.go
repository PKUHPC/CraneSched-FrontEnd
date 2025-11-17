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

	"io"
	"os/user"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"

	"google.golang.org/protobuf/types/known/emptypb"
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

type CpuLevelSummary struct {
	CpuTime  []float64
	JobCount []int64
}

type UserSummary struct {
	Cluster      string
	Username     string
	Accounts     map[string]struct{}
	TotalCpuTime float64
}

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

func ActiveAggregationManually() error {
	if os.Geteuid() == 0 {
		_, err := stub.ActiveAggregationManually(context.Background(), &emptypb.Empty{})
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("ActiveAggregationManually err: %v", err))
		}
		return nil
	} else {
		return util.NewCraneErr(util.ErrorCmdArg, "Only the root user can perform this operation")
	}
}

func QueryUsersTopSummaryItem() error {
	request := &protos.QueryJobSummaryRequest{}
	if FlagFilterAccounts != "" {
		filterAccountList, err := util.ParseStringParamList(FlagFilterAccounts, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid account list specified: %s.", err))
		}
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterUsers != "" {
		filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid user list specified: %s.", err))
		}
		request.FilterUsers = filterUserList
	}

	var start_time, end_time time.Time
	var err error
	if FlagFilterStartTime != "" {
		start_time, err = util.ParseTime(FlagFilterStartTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to parse the StartTime filter: %s.", err))
		}
		request.FilterStartTime = timestamppb.New(start_time)
	}

	if FlagFilterEndTime != "" {
		end_time, err = util.ParseTime(FlagFilterEndTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to parse the EndTime filter: %s.", err))
		}
		request.FilterEndTime = timestamppb.New(end_time)
		if request.FilterStartTime != nil {
			start := request.FilterStartTime.AsTime()
			end := request.FilterEndTime.AsTime()
			if !end.After(start) {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("End_time %s must be after start_time %s", FlagFilterEndTime, FlagFilterStartTime))
			}
		}
	}

	if FlagOutType != "" {
		if !util.CheckCreportOutType(FlagOutType) {
			return util.NewCraneErr(util.ErrorCmdArg, "Invalid --time/-t, please input seconds/minutes/hours")
		}
	}

	stream, err := stub.QueryJobSummary(context.Background(), request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query AccountUserSummary info")
		return util.NewCraneErr(util.ErrorNetwork, "")
	}

	var JobSummaryItemList []*protos.JobSummaryItem
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			util.GrpcErrorPrintf(err, "Failed to receive item")
			return util.NewCraneErr(util.ErrorNetwork, "")
		}
		JobSummaryItemList = append(JobSummaryItemList, batch.ItemList...)
	}
	PrintUsersTopSumList(JobSummaryItemList, start_time, end_time, FlagGroupSet)

	return nil
}

func QueryJobSummary(CheckType CheckStatus) error {
	request := &protos.QueryJobSummaryRequest{}
	if FlagFilterAccounts != "" {
		filterAccountList, err := util.ParseStringParamList(FlagFilterAccounts, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid account list specified: %s.", err))
		}
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterUsers != "" {
		filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid user list specified: %s.", err))
		}
		request.FilterUsers = filterUserList
	}

	if FlagFilterQosList != "" {
		filterQosList, err := util.ParseStringParamList(FlagFilterQosList, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid qos list specified: %s.", err))
		}
		request.FilterQoss = filterQosList
	}

	if FlagFilterGids != "" {
		filterGidList, err := util.GetUsersByGIDs(FlagFilterGids)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid gid list specified: %s.", err))
		}
		request.FilterUsers = util.MergeAndDedup(request.FilterUsers, filterGidList)
	}

	var start_time, end_time time.Time
	var err error
	if FlagFilterStartTime != "" {
		start_time, err = util.ParseTime(FlagFilterStartTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to parse the StartTime filter: %s.", err))
		}
		request.FilterStartTime = timestamppb.New(start_time)
	}

	if FlagFilterEndTime != "" {
		end_time, err = util.ParseTime(FlagFilterEndTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to parse the EndTime filter: %s.", err))
		}
		request.FilterEndTime = timestamppb.New(end_time)
		if request.FilterStartTime != nil {
			start := request.FilterStartTime.AsTime()
			end := request.FilterEndTime.AsTime()
			if !end.After(start) {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("End_time %s must be after start_time %s", FlagFilterEndTime, FlagFilterStartTime))
			}
		}
	}

	if FlagOutType != "" {
		if !util.CheckCreportOutType(FlagOutType) {
			return util.NewCraneErr(util.ErrorCmdArg, "Invalid --time/-t, please input seconds/minutes/hours")
		}
	}

	stream, err := stub.QueryJobSummary(context.Background(), request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query AccountUserSummary info")
		return util.NewCraneErr(util.ErrorNetwork, "")
	}

	var JobSummaryItemList []*protos.JobSummaryItem
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			util.GrpcErrorPrintf(err, "Failed to receive item")
			return util.NewCraneErr(util.ErrorNetwork, "")
		}
		JobSummaryItemList = append(JobSummaryItemList, batch.ItemList...)
	}

	switch CheckType {
	case CheckAccountUserStatus:
		PrintAccountUserList(JobSummaryItemList, start_time, end_time)
	case CheckUserAccountStatus:
		PrintUserAccountList(JobSummaryItemList, start_time, end_time)
	case CheckClusterStatus:
		PrintClusterList(JobSummaryItemList, start_time, end_time)
	case CheckAccountQosStatus:
		PrintAccountQosList(JobSummaryItemList, start_time, end_time)
	case CheckUserWckeyStatus:
		PrintUserWckeyList(JobSummaryItemList, start_time, end_time)
	case CheckWckeyUserStatus:
		PrintWckeyUserList(JobSummaryItemList, start_time, end_time)
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
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid account list specified: %s.", err))
		}
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterUsers != "" {
		filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid user list specified: %s.", err))

		}
		request.FilterUsers = filterUserList
	}

	if FlagFilterQosList != "" {
		filterQosList, err := util.ParseStringParamList(FlagFilterQosList, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid qos list specified: %s.", err))
		}
		request.FilterQoss = filterQosList
	}

	if FlagFilterGids != "" {
		filterGidList, err := util.GetUsersByGIDs(FlagFilterGids)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid gid list specified: %s.", err))

		}
		request.FilterUsers = util.MergeAndDedup(request.FilterUsers, filterGidList)
	}

	var start_time, end_time time.Time
	var err error
	if FlagFilterStartTime != "" {
		start_time, err = util.ParseTime(FlagFilterStartTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to parse the StartTime filter: %s.", err))
		}
		request.FilterStartTime = timestamppb.New(start_time)
	}

	if FlagFilterEndTime != "" {
		end_time, err = util.ParseTime(FlagFilterEndTime)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to parse the EndTime filter: %s.", err))
		}
		request.FilterEndTime = timestamppb.New(end_time)
		if request.FilterStartTime != nil {
			start := request.FilterStartTime.AsTime()
			end := request.FilterEndTime.AsTime()
			if !end.After(start) {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("End_time %s must be after start_time %s", FlagFilterEndTime, FlagFilterStartTime))
			}
		}
	}

	if FlagOutType != "" {
		if !util.CheckCreportOutType(FlagOutType) {
			return util.NewCraneErr(util.ErrorCmdArg, "Invalid --time/-t, please input seconds/minutes/hours")
		}
	}

	if FlagFilterGrouping != "" {
		result, err := util.ParseAndSortUintList(FlagFilterGrouping)
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid grouping: %s", err))
		}
		request.FilterGroupingList = result
	}

	if FlagFilterJobIDs != "" {
		filterJobIdList, err := util.ParseJobIdList(FlagFilterJobIDs, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid job list specified: %s.", err))
		}
		request.FilterJobIds = filterJobIdList
	}

	if FlagFilterPartitions != "" {
		filterPartitionList, err := util.ParseStringParamList(FlagFilterPartitions, ",")
		if err != nil {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid partition list specified: %s.", err))
		}
		request.FilterPartitions = filterPartitionList
	}

	if FlagFilterNodeNames != "" {
		filterNodenameList, ok := util.ParseHostList(FlagFilterNodeNames)
		if !ok {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid node pattern: %s.", FlagFilterNodeNames))
		}
		request.FilterNodenameList = filterNodenameList
	}

	stream, err := stub.QueryJobSizeSummary(context.Background(), request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query JobSizeSummary info")
		return util.NewCraneErr(util.ErrorNetwork, "")
	}

	var JobSummaryItemList []*protos.JobSizeSummaryItem
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			util.GrpcErrorPrintf(err, "Failed to receive item")
			return util.NewCraneErr(util.ErrorNetwork, "")
		}
		JobSummaryItemList = append(JobSummaryItemList, batch.ItemList...)
	}

	if CheckType == CheckAccountCpusStatus {
		PrintAccountCpusList(JobSummaryItemList, start_time, end_time, request.FilterGroupingList, FlagPrintJobCount)
	} else if CheckType == CheckWckeyCpusStatus {
		PrintWckeyCpusList(JobSummaryItemList, start_time, end_time, request.FilterGroupingList, FlagPrintJobCount)
	} else if CheckType == CheckAccountWckeyCpusStatus {
		PrintAccountWckeyCpusList(JobSummaryItemList, start_time, end_time, request.FilterGroupingList, FlagPrintJobCount)
	}

	return nil
}

func PrintUsersTopSumList(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time, group bool) {
	sort.Slice(JobSummaryItemList, func(i, j int) bool {
		return JobSummaryItemList[i].TotalCpuTime > JobSummaryItemList[j].TotalCpuTime
	})

	countMax := FlagTopCount
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Top %v Users %s - %s (%d secs)\n",
			countMax, startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	util.PrintUsageTypeInfo(FlagOutType, false, false)
	divisor := util.ReportUsageDivisor(FlagOutType)

	header := []string{"Cluster", "Login", "Proper_name", "Account", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(JobSummaryItemList))

	usernameToName := make(map[string]string)

	if group {
		// Group by username, merge accounts and sum total CPU time
		userSummaryMap := make(map[string]*UserSummary)
		for _, item := range JobSummaryItemList {
			summary, exists := userSummaryMap[item.Username]
			if !exists {
				summary = &UserSummary{
					Cluster:      item.Cluster,
					Username:     item.Username,
					Accounts:     make(map[string]struct{}),
					TotalCpuTime: 0,
				}
				userSummaryMap[item.Username] = summary
			}
			summary.Accounts[item.Account] = struct{}{}
			summary.TotalCpuTime += item.TotalCpuTime
		}

		// Sort by total CPU time descending
		groupedList := make([]*UserSummary, 0, len(userSummaryMap))
		for _, v := range userSummaryMap {
			groupedList = append(groupedList, v)
		}
		sort.Slice(groupedList, func(i, j int) bool {
			return groupedList[i].TotalCpuTime > groupedList[j].TotalCpuTime
		})

		count := 0
		for _, summary := range groupedList {
			if count >= int(countMax) {
				break
			}
			properName, ok := usernameToName[summary.Username]
			if !ok {
				usr, err := user.Lookup(summary.Username)
				if err != nil || usr.Name == "" {
					properName = summary.Username
				} else {
					properName = usr.Name
				}
				usernameToName[summary.Username] = properName
			}
			// Merge accounts to a comma separated string
			accountList := make([]string, 0, len(summary.Accounts))
			for acc := range summary.Accounts {
				accountList = append(accountList, acc)
			}
			sort.Strings(accountList)
			accountStr := strings.Join(accountList, ",")
			tableData = append(tableData, []string{
				summary.Cluster,
				summary.Username,
				properName,
				accountStr,
				strconv.FormatFloat(float64(summary.TotalCpuTime)/float64(divisor), 'f', 2, 64),
				"0",
			})
			count++
		}
	} else {
		// Original logic, no grouping
		for count, item := range JobSummaryItemList {
			if count >= int(countMax) {
				break
			}
			properName, ok := usernameToName[item.Username]
			if !ok {
				usr, err := user.Lookup(item.Username)
				if err != nil || usr.Name == "" {
					properName = item.Username
				} else {
					properName = usr.Name
				}
				usernameToName[item.Username] = properName
			}
			tableData = append(tableData, []string{
				item.Cluster,
				item.Username,
				properName,
				item.Account,
				strconv.FormatFloat(float64(item.TotalCpuTime)/float64(divisor), 'f', 2, 64),
				"0",
			})
		}
	}

	table.AppendBulk(tableData)
	table.Render()
}

func PrintAccountUserList(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time) {
	sort.Slice(JobSummaryItemList, func(i, j int) bool {
		if JobSummaryItemList[i].Account < JobSummaryItemList[j].Account {
			return true
		}
		if JobSummaryItemList[i].Account > JobSummaryItemList[j].Account {
			return false
		}
		return JobSummaryItemList[i].Username < JobSummaryItemList[j].Username
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/Account/User Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	util.PrintUsageTypeInfo(FlagOutType, false, false)
	divisor := util.ReportUsageDivisor(FlagOutType)

	header := []string{"Cluster", "Account", "Login", "Proper_name", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(JobSummaryItemList))

	usernameToName := make(map[string]string)
	for _, item := range JobSummaryItemList {
		properName, ok := usernameToName[item.Username]
		if !ok {
			usr, err := user.Lookup(item.Username)
			if err != nil {
				continue
			}
			properName = usr.Name
			usernameToName[item.Username] = properName
		}
		tableData = append(tableData, []string{
			item.Cluster,
			item.Account,
			item.Username,
			properName,
			strconv.FormatFloat(float64(item.TotalCpuTime/divisor), 'f', 2, 64),
			"0",
		})
	}
	table.AppendBulk(tableData)
	table.Render()
}

func PrintUserAccountList(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time) {
	sort.Slice(JobSummaryItemList, func(i, j int) bool {
		if JobSummaryItemList[i].Username < JobSummaryItemList[j].Username {
			return true
		}
		if JobSummaryItemList[i].Username > JobSummaryItemList[j].Username {
			return false
		}
		return JobSummaryItemList[i].Account < JobSummaryItemList[j].Account
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/User/Account Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	util.PrintUsageTypeInfo(FlagOutType, false, false)
	divisor := util.ReportUsageDivisor(FlagOutType)

	header := []string{"Cluster", "Login", "Proper_name", "Account", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(JobSummaryItemList))
	usernameToName := make(map[string]string)
	for _, item := range JobSummaryItemList {
		properName, ok := usernameToName[item.Username]
		if !ok {
			usr, err := user.Lookup(item.Username)
			if err != nil {
				continue
			}
			properName = usr.Name
			usernameToName[item.Username] = properName
		}
		tableData = append(tableData, []string{
			item.Cluster,
			item.Username,
			properName,
			item.Account,
			strconv.FormatFloat(float64(item.TotalCpuTime/divisor), 'f', 2, 64),
			"0",
		})
	}
	table.AppendBulk(tableData)
	table.Render()
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
	util.PrintUsageTypeInfo(FlagOutType, false, false)
	divisor := util.ReportUsageDivisor(FlagOutType)

	header := []string{"Cluster", "Login", "Proper_name", "Wckey", "Used"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(accountUserWckeyList))

	usernameToName := make(map[string]string)
	for _, item := range accountUserWckeyList {
		properName, ok := usernameToName[item.Username]
		if !ok {
			usr, err := user.Lookup(item.Username)
			if err != nil {
				continue
			}
			properName = usr.Name
			usernameToName[item.Username] = properName
		}
		tableData = append(tableData, []string{
			item.Cluster,
			item.Username,
			properName,
			item.Wckey,
			strconv.FormatFloat(float64(item.TotalCpuTime/divisor), 'f', 2, 64),
		})
	}
	table.AppendBulk(tableData)
	table.Render()
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
	util.PrintUsageTypeInfo(FlagOutType, false, false)
	divisor := util.ReportUsageDivisor(FlagOutType)

	header := []string{"Cluster", "Wckey", "Login", "Proper_name", "Used"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(accountUserWckeyList))

	usernameToName := make(map[string]string)
	for _, item := range accountUserWckeyList {
		properName, ok := usernameToName[item.Username]
		if !ok {
			usr, err := user.Lookup(item.Username)
			if err != nil {
				continue
			}
			properName = usr.Name
			usernameToName[item.Username] = properName
		}
		tableData = append(tableData, []string{
			item.Cluster,
			item.Wckey,
			item.Username,
			properName,
			strconv.FormatFloat(float64(item.TotalCpuTime/divisor), 'f', 2, 64),
		})
	}
	table.AppendBulk(tableData)
	table.Render()

}

func PrintAccountQosList(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time) {
	sort.Slice(JobSummaryItemList, func(i, j int) bool {
		if JobSummaryItemList[i].Account < JobSummaryItemList[j].Account {
			return true
		}
		if JobSummaryItemList[i].Account > JobSummaryItemList[j].Account {
			return false
		}
		return JobSummaryItemList[i].Qos < JobSummaryItemList[j].Qos
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/Account/Qos Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	util.PrintUsageTypeInfo(FlagOutType, false, false)
	divisor := util.ReportUsageDivisor(FlagOutType)

	header := []string{"Cluster", "Account", "Qos", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(JobSummaryItemList))

	for _, item := range JobSummaryItemList {
		tableData = append(tableData, []string{
			item.Cluster,
			item.Account,
			item.Qos,
			strconv.FormatFloat(float64(item.TotalCpuTime/divisor), 'f', 2, 64),
			"0",
		})
	}
	table.AppendBulk(tableData)
	table.Render()
}

func PrintClusterList(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time) {
	clusterMap := make(map[string]float64)
	for _, item := range JobSummaryItemList {
		clusterMap[item.Cluster] += item.TotalCpuTime
	}

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	util.PrintUsageTypeInfo(FlagOutType, false, false)
	divisor := util.ReportUsageDivisor(FlagOutType)

	header := []string{"Cluster", "Allocate", "Down", "Planned", "Reported"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(JobSummaryItemList))
	for cluster, TotalCpuTime := range clusterMap {
		tableData = append(tableData, []string{
			cluster,
			strconv.FormatFloat(float64(TotalCpuTime/divisor), 'f', 2, 64),
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

func FindCpuGroupIndex(cpusAlloc uint32, groupList []uint32) int {
	if len(groupList) == 1 {
		return 0
	}
	for i := 0; i < len(groupList)-1; i++ {
		if cpusAlloc < groupList[i+1] {
			return i
		}
	}
	return len(groupList) - 1
}

func PrintAccountCpusList(accountUserWckeyList []*protos.JobSizeSummaryItem, startTime, endTime time.Time, groupList []uint32, isPrintCount bool) {
	// Mapping: cluster -> account -> summary
	clusterAccountMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotal := make(map[string]float64) // Holds total per cluster

	var actualGroupList []uint32
	var cpuAllocIndexMap map[uint32]int

	if len(groupList) == 0 {
		cpuAllocSet := make(map[uint32]struct{})
		for _, item := range accountUserWckeyList {
			cpuAllocSet[item.CpusAlloc] = struct{}{}
		}
		actualGroupList = make([]uint32, 0, len(cpuAllocSet))
		for cpus := range cpuAllocSet {
			actualGroupList = append(actualGroupList, cpus)
		}
		sort.Slice(actualGroupList, func(i, j int) bool { return actualGroupList[i] < actualGroupList[j] })
		cpuAllocIndexMap = make(map[uint32]int)
		for i, v := range actualGroupList {
			cpuAllocIndexMap[v] = i
		}
	} else {
		actualGroupList = groupList
	}

	// Aggregate data into clusterAccountMap
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
			// Group by actual cpusAlloc value
			groupIdx = cpuAllocIndexMap[item.CpusAlloc]
		} else {
			groupIdx = FindCpuGroupIndex(item.CpusAlloc, groupList)
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
			startTime.Format("2006-01-02T15:04:05"),
			endTime.Format("2006-01-02T15:04:05"),
			totalSecs)
	}
	util.PrintUsageTypeInfo(FlagOutType, true, isPrintCount)
	divisor := util.ReportUsageDivisor(FlagOutType)

	// Build table header
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

	// Sort and print each cluster/account row
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
					row = append(row, fmt.Sprintf("%.2f", cpu/divisor))
					accountTotal += cpu
				}
				accountTotal /= divisor
			}
			clusterTotalValue := clusterTotal[cluster]
			if !isPrintCount {
				clusterTotalValue /= divisor
			}
			percent := 0.0
			if clusterTotalValue > 0 {
				percent = accountTotal / clusterTotalValue * 100
			}
			// Format total and percent columns properly
			if isPrintCount {
				row = append(row,
					fmt.Sprintf("%d", int64(accountTotal)),
					fmt.Sprintf("%.2f%%", percent))
			} else {
				row = append(row,
					fmt.Sprintf("%.2f", accountTotal),
					fmt.Sprintf("%.2f%%", percent))
			}
			table.Append(row)
		}
	}
	table.Render()
}

func PrintWckeyCpusList(accountUserWckeyList []*protos.JobSizeSummaryItem, startTime, endTime time.Time, groupList []uint32, isPrintCount bool) {
	// Mapping: cluster -> wckey -> summary
	clusterWckeyMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotal := make(map[string]float64) // Holds total per cluster

	var actualGroupList []uint32
	var cpuAllocIndexMap map[uint32]int

	if len(groupList) == 0 {
		cpuAllocSet := make(map[uint32]struct{})
		for _, item := range accountUserWckeyList {
			cpuAllocSet[item.CpusAlloc] = struct{}{}
		}
		actualGroupList = make([]uint32, 0, len(cpuAllocSet))
		for cpus := range cpuAllocSet {
			actualGroupList = append(actualGroupList, cpus)
		}
		sort.Slice(actualGroupList, func(i, j int) bool { return actualGroupList[i] < actualGroupList[j] })
		cpuAllocIndexMap = make(map[uint32]int)
		for i, v := range actualGroupList {
			cpuAllocIndexMap[v] = i
		}
	} else {
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
			// Group by actual cpusAlloc value
			groupIdx = cpuAllocIndexMap[item.CpusAlloc]
		} else {
			groupIdx = FindCpuGroupIndex(item.CpusAlloc, groupList)
		}

		if isPrintCount {
			summary.JobCount[groupIdx] += item.TotalCount
			clusterTotal[item.Cluster] += float64(item.TotalCount)
		} else {
			summary.CpuTime[groupIdx] += item.TotalCpuTime
			clusterTotal[item.Cluster] += item.TotalCpuTime
		}
	}

	// Print table header and info
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Job Sizes by Wckey %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"),
			endTime.Format("2006-01-02T15:04:05"),
			totalSecs)
	}
	util.PrintUsageTypeInfo(FlagOutType, true, isPrintCount)
	divisor := util.ReportUsageDivisor(FlagOutType)

	// Build table header
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
					row = append(row, fmt.Sprintf("%.2f", cpu/divisor))
					wckeyTotal += cpu
				}
				wckeyTotal /= divisor
			}
			clusterTotalValue := clusterTotal[cluster]
			if !isPrintCount {
				clusterTotalValue /= divisor
			}
			percent := 0.0
			if clusterTotalValue > 0 {
				percent = wckeyTotal / clusterTotalValue * 100
			}
			// Format total and percent columns properly
			if isPrintCount {
				row = append(row,
					fmt.Sprintf("%d", int64(wckeyTotal)),
					fmt.Sprintf("%.2f%%", percent),
				)
			} else {
				row = append(row,
					fmt.Sprintf("%.2f", wckeyTotal),
					fmt.Sprintf("%.2f%%", percent),
				)
			}
			table.Append(row)
		}
	}
	table.Render()
}

func PrintAccountWckeyCpusList(accountUserWckeyList []*protos.JobSizeSummaryItem, startTime, endTime time.Time, groupList []uint32, isPrintCount bool) {
	// Mapping: cluster -> account:wckey -> summary
	clusterAccountWckeyMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotal := make(map[string]float64) // Holds total per cluster

	var actualGroupList []uint32
	var cpuAllocIndexMap map[uint32]int

	if len(groupList) == 0 {
		cpuAllocSet := make(map[uint32]struct{})
		for _, item := range accountUserWckeyList {
			cpuAllocSet[item.CpusAlloc] = struct{}{}
		}
		actualGroupList = make([]uint32, 0, len(cpuAllocSet))
		for cpus := range cpuAllocSet {
			actualGroupList = append(actualGroupList, cpus)
		}
		sort.Slice(actualGroupList, func(i, j int) bool { return actualGroupList[i] < actualGroupList[j] })
		cpuAllocIndexMap = make(map[uint32]int)
		for i, v := range actualGroupList {
			cpuAllocIndexMap[v] = i
		}
	} else {
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
			groupIdx = cpuAllocIndexMap[item.CpusAlloc]
		} else {
			groupIdx = FindCpuGroupIndex(item.CpusAlloc, groupList)
		}

		if isPrintCount {
			summary.JobCount[groupIdx] += item.TotalCount
			clusterTotal[item.Cluster] += float64(item.TotalCount)
		} else {
			summary.CpuTime[groupIdx] += item.TotalCpuTime
			clusterTotal[item.Cluster] += item.TotalCpuTime
		}
	}

	// Print table header and info
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Job Sizes %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"),
			endTime.Format("2006-01-02T15:04:05"),
			totalSecs)
	}
	util.PrintUsageTypeInfo(FlagOutType, true, isPrintCount)
	divisor := util.ReportUsageDivisor(FlagOutType)

	// Build table header
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
					row = append(row, fmt.Sprintf("%.2f", cpu/divisor))
					accountWckeyTotal += cpu
				}
				accountWckeyTotal /= divisor
			}
			clusterTotalValue := clusterTotal[cluster]
			if !isPrintCount {
				clusterTotalValue /= divisor
			}
			percent := 0.0
			if clusterTotalValue > 0 {
				percent = accountWckeyTotal / clusterTotalValue * 100
			}
			// Format total and percent columns properly
			if isPrintCount {
				row = append(row,
					fmt.Sprintf("%d", int64(accountWckeyTotal)),
					fmt.Sprintf("%.2f%%", percent),
				)
			} else {
				row = append(row,
					fmt.Sprintf("%.2f", accountWckeyTotal),
					fmt.Sprintf("%.2f%%", percent),
				)
			}
			table.Append(row)
		}
	}
	table.Render()
}
