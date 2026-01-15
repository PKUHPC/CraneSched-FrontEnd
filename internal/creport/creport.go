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
	"encoding/json"
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
type UserTopSummaryJson struct {
	Cluster      string   `json:"cluster"`
	Username     string   `json:"login"`
	ProperName   string   `json:"proper_name"`
	Accounts     []string `json:"accounts"`
	TotalCpuTime float64  `json:"used"`
	Energy       string   `json:"energy"`
}

type AccountUserJson struct {
	Cluster    string  `json:"cluster"`
	Account    string  `json:"account"`
	Username   string  `json:"login"`
	ProperName string  `json:"proper_name"`
	Used       float64 `json:"used"`
	Energy     string  `json:"energy"`
}
type UserWckeyJson struct {
	Cluster    string  `json:"cluster"`
	Username   string  `json:"login"`
	ProperName string  `json:"proper_name"`
	Wckey      string  `json:"wckey"`
	Used       float64 `json:"used"`
}

type AccountQosJson struct {
	Cluster string  `json:"cluster"`
	Account string  `json:"account"`
	Qos     string  `json:"qos"`
	Used    float64 `json:"used"`
	Energy  string  `json:"energy"`
}

type ClusterUtilizationJson struct {
	Cluster  string  `json:"cluster"`
	Allocate float64 `json:"allocate"`
	Down     string  `json:"down"`
	Planned  string  `json:"planned"`
	Reported string  `json:"reported"`
}

type AccountCpusSummaryJson struct {
	Cluster          string    `json:"cluster"`
	Account          string    `json:"account"`
	GroupHeaders     []string  `json:"group_headers"`
	GroupValues      []float64 `json:"group_values"` // for cpu time or job count
	Total            float64   `json:"total"`
	PercentOfCluster float64   `json:"percent_of_cluster"`
}

type WckeyCpusSummaryJson struct {
	Cluster          string    `json:"cluster"`
	Wckey            string    `json:"wckey"`
	GroupHeaders     []string  `json:"group_headers"`
	GroupValues      []float64 `json:"group_values"` // for cpu time or job count
	Total            float64   `json:"total"`
	PercentOfCluster float64   `json:"percent_of_cluster"`
}

type AccountWckeyCpusSummaryJson struct {
	Cluster          string    `json:"cluster"`
	AccountWckey     string    `json:"account_wckey"`
	GroupHeaders     []string  `json:"group_headers"`
	GroupValues      []float64 `json:"group_values"` // for cpu time or job count
	Total            float64   `json:"total"`
	PercentOfCluster float64   `json:"percent_of_cluster"`
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
	request := &protos.ActiveAggregationManuallyRequest{}
	request.Uid = uint32(os.Getuid())
	reply, err := stub.ActiveAggregationManually(context.Background(), request)
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to manually trigger aggregation: %v", err))
	}
	if reply.GetOk() {
		return nil
	} else {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Failed to manually trigger aggregation : %v", reply.GetReason()))
	}
}

func QueryUsersTopSummaryItem() error {
	request := &protos.QueryJobSummaryRequest{ReportType: protos.QueryJobSummaryRequest_USER_TOP_USAGE}
	request.NumLimit = FlagTopCount
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
		if !CheckCreportOutType(FlagOutType) {
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
		fmt.Println(util.FmtJson.FormatReply(batch))
		if err == io.EOF {
			break
		}
		if err != nil {
			util.GrpcErrorPrintf(err, "Failed to receive item")
			return util.NewCraneErr(util.ErrorNetwork, "")
		}
		JobSummaryItemList = append(JobSummaryItemList, batch.ItemList...)
	}
	PrintUsersTopSumList(JobSummaryItemList, start_time, end_time, FlagGroupSet, FlagJson)

	return nil
}

func QueryJobSummary(reportType protos.QueryJobSummaryRequest_JobSummaryReportType) error {
	request := &protos.QueryJobSummaryRequest{ReportType: reportType}
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
		if FlagFilterGids == "individual" {
			request.FilterGids = []uint32{}
		} else {
			filterGids, err := util.ParseAndSortUintList(FlagFilterGids)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid gid list specified: %s.", err))
			}
			request.FilterGids = filterGids
		}
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
		if !CheckCreportOutType(FlagOutType) {
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

	switch reportType {
	case protos.QueryJobSummaryRequest_ACCOUNT_UTILIZATION_BY_USER:
		PrintAccountUserSummary(JobSummaryItemList, start_time, end_time, FlagJson, true)
	case protos.QueryJobSummaryRequest_USER_UTILIZATION_BY_ACCOUNT:
		PrintAccountUserSummary(JobSummaryItemList, start_time, end_time, FlagJson, false)
	// case protos.QueryJobSummaryRequest_CLUSTER_STATUS:
	// 	PrintClusterList(JobSummaryItemList, start_time, end_time, FlagJson)
	case protos.QueryJobSummaryRequest_ACCOUNT_UTILIZATION_BY_QOS:
		PrintAccountQosList(JobSummaryItemList, start_time, end_time, FlagJson)
	case protos.QueryJobSummaryRequest_USER_UTILIZATION_BY_WCKEY:
		PrintUserWckeySummary(JobSummaryItemList, start_time, end_time, FlagJson, true)
	case protos.QueryJobSummaryRequest_WCKEY_UTILIZATION_BY_USER:
		PrintUserWckeySummary(JobSummaryItemList, start_time, end_time, FlagJson, false)
	default:
		return util.NewCraneErr(util.ErrorCmdArg, "Unsupported cmd")
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
		if FlagFilterGids == "individual" {
			request.FilterGids = []uint32{}
		} else {
			filterGids, err := util.ParseAndSortUintList(FlagFilterGids)
			if err != nil {
				return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid gid list specified: %s.", err))
			}
			request.FilterGids = filterGids
		}
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
		if !CheckCreportOutType(FlagOutType) {
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
		PrintAccountCpusList(JobSummaryItemList, start_time, end_time, request.FilterGroupingList, FlagPrintJobCount, FlagJson)
	} else if CheckType == CheckWckeyCpusStatus {
		PrintWckeyCpusList(JobSummaryItemList, start_time, end_time, request.FilterGroupingList, FlagPrintJobCount, FlagJson)
	} else if CheckType == CheckAccountWckeyCpusStatus {
		PrintAccountWckeyCpusList(JobSummaryItemList, start_time, end_time, request.FilterGroupingList, FlagPrintJobCount, FlagJson)
	}

	return nil
}

func PrintUsersTopSumList(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time, group bool, isJson bool) {
	sort.Slice(JobSummaryItemList, func(i, j int) bool {
		return JobSummaryItemList[i].TotalCpuTime > JobSummaryItemList[j].TotalCpuTime
	})

	countMax := FlagTopCount
	divisor := ReportUsageDivisor(FlagOutType)
	usernameToName := make(map[string]string)
	outputList := make([]UserTopSummaryJson, 0, countMax)

	if group {
		// Group by username, merge accounts and sum total CPU time
		userSummaryMap := make(map[string]*UserTopSummaryJson)
		for _, item := range JobSummaryItemList {
			summary, exists := userSummaryMap[item.Username]
			if !exists {
				summary = &UserTopSummaryJson{
					Cluster:      item.Cluster,
					Username:     item.Username,
					Accounts:     []string{},
					TotalCpuTime: 0,
					Energy:       "0",
				}
				userSummaryMap[item.Username] = summary
			}
			// Avoid duplicate accounts
			accountSet := make(map[string]struct{})
			for _, acc := range summary.Accounts {
				accountSet[acc] = struct{}{}
			}
			if _, found := accountSet[item.Account]; !found {
				summary.Accounts = append(summary.Accounts, item.Account)
			}
			summary.TotalCpuTime += item.TotalCpuTime
		}
		// Convert map to slice and sort by TotalCpuTime descending
		groupedList := make([]*UserTopSummaryJson, 0, len(userSummaryMap))
		for _, v := range userSummaryMap {
			groupedList = append(groupedList, v)
		}
		sort.Slice(groupedList, func(i, j int) bool {
			return groupedList[i].TotalCpuTime > groupedList[j].TotalCpuTime
		})

		for count, summary := range groupedList {
			if count >= int(countMax) {
				break
			}
			// Lookup proper name, skip if not found
			properName, ok := usernameToName[summary.Username]
			if !ok {
				usr, err := user.Lookup(summary.Username)
				if err != nil || usr.Name == "" {
					continue // skip this user if lookup failed
				}
				properName = usr.Name
				usernameToName[summary.Username] = properName
			}
			summary.ProperName = properName
			sort.Strings(summary.Accounts)
			summary.TotalCpuTime = summary.TotalCpuTime / float64(divisor)
			outputList = append(outputList, *summary)
		}
	} else {
		// No grouping, original logic
		for count, item := range JobSummaryItemList {
			if count >= int(countMax) {
				break
			}
			// Lookup proper name, skip if not found
			properName, ok := usernameToName[item.Username]
			if !ok {
				usr, err := user.Lookup(item.Username)
				if err != nil {
					continue // skip this user if lookup failed
				}
				properName = usr.Name
				usernameToName[item.Username] = properName
			}
			outputList = append(outputList, UserTopSummaryJson{
				Cluster:      item.Cluster,
				Username:     item.Username,
				ProperName:   properName,
				Accounts:     []string{item.Account},
				TotalCpuTime: item.TotalCpuTime / float64(divisor),
				Energy:       "0",
			})
		}
	}

	// Output as JSON or table
	if isJson {
		PrintAsJsonToStdout(outputList)
		return
	}

	PrintUserTopTable(outputList, startTime, endTime, countMax)
}

// PrintUserTopTable prints the user summary as a formatted table
func PrintUserTopTable(outputList []UserTopSummaryJson, startTime, endTime time.Time, countMax uint32) {
	fmt.Println(strings.Repeat("-", 100))
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	if totalSecs > 0 {
		fmt.Printf("Top %v Users %s - %s (%d secs)\n",
			countMax, startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, false, false)
	header := []string{"Cluster", "Login", "Proper_name", "Account", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(outputList))
	for _, summary := range outputList {
		accountStr := strings.Join(summary.Accounts, ",")
		tableData = append(tableData, []string{
			summary.Cluster,
			summary.Username,
			summary.ProperName,
			accountStr,
			strconv.FormatFloat(summary.TotalCpuTime, 'f', 2, 64),
			summary.Energy,
		})
	}
	table.AppendBulk(tableData)
	table.Render()
}

// PrintAccountUserTable prints a list of AccountUserJson in table format
func PrintAccountUserTable(outputList []AccountUserJson, startTime, endTime time.Time, header []string, title string) {
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("%s %s - %s (%d secs)\n",
			title, startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, false, false)
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(outputList))
	for _, row := range outputList {
		var rowSlice []string
		for _, col := range header { // Use header as column order
			switch col {
			case "Cluster":
				rowSlice = append(rowSlice, row.Cluster)
			case "Account":
				rowSlice = append(rowSlice, row.Account)
			case "Login":
				rowSlice = append(rowSlice, row.Username)
			case "Proper_name":
				rowSlice = append(rowSlice, row.ProperName)
			case "Used":
				rowSlice = append(rowSlice, strconv.FormatFloat(row.Used, 'f', 2, 64))
			case "Energy":
				rowSlice = append(rowSlice, row.Energy)
			}
		}
		tableData = append(tableData, rowSlice)
	}
	table.AppendBulk(tableData)
	table.Render()
}

// PrintAccountUserSummary summarizes job usage by account and user, and prints as table or JSON
func PrintAccountUserSummary(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time, isJson bool, sortByAccountFirst bool) {
	divisor := ReportUsageDivisor(FlagOutType)
	usernameToName := make(map[string]string)
	var outputList []AccountUserJson

	// Sorting
	if sortByAccountFirst {
		sort.Slice(JobSummaryItemList, func(i, j int) bool {
			if JobSummaryItemList[i].Account < JobSummaryItemList[j].Account {
				return true
			}
			if JobSummaryItemList[i].Account > JobSummaryItemList[j].Account {
				return false
			}
			return JobSummaryItemList[i].Username < JobSummaryItemList[j].Username
		})
	} else {
		sort.Slice(JobSummaryItemList, func(i, j int) bool {
			if JobSummaryItemList[i].Username < JobSummaryItemList[j].Username {
				return true
			}
			if JobSummaryItemList[i].Username > JobSummaryItemList[j].Username {
				return false
			}
			return JobSummaryItemList[i].Account < JobSummaryItemList[j].Account
		})
	}

	// Build output list
	for _, item := range JobSummaryItemList {
		properName, ok := usernameToName[item.Username]
		if !ok {
			usr, err := user.Lookup(item.Username)
			if err != nil {
				continue // skip this user if lookup failed
			}
			properName = usr.Name
			usernameToName[item.Username] = properName
		}
		outputList = append(outputList, AccountUserJson{
			Cluster:    item.Cluster,
			Account:    item.Account,
			Username:   item.Username,
			ProperName: properName,
			Used:       float64(item.TotalCpuTime) / float64(divisor),
			Energy:     "0",
		})
	}

	// Output as JSON or table
	if isJson {
		PrintAsJsonToStdout(outputList)
		return
	}

	// Table output
	if sortByAccountFirst {
		header := []string{"Cluster", "Account", "Login", "Proper_name", "Used", "Energy"}
		title := "Cluster/Account/User Utilization"
		PrintAccountUserTable(outputList, startTime, endTime, header, title)
	} else {
		header := []string{"Cluster", "Login", "Proper_name", "Account", "Used", "Energy"}
		title := "Cluster/User/Account Utilization"
		PrintAccountUserTable(outputList, startTime, endTime, header, title)
	}
}

func PrintUserWckeyTable(outputList []UserWckeyJson, startTime, endTime time.Time, header []string, title string) {
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("%s %s - %s (%d secs)\n",
			title, startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, false, false)
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	for _, row := range outputList {
		var rowSlice []string
		for _, col := range header { // Use header as column order
			switch col {
			case "Cluster":
				rowSlice = append(rowSlice, row.Cluster)
			case "Login":
				rowSlice = append(rowSlice, row.Username)
			case "Proper_name":
				rowSlice = append(rowSlice, row.ProperName)
			case "Wckey":
				rowSlice = append(rowSlice, row.Wckey)
			case "Used":
				rowSlice = append(rowSlice, strconv.FormatFloat(row.Used, 'f', 2, 64))
			}
		}
		table.Append(rowSlice)
	}
	table.Render()
}

func PrintUserWckeySummary(itemList []*protos.JobSummaryItem, startTime, endTime time.Time, isJson bool, sortByUserFirst bool) {
	divisor := ReportUsageDivisor(FlagOutType)
	usernameToName := make(map[string]string)
	var outputList []UserWckeyJson

	// Sorting
	if sortByUserFirst {
		sort.Slice(itemList, func(i, j int) bool {
			if itemList[i].Username < itemList[j].Username {
				return true
			}
			if itemList[i].Username > itemList[j].Username {
				return false
			}
			return itemList[i].Wckey < itemList[j].Wckey
		})
	} else {
		sort.Slice(itemList, func(i, j int) bool {
			if itemList[i].Wckey < itemList[j].Wckey {
				return true
			}
			if itemList[i].Wckey > itemList[j].Wckey {
				return false
			}
			return itemList[i].Username < itemList[j].Username
		})
	}

	// Build output list
	for _, item := range itemList {
		properName, ok := usernameToName[item.Username]
		if !ok {
			usr, err := user.Lookup(item.Username)
			if err != nil {
				continue // skip if lookup failed
			}
			properName = usr.Name
			usernameToName[item.Username] = properName
		}
		outputList = append(outputList, UserWckeyJson{
			Cluster:    item.Cluster,
			Username:   item.Username,
			ProperName: properName,
			Wckey:      item.Wckey,
			Used:       float64(item.TotalCpuTime) / float64(divisor),
		})
	}

	if isJson {
		PrintAsJsonToStdout(outputList)
		return
	}

	// Table output
	if sortByUserFirst {
		header := []string{"Cluster", "Login", "Proper_name", "Wckey", "Used"}
		title := "Cluster/User/Wckey Utilization"
		PrintUserWckeyTable(outputList, startTime, endTime, header, title)
	} else {
		header := []string{"Cluster", "Wckey", "Login", "Proper_name", "Used"}
		title := "Cluster/Wckey/User Utilization"
		PrintUserWckeyTable(outputList, startTime, endTime, header, title)
	}
}

func PrintAccountQosList(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time, isJson bool) {
	sort.Slice(JobSummaryItemList, func(i, j int) bool {
		if JobSummaryItemList[i].Account < JobSummaryItemList[j].Account {
			return true
		}
		if JobSummaryItemList[i].Account > JobSummaryItemList[j].Account {
			return false
		}
		return JobSummaryItemList[i].Qos < JobSummaryItemList[j].Qos
	})

	divisor := ReportUsageDivisor(FlagOutType)
	var outputList []AccountQosJson

	for _, item := range JobSummaryItemList {
		outputList = append(outputList, AccountQosJson{
			Cluster: item.Cluster,
			Account: item.Account,
			Qos:     item.Qos,
			Used:    float64(item.TotalCpuTime) / float64(divisor),
			Energy:  "0",
		})
	}

	if isJson {
		PrintAsJsonToStdout(outputList)
		return
	}

	// Table output
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/Account/Qos Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, false, false)
	header := []string{"Cluster", "Account", "Qos", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(outputList))
	for _, row := range outputList {
		tableData = append(tableData, []string{
			row.Cluster,
			row.Account,
			row.Qos,
			strconv.FormatFloat(row.Used, 'f', 2, 64),
			row.Energy,
		})
	}
	table.AppendBulk(tableData)
	table.Render()
}

func PrintClusterList(JobSummaryItemList []*protos.JobSummaryItem, startTime, endTime time.Time, isJson bool) {
	clusterMap := make(map[string]float64)
	for _, item := range JobSummaryItemList {
		clusterMap[item.Cluster] += item.TotalCpuTime
	}

	divisor := ReportUsageDivisor(FlagOutType)
	var outputList []ClusterUtilizationJson

	for cluster, totalCpuTime := range clusterMap {
		outputList = append(outputList, ClusterUtilizationJson{
			Cluster:  cluster,
			Allocate: float64(totalCpuTime) / float64(divisor),
			Down:     "-",
			Planned:  "-",
			Reported: "-",
		})
	}

	if isJson {
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(outputList); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to encode json: %v\n", err)
		}
		return
	}

	// Table output
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, false, false)
	header := []string{"Cluster", "Allocate", "Down", "Planned", "Reported"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, 0, len(outputList))
	for _, row := range outputList {
		tableData = append(tableData, []string{
			row.Cluster,
			strconv.FormatFloat(row.Allocate, 'f', 2, 64),
			row.Down,
			row.Planned,
			row.Reported,
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

func PrintAccountCpusList(accountUserWckeyList []*protos.JobSizeSummaryItem, startTime, endTime time.Time, groupList []uint32, isPrintCount bool, isJson bool) {
	// Mapping: cluster -> account -> summary
	clusterAccountMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotal := make(map[string]float64)

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

	divisor := ReportUsageDivisor(FlagOutType)

	// Build group headers
	var groupHeaders []string
	if len(groupList) == 0 {
		for _, cpu := range actualGroupList {
			groupHeaders = append(groupHeaders, fmt.Sprintf("%d CPUs", cpu))
		}
	} else {
		groupHeaders = GetCpusGroupHeaders(groupList)
	}

	// Prepare outputList for JSON
	var outputList []AccountCpusSummaryJson

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
			var groupValues []float64
			if isPrintCount {
				for _, cnt := range summary.JobCount {
					groupValues = append(groupValues, float64(cnt))
					accountTotal += float64(cnt)
				}
			} else {
				for _, cpu := range summary.CpuTime {
					groupValues = append(groupValues, cpu/divisor)
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
			outputList = append(outputList, AccountCpusSummaryJson{
				Cluster:          cluster,
				Account:          account,
				GroupHeaders:     groupHeaders,
				GroupValues:      groupValues,
				Total:            accountTotal,
				PercentOfCluster: percent,
			})
		}
	}

	if isJson {
		PrintAsJsonToStdout(outputList)
		return
	}

	// Table output
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Job Sizes %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"),
			endTime.Format("2006-01-02T15:04:05"),
			totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, true, isPrintCount)

	// Build table header
	header := []string{"Cluster", "Account"}
	header = append(header, groupHeaders...)
	if isPrintCount {
		header = append(header, "Total Job Count", "% of cluster")
	} else {
		header = append(header, "Total Cpu Time", "% of cluster")
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)

	for _, row := range outputList {
		tableRow := []string{row.Cluster, row.Account}
		for _, v := range row.GroupValues {
			if isPrintCount {
				tableRow = append(tableRow, fmt.Sprintf("%.0f", v))
			} else {
				tableRow = append(tableRow, fmt.Sprintf("%.2f", v))
			}
		}
		if isPrintCount {
			tableRow = append(tableRow,
				fmt.Sprintf("%.0f", row.Total),
				fmt.Sprintf("%.2f%%", row.PercentOfCluster))
		} else {
			tableRow = append(tableRow,
				fmt.Sprintf("%.2f", row.Total),
				fmt.Sprintf("%.2f%%", row.PercentOfCluster))
		}
		table.Append(tableRow)
	}
	table.Render()
}

func PrintWckeyCpusList(accountUserWckeyList []*protos.JobSizeSummaryItem, startTime, endTime time.Time, groupList []uint32, isPrintCount bool, isJson bool) {
	// Mapping: cluster -> wckey -> summary
	clusterWckeyMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotal := make(map[string]float64)

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

	divisor := ReportUsageDivisor(FlagOutType)

	// Build group headers
	var groupHeaders []string
	if len(groupList) == 0 {
		for _, cpu := range actualGroupList {
			groupHeaders = append(groupHeaders, fmt.Sprintf("%d CPUs", cpu))
		}
	} else {
		groupHeaders = GetCpusGroupHeaders(groupList)
	}

	// Prepare outputList for JSON
	var outputList []WckeyCpusSummaryJson

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
			var wckeyTotal float64
			var groupValues []float64
			if isPrintCount {
				for _, cnt := range summary.JobCount {
					groupValues = append(groupValues, float64(cnt))
					wckeyTotal += float64(cnt)
				}
			} else {
				for _, cpu := range summary.CpuTime {
					groupValues = append(groupValues, cpu/divisor)
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
			outputList = append(outputList, WckeyCpusSummaryJson{
				Cluster:          cluster,
				Wckey:            wckey,
				GroupHeaders:     groupHeaders,
				GroupValues:      groupValues,
				Total:            wckeyTotal,
				PercentOfCluster: percent,
			})
		}
	}

	if isJson {
		PrintAsJsonToStdout(outputList)
		return
	}

	// Table output
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Job Sizes by Wckey %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"),
			endTime.Format("2006-01-02T15:04:05"),
			totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, true, isPrintCount)

	// Build table header
	header := []string{"Cluster", "Wckey"}
	header = append(header, groupHeaders...)
	if isPrintCount {
		header = append(header, "Total Job Count", "% of cluster")
	} else {
		header = append(header, "Total Cpu Time", "% of cluster")
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)

	for _, row := range outputList {
		tableRow := []string{row.Cluster, row.Wckey}
		for _, v := range row.GroupValues {
			if isPrintCount {
				tableRow = append(tableRow, fmt.Sprintf("%.0f", v))
			} else {
				tableRow = append(tableRow, fmt.Sprintf("%.2f", v))
			}
		}
		if isPrintCount {
			tableRow = append(tableRow,
				fmt.Sprintf("%.0f", row.Total),
				fmt.Sprintf("%.2f%%", row.PercentOfCluster))
		} else {
			tableRow = append(tableRow,
				fmt.Sprintf("%.2f", row.Total),
				fmt.Sprintf("%.2f%%", row.PercentOfCluster))
		}
		table.Append(tableRow)
	}
	table.Render()
}

func PrintAccountWckeyCpusList(accountUserWckeyList []*protos.JobSizeSummaryItem, startTime, endTime time.Time, groupList []uint32, isPrintCount bool, isJson bool) {
	// Mapping: cluster -> account:wckey -> summary
	clusterAccountWckeyMap := make(map[string]map[string]*CpuLevelSummary)
	clusterTotal := make(map[string]float64)

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

	divisor := ReportUsageDivisor(FlagOutType)

	// Build group headers
	var groupHeaders []string
	if len(groupList) == 0 {
		for _, cpu := range actualGroupList {
			groupHeaders = append(groupHeaders, fmt.Sprintf("%d CPUs", cpu))
		}
	} else {
		groupHeaders = GetCpusGroupHeaders(groupList)
	}

	// Prepare outputList for JSON
	var outputList []AccountWckeyCpusSummaryJson

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
			var accountWckeyTotal float64
			var groupValues []float64
			if isPrintCount {
				for _, cnt := range summary.JobCount {
					groupValues = append(groupValues, float64(cnt))
					accountWckeyTotal += float64(cnt)
				}
			} else {
				for _, cpu := range summary.CpuTime {
					groupValues = append(groupValues, cpu/divisor)
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
			outputList = append(outputList, AccountWckeyCpusSummaryJson{
				Cluster:          cluster,
				AccountWckey:     accountWckey,
				GroupHeaders:     groupHeaders,
				GroupValues:      groupValues,
				Total:            accountWckeyTotal,
				PercentOfCluster: percent,
			})
		}
	}

	if isJson {
		PrintAsJsonToStdout(outputList)
		return
	}

	// Table output
	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Job Sizes %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"),
			endTime.Format("2006-01-02T15:04:05"),
			totalSecs)
	}
	PrintUsageTypeInfo(FlagOutType, true, isPrintCount)

	// Build table header
	header := []string{"Cluster", "Account:Wckey"}
	header = append(header, groupHeaders...)
	if isPrintCount {
		header = append(header, "Total Job Count", "% of cluster")
	} else {
		header = append(header, "Total Cpu Time", "% of cluster")
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)

	for _, row := range outputList {
		tableRow := []string{row.Cluster, row.AccountWckey}
		for _, v := range row.GroupValues {
			if isPrintCount {
				tableRow = append(tableRow, fmt.Sprintf("%.0f", v))
			} else {
				tableRow = append(tableRow, fmt.Sprintf("%.2f", v))
			}
		}
		if isPrintCount {
			tableRow = append(tableRow,
				fmt.Sprintf("%.0f", row.Total),
				fmt.Sprintf("%.2f%%", row.PercentOfCluster))
		} else {
			tableRow = append(tableRow,
				fmt.Sprintf("%.2f", row.Total),
				fmt.Sprintf("%.2f%%", row.PercentOfCluster))
		}
		table.Append(tableRow)
	}
	table.Render()
}
