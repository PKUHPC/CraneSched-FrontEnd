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

package cacct

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	stub protos.CraneCtldClient
)

const (
	kCraneExitCodeBase = 256
)

// QueryJob will query all pending, running and completed tasks
func QueryJob() util.CraneCmdError {
	request := protos.QueryTasksInfoRequest{OptionIncludeCompletedTasks: true}

	if FlagFilterStartTime != "" {
		request.FilterStartTimeInterval = &protos.TimeInterval{}
		if !strings.Contains(FlagFilterStartTime, "~") {
			log.Fatalf("Failed to parse the time string: char '~' not found in \"%s\"! Please input an interval!", FlagFilterStartTime)
		}
		split := strings.Split(FlagFilterStartTime, "~")
		if split[0] != "" {
			tl, err := util.ParseTime(split[0])
			if err != nil {
				log.Errorf("Failed to parse the time string: %s.\n", err)
				return util.ErrorCmdArg
			}
			request.FilterStartTimeInterval.LowerBound = timestamppb.New(tl)
		}
		if len(split) >= 2 && split[1] != "" {
			tr, err := util.ParseTime(split[1])
			if err != nil {
				log.Errorf("Failed to parse the time string: %s.\n", err)
				return util.ErrorCmdArg
			}
			request.FilterStartTimeInterval.UpperBound = timestamppb.New(tr)
			if request.FilterStartTimeInterval.UpperBound.AsTime().Before(request.FilterStartTimeInterval.LowerBound.AsTime()) {
				log.Fatalf("Parameter error: the right time is earlier than the left time in '%s'", FlagFilterStartTime)
			}
		}
	}
	if FlagFilterEndTime != "" {
		request.FilterEndTimeInterval = &protos.TimeInterval{}
		if !strings.Contains(FlagFilterEndTime, "~") {
			log.Fatalf("Failed to parse the time string: char '~' not found in \"%s\"! Please input an interval!", FlagFilterEndTime)
		}
		split := strings.Split(FlagFilterEndTime, "~")
		if split[0] != "" {
			tl, err := util.ParseTime(split[0])
			if err != nil {
				log.Errorf("Failed to parse the time string: %s.\n", err)
				return util.ErrorCmdArg
			}
			request.FilterEndTimeInterval.LowerBound = timestamppb.New(tl)
		}
		if len(split) >= 2 && split[1] != "" {
			tr, err := util.ParseTime(split[1])
			if err != nil {
				log.Errorf("Failed to parse the time string: %s.\n", err)
				return util.ErrorCmdArg
			}
			request.FilterEndTimeInterval.UpperBound = timestamppb.New(tr)
			if request.FilterEndTimeInterval.UpperBound.AsTime().Before(request.FilterEndTimeInterval.LowerBound.AsTime()) {
				log.Fatalf("Parameter error: the right time is earlier than the left time in '%s'", FlagFilterEndTime)
			}
		}
	}
	if FlagFilterSubmitTime != "" {
		request.FilterSubmitTimeInterval = &protos.TimeInterval{}
		if !strings.Contains(FlagFilterSubmitTime, "~") {
			log.Fatalf("Failed to parse the time string: char '~' not found in '%s'! Please input an interval!", FlagFilterSubmitTime)
		}
		split := strings.Split(FlagFilterSubmitTime, "~")
		if split[0] != "" {
			tl, err := util.ParseTime(split[0])
			if err != nil {
				log.Errorf("Failed to parse the time string: %s.\n", err)
				return util.ErrorCmdArg
			}
			request.FilterSubmitTimeInterval.LowerBound = timestamppb.New(tl)
		}
		if len(split) >= 2 && split[1] != "" {
			tr, err := util.ParseTime(split[1])
			if err != nil {
				log.Errorf("Failed to parse the time string: %s.\n", err)
				return util.ErrorCmdArg
			}
			request.FilterSubmitTimeInterval.UpperBound = timestamppb.New(tr)
			if request.FilterSubmitTimeInterval.UpperBound.AsTime().Before(request.FilterSubmitTimeInterval.LowerBound.AsTime()) {
				log.Fatalf("Parameter error: the right time is earlier than the left time in '%s'", FlagFilterSubmitTime)
			}
		}
	}

	if FlagFilterAccounts != "" {
		filterAccountList := strings.Split(FlagFilterAccounts, ",")
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterJobIDs != "" {
		filterJobIdList := strings.Split(FlagFilterJobIDs, ",")

		var filterJobIdListInt []uint32
		for i := 0; i < len(filterJobIdList); i++ {
			id, err := strconv.ParseUint(filterJobIdList[i], 10, 32)
			if err != nil {
				log.Errorf("Invalid job id given: %s.\n", filterJobIdList[i])
				return util.ErrorCmdArg
			}
			filterJobIdListInt = append(filterJobIdListInt, uint32(id))
		}
		request.FilterTaskIds = filterJobIdListInt
	}

	if FlagFilterUsers != "" {
		filterUserList := strings.Split(FlagFilterUsers, ",")
		request.FilterUsers = filterUserList
	}

	if FlagFilterJobNames != "" {
		filterJobNameList := strings.Split(FlagFilterJobNames, ",")
		request.FilterTaskNames = filterJobNameList
	}

	if FlagFilterStates != "" {
		var stateList []protos.TaskStatus
		has_all := false
		filterStateList := strings.Split(strings.ToLower(FlagFilterStates), ",")
		for i := 0; i < len(filterStateList); i++ {
			switch filterStateList[i] {
			case "p", "pending":
				stateList = append(stateList, protos.TaskStatus_Pending)
			case "r", "running":
				stateList = append(stateList, protos.TaskStatus_Running)
			case "c", "completed":
				stateList = append(stateList, protos.TaskStatus_Completed)
			case "f", "failed":
				stateList = append(stateList, protos.TaskStatus_Failed)
			case "t", "tle", "time-limit-exceeded", "timelimitexceeded":
				stateList = append(stateList, protos.TaskStatus_ExceedTimeLimit)
			case "x", "canceled", "cancelled":
				stateList = append(stateList, protos.TaskStatus_Cancelled)
			case "i", "invalid":
				stateList = append(stateList, protos.TaskStatus_Invalid)
			case "all":
				has_all = true
			default:
				log.Errorf("Invalid state given: %s.\n", filterStateList[i])
				return util.ErrorCmdArg
			}
		}
		if !has_all {
			request.FilterTaskStates = stateList
		}
	}

	if FlagFilterQos != "" {
		filterJobQosList := strings.Split(FlagFilterQos, ",")
		request.FilterQos = filterJobQosList
	}

	if FlagFilterPartitions != "" {
		filterPartitionList := strings.Split(FlagFilterPartitions, ",")
		request.FilterPartitions = filterPartitionList
	}

	if FlagNumLimit != 0 {
		request.NumLimit = FlagNumLimit
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show tasks")
		return util.ErrorNetwork
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	header := []string{"JobId", "JobName", "Partition", "Account", "AllocCPUs", "State", "ExitCode"}

	tableData := make([][]string, len(reply.TaskInfoList))
	for i := 0; i < len(reply.TaskInfoList); i++ {
		exitCode := ""
		if reply.TaskInfoList[i].ExitCode >= kCraneExitCodeBase {
			exitCode = fmt.Sprintf("0:%d", reply.TaskInfoList[i].ExitCode-kCraneExitCodeBase)
		} else {
			exitCode = fmt.Sprintf("%d:0", reply.TaskInfoList[i].ExitCode)
		}
		tableData[i] = []string{
			strconv.FormatUint(uint64(reply.TaskInfoList[i].TaskId), 10),
			reply.TaskInfoList[i].Name,
			reply.TaskInfoList[i].Partition,
			reply.TaskInfoList[i].Account,
			strconv.FormatFloat(reply.TaskInfoList[i].AllocCpu, 'f', 2, 64),
			reply.TaskInfoList[i].Status.String(),
			exitCode}
	}

	if FlagFormat != "" {
		header, tableData = FormatData(reply)
		table.SetTablePadding("")
		table.SetAutoFormatHeaders(false)
	}

	if FlagFilterStartTime != "" {
		header = append(header, "StartTime")
		for i := 0; i < len(tableData); i++ {
			tableData[i] = append(tableData[i],
				reply.TaskInfoList[i].StartTime.AsTime().In(time.Local).String())
		}
	}

	if FlagFilterEndTime != "" {
		header = append(header, "EndTime")
		for i := 0; i < len(tableData); i++ {
			tableData[i] = append(tableData[i],
				reply.TaskInfoList[i].EndTime.AsTime().In(time.Local).String())
		}
	}

	if FlagFilterSubmitTime != "" {
		header = append(header, "SubmitTime")
		for i := 0; i < len(tableData); i++ {
			tableData[i] = append(tableData[i],
				reply.TaskInfoList[i].SubmitTime.AsTime().In(time.Local).String())
		}
	}

	if !FlagNoHeader {
		table.SetHeader(header)
	}

	// Get index of "JobId" column
	idx := -1
	for i, val := range header {
		if val == "JobId" {
			idx = i
			break
		}
	}

	// If "JobId" column exists, sort all rows by descending order of "JobId".
	if idx != -1 {
		less := func(i, j int) bool {
			x, _ := strconv.ParseUint(tableData[i][idx], 10, 32)
			y, _ := strconv.ParseUint(tableData[j][idx], 10, 32)
			return x > y
		}
		sort.Slice(tableData, less)
	}

	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableData)
	}

	table.AppendBulk(tableData)
	table.Render()
	return util.ErrorSuccess
}

func FormatData(reply *protos.QueryTasksInfoReply) (header []string, tableData [][]string) {
	re := regexp.MustCompile(`%(\.\d+)?([a-zA-Z])`)
	specifiers := re.FindAllStringSubmatchIndex(FlagFormat, -1)
	if specifiers == nil {
		log.Errorln("Invalid format specifier.")
		os.Exit(util.ErrorInvalidFormat)
	}

	tableOutputWidth := make([]int, 0, len(specifiers))
	tableOutputHeader := make([]string, 0, len(specifiers))
	tableOutputCell := make([][]string, len(reply.TaskInfoList))

	// Get the prefix of the format string
	if specifiers[0][0] != 0 {
		prefix := FlagFormat[0:specifiers[0][0]]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, prefix)
		for j := 0; j < len(reply.TaskInfoList); j++ {
			tableOutputCell[j] = append(tableOutputCell[j], prefix)
		}
	}

	for i, spec := range specifiers {
		// Get the padding string between specifiers
		if i > 0 && spec[0]-specifiers[i-1][1] > 0 {
			padding := FlagFormat[specifiers[i-1][1]:spec[0]]
			tableOutputWidth = append(tableOutputWidth, -1)
			tableOutputHeader = append(tableOutputHeader, padding)
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], padding)
			}
		}

		// Parse width specifier
		if spec[2] == -1 {
			// w/o width specifier
			tableOutputWidth = append(tableOutputWidth, -1)
		} else {
			// with width specifier
			width, err := strconv.ParseUint(FlagFormat[spec[2]+1:spec[3]], 10, 32)
			if err != nil {
				log.Errorln("Invalid width specifier.")
				os.Exit(util.ErrorInvalidFormat)
			}
			tableOutputWidth = append(tableOutputWidth, int(width))
		}

		// Parse format specifier
		header := ""
		switch FlagFormat[spec[4]:spec[5]] {
		// a-Account, c-AllocCPUs, e-ExitCode, j-JobId, n-JobName
		// P-Partition, t-State
		case "a":
			header = "Account"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Account)
			}
		case "c":
			header = "AllocCPUs"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],
					strconv.FormatFloat(reply.TaskInfoList[j].AllocCpu, 'f', 2, 64))
			}
		case "e":
			header = "ExitCode"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				exitCode := ""
				if reply.TaskInfoList[j].ExitCode >= kCraneExitCodeBase {
					exitCode = fmt.Sprintf("0:%d", reply.TaskInfoList[j].ExitCode-kCraneExitCodeBase)
				} else {
					exitCode = fmt.Sprintf("%d:0", reply.TaskInfoList[j].ExitCode)
				}
				tableOutputCell[j] = append(tableOutputCell[j], exitCode)
			}
		case "j":
			header = "JobId"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[j].TaskId), 10))
			}
		case "n":
			header = "JobName"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Name)
			}
		case "P":
			header = "Partition"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Partition)
			}
		case "t":
			header = "State"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Status.String())
			}
		default:
			// a-Account, c-AllocCPUs, e-ExitCode, j-JobId, n-JobName
			// P-Partition, t-State
			log.Errorln("Invalid format specifier, shorthand reference:\n" +
				"a-Account, c-AllocCPUs, e-ExitCode, j-JobId, n-JobName, P-Partition, t-State")
			os.Exit(util.ErrorInvalidFormat)
		}
		tableOutputHeader = append(tableOutputHeader, strings.ToUpper(header))
	}

	// Get the suffix of the format string
	if len(FlagFormat)-specifiers[len(specifiers)-1][1] > 0 {
		suffix := FlagFormat[specifiers[len(specifiers)-1][1]:]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, suffix)
		for j := 0; j < len(reply.TaskInfoList); j++ {
			tableOutputCell[j] = append(tableOutputCell[j], suffix)
		}
	}

	return util.FormatTable(tableOutputWidth, tableOutputHeader, tableOutputCell)
}
