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
		for i := 0; i < len(filterStateList) && !has_all; i++ {
			switch filterStateList[i] {
			case "p", "pending":
				stateList = append(stateList, protos.TaskStatus_Pending)
			case "r", "running":
				stateList = append(stateList, protos.TaskStatus_Running)
			case "c", "completed":
				stateList = append(stateList, protos.TaskStatus_Cancelled)
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

	table.AppendBulk(tableData)
	table.Render()
	return util.ErrorSuccess
}

func FormatData(reply *protos.QueryTasksInfoReply) (header []string, tableData [][]string) {
	formatTableData := make([][]string, len(reply.TaskInfoList))
	formatReq := strings.Fields(FlagFormat)
	tableOutputWidth := make([]int, len(formatReq))
	tableOutputHeader := make([]string, len(formatReq))
	for i := 0; i < len(formatReq); i++ {
		if formatReq[i][0] != '%' || len(formatReq[i]) < 2 {
			log.Error("Invalid format.")
			os.Exit(util.ErrorInvalidFormat)
		}
		if formatReq[i][1] == '.' {
			if len(formatReq[i]) < 4 {
				log.Error("Invalid format.")
				os.Exit(util.ErrorInvalidFormat)
			}
			width, err := strconv.ParseUint(formatReq[i][2:len(formatReq[i])-1], 10, 32)
			if err != nil {
				log.Error("Invalid format.")
				os.Exit(util.ErrorInvalidFormat)
			}
			tableOutputWidth[i] = int(width)
		} else {
			tableOutputWidth[i] = -1
		}
		tableOutputHeader[i] = formatReq[i][len(formatReq[i])-1:]
		switch tableOutputHeader[i] {
		case "j":
			tableOutputHeader[i] = "JobId"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[j].TaskId), 10))
			}
		case "n":
			tableOutputHeader[i] = "JobName"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Name)
			}
		case "P":
			tableOutputHeader[i] = "Partition"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Partition)
			}
		case "a":
			tableOutputHeader[i] = "Account"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Account)
			}
		case "c":
			tableOutputHeader[i] = "AllocCPUs"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					strconv.FormatFloat(reply.TaskInfoList[j].AllocCpu, 'f', 2, 64))
			}
		case "t":
			tableOutputHeader[i] = "State"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Status.String())
			}
		case "e":
			tableOutputHeader[i] = "ExitCode"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				exitCode := ""
				if reply.TaskInfoList[j].ExitCode >= kCraneExitCodeBase {
					exitCode = fmt.Sprintf("0:%d", reply.TaskInfoList[j].ExitCode-kCraneExitCodeBase)
				} else {
					exitCode = fmt.Sprintf("%d:0", reply.TaskInfoList[j].ExitCode)
				}
				formatTableData[j] = append(formatTableData[j], exitCode)
			}
		default:
			log.Error("Invalid format.")
			os.Exit(util.ErrorInvalidFormat)
		}
	}
	return util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
}
