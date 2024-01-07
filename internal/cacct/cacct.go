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
	"github.com/olekukonko/tablewriter"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	stub protos.CraneCtldClient
)

const (
	kCraneExitCodeBase = 256
)

// QueryJob will query all pending, running and completed tasks
func QueryJob() {
	request := protos.QueryTasksInfoRequest{OptionIncludeCompletedTasks: true}

	timeFormat := "2006-01-02T15:04:05"
	if FlagFilterStartTime != "" {
		request.FilterStartTimeInterval = &protos.TimeInterval{}
		split := strings.Split(FlagFilterStartTime, "~")
		if split[0] != "" {
			tl, err := time.Parse(timeFormat, split[0])
			if err != nil {
				log.Fatalf("Failed to parse the time string: %s", err)
			}
			request.FilterStartTimeInterval.LowerBound = timestamppb.New(tl)
		}
		if len(split) >= 2 && split[1] != "" {
			tr, err := time.Parse(timeFormat, split[1])
			if err != nil {
				log.Fatalf("Failed to parse the time string: %s", err)
			}
			request.FilterStartTimeInterval.UpperBound = timestamppb.New(tr)
		}
	}
	if FlagFilterEndTime != "" {
		request.FilterEndTimeInterval = &protos.TimeInterval{}
		split := strings.Split(FlagFilterEndTime, "~")
		if split[0] != "" {
			tl, err := time.Parse(timeFormat, split[0])
			if err != nil {
				log.Fatalf("Failed to parse the time string: %s", err)
			}
			request.FilterEndTimeInterval.LowerBound = timestamppb.New(tl)
		}
		if len(split) >= 2 && split[1] != "" {
			tr, err := time.Parse(timeFormat, split[1])
			if err != nil {
				log.Fatalf("Failed to parse the time string: %s", err)
			}
			request.FilterEndTimeInterval.UpperBound = timestamppb.New(tr)
		}
	}
	if FlagFilterSubmitTime != "" {
		request.FilterSubmitTimeInterval = &protos.TimeInterval{}
		split := strings.Split(FlagFilterSubmitTime, "~")
		if split[0] != "" {
			tl, err := time.Parse(timeFormat, split[0])
			if err != nil {
				log.Fatalf("Failed to parse the time string: %s", err)
			}
			request.FilterSubmitTimeInterval.LowerBound = timestamppb.New(tl)
		}
		if len(split) >= 2 && split[1] != "" {
			tr, err := time.Parse(timeFormat, split[1])
			if err != nil {
				log.Fatalf("Failed to parse the time string: %s", err)
			}
			request.FilterSubmitTimeInterval.UpperBound = timestamppb.New(tr)
		}
	}

	if FlagFilterAccounts != "" {
		filterAccountList := strings.Split(FlagFilterAccounts, ",")
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterJobIDs != "" {
		filterJobIdList := strings.Split(FlagFilterJobIDs, ",")
		request.NumLimit = int32(len(filterJobIdList))
		var filterJobIdListInt []uint32
		for i := 0; i < len(filterJobIdList); i++ {
			id, err := strconv.ParseUint(filterJobIdList[i], 10, 32)
			if err != nil {
				log.Fatalf("Invalid job id given: %s\n", filterJobIdList[i])
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

	if FlagNumLimit != 0 {
		request.NumLimit = FlagNumLimit
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &request)
	if err != nil {
		util.GrpcErrorWithMsg(err, "QueryJobsInPartition")
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	header := []string{"TaskId", "TaskName", "Partition", "Account", "AllocCPUs", "State", "ExitCode"}

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

	// Get index of "TaskId" column
	idx := -1
	for i, val := range header {
		if val == "TaskId" {
			idx = i
			break
		}
	}

	// If "TaskId" column exists, sort all rows by descending order of "TaskId".
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
}

func FormatData(reply *protos.QueryTasksInfoReply) (header []string, tableData [][]string) {
	formatTableData := make([][]string, len(reply.TaskInfoList))
	formatReq := strings.Split(FlagFormat, ",")
	tableOutputWidth := make([]int, len(formatReq))
	tableOutputHeader := make([]string, len(formatReq))
	for i := 0; i < len(formatReq); i++ {
		formatLines := strings.Split(formatReq[i], "%")
		if len(formatLines) > 2 {
			fmt.Println("Invalid format.")
			os.Exit(1)
		}
		if len(formatLines) == 2 {
			width, err := strconv.ParseUint(formatLines[1], 10, 32)
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
		tableOutputHeader[i] = formatLines[0]
		switch tableOutputHeader[i] {
		case "TaskId":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[j].TaskId), 10))
			}
		case "TaskName":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Name)
			}
		case "Partition":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Partition)
			}
		case "Account":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Account)
			}
		case "AllocCPUs":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					strconv.FormatFloat(reply.TaskInfoList[j].AllocCpu, 'f', 2, 64))
			}
		case "State":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Status.String())
			}
		case "ExitCode":
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
			fmt.Println("Invalid format.")
			os.Exit(1)
		}
	}
	return util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
}

func Preparation() {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
}
