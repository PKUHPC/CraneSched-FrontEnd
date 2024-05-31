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

package cqueue

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
)

var (
	stub protos.CraneCtldClient
)

func Query() util.CraneCmdError {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	req := protos.QueryTasksInfoRequest{OptionIncludeCompletedTasks: false}

	var stateList []protos.TaskStatus
	if FlagFilterStates != "" {
		filterStateList := strings.Split(strings.ToLower(FlagFilterStates), ",")
		for i := 0; i < len(filterStateList); i++ {
			switch filterStateList[i] {
			case "r", "running":
				stateList = append(stateList, protos.TaskStatus_Running)
			case "p", "pending":
				stateList = append(stateList, protos.TaskStatus_Pending)
			case "c", "cancelled":
				stateList = append(stateList, protos.TaskStatus_Cancelled)
			case "etl", "exceed-time-limit":
				stateList = append(stateList, protos.TaskStatus_ExceedTimeLimit)
			default:
				log.Errorf("Invalid state given: %s\n", filterStateList[i])
				return util.ErrorCmdArg
			}
		}
		req.FilterTaskStates = stateList
	}

	if FlagFilterJobNames != "" {
		filterJobNameList := strings.Split(FlagFilterJobNames, ",")
		req.FilterTaskNames = filterJobNameList
	}
	if FlagFilterUsers != "" {
		filterUserList := strings.Split(FlagFilterUsers, ",")
		req.FilterUsers = filterUserList
	}
	if FlagFilterQos != "" {
		filterJobQosList := strings.Split(FlagFilterQos, ",")
		req.FilterQos = filterJobQosList
	}
	if FlagFilterAccounts != "" {
		filterAccountList := strings.Split(FlagFilterAccounts, ",")
		req.FilterAccounts = filterAccountList
	}
	if FlagFilterPartitions != "" {
		filterPartitionList := strings.Split(FlagFilterPartitions, ",")
		req.FilterPartitions = filterPartitionList
	}
	if FlagFilterJobIDs != "" {
		filterJobIdList := strings.Split(FlagFilterJobIDs, ",")
		req.NumLimit = int32(len(filterJobIdList))
		var filterJobIdListInt []uint32
		for i := 0; i < len(filterJobIdList); i++ {
			id, err := strconv.ParseUint(filterJobIdList[i], 10, 32)
			if err != nil {
				log.Errorf("Invalid job id given: %s\n", filterJobIdList[i])
				return util.ErrorCmdArg
			}
			filterJobIdListInt = append(filterJobIdListInt, uint32(id))
		}
		req.FilterTaskIds = filterJobIdListInt
	}

	if FlagNumLimit != 0 {
		req.NumLimit = FlagNumLimit
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query job queue")
		return util.ErrorGrpc
	}

	sort.SliceStable(reply.TaskInfoList, func(i, j int) bool {
		return reply.TaskInfoList[i].Priority > reply.TaskInfoList[j].Priority
	})

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	header := []string{"JobId", "Partition", "Name", "User",
		"Account", "Status", "Type", "TimeLimit", "Nodes", "NodeList"}
	tableData := make([][]string, len(reply.TaskInfoList))
	for i := 0; i < len(reply.TaskInfoList); i++ {
		var timeLimitStr string
		if reply.TaskInfoList[i].TimeLimit.Seconds >= util.InvalidDuration().Seconds {
			timeLimitStr = "unlimited"
		} else {
			timeLimitStr = util.SecondTimeFormat(reply.TaskInfoList[i].TimeLimit.Seconds)
		}
		tableData[i] = []string{
			strconv.FormatUint(uint64(reply.TaskInfoList[i].TaskId), 10),
			reply.TaskInfoList[i].Partition,
			reply.TaskInfoList[i].Name,
			reply.TaskInfoList[i].Username,
			reply.TaskInfoList[i].Account,
			reply.TaskInfoList[i].Status.String(),
			reply.TaskInfoList[i].Type.String(),
			timeLimitStr,
			strconv.FormatUint(uint64(reply.TaskInfoList[i].NodeNum), 10),
			reply.TaskInfoList[i].CranedList,
		}
	}

	if FlagFormat != "" {
		header, tableData = FormatData(reply)
	}

	if FlagStartTime {
		header = append(header, "StartTime")
		for i := 0; i < len(tableData); i++ {
			tableData[i] = append(tableData[i],
				reply.TaskInfoList[i].StartTime.AsTime().
					In(time.Local).Format("2006-01-02 15:04:05"))
		}
	}
	if FlagFilterQos != "" {
		header = append(header, "QoS")
		for i := 0; i < len(tableData); i++ {
			tableData[i] = append(tableData[i], reply.TaskInfoList[i].Qos)
		}
	}

	if !FlagNoHeader {
		table.SetHeader(header)
	}

	table.AppendBulk(tableData)
	table.Render()
	return util.ErrorSuccess
}

func FormatData(reply *protos.QueryTasksInfoReply) (header []string, tableData [][]string) {
	formatTableData := make([][]string, len(reply.TaskInfoList))
	formatReq := strings.Split(FlagFormat, " ")
	tableOutputWidth := make([]int, len(formatReq))
	tableOutputHeader := make([]string, len(formatReq))
	for i := 0; i < len(formatReq); i++ {
		if formatReq[i][0] != '%' || len(formatReq[i]) < 2 {
			fmt.Println("Invalid format.")
			os.Exit(util.ErrorInvalidTableFormat)
		}
		if formatReq[i][1] == '.' {
			if len(formatReq[i]) < 4 {
				fmt.Println("Invalid format.")
				os.Exit(util.ErrorInvalidTableFormat)
			}
			width, err := strconv.ParseUint(formatReq[i][2:len(formatReq[i])-1], 10, 32)
			if err != nil {
				fmt.Println("Invalid format.")
				os.Exit(util.ErrorInvalidTableFormat)
			}
			tableOutputWidth[i] = int(width)
		} else {
			tableOutputWidth[i] = -1
		}
		tableOutputHeader[i] = formatReq[i][len(formatReq[i])-1:]
		switch tableOutputHeader[i] {
		//j-TaskId, n-Name, t-State, p-Partition, u-User, a-Account, T-Type, I-NodeIndex,l-TimeLimit,N-Nodes
		case "j":
			tableOutputHeader[i] = "JobId"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[j].TaskId), 10))
			}
		case "n":
			tableOutputHeader[i] = "Name"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Name)
			}
		case "t":
			tableOutputHeader[i] = "Status"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Status.String())
			}
		case "P":
			tableOutputHeader[i] = "Partition"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Partition)

			}
		case "p":
			tableOutputHeader[i] = "Priority"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[j].Priority), 10))
			}
		case "u":
			tableOutputHeader[i] = "User"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Username)
			}
		case "a":
			tableOutputHeader[i] = "Account"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Account)
			}
		case "T":
			tableOutputHeader[i] = "Type"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Type.String())
			}
		case "I":
			tableOutputHeader[i] = "NodeList"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].CranedList)
			}
		case "l":
			tableOutputHeader[i] = "TimeLimit"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[i].TimeLimit.String())
			}
		case "N":
			tableOutputHeader[i] = "Nodes"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[i].NodeNum), 10))
			}
		case "s":
			tableOutputHeader[i] = "SubmitTime"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					reply.TaskInfoList[j].SubmitTime.AsTime().
						In(time.Local).Format("2006-01-02 15:04:05"))
			}
		case "q":
			tableOutputHeader[i] = "QoS"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					reply.TaskInfoList[j].Qos)
			}
		default:
			fmt.Println("Invalid format, shorthand reference:\n" +
				"j-JobId, n-Name, t-State, P-Partition, p-Priority, " +
				"s-SubmitTime, -u-User, a-Account, T-Type, N-NodeList, q-QoS")
			os.Exit(util.ErrorInvalidTableFormat)
		}
	}
	return util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
}

func loopedQuery(iterate uint64) util.CraneCmdError {
	interval, _ := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	for {
		fmt.Println(time.Now().String()[0:19])
		err := Query()
		if err != util.ErrorSuccess {
			return err
		}
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
