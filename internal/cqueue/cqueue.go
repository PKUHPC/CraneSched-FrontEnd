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
	"regexp"
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

	if FlagFilterStates != "" {
		var stateList []protos.TaskStatus
		has_all := false
		filterStateList := strings.Split(strings.ToLower(FlagFilterStates), ",")
		for i := 0; i < len(filterStateList); i++ {
			switch filterStateList[i] {
			case "r", "running":
				stateList = append(stateList, protos.TaskStatus_Running)
			case "p", "pending":
				stateList = append(stateList, protos.TaskStatus_Pending)
			case "all":
				has_all = true
			default:
				log.Errorf("Invalid state given: %s.\n", filterStateList[i])
				return util.ErrorCmdArg
			}
		}
		if !has_all {
			req.FilterTaskStates = stateList
		}
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
		req.NumLimit = uint32(len(filterJobIdList))
		var filterJobIdListInt []uint32
		for i := 0; i < len(filterJobIdList); i++ {
			id, err := strconv.ParseUint(filterJobIdList[i], 10, 32)
			if err != nil {
				log.Errorf("Invalid job id given: %s.\n", filterJobIdList[i])
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
		return util.ErrorNetwork
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	header := []string{"JobId", "Partition", "Name", "User",
		"Account", "Status", "Type", "Time", "TimeLimit", "Nodes", "NodeList/Reason"}
	tableData := make([][]string, len(reply.TaskInfoList))
	for i := 0; i < len(reply.TaskInfoList); i++ {
		var timeLimitStr string
		if reply.TaskInfoList[i].TimeLimit.Seconds >= util.InvalidDuration().Seconds {
			timeLimitStr = "unlimited"
		} else {
			timeLimitStr = util.SecondTimeFormat(reply.TaskInfoList[i].TimeLimit.Seconds)
		}

		var timeElapsedStr string
		if reply.TaskInfoList[i].Status == protos.TaskStatus_Running {
			timeElapsedStr = util.SecondTimeFormat(reply.TaskInfoList[i].ElapsedTime.Seconds)
		} else {
			timeElapsedStr = "-"
		}

		var reasonOrListStr string
		if reply.TaskInfoList[i].Status == protos.TaskStatus_Pending {
			reasonOrListStr = reply.TaskInfoList[i].GetPendingReason()
		} else {
			reasonOrListStr = reply.TaskInfoList[i].GetCranedList()
		}

		tableData[i] = []string{
			strconv.FormatUint(uint64(reply.TaskInfoList[i].TaskId), 10),
			reply.TaskInfoList[i].Partition,
			reply.TaskInfoList[i].Name,
			reply.TaskInfoList[i].Username,
			reply.TaskInfoList[i].Account,
			reply.TaskInfoList[i].Status.String(),
			reply.TaskInfoList[i].Type.String(),
			timeElapsedStr,
			timeLimitStr,
			strconv.FormatUint(uint64(reply.TaskInfoList[i].NodeNum), 10),
			reasonOrListStr,
		}
	}

	if FlagFormat != "" {
		header, tableData = FormatData(reply)
		table.SetTablePadding("")
		table.SetAutoFormatHeaders(false)
	}

	if FlagStartTime {
		header = append(header, "StartTime")
		for i := 0; i < len(tableData); i++ {
			startTime := reply.TaskInfoList[i].StartTime
			if startTime.Seconds != 0 {
				tableData[i] = append(tableData[i],
					startTime.AsTime().In(time.Local).
						Format("2006-01-02 15:04:05"))
			} else {
				tableData[i] = append(tableData[i], "")
			}
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

	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableData)
	}

	table.AppendBulk(tableData)
	table.Render()
	return util.ErrorSuccess
}

// FormatData formats the output data according to the format string.
// The format string can accept specifiers in the form of %.<width><format character>.
// Besides, it can contain prefix, padding and suffix strings, e.g.,
// "prefix%j_xx%t_x%.5L(Suffix)"
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
		// a-Account, e-ElapsedTime, j-JobId, l-TimeLimit, L-NodeList, n-Name, N-Nodes,
		// p-Priority, P-Partition, s-SubmitTime, t-State, T-Type, u-User
		case "a":
			header = "Account"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Account)
			}
		case "c":
			header = "CpuPerNode"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], strconv.FormatFloat(reply.TaskInfoList[j].Cpu, 'f', 2, 64))
			}
		case "C":
			header = "AllocCpus"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], strconv.FormatFloat(reply.TaskInfoList[j].AllocCpu, 'f', 2, 64))
			}
		case "D":
			header = "NodeNum"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], strconv.FormatUint(uint64(reply.TaskInfoList[j].NodeNum), 10))
			}
		case "e":
			header = "Time"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				if reply.TaskInfoList[j].Status == protos.TaskStatus_Running {
					tableOutputCell[j] = append(tableOutputCell[j],
						util.SecondTimeFormat(reply.TaskInfoList[j].ElapsedTime.Seconds))
				} else {
					tableOutputCell[j] = append(tableOutputCell[j], "-")
				}
			}
		case "j":
			header = "JobId"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[j].TaskId), 10))
			}
		case "l":
			header = "TimeLimit"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].TimeLimit.String())
			}
		case "L":
			header = "NodeList"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].GetCranedList())
			}
		case "m":
			header = "MemPerNode"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], strconv.FormatUint(reply.TaskInfoList[j].Mem/(1024*1024), 10))
			}
		case "n":
			header = "Name"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Name)
			}
		case "N":
			header = "Nodes"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[j].NodeNum), 10))
			}
		case "t":
			header = "Status"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Status.String())
			}
		case "p":
			header = "Priority"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[j].Priority), 10))
			}
		case "P":
			header = "Partition"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Partition)
			}
		case "q":
			header = "QoS"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],
					reply.TaskInfoList[j].Qos)
			}
		case "s":
			header = "SubmitTime"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],
					reply.TaskInfoList[j].SubmitTime.AsTime().
						In(time.Local).Format("2006-01-02 15:04:05"))
			}
		case "T":
			header = "Type"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Type.String())
			}
		case "u":
			header = "User"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Username)
			}
		default:
			// a-Account, c-AllocCPUs, D-NodeNum, e-ElapsedTime, j-JobId, l-TimeLimit, L-NodeList,
			// M-MemPerNode, n-Name, N-Nodes, p-Priority, P-Partition, s-SubmitTime, t-State,
			// T-Type, u-User
			log.Errorln("Invalid format specifier, shorthand reference:\n" +
				"a-Account, c-CpuPerNode, C-AllocCpus, D-NodeNum, e-ElapsedTime, j-JobId, l-TimeLimit,\n" +
				"L-NodeList, m-MemPerNode, n-Name, N-Nodes, p-Priority, P-Partition, s-SubmitTime,\n" +
				"t-State, T-Type, u-User")
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
