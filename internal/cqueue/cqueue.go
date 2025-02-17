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

package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"os"
	"os/user"
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

func QueryTasksInfo() (*protos.QueryTasksInfoReply, util.CraneCmdError) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	req := protos.QueryTasksInfoRequest{OptionIncludeCompletedTasks: false}
	var reply *protos.QueryTasksInfoReply

	if FlagFilterStates != "" {
		stateList, err := util.ParseInRamTaskStatusList(FlagFilterStates)
		if err != nil {
			log.Errorln(err)
			return reply, util.ErrorCmdArg
		}
		req.FilterTaskStates = stateList
	}

	if FlagSelf {
		cu, err := user.Current()
		if err != nil {
			log.Errorf("Failed to get current username: %v\n", err)
			return reply, util.ErrorCmdArg
		}
		req.FilterUsers = []string{cu.Username}
	}
	if FlagFilterJobNames != "" {
		filterJobNameList, err := util.ParseStringParamList(FlagFilterJobNames, ",")
		if err != nil {
			log.Errorf("Invalid job name list specified: %v.\n", err)
			return reply, util.ErrorCmdArg
		}
		req.FilterTaskNames = filterJobNameList
	}
	if FlagFilterUsers != "" {
		filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
		if err != nil {
			log.Errorf("Invalid user list specified: %v.\n", err)
			return reply, util.ErrorCmdArg
		}
		req.FilterUsers = filterUserList
	}
	if FlagFilterQos != "" {
		filterJobQosList, err := util.ParseStringParamList(FlagFilterQos, ",")
		if err != nil {
			log.Errorf("Invalid Qos list specified: %v.\n", err)
			return reply, util.ErrorCmdArg
		}
		req.FilterQos = filterJobQosList
	}
	if FlagFilterAccounts != "" {
		filterAccountList, err := util.ParseStringParamList(FlagFilterAccounts, ",")
		if err != nil {
			log.Errorf("Invalid account list specified: %v.\n", err)
			return reply, util.ErrorCmdArg
		}
		req.FilterAccounts = filterAccountList
	}
	if FlagFilterPartitions != "" {
		filterPartitionList, err := util.ParseStringParamList(FlagFilterPartitions, ",")
		if err != nil {
			log.Errorf("Invalid partition list specified: %v.\n", err)
			return reply, util.ErrorCmdArg
		}
		req.FilterPartitions = filterPartitionList
	}

	if FlagFilterJobIDs != "" {
		filterJobIdList, err := util.ParseJobIdList(FlagFilterJobIDs, ",")
		if err != nil {
			log.Errorf("Invalid job list specified: %v.\n", err)
			return reply, util.ErrorCmdArg
		}
		req.FilterTaskIds = filterJobIdList
		req.NumLimit = uint32(len(filterJobIdList))
	}
	if FlagNumLimit != 0 {
		req.NumLimit = FlagNumLimit
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query job queue")
		return reply, util.ErrorNetwork
	}

	return reply, util.ErrorSuccess
}

func QueryTableOutput(reply *protos.QueryTasksInfoReply) util.CraneCmdError {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	var header []string
	tableData := make([][]string, len(reply.TaskInfoList))
	if  FlagFull {
		header = []string{"JobId", "JobName", "UserName", "Partition", 
		 "Account", "NodeNum", "AllocCPUs", "MemPerNode", "Status", "Time", "TimeLimit", 
		 "StartTime", "SubmitTime", "Type", "Qos",  "Held", "Priority", "NodeList/Reason"}
		for i := 0; i < len(reply.TaskInfoList); i++ {
			taskInfo := reply.TaskInfoList[i]

			var timeElapsedStr string
			if reply.TaskInfoList[i].Status == protos.TaskStatus_Running {
				timeElapsedStr = util.SecondTimeFormat(reply.TaskInfoList[i].ElapsedTime.Seconds)
			} else {
				timeElapsedStr = "-"
			}

			var timeLimitStr string
			if taskInfo.TimeLimit.Seconds >= util.InvalidDuration().Seconds {
				timeLimitStr = "unlimited"
			} else {
				timeLimitStr = util.SecondTimeFormat(taskInfo.TimeLimit.Seconds)
			}

			startTimeStr := "unknown"
			startTime := taskInfo.StartTime.AsTime()
			if !startTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) &&
			startTime.Before(time.Now()) {
				startTimeStr = startTime.In(time.Local).Format("2006-01-02 15:04:05")
			}

			submitTimeStr := "unknown"
			submitTime := taskInfo.SubmitTime.AsTime()
			if !submitTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
				submitTimeStr = submitTime.In(time.Local).Format("2006-01-02 15:04:05")
			}


			var reasonOrListStr string
			if reply.TaskInfoList[i].Status == protos.TaskStatus_Pending {
				reasonOrListStr = reply.TaskInfoList[i].GetPendingReason()
			} else {
				reasonOrListStr = reply.TaskInfoList[i].GetCranedList()
			}

			tableData[i] = []string {
				strconv.FormatUint(uint64(taskInfo.TaskId), 10),
				taskInfo.Name,
				taskInfo.Username,
				taskInfo.Partition,
				taskInfo.Account,
				strconv.FormatUint(uint64(taskInfo.NodeNum), 10),
				strconv.FormatFloat(taskInfo.ResView.AllocatableRes.CpuCoreLimit*float64(taskInfo.NodeNum), 'f', 2, 64),
				strconv.FormatUint(taskInfo.ResView.AllocatableRes.MemoryLimitBytes/(1024*1024), 10),
				taskInfo.Status.String(),
				timeElapsedStr,
				timeLimitStr,
				startTimeStr,
				submitTimeStr,
				reply.TaskInfoList[i].Type.String(),
				taskInfo.Qos,
				strconv.FormatBool(taskInfo.Held),
				strconv.FormatUint(uint64(taskInfo.Priority), 10),
				reasonOrListStr}
		}
	} else {
		header = []string{"JobId", "Partition", "Name", "User",
			"Account", "Status", "Type", "Time", "TimeLimit", "Nodes", "NodeList/Reason"}

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

func Query() util.CraneCmdError {
	reply, err := QueryTasksInfo()
	if err != util.ErrorSuccess {
		return err
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}

	return QueryTableOutput(reply)
}

// FormatData formats the output data according to the format string.
// The format string can accept specifiers in the form of %.<width><format character>.
// Besides, it can contain prefix, padding and suffix strings, e.g.,
// "prefix%j_xx%t_x%.5L(Suffix)"
func FormatData(reply *protos.QueryTasksInfoReply) (header []string, tableData [][]string) {
	re := regexp.MustCompile(`%(\.\d+)?([a-zA-Z]+)`)
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
		field := FlagFormat[spec[4]:spec[5]]
		if len(field) > 1 {
			field = strings.ToLower(field)
		}
		switch field {
		// a-Account, c-CpuPerNode, C-AllocCPUs, N-NodeNum, e-ElapsedTime, j-JobId, l-TimeLimit, S-StartTime
		// s-SubmitTime, L-NodeList, m-MemPerNode, n-Name, t-State, p-Priority, P-Partition, q-Qos, T-JobType
		// u-User, u-Uid, r-Reason, r-ReqNodes, x-ExcludeNodes, h-Held
		case "a", "account":
			header = "Account"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Account)
			}
		case "c", "cpupernode":
			header = "CpuPerNode"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], strconv.FormatFloat(
					reply.TaskInfoList[j].ResView.AllocatableRes.CpuCoreLimit, 'f', 2, 64))
			}
		case "C", "alloccpus":
			header = "AllocCpus"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], strconv.FormatFloat(
					reply.TaskInfoList[j].ResView.AllocatableRes.CpuCoreLimit*float64(reply.TaskInfoList[j].NodeNum), 'f', 2, 64))
			}
		case "N", "nodenum":
			header = "NodeNum"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],
					 strconv.FormatUint(uint64(reply.TaskInfoList[j].NodeNum), 10))
			}
		case "e", "elapsedtime":
			header = "ElapsedTime"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				if reply.TaskInfoList[j].Status == protos.TaskStatus_Running {
					tableOutputCell[j] = append(tableOutputCell[j],
						util.SecondTimeFormat(reply.TaskInfoList[j].ElapsedTime.Seconds))
				} else {
					tableOutputCell[j] = append(tableOutputCell[j], "-")
				}
			}
		case "j", "jobid":
			header = "JobId"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[j].TaskId), 10))
			}
		case "l", "timelimit":
			header = "TimeLimit"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				var timeLimitStr string
				if reply.TaskInfoList[j].TimeLimit.Seconds >= util.InvalidDuration().Seconds {
					timeLimitStr = "unlimited"
				} else {
					timeLimitStr = util.SecondTimeFormat(reply.TaskInfoList[j].TimeLimit.Seconds)
				}
				tableOutputCell[j] = append(tableOutputCell[j], timeLimitStr)
			}
		case "S", "starttime":
			header = "StartTime"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				startTimeStr := "unknown"
				startTime := reply.TaskInfoList[j].StartTime.AsTime()
				if !startTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) &&
				startTime.Before(time.Now()) {
					startTimeStr = startTime.In(time.Local).Format("2006-01-02 15:04:05")
				}
				tableOutputCell[j] = append(tableOutputCell[j], startTimeStr)
			}
		case "s", "submittime":
			header = "SubmitTime"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				submitTimeStr := "unknown"
				submitTime := reply.TaskInfoList[j].SubmitTime.AsTime()
				if !submitTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
					submitTimeStr = submitTime.In(time.Local).Format("2006-01-02 15:04:05")
				}
				tableOutputCell[j] = append(tableOutputCell[j], submitTimeStr)
			}
		case "L", "nodelist":
			header = "NodeList(Reason)"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].GetCranedList())
			}
		case "m", "mempernode":
			header = "MemPerNode"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],
					strconv.FormatUint(reply.TaskInfoList[j].ResView.AllocatableRes.MemoryLimitBytes/(1024*1024), 10))
			}
		case "n", "name":
			header = "Name"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Name)
			}
		case "t", "status":
			header = "Status"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Status.String())
			}
		case "p", "priority":
			header = "Priority"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[j].Priority), 10))
			}
		case "P", "partition":
			header = "Partition"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Partition)
			}
		case "q", "qos":
			header = "QoS"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],
					reply.TaskInfoList[j].Qos)
			}
		case "T", "jobtype":
			header = "JobType"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Type.String())
			}
		case "u", "user":
			header = "User"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], reply.TaskInfoList[j].Username)
			}
		case "U", "uid":
			header = "Uid"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], strconv.FormatUint(uint64(reply.TaskInfoList[j].Uid), 10))
			}
		case "r", "reason":
			header = "Reason"
			var reasonOrListStr string
			for j := 0; j < len(reply.TaskInfoList); j++ {
				if reply.TaskInfoList[j].Status == protos.TaskStatus_Pending {
					reasonOrListStr = reply.TaskInfoList[j].GetPendingReason()
				} else {
					reasonOrListStr = " "
				}
				tableOutputCell[j] = append(tableOutputCell[j], reasonOrListStr)
			}
		case "R", "reqnodes":
			header = "ReqNodes"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], strings.Join(reply.TaskInfoList[j].ReqNodes, ","))
			}
		case "x", "excludenodes":
			header = "ExcludeNodes"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], strings.Join(reply.TaskInfoList[j].ExcludeNodes, ","))
			}
		case "h", "held":
			header = "Held"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j],  strconv.FormatBool(reply.TaskInfoList[j].Held))
			}
		default:
		// a-Account, c-CpuPerNode, C-AllocCPUs, N-NodeNum, e-ElapsedTime, j-JobId, l-TimeLimit, S-StartTime
		// s-SubmitTime, L-NodeList, m-MemPerNode, n-Name, t-State, p-Priority, P-Partition, q-Qos, T-JobType
		// u-User, u-Uid, r-Reason, r-ReqNodes, x-ExcludeNodes, h-Held
		log.Errorln("Invalid format specifier or string, string unfold case insensitive, reference:\n" +
				"a/Account, c/CpuPerNode, C/AllocCPUs, N/NodeNum, e/ElapsedTime, j/JobId, l/TimeLimit, \n" +
				"S/StartTime, s/SubmitTime, L/NodeList, m/MemPerNode, n/Name, t/State, p/Priority, P/Partition,\n" +
				"q/Qos, T/JobType, u/User, u/Uid, r/Reason, r/ReqNodes, x/ExcludeNodes, h/Held.")
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
	interval, err := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	if err != nil {
		log.Errorln("Invalid time interval.")
		return util.ErrorCmdArg
	}
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
