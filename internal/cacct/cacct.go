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
)

var (
	stub protos.CraneCtldClient
)

const (
	kTerminationSignalBase = 256
	kCraneExitCodeBase     = 320
)

// QueryJob will query all pending, running and completed tasks
func QueryJob() util.CraneCmdError {
	request := protos.QueryTasksInfoRequest{OptionIncludeCompletedTasks: true}

	if FlagFilterStartTime != "" {
		request.FilterStartTimeInterval = &protos.TimeInterval{}
		err := util.ParseInterval(FlagFilterStartTime, request.FilterStartTimeInterval)
		if err != nil {
			log.Errorf("Failed to parse the StartTime filter: %s.\n", err)
			return util.ErrorCmdArg
		}
	}
	if FlagFilterEndTime != "" {
		request.FilterEndTimeInterval = &protos.TimeInterval{}
		err := util.ParseInterval(FlagFilterEndTime, request.FilterEndTimeInterval)
		if err != nil {
			log.Errorf("Failed to parse the EndTime filter: %s.\n", err)
			return util.ErrorCmdArg
		}
	}
	if FlagFilterSubmitTime != "" {
		request.FilterSubmitTimeInterval = &protos.TimeInterval{}
		err := util.ParseInterval(FlagFilterSubmitTime, request.FilterSubmitTimeInterval)
		if err != nil {
			log.Errorf("Failed to parse the SubmitTime filter: %s.\n", err)
			return util.ErrorCmdArg
		}
	}

	if FlagFilterAccounts != "" {
		filterAccountList, err := util.ParseStringParamList(FlagFilterAccounts, ",")
		if err != nil {
			log.Errorf("Invalid account list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterJobIDs != "" {
		filterJobIdList, err := util.ParseJobIdList(FlagFilterJobIDs, ",")
		if err != nil {
			log.Errorf("Invalid job list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
		request.FilterTaskIds = filterJobIdList
	}

	if FlagFilterUsers != "" {
		filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
		if err != nil {
			log.Errorf("Invalid user list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
		request.FilterUsers = filterUserList
	}

	if FlagFilterJobNames != "" {
		filterJobNameList, err := util.ParseStringParamList(FlagFilterJobNames, ",")
		if err != nil {
			log.Errorf("Invalid job name list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
		request.FilterTaskNames = filterJobNameList
	}

	if FlagFilterStates != "" {
		stateList, err := util.ParseTaskStatusList(FlagFilterStates)
		if err != nil {
			log.Errorf("Failed to parse the state filter: %s.\n", err)
			return util.ErrorCmdArg
		}
		request.FilterTaskStates = stateList
	}

	if FlagFilterQos != "" {
		filterJobQosList, err := util.ParseStringParamList(FlagFilterQos, ",")
		if err != nil {
			log.Errorf("Invalid Qos list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
		request.FilterQos = filterJobQosList
	}

	if FlagFilterPartitions != "" {
		filterPartitionList, err := util.ParseStringParamList(FlagFilterPartitions, ",")
		if err != nil {
			log.Errorf("Invalid partition list specified: %v.\n", err)
			return util.ErrorCmdArg
		}
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

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	var header []string
	tableData := make([][]string, len(reply.TaskInfoList))
	if  FlagFull {
			header = []string{"JobId", "JobName", "UserName", "Partition", 
			"NodeNum", "Account", "AllocCPUs", "MemPerNode", "State", "TimeLimit",
			 "StartTime", "EndTime", "SubmitTime", "Qos",  "Held", "Priority", "CranedList", "ExitCode"}

		for i := 0; i < len(reply.TaskInfoList); i++ {
			taskInfo := reply.TaskInfoList[i]

			exitCode := ""
			if taskInfo.ExitCode >= kTerminationSignalBase {
				exitCode = fmt.Sprintf("0:%d", taskInfo.ExitCode-kTerminationSignalBase)
			} else {
				exitCode = fmt.Sprintf("%d:0", taskInfo.ExitCode)
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

			endTimeStr := "unknown"
			if !(taskInfo.Status == protos.TaskStatus_Pending ||
			taskInfo.Status == protos.TaskStatus_Running) {
				endTime := taskInfo.EndTime.AsTime()
				if startTime.Before(time.Now()) && endTime.After(startTime) {
					endTimeStr = endTime.In(time.Local).Format("2006-01-02 15:04:05")
				}
			}

			submitTimeStr := "unknown"
			submitTime := taskInfo.SubmitTime.AsTime()
			if !submitTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
				submitTimeStr = submitTime.In(time.Local).Format("2006-01-02 15:04:05")
			}

			tableData[i] = []string {
				strconv.FormatUint(uint64(taskInfo.TaskId), 10),
				taskInfo.Name,
				taskInfo.Username,
				taskInfo.Partition,
				strconv.FormatUint(uint64(taskInfo.NodeNum), 10),
				taskInfo.Account,
				strconv.FormatFloat(taskInfo.ResView.AllocatableRes.CpuCoreLimit*float64(taskInfo.NodeNum), 'f', 2, 64),
				strconv.FormatUint(taskInfo.ResView.AllocatableRes.MemoryLimitBytes/(1024*1024), 10),
				taskInfo.Status.String(),
				timeLimitStr,
				startTimeStr,
				endTimeStr,
				submitTimeStr,
				taskInfo.Qos,
				strconv.FormatBool(taskInfo.Held),
				strconv.FormatUint(uint64(taskInfo.Priority), 10),
				taskInfo.GetCranedList(),
				exitCode}
		}
	} else {
		header = []string{"JobId", "JobName", "Partition", "Account", "AllocCPUs", "State", "ExitCode"}

		for i := 0; i < len(reply.TaskInfoList); i++ {
			taskInfo := reply.TaskInfoList[i]

			exitCode := ""
			if taskInfo.ExitCode >= kTerminationSignalBase {
				exitCode = fmt.Sprintf("0:%d", taskInfo.ExitCode-kTerminationSignalBase)
			} else {
				exitCode = fmt.Sprintf("%d:0", taskInfo.ExitCode)
			}
			tableData[i] = []string{
				strconv.FormatUint(uint64(taskInfo.TaskId), 10),
				taskInfo.Name,
				taskInfo.Partition,
				taskInfo.Account,
				strconv.FormatFloat(taskInfo.ResView.AllocatableRes.CpuCoreLimit*float64(taskInfo.NodeNum), 'f', 2, 64),
				taskInfo.Status.String(),
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

type FieldProcessor struct {
	header  string
	process func(task *protos.TaskInfo) string
}

// Account
func ProcessAccount(task *protos.TaskInfo) string {
	return task.Account
}

// AllocCPUs
func ProcessAllocCPUs(task *protos.TaskInfo) string {
	return strconv.FormatFloat(task.ResView.AllocatableRes.CpuCoreLimit*float64(task.NodeNum), 'f', 2, 64)
}

// ExitCode
func ProcessExitCode(task *protos.TaskInfo) string {
	exitCode := ""
	if task.ExitCode >= kCraneExitCodeBase {
		exitCode = fmt.Sprintf("0:%d", task.ExitCode-kCraneExitCodeBase)
	} else {
		exitCode = fmt.Sprintf("%d:0", task.ExitCode)
	}
	return exitCode
}

// JobID
func ProcessJobID(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.TaskId), 10)
}

// JobName
func ProcessJobName(task *protos.TaskInfo) string {
	return task.Name
}

// Partition
func ProcessPartition(task *protos.TaskInfo) string {
	return task.Partition
}

// State
func ProcessState(task *protos.TaskInfo) string {
	return task.Status.String()
}

// Uid
func ProcessUid(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.Uid), 10)
}

// TimeLimit
func ProcessTimeLimit(task *protos.TaskInfo) string {
	if task.TimeLimit.Seconds >= util.InvalidDuration().Seconds {
		return "unlimited"
	}
	return util.SecondTimeFormat(task.TimeLimit.Seconds)
}

// StartTime
func ProcessStartTime(task *protos.TaskInfo) string {
	startTimeStr := "unknown"
	startTime := task.StartTime.AsTime()
	if !startTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) &&
		startTime.Before(time.Now()) {
		startTimeStr = startTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return startTimeStr
}

// EndTime
func ProcessEndTime(task *protos.TaskInfo) string {
	endTimeStr := "unknown"
	if task.Status != protos.TaskStatus_Pending && task.Status != protos.TaskStatus_Running {
		startTime := task.StartTime.AsTime()
		endTime := task.EndTime.AsTime()
		if startTime.Before(time.Now()) && endTime.After(startTime) {
			endTimeStr = endTime.In(time.Local).Format("2006-01-02 15:04:05")
		}
	}
	return endTimeStr
}

// SubmitTime
func ProcessSubmitTime(task *protos.TaskInfo) string {
	submitTimeStr := "unknown"
	submitTime := task.SubmitTime.AsTime()
	if !submitTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
		submitTimeStr = submitTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return submitTimeStr
}

// ElapsedTime
func ProcessElapsedTime(task *protos.TaskInfo) string {
	if task.Status == protos.TaskStatus_Running {
		return util.SecondTimeFormat(task.ElapsedTime.Seconds)
	} else if task.Status == protos.TaskStatus_Completed {
		if task.StartTime == nil || task.EndTime == nil {
			return "-"
		}
		startTime := task.StartTime.AsTime()
		endTime := task.EndTime.AsTime()
		if startTime.Before(time.Now()) && endTime.After(startTime) {
			duration := endTime.Sub(startTime)
			return util.SecondTimeFormat(int64(duration.Seconds()))
		}		
	}

	return "-"
}

// NodeNum
func ProcessNodeNum(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.NodeNum), 10)
}

// UserName
func ProcessUserName(task *protos.TaskInfo) string {
	return task.Username
}

// Qos
func ProcessQos(task *protos.TaskInfo) string {
	return task.Qos
}

// ReqNodes
func ProcessReqNodes(task *protos.TaskInfo) string {
	return strings.Join(task.ReqNodes, ",")
}

// ExcludeNodes
func ProcessExcludeNodes(task *protos.TaskInfo) string {
	return strings.Join(task.ExcludeNodes, ",")
}

// Held
func ProcessHeld(task *protos.TaskInfo) string {
	return strconv.FormatBool(task.Held)
}

// Priority
func ProcessPriority(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.Priority), 10)
}

// NodeList
func ProcessNodeList(task *protos.TaskInfo) string {
	return task.GetCranedList()
}

// JobType
func ProcessJobType(task *protos.TaskInfo) string {
	return task.Type.String()
}

// Reason
func ProcessReason(task *protos.TaskInfo) string {
	if task.Status == protos.TaskStatus_Pending {
		return task.GetPendingReason()
	}
	return " "
}

// MemPerNode
func ProcessMemPerNode(task *protos.TaskInfo) string {
	return strconv.FormatUint(task.ResView.AllocatableRes.MemoryLimitBytes/(1024*1024), 10)
}

var fieldProcessors = map[string]FieldProcessor{
	"a":         {"Account", ProcessAccount},
	"account":   {"Account", ProcessAccount},
	"c":         {"AllocCPUs", ProcessAllocCPUs},
	"alloccpus": {"AllocCPUs", ProcessAllocCPUs},
	"e":         {"ExitCode", ProcessExitCode},
	"exitcode":  {"ExitCode", ProcessExitCode},
	"j":         {"JobID", ProcessJobID},
	"jobid":     {"JobID", ProcessJobID},
	"n":         {"JobName", ProcessJobName},
	"jobname":   {"JobName", ProcessJobName},
	"P":         {"Partition", ProcessPartition},
	"partition": {"Partition", ProcessPartition},
	"t":         {"State", ProcessState},
	"state":     {"State", ProcessState},
	"u":         {"Uid", ProcessUid},
	"uid":       {"Uid", ProcessUid},
	"l":         {"TimeLimit", ProcessTimeLimit},
	"timelimit": {"TimeLimit", ProcessTimeLimit},
	"S":         {"StartTime", ProcessStartTime},
	"starttime": {"StartTime", ProcessStartTime},
	"E":         {"EndTime", ProcessEndTime},
	"endtime":   {"EndTime", ProcessEndTime},
	"s":         {"SubmitTime", ProcessSubmitTime},
	"submittime": {"SubmitTime", ProcessSubmitTime},
	"D":         {"ElapsedTime", ProcessElapsedTime},
	"elapsedtime": {"ElapsedTime", ProcessElapsedTime},
	"N":         {"NodeNum", ProcessNodeNum},
	"nodenum":   {"NodeNum", ProcessNodeNum},
	"U":         {"UserName", ProcessUserName},
	"username":  {"UserName", ProcessUserName},
	"q":         {"Qos", ProcessQos},
	"qos":       {"Qos", ProcessQos},
	"r":         {"ReqNodes", ProcessReqNodes},
	"reqnodes":  {"ReqNodes", ProcessReqNodes},
	"x":         {"ExcludeNodes", ProcessExcludeNodes},
	"excludenodes": {"ExcludeNodes", ProcessExcludeNodes},
	"h":         {"Held", ProcessHeld},
	"held":      {"Held", ProcessHeld},
	"p":         {"Priority", ProcessPriority},
	"priority":  {"Priority", ProcessPriority},
	"L":         {"NodeList", ProcessNodeList},
	"nodelist":  {"NodeList", ProcessNodeList},
	"T":         {"JobType", ProcessJobType},
	"jobtype":   {"JobType", ProcessJobType},
	"R":         {"Reason", ProcessReason},
	"reason":    {"Reason", ProcessReason},
	"m":         {"MemPerNode", ProcessMemPerNode},
	"mempernode": {"MemPerNode", ProcessMemPerNode},
}

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
		field := FlagFormat[spec[4]:spec[5]]
		if len(field) > 1 {
			field = strings.ToLower(field)
		}

		fieldProcessor, found := fieldProcessors[field]
		if !found {
			log.Errorln("Invalid format specifier or string, string unfold case insensitive, reference:\n" +
				"a/Account, c/AllocCPUs, D/ElapsedTime, E/EndTime, e/ExitCode, h/Held, j/JobID, L/NodeList, l/TimeLimit,\n" +
				"m/MemPerNode, N/NodeNum, n/JobName, P/Partition, p/Priority, q/Qos, r/ReqNodes, R/Reason, S/StartTime,\n" +
				"s/SubmitTime, T/JobType, t/State, U/UserName, u/Uid, x/ExcludeNodes.")
			os.Exit(util.ErrorInvalidFormat)
		}

		// Add header and process data
		tableOutputHeader = append(tableOutputHeader, strings.ToUpper(fieldProcessor.header))
		for j, task := range reply.TaskInfoList {
			tableOutputCell[j] = append(tableOutputCell[j], fieldProcessor.process(task))
		}

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