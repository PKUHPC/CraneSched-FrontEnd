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
	"github.com/tidwall/gjson"
)

var (
	stub protos.CraneCtldClient
)

const (
	kTerminationSignalBase = 256
	kCraneExitCodeBase     = 320
)

// QueryJob will query all pending, running and completed tasks
func QueryJob() error {
	request := protos.QueryTasksInfoRequest{OptionIncludeCompletedTasks: true}

	if FlagFilterStartTime != "" {
		request.FilterStartTimeInterval = &protos.TimeInterval{}
		err := util.ParseInterval(FlagFilterStartTime, request.FilterStartTimeInterval)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to parse the StartTime filter: %s.", err),
			}
		}
	}
	if FlagFilterEndTime != "" {
		request.FilterEndTimeInterval = &protos.TimeInterval{}
		err := util.ParseInterval(FlagFilterEndTime, request.FilterEndTimeInterval)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to parse the EndTime filter: %s.", err),
			}
		}
	}
	if FlagFilterSubmitTime != "" {
		request.FilterSubmitTimeInterval = &protos.TimeInterval{}
		err := util.ParseInterval(FlagFilterSubmitTime, request.FilterSubmitTimeInterval)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to parse the SubmitTime filter: %s.", err),
			}
		}
	}

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

	if FlagFilterJobIDs != "" {
		filterJobIdList, err := util.ParseJobIdList(FlagFilterJobIDs, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid job list specified: %s.", err),
			}
		}
		request.FilterTaskIds = filterJobIdList
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

	if FlagFilterJobNames != "" {
		filterJobNameList, err := util.ParseStringParamList(FlagFilterJobNames, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid job name list specified: %s.", err),
			}
		}
		request.FilterTaskNames = filterJobNameList
	}

	if FlagFilterStates != "" {
		stateList, err := util.ParseTaskStatusList(FlagFilterStates)
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to parse the state filter: %s.", err),
			}
		}
		request.FilterTaskStates = stateList
	}

	if FlagFilterQos != "" {
		filterJobQosList, err := util.ParseStringParamList(FlagFilterQos, ",")
		if err != nil {
			return &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid Qos list specified: %s.", err),
			}
		}
		request.FilterQos = filterJobQosList
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

	if FlagNumLimit != 0 {
		request.NumLimit = FlagNumLimit
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show tasks")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	var header []string
	tableData := make([][]string, len(reply.TaskInfoList))
	if FlagFull {
		header = []string{"JobId", "JobName", "UserName", "Partition",
			"NodeNum", "Account", "ReqCPUs", "ReqMemPerNode", "AllocCPUs", "AllocMemPerNode", "State", "TimeLimit",
			"StartTime", "EndTime", "SubmitTime", "Qos", "Exclusive", "Held", "Priority", "CranedList", "ExitCode", "Deadline"}

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

			deadlineTimeStr := "unknown"
			deadlineTime := taskInfo.DeadlineTime.AsTime()
			if !deadlineTime.Equal(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)) {
				deadlineTimeStr = deadlineTime.In(time.Local).Format("2006-01-02 15:04:05")
			}

			tableData[i] = []string{
				strconv.FormatUint(uint64(taskInfo.TaskId), 10),
				taskInfo.Name,
				taskInfo.Username,
				taskInfo.Partition,
				strconv.FormatUint(uint64(taskInfo.NodeNum), 10),
				taskInfo.Account,
				ProcessReqCPUs(taskInfo),
				ProcessReqMemPerNode(taskInfo),
				ProcessAllocCPUs(taskInfo),
				ProcessAllocMemPerNode(taskInfo),
				taskInfo.Status.String(),
				timeLimitStr,
				startTimeStr,
				endTimeStr,
				submitTimeStr,
				taskInfo.Qos,
				ProcessExclusive(taskInfo),
				strconv.FormatBool(taskInfo.Held),
				strconv.FormatUint(uint64(taskInfo.Priority), 10),
				taskInfo.GetCranedList(),
				exitCode,
				deadlineTimeStr}
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
				ProcessAllocCPUs(taskInfo),
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

	if FlagDeadlineTime {
		header = append(header, "Deadline")
		for i := 0; i < len(tableData); i++ {
			tableData[i] = append(tableData[i], ProcessDeadline(reply.TaskInfoList[i]))
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
	return nil
}

type FieldProcessor struct {
	header  string
	process func(task *protos.TaskInfo) string
}

// Account (a)
func ProcessAccount(task *protos.TaskInfo) string {
	return task.Account
}

// ReqCPUs (C)
func ProcessReqCPUs(task *protos.TaskInfo) string {
	return strconv.FormatFloat(task.ReqResView.AllocatableRes.CpuCoreLimit*float64(task.NodeNum), 'f', 2, 64)
}

// AllocCPUs (c)
func ProcessAllocCPUs(task *protos.TaskInfo) string {
	return strconv.FormatFloat(task.AllocatedResView.AllocatableRes.CpuCoreLimit, 'f', 2, 64)
}

// ElapsedTime (D)
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

// Deadline (D)
func ProcessDeadline(task *protos.TaskInfo) string {
	deadlineTime := task.DeadlineTime.AsTime()
	if !deadlineTime.Equal(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)) {
		return deadlineTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return "unknown"
}

// EndTime (E)
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

// ExitCode (e)
func ProcessExitCode(task *protos.TaskInfo) string {
	exitCode := ""
	if task.ExitCode >= kTerminationSignalBase {
		exitCode = fmt.Sprintf("0:%d", task.ExitCode-kTerminationSignalBase)
	} else {
		exitCode = fmt.Sprintf("%d:0", task.ExitCode)
	}
	return exitCode
}

// Held (h)
func ProcessHeld(task *protos.TaskInfo) string {
	return strconv.FormatBool(task.Held)
}

// JobID (j)
func ProcessJobID(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.TaskId), 10)
}

// Comment (k)
func ProcessComment(task *protos.TaskInfo) string {
	if !gjson.Valid(task.ExtraAttr) {
		return ""
	}
	return gjson.Get(task.ExtraAttr, "comment").String()
}

// NodeList (L)
func ProcessNodeList(task *protos.TaskInfo) string {
	return task.GetCranedList()
}

// TimeLimit (l)
func ProcessTimeLimit(task *protos.TaskInfo) string {
	if task.TimeLimit.Seconds >= util.InvalidDuration().Seconds {
		return "unlimited"
	}
	return util.SecondTimeFormat(task.TimeLimit.Seconds)
}

// ReqMemPerNode (M)
func ProcessReqMemPerNode(task *protos.TaskInfo) string {
	return util.FormatMemToMB(task.ReqResView.AllocatableRes.MemoryLimitBytes)
}

// AllocMemPerNode (m)
func ProcessAllocMemPerNode(task *protos.TaskInfo) string {
	if task.NodeNum == 0 {
		return "0"
	}
	allocMemPerNode := task.AllocatedResView.AllocatableRes.MemoryLimitBytes / uint64(task.NodeNum)
	return util.FormatMemToMB(allocMemPerNode)
}

// NodeNum (N)
func ProcessNodeNum(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.NodeNum), 10)
}

// JobName (n)
func ProcessJobName(task *protos.TaskInfo) string {
	return task.Name
}

// Partition (P)
func ProcessPartition(task *protos.TaskInfo) string {
	return task.Partition
}

// Priority (p)
func ProcessPriority(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.Priority), 10)
}

// Qos (q)
func ProcessQos(task *protos.TaskInfo) string {
	return task.Qos
}

// Reason (R)
func ProcessReason(task *protos.TaskInfo) string {
	if task.Status == protos.TaskStatus_Pending {
		return task.GetPendingReason()
	}
	return " "
}

// ReqNodes (r)
func ProcessReqNodes(task *protos.TaskInfo) string {
	return strings.Join(task.ReqNodes, ",")
}

// StartTime (S)
func ProcessStartTime(task *protos.TaskInfo) string {
	startTimeStr := "unknown"
	startTime := task.StartTime.AsTime()
	if !startTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) &&
		startTime.Before(time.Now()) {
		startTimeStr = startTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return startTimeStr
}

// SubmitTime (s)
func ProcessSubmitTime(task *protos.TaskInfo) string {
	submitTimeStr := "unknown"
	submitTime := task.SubmitTime.AsTime()
	if !submitTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
		submitTimeStr = submitTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return submitTimeStr
}

// JobType (T)
func ProcessJobType(task *protos.TaskInfo) string {
	return task.Type.String()
}

// State (t)
func ProcessState(task *protos.TaskInfo) string {
	return task.Status.String()
}

// UserName (U)
func ProcessUserName(task *protos.TaskInfo) string {
	return task.Username
}

// Uid (u)
func ProcessUid(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.Uid), 10)
}

// Exclusive (X)
func ProcessExclusive(task *protos.TaskInfo) string {
	return strconv.FormatBool(task.Exclusive)
}

// ExcludeNodes (x)
func ProcessExcludeNodes(task *protos.TaskInfo) string {
	return strings.Join(task.ExcludeNodes, ",")
}

var fieldProcessors = map[string]FieldProcessor{
	// Group a
	"a":       {"Account", ProcessAccount},
	"account": {"Account", ProcessAccount},

	// Group C
	"C":       {"ReqCpus", ProcessReqCPUs},
	"reqcpus": {"ReqCpus", ProcessReqCPUs},

	// Group c
	"c":         {"AllocCPUs", ProcessAllocCPUs},
	"alloccpus": {"AllocCPUs", ProcessAllocCPUs},

	// Group D
	"D":           {"ElapsedTime", ProcessElapsedTime},
	"elapsedtime": {"ElapsedTime", ProcessElapsedTime},
	"deadline":    {"Deadline", ProcessDeadline},

	// Group E
	"E":       {"EndTime", ProcessEndTime},
	"endtime": {"EndTime", ProcessEndTime},

	// Group e
	"e":        {"ExitCode", ProcessExitCode},
	"exitcode": {"ExitCode", ProcessExitCode},

	// Group h
	"h":    {"Held", ProcessHeld},
	"held": {"Held", ProcessHeld},

	// Group j
	"j":     {"JobID", ProcessJobID},
	"jobid": {"JobID", ProcessJobID},

	// Group k
	"k":       {"Comment", ProcessComment},
	"comment": {"Comment", ProcessComment},

	// Group L
	"L":        {"NodeList", ProcessNodeList},
	"nodelist": {"NodeList", ProcessNodeList},

	// Group l
	"l":         {"TimeLimit", ProcessTimeLimit},
	"timelimit": {"TimeLimit", ProcessTimeLimit},

	// Group M
	"M":             {"ReqMemPerNode", ProcessReqMemPerNode},
	"reqmempernode": {"ReqMemPerNode", ProcessReqMemPerNode},

	// Group m
	"m":               {"AllocMemPerNode", ProcessAllocMemPerNode},
	"allocmempernode": {"AllocMemPerNode", ProcessAllocMemPerNode},

	// Group N
	"N":       {"NodeNum", ProcessNodeNum},
	"nodenum": {"NodeNum", ProcessNodeNum},

	// Group n
	"n":       {"JobName", ProcessJobName},
	"jobname": {"JobName", ProcessJobName},

	// Group P
	"P":         {"Partition", ProcessPartition},
	"partition": {"Partition", ProcessPartition},

	// Group p
	"p":        {"Priority", ProcessPriority},
	"priority": {"Priority", ProcessPriority},

	// Group q
	"q":   {"Qos", ProcessQos},
	"qos": {"Qos", ProcessQos},

	// Group R
	"R":      {"Reason", ProcessReason},
	"reason": {"Reason", ProcessReason},

	// Group r
	"r":        {"ReqNodes", ProcessReqNodes},
	"reqnodes": {"ReqNodes", ProcessReqNodes},

	// Group S
	"S":         {"StartTime", ProcessStartTime},
	"starttime": {"StartTime", ProcessStartTime},

	// Group s
	"s":          {"SubmitTime", ProcessSubmitTime},
	"submittime": {"SubmitTime", ProcessSubmitTime},

	// Group T
	"T":       {"JobType", ProcessJobType},
	"jobtype": {"JobType", ProcessJobType},

	// Group t
	"t":     {"State", ProcessState},
	"state": {"State", ProcessState},

	// Group U
	"U":        {"UserName", ProcessUserName},
	"username": {"UserName", ProcessUserName},

	// Group u
	"u":   {"Uid", ProcessUid},
	"uid": {"Uid", ProcessUid},

	// Group X
	"X":         {"Exclusive", ProcessExclusive},
	"exclusive": {"Exclusive", ProcessExclusive},

	// Group x
	"x":            {"ExcludeNodes", ProcessExcludeNodes},
	"excludenodes": {"ExcludeNodes", ProcessExcludeNodes},
}

// FormatData formats task information according to a format string.
// Format: %[[.]size]type[suffix]
// Examples:
//   - %j      : JobID without width constraint, left-aligned
//   - %5j     : JobID with minimum width 5, left-aligned (pad right)
//   - %.5j    : JobID with minimum width 5, right-aligned (pad left)
//   - %10t    : State with minimum width 10, left-aligned
//   - %.10t   : State with minimum width 10, right-aligned
func FormatData(reply *protos.QueryTasksInfoReply) (header []string, tableData [][]string) {
	re := regexp.MustCompile(`%(\.)?(\d+)?([a-zA-Z]+)`)
	specifiers := re.FindAllStringSubmatchIndex(FlagFormat, -1)
	if specifiers == nil {
		log.Errorln("Invalid format specifier.")
		os.Exit(util.ErrorInvalidFormat)
	}
	tableOutputWidth := make([]int, 0, len(specifiers))
	tableOutputRightAlign := make([]bool, 0, len(specifiers))
	tableOutputHeader := make([]string, 0, len(specifiers))
	tableOutputCell := make([][]string, len(reply.TaskInfoList))
	// Get the prefix of the format string
	if specifiers[0][0] != 0 {
		prefix := FlagFormat[0:specifiers[0][0]]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputRightAlign = append(tableOutputRightAlign, false)
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
			tableOutputRightAlign = append(tableOutputRightAlign, false)
			tableOutputHeader = append(tableOutputHeader, padding)
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], padding)
			}
		}

		// Check for right alignment (dot present)
		rightAlign := false
		if spec[2] != -1 {
			rightAlign = true
		}
		tableOutputRightAlign = append(tableOutputRightAlign, rightAlign)

		// Parse width specifier
		if spec[4] == -1 {
			// w/o width specifier
			tableOutputWidth = append(tableOutputWidth, -1)
		} else {
			// with width specifier
			width, err := strconv.ParseUint(FlagFormat[spec[4]:spec[5]], 10, 32)
			if err != nil {
				log.Errorln("Invalid width specifier.")
				os.Exit(util.ErrorInvalidFormat)
			}
			tableOutputWidth = append(tableOutputWidth, int(width))
		}

		// Parse format specifier
		field := FlagFormat[spec[6]:spec[7]]
		if len(field) > 1 {
			field = strings.ToLower(field)
		}

		fieldProcessor, found := fieldProcessors[field]
		if !found {
			log.Errorln("Invalid format specifier or string, string unfold case insensitive, reference:\n" +
				"a/Account, C/ReqCpus, c/AllocCPUs, deadline/Deadline, D/ElapsedTime, E/EndTime, e/ExitCode, h/Held, j/JobID, L/NodeList, l/TimeLimit,\n" +
				"M/ReqMemPerNode, m/AllocMemPerNode, N/NodeNum, n/JobName, P/Partition, p/Priority, q/Qos, r/ReqNodes, R/Reason, S/StartTime,\n" +
				"s/SubmitTime, T/JobType, t/State, U/UserName, u/Uid, X/Exclusive, x/ExcludeNodes.")
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
		tableOutputRightAlign = append(tableOutputRightAlign, false)
		tableOutputHeader = append(tableOutputHeader, suffix)
		for j := 0; j < len(reply.TaskInfoList); j++ {
			tableOutputCell[j] = append(tableOutputCell[j], suffix)
		}
	}
	return util.FormatTable(tableOutputWidth, tableOutputHeader, tableOutputCell, tableOutputRightAlign)
}
