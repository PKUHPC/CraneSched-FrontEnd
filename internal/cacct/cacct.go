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
			return util.WrapCraneErr(util.ErrorCmdArg, "Failed to parse the StartTime filter: %s.", err)
		}
	}
	if FlagFilterEndTime != "" {
		request.FilterEndTimeInterval = &protos.TimeInterval{}
		err := util.ParseInterval(FlagFilterEndTime, request.FilterEndTimeInterval)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Failed to parse the EndTime filter: %s.", err)
		}
	}
	if FlagFilterSubmitTime != "" {
		request.FilterSubmitTimeInterval = &protos.TimeInterval{}
		err := util.ParseInterval(FlagFilterSubmitTime, request.FilterSubmitTimeInterval)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Failed to parse the SubmitTime filter: %s.", err)
		}
	}

	if FlagFilterAccounts != "" {
		filterAccountList, err := util.ParseStringParamList(FlagFilterAccounts, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid account list specified: %s.", err)
		}
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterJobIDs != "" {
		filterStepList, err := util.ParseStepIdList(FlagFilterJobIDs, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid job list specified: %s.", err)
		}
		request.FilterIds = filterStepList
	}

	if FlagFilterUsers != "" {
		filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid user list specified: %s.", err)
		}
		request.FilterUsers = filterUserList
	}

	if FlagFilterJobNames != "" {
		filterJobNameList, err := util.ParseStringParamList(FlagFilterJobNames, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid job name list specified: %s.", err)
		}
		request.FilterTaskNames = filterJobNameList
	}

	if FlagFilterStates != "" {
		stateList, err := util.ParseTaskStatusList(FlagFilterStates)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Failed to parse the state filter: %s.", err)
		}
		request.FilterStates = stateList
	}

	if FlagFilterQos != "" {
		filterJobQosList, err := util.ParseStringParamList(FlagFilterQos, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid Qos list specified: %s.", err)
		}
		request.FilterQos = filterJobQosList
	}

	if FlagFilterPartitions != "" {
		filterPartitionList, err := util.ParseStringParamList(FlagFilterPartitions, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid partition list specified: %s.", err)
		}
		request.FilterPartitions = filterPartitionList
	}

	if FlagFilterTaskTypes != "" {
		filterTaskTypeList, err := util.ParseTaskTypeList(FlagFilterTaskTypes)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid task type list specified: %s.", err)
		}
		request.FilterTaskTypes = filterTaskTypeList
	}

	if FlagFilterNodeNames != "" {
		filterNodenameList, ok := util.ParseHostList(FlagFilterNodeNames)
		if !ok {
			return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("Invalid node pattern: %s.", FlagFilterNodeNames))
		}
		request.FilterNodenameList = filterNodenameList
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
	items := make([]*JobOrStep, 0)
	for _, task := range reply.TaskInfoList {
		items = append(items, &JobOrStep{task: task, stepInfo: nil, isStep: false})
		for _, step := range task.StepInfoList {
			items = append(items, &JobOrStep{task: task, stepInfo: step, isStep: true})
		}
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	var header []string
	tableData := make([][]string, len(items))
	if FlagFull {
		header = []string{"JobId", "JobName", "UserName", "Partition",
			"NodeNum", "Account", "ReqCPUs", "ReqMemPerNode", "AllocCPUs", "AllocMemPerNode", "State", "TimeLimit",
			"StartTime", "EndTime", "SubmitTime", "Qos", "Exclusive", "Held", "Priority", "CranedList", "ExitCode", "wckey"}
		for i, jobOrStep := range items {
			tableData[i] = []string{
				ProcessJobID(jobOrStep),
				ProcessName(jobOrStep),
				jobOrStep.task.Username,
				jobOrStep.task.Partition,
				ProcessNodeNum(jobOrStep),
				ProcessAccount(jobOrStep),
				ProcessReqCPUs(jobOrStep),
				ProcessReqMemPerNode(jobOrStep),
				ProcessAllocCPUs(jobOrStep),
				ProcessAllocMemPerNode(jobOrStep),
				ProcessState(jobOrStep),
				ProcessTimeLimit(jobOrStep),
				ProcessStartTime(jobOrStep),
				ProcessEndTime(jobOrStep),
				ProcessSubmitTime(jobOrStep),
				jobOrStep.task.Qos,
				ProcessExclusive(jobOrStep),
				ProcessHeld(jobOrStep),
				strconv.FormatUint(uint64(jobOrStep.task.Priority), 10),
				ProcessNodeList(jobOrStep),
				ProcessExitCode(jobOrStep),
				jobOrStep.task.Wckey,
			}
		}
	} else {
		header = []string{"JobId", "JobName", "Partition", "Account", "AllocCPUs", "State", "ExitCode"}

		for i, jobOrStep := range items {

			tableData[i] = []string{
				ProcessJobID(jobOrStep),
				ProcessName(jobOrStep),
				ProcessPartition(jobOrStep),
				ProcessAccount(jobOrStep),
				ProcessAllocCPUs(jobOrStep),
				ProcessState(jobOrStep),
				ProcessExitCode(jobOrStep)}
		}

		if FlagFormat != "" {
			header, tableData = FormatData(items)
			table.SetTablePadding("")
			table.SetAutoFormatHeaders(false)
		}

		if FlagFilterStartTime != "" {
			header = append(header, "StartTime")
			for i := 0; i < len(tableData); i++ {
				tableData[i] = append(tableData[i], ProcessStartTime(items[i]))
			}
		}

		if FlagFilterEndTime != "" {
			header = append(header, "EndTime")
			for i := 0; i < len(tableData); i++ {
				tableData[i] = append(tableData[i], ProcessEndTime(items[i]))
			}
		}

		if FlagFilterSubmitTime != "" {
			header = append(header, "SubmitTime")
			for i := 0; i < len(tableData); i++ {
				tableData[i] = append(tableData[i], ProcessSubmitTime(items[i]))
			}
		}
	}

	if !FlagNoHeader {
		table.SetHeader(header)
	}

	// Get index of "JobId" column
	jobIdIdx := -1
	for i, val := range header {
		if val == "JobId" {
			jobIdIdx = i
			break
		}
	}

	// If "JobId" column exists, sort all rows by descending order of "JobId".
	if jobIdIdx != -1 {
		less := func(i, j int) bool {
			jobId1, stepId1, _ := util.ParseJobIdStepId(tableData[i][jobIdIdx])
			jobId2, stepId2, _ := util.ParseJobIdStepId(tableData[j][jobIdIdx])
			if jobId1 != jobId2 {
				return jobId1 > jobId2
			}
			return stepId1 < stepId2
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

// JobOrStep represents either a job (TaskInfo) or a step (StepInfo)
type JobOrStep struct {
	task     *protos.TaskInfo
	stepInfo *protos.StepInfo
	isStep   bool
}

type FieldProcessor struct {
	header  string
	process func(item *JobOrStep) string
}

// Account (a)
func ProcessAccount(item *JobOrStep) string {
	return item.task.Account
}

// ReqCPUs (C)
func ProcessReqCPUs(item *JobOrStep) string {
	var cpuCores float64
	if item.isStep {
		cpuCores = item.stepInfo.ReqResView.AllocatableRes.CpuCoreLimit
	} else {
		cpuCores = item.task.ReqResView.AllocatableRes.CpuCoreLimit
	}
	return strconv.FormatFloat(cpuCores, 'f', 2, 64)
}

// AllocCPUs (c)
func ProcessAllocCPUs(item *JobOrStep) string {
	var cpuCores float64
	if item.isStep {
		cpuCores = item.stepInfo.AllocatedResView.AllocatableRes.CpuCoreLimit
	} else {
		cpuCores = item.task.AllocatedResView.AllocatableRes.CpuCoreLimit
	}
	if item.task.AllocatedResView != nil {
		return strconv.FormatFloat(cpuCores, 'f', 2, 64)
	}
	return ""
}

// ElapsedTime (D)
func ProcessElapsedTime(item *JobOrStep) string {
	var status protos.TaskStatus
	var startTime, endTime time.Time
	if item.isStep {
		status = item.stepInfo.Status
		if item.stepInfo.StartTime == nil || item.stepInfo.EndTime == nil {
			return ""
		}
		startTime = item.stepInfo.StartTime.AsTime()
		endTime = item.stepInfo.EndTime.AsTime()
	} else {
		status = item.task.Status
		if item.task.StartTime == nil || item.task.EndTime == nil {
			return ""
		}
		startTime = item.task.StartTime.AsTime()
		endTime = item.task.EndTime.AsTime()
	}

	if status == protos.TaskStatus_Running {
		if item.isStep {
			return util.SecondTimeFormat(item.stepInfo.ElapsedTime.Seconds)
		} else {
			return util.SecondTimeFormat(item.task.ElapsedTime.Seconds)
		}
	} else if status == protos.TaskStatus_Completed {
		if startTime.IsZero() || endTime.IsZero() {
			return "-"
		}
		if startTime.Before(time.Now()) && endTime.After(startTime) {
			duration := endTime.Sub(startTime)
			return util.SecondTimeFormat(int64(duration.Seconds()))
		}
	}
	return ""
}

// EndTime (E)
func ProcessEndTime(item *JobOrStep) string {
	endTimeStr := "unknown"
	var status protos.TaskStatus
	var startTime, endTime time.Time

	if item.isStep {
		status = item.stepInfo.Status
		startTime = item.stepInfo.StartTime.AsTime()
		endTime = item.stepInfo.EndTime.AsTime()
	} else {
		status = item.task.Status
		startTime = item.task.StartTime.AsTime()
		endTime = item.task.EndTime.AsTime()
	}
	if status != protos.TaskStatus_Pending && status != protos.TaskStatus_Running {
		if startTime.Before(time.Now()) && endTime.After(startTime) {
			endTimeStr = endTime.In(time.Local).Format("2006-01-02 15:04:05")
		}
	}
	return endTimeStr
}

// ExitCode (e)
func ProcessExitCode(item *JobOrStep) string {
	exitCode := ""
	var code uint32

	if item.isStep {
		code = item.stepInfo.ExitCode
	} else {
		code = item.task.ExitCode
	}

	if code >= kTerminationSignalBase {
		exitCode = fmt.Sprintf("0:%d", code-kTerminationSignalBase)
	} else {
		exitCode = fmt.Sprintf("%d:0", code)
	}
	return exitCode
}

// Held (h)
func ProcessHeld(item *JobOrStep) string {
	if item.isStep {
		return strconv.FormatBool(item.stepInfo.Held)
	}
	return strconv.FormatBool(item.task.Held)
}

// JobID (j)
func ProcessJobID(item *JobOrStep) string {
	if item.isStep {
		return fmt.Sprintf("%d.%d", item.stepInfo.JobId, item.stepInfo.StepId)
	}
	return strconv.FormatUint(uint64(item.task.TaskId), 10)
}

// Wckey (K)
func ProcessWckey(item *JobOrStep) string {
	if item.isStep {
		return ""
	}
	return item.task.Wckey
}

// Comment (k)
func ProcessComment(item *JobOrStep) string {
	var extraAttr string
	if item.isStep {
		extraAttr = item.stepInfo.ExtraAttr
	} else {
		extraAttr = item.task.ExtraAttr
	}

	if !gjson.Valid(extraAttr) {
		return ""
	}
	comment := gjson.Get(extraAttr, "comment").String()
	if comment == "" {
		return ""
	}
	return comment
}

// NodeList (L)
func ProcessNodeList(item *JobOrStep) string {
	if item.isStep {
		return item.stepInfo.GetCranedList()
	}
	return item.task.GetCranedList()
}

// TimeLimit (l)
func ProcessTimeLimit(item *JobOrStep) string {
	var seconds int64
	if item.isStep {
		seconds = item.stepInfo.TimeLimit.Seconds
	} else {
		seconds = item.task.TimeLimit.Seconds
	}

	if seconds >= util.InvalidDuration().Seconds {
		return "unlimited"
	}
	return util.SecondTimeFormat(seconds)
}

// ReqMemPerNode (M)
func ProcessReqMemPerNode(item *JobOrStep) string {
	var totalMem uint64
	var nodeNum uint32
	if item.isStep {
		totalMem = item.stepInfo.ReqResView.AllocatableRes.MemoryLimitBytes
		nodeNum = item.stepInfo.NodeNum
	} else {
		totalMem = item.task.ReqResView.AllocatableRes.MemoryLimitBytes
		nodeNum = item.task.NodeNum
	}
	if nodeNum > 0 {
		return util.FormatMemToMB(totalMem / uint64(nodeNum))
	}
	return util.FormatMemToMB(totalMem)
}

// AllocMemPerNode (m)
func ProcessAllocMemPerNode(item *JobOrStep) string {
	var nodeNum uint32
	var allocMem uint64

	if item.isStep {
		nodeNum = item.stepInfo.NodeNum
		allocMem = item.stepInfo.AllocatedResView.AllocatableRes.MemoryLimitBytes
	} else {
		nodeNum = item.task.NodeNum
		allocMem = item.task.AllocatedResView.AllocatableRes.MemoryLimitBytes
	}
	if nodeNum == 0 {
		return "0"
	}
	allocMemPerNode := allocMem / uint64(nodeNum)
	return util.FormatMemToMB(allocMemPerNode)
}

// NodeNum (N)
func ProcessNodeNum(item *JobOrStep) string {
	if item.isStep {
		return strconv.FormatUint(uint64(item.stepInfo.NodeNum), 10)
	}
	return strconv.FormatUint(uint64(item.task.NodeNum), 10)
}

// JobName (n)
func ProcessName(item *JobOrStep) string {
	if item.isStep {
		return item.stepInfo.Name
	}
	return item.task.Name
}

// Partition (P)
func ProcessPartition(item *JobOrStep) string {
	if item.isStep {
		// StepInfo doesn't have Partition field
		return ""
	}
	return item.task.Partition
}

// Priority (p)
func ProcessPriority(item *JobOrStep) string {
	if item.isStep {
		// StepInfo doesn't have Priority field
		return ""
	}
	return strconv.FormatUint(uint64(item.task.Priority), 10)
}

// Qos (q)
func ProcessQos(item *JobOrStep) string {
	if item.isStep {
		// StepInfo doesn't have Qos field
		return ""
	}
	return item.task.Qos
}

// Reason (R)
func ProcessReason(item *JobOrStep) string {
	if item.isStep {
		return ""
	}

	if item.task.Status == protos.TaskStatus_Pending {
		return item.task.GetPendingReason()
	}
	return " "
}

// ReqNodes (r)
func ProcessReqNodes(item *JobOrStep) string {
	if item.isStep {
		return strings.Join(item.stepInfo.ReqNodes, ",")
	}
	return strings.Join(item.task.ReqNodes, ",")
}

// StartTime (S)
func ProcessStartTime(item *JobOrStep) string {
	startTimeStr := "unknown"
	var startTime time.Time
	if item.isStep {
		startTime = item.stepInfo.StartTime.AsTime()
	} else {
		startTime = item.task.StartTime.AsTime()
	}

	if !startTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) &&
		startTime.Before(time.Now()) {
		startTimeStr = startTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return startTimeStr
}

// SubmitTime (s)
func ProcessSubmitTime(item *JobOrStep) string {
	submitTimeStr := "unknown"
	var submitTime time.Time
	if item.isStep {
		submitTime = item.stepInfo.SubmitTime.AsTime()
	} else {
		submitTime = item.task.SubmitTime.AsTime()
	}

	if !submitTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
		submitTimeStr = submitTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return submitTimeStr
}

// JobType (T)
func ProcessJobType(item *JobOrStep) string {
	if item.isStep {
		return item.stepInfo.Type.String()
	}
	return item.task.Type.String()
}

// State (t)
func ProcessState(item *JobOrStep) string {
	if item.isStep {
		return item.stepInfo.Status.String()
	}
	return item.task.Status.String()
}

// UserName (U)
func ProcessUserName(item *JobOrStep) string {
	if item.isStep {
		// StepInfo doesn't have Username field
		return ""
	}
	return item.task.Username
}

// Uid (u)
func ProcessUid(item *JobOrStep) string {
	if item.isStep {
		return strconv.FormatUint(uint64(item.stepInfo.Uid), 10)
	}
	return strconv.FormatUint(uint64(item.task.Uid), 10)
}

// Exclusive (X)
func ProcessExclusive(item *JobOrStep) string {
	if item.isStep {
		// StepInfo doesn't have Exclusive field
		return ""
	}
	return strconv.FormatBool(item.task.Exclusive)
}

// ExcludeNodes (x)
func ProcessExcludeNodes(item *JobOrStep) string {
	if item.isStep {
		return strings.Join(item.stepInfo.ExcludeNodes, ",")
	}
	return strings.Join(item.task.ExcludeNodes, ",")
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

	// Group K
	"K":     {"Wckey", ProcessWckey},
	"wckey": {"Wckey", ProcessWckey},

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
	"n":       {"JobName", ProcessName},
	"jobname": {"JobName", ProcessName},

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
func FormatData(items []*JobOrStep) (header []string, tableData [][]string) {
	re := regexp.MustCompile(`%(\.)?(\d+)?([a-zA-Z]+)`)
	specifiers := re.FindAllStringSubmatchIndex(FlagFormat, -1)
	if specifiers == nil {
		log.Errorln("Invalid format specifier.")
		os.Exit(util.ErrorInvalidFormat)
	}

	tableOutputWidth := make([]int, 0, len(specifiers))
	tableOutputRightAlign := make([]bool, 0, len(specifiers))
	tableOutputHeader := make([]string, 0, len(specifiers))
	tableOutputCell := make([][]string, len(items))

	// Get the prefix of the format string
	if specifiers[0][0] != 0 {
		prefix := FlagFormat[0:specifiers[0][0]]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputRightAlign = append(tableOutputRightAlign, false)
		tableOutputHeader = append(tableOutputHeader, prefix)
		for j := 0; j < len(items); j++ {
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
			for j := 0; j < len(items); j++ {
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
				"a/Account, C/ReqCpus, c/AllocCPUs, D/ElapsedTime, E/EndTime, e/ExitCode, h/Held, j/JobID, K-Wckey, k/Comment, L/NodeList, l/TimeLimit,\n" +
				"M/ReqMemPerNode, m/AllocMemPerNode, N/NodeNum, n/JobName, P/Partition, p/Priority, q/Qos, r/ReqNodes, R/Reason, S/StartTime,\n" +
				"s/SubmitTime, T/JobType, t/State, U/UserName, u/Uid, X/Exclusive, x/ExcludeNodes.")
			os.Exit(util.ErrorInvalidFormat)
		}

		// Add header and process data
		tableOutputHeader = append(tableOutputHeader, strings.ToUpper(fieldProcessor.header))
		for j, item := range items {
			// Use unified processing for both tasks and steps
			tableOutputCell[j] = append(tableOutputCell[j], fieldProcessor.process(item))
		}
	}

	// Get the suffix of the format string
	if len(FlagFormat)-specifiers[len(specifiers)-1][1] > 0 {
		suffix := FlagFormat[specifiers[len(specifiers)-1][1]:]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputRightAlign = append(tableOutputRightAlign, false)
		tableOutputHeader = append(tableOutputHeader, suffix)
		for j := 0; j < len(items); j++ {
			tableOutputCell[j] = append(tableOutputCell[j], suffix)
		}
	}
	return util.FormatTable(tableOutputWidth, tableOutputHeader, tableOutputCell, tableOutputRightAlign)
}
