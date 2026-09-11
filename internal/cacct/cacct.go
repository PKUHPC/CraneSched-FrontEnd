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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	stub protos.CraneCtldClient
)

const (
	kTerminationSignalBase = 256
	kCraneExitCodeBase     = 320
	// Keep this aligned with google.protobuf.util.TimeUtil's timestamp max;
	// the backend uses that exact second as the unset/future sentinel.
	kAccountingMaxTimestampSeconds int64 = 253402300799
)

// QueryJob will query all pending, running and completed jobs.
func QueryJob() error {
	request := protos.QueryJobsInfoRequest{OptionIncludeCompletedJobs: true}

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
		selectors, err := util.ParseJobIdSelectorList(FlagFilterJobIDs, ",")
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid job list specified: %s.", err)
		}
		request.FilterJobIds = selectors
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
		request.FilterJobNames = filterJobNameList
	}

	if FlagFilterStates != "" {
		stateList, err := util.ParseJobStatusList(FlagFilterStates)
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

	if FlagFilterJobTypes != "" {
		filterJobTypeList, err := util.ParseJobTypeList(FlagFilterJobTypes)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "Invalid job type list specified: %s.", err)
		}
		request.FilterJobTypes = filterJobTypeList
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

	reply, err := stub.QueryJobsInfo(context.Background(), &request)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show jobs")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	if FlagJson {
		if util.IsSlurmOutputMode() {
			output, err := util.FormatSlurmJobsJSON(reply)
			if err != nil {
				return util.WrapCraneErr(util.ErrorInvalidFormat, "%v", err)
			}
			fmt.Println(output)
		} else {
			fmt.Println(util.FmtJson.FormatReply(reply))
		}
		if reply.GetOk() {
			printIncompleteQueryWarning(reply.GetHasMore(), len(reply.GetJobInfoList()))
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}
	items := make([]*JobOrStep, 0)
	for _, job := range reply.JobInfoList {
		items = append(items, &JobOrStep{job: job, stepInfo: nil, isStep: false})
		for _, step := range job.StepInfoList {
			items = append(items, &JobOrStep{job: job, stepInfo: step, isStep: true})
		}
	}

	sort.Slice(items, func(i, j int) bool {
		jobId1, stepId1 := items[i].job.JobId, uint32(0)
		jobId2, stepId2 := items[j].job.JobId, uint32(0)
		if items[i].isStep {
			stepId1 = items[i].stepInfo.StepId
		}
		if items[j].isStep {
			stepId2 = items[j].stepInfo.StepId
		}
		if jobId1 != jobId2 {
			return jobId1 > jobId2
		}
		return stepId1 < stepId2
	})

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	var header []string
	tableData := make([][]string, len(items))
	if FlagFull {
		nodeListHeader := "CranedList"
		if util.IsSlurmOutputMode() {
			nodeListHeader = "NodeList"
		}
		header = []string{"JobId", "JobName", "UserName", "Partition",
			"NodeNum", "Account", "ReqCPUs", "ReqMemPerNode", "AllocCPUs", "AllocMemPerNode", "State", "TimeLimit",
			"StartTime", "EndTime", "SubmitTime", "Qos", "Exclusive", "Held", "Priority", nodeListHeader, "ExitCode", "wckey", "Deadline"}
		for i, jobOrStep := range items {
			tableData[i] = []string{
				ProcessJobID(jobOrStep),
				ProcessName(jobOrStep),
				jobOrStep.job.Username,
				jobOrStep.job.Partition,
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
				jobOrStep.job.Qos,
				ProcessExclusive(jobOrStep),
				ProcessHeld(jobOrStep),
				strconv.FormatUint(uint64(jobOrStep.job.Priority), 10),
				ProcessNodeList(jobOrStep),
				ProcessExitCode(jobOrStep),
				jobOrStep.job.Wckey,
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

		if FlagFormat == "" {
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

			if FlagDeadlineTime {
				header = append(header, "Deadline")
				for i := 0; i < len(tableData); i++ {
					tableData[i] = append(tableData[i], ProcessDeadline(items[i]))
				}
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
	printIncompleteQueryWarning(reply.GetHasMore(), len(reply.GetJobInfoList()))
	return nil
}

func printIncompleteQueryWarning(hasMore bool, returnedJobs int) {
	if !hasMore {
		return
	}

	fmt.Fprintf(os.Stderr,
		"Query returned %d jobs, and more matching jobs exist. Use -m to adjust the number of jobs returned.\n",
		returnedJobs)
}

// JobOrStep represents either a job (JobInfo) or a step (StepInfo)
type JobOrStep struct {
	job      *protos.JobInfo
	stepInfo *protos.StepInfo
	isStep   bool
}

type accountingTiming struct {
	status    protos.JobStatus
	elapsed   *durationpb.Duration
	startTime *timestamppb.Timestamp
	endTime   *timestamppb.Timestamp
}

func getAccountingTiming(item *JobOrStep) accountingTiming {
	if item == nil {
		return accountingTiming{}
	}
	if item.isStep {
		if item.stepInfo == nil {
			return accountingTiming{}
		}
		return accountingTiming{
			status:    item.stepInfo.Status,
			elapsed:   item.stepInfo.ElapsedTime,
			startTime: item.stepInfo.StartTime,
			endTime:   item.stepInfo.EndTime,
		}
	}
	if item.job == nil {
		return accountingTiming{}
	}
	return accountingTiming{
		status:    item.job.Status,
		elapsed:   item.job.ElapsedTime,
		startTime: item.job.StartTime,
		endTime:   item.job.EndTime,
	}
}

func isTerminalAccountingStatus(status protos.JobStatus) bool {
	switch status {
	case protos.JobStatus_Completed,
		protos.JobStatus_Failed,
		protos.JobStatus_ExceedTimeLimit,
		protos.JobStatus_Cancelled,
		protos.JobStatus_OutOfMemory,
		protos.JobStatus_Deadline:
		return true
	default:
		return false
	}
}

func validAccountingTime(timestamp *timestamppb.Timestamp) (time.Time, bool) {
	if timestamp == nil || !timestamp.IsValid() ||
		(timestamp.Seconds == 0 && timestamp.Nanos == 0) {
		return time.Time{}, false
	}
	if timestamp.Seconds >= kAccountingMaxTimestampSeconds {
		return time.Time{}, false
	}
	timestampTime := timestamp.AsTime()
	accountingEpoch := time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
	// The protobuf timestamp maximum is the backend's unset/future sentinel,
	// not a real accounting event.
	if timestampTime.Before(accountingEpoch) {
		return time.Time{}, false
	}
	return timestampTime, true
}

func validElapsedSeconds(duration *durationpb.Duration) (int64, bool) {
	if duration == nil || !duration.IsValid() || duration.Seconds < 0 ||
		duration.Nanos < 0 {
		return 0, false
	}
	return duration.Seconds, true
}

func formatAccountingTime(timestamp *timestamppb.Timestamp,
	requirePast bool) string {
	value, ok := validAccountingTime(timestamp)
	if !ok || (requirePast && value.After(time.Now())) {
		return "unknown"
	}
	return value.In(time.Local).Format("2006-01-02 15:04:05")
}

type FieldProcessor struct {
	header  string
	process func(item *JobOrStep) string
}

// Account (a)
func ProcessAccount(item *JobOrStep) string {
	return item.job.Account
}

// ReqCPUs (C)
func ProcessReqCPUs(item *JobOrStep) string {
	var cpuCores float64
	if item.isStep {
		cpuCores = item.stepInfo.GetReqTotalResView().GetCpuCount()
	} else {
		cpuCores = item.job.GetReqTotalResView().GetCpuCount()
	}
	return strconv.FormatFloat(cpuCores, 'f', 2, 64)
}

// AllocCPUs (c)
func ProcessAllocCPUs(item *JobOrStep) string {
	var cpuCores float64
	if item.isStep {
		cpuCores = item.stepInfo.GetAllocatedResView().GetCpuCount()
	} else {
		cpuCores = item.job.GetAllocatedResView().GetCpuCount()
	}
	if item.job.GetAllocatedResView() != nil {
		return strconv.FormatFloat(cpuCores, 'f', 2, 64)
	}
	return ""
}

// ElapsedTime (D)
func ProcessElapsedTime(item *JobOrStep) string {
	timing := getAccountingTiming(item)
	if timing.status == protos.JobStatus_Running {
		if seconds, ok := validElapsedSeconds(timing.elapsed); ok {
			return util.SecondTimeFormat(seconds)
		}
		return ""
	}
	if !isTerminalAccountingStatus(timing.status) {
		return ""
	}

	startTime, startOK := validAccountingTime(timing.startTime)
	endTime, endOK := validAccountingTime(timing.endTime)
	// A persisted reversal is corrupt even when a stale elapsed field happens
	// to be present. Never let that field hide the invalid timestamp pair.
	if startOK && endOK && endTime.Before(startTime) {
		return "unknown"
	}

	// The backend value is authoritative when present, including a valid zero
	// duration for a job cancelled before it started. A malformed value is
	// evidence of corrupt accounting data, not an invitation to fabricate a
	// replacement duration from timestamps.
	if timing.elapsed != nil {
		seconds, ok := validElapsedSeconds(timing.elapsed)
		if !ok {
			return "unknown"
		}
		return util.SecondTimeFormat(seconds)
	}

	if !startOK || !endOK || endTime.Before(startTime) {
		return "unknown"
	}
	return util.SecondTimeFormat(int64(endTime.Sub(startTime) / time.Second))
}

// Deadline (D)
func ProcessDeadline(item *JobOrStep) string {
	deadlineTime := item.job.DeadlineTime.AsTime()
	if !deadlineTime.Equal(util.InfiniteFuture) {
		return deadlineTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return "unknown"
}

// EndTime (E)
func ProcessEndTime(item *JobOrStep) string {
	timing := getAccountingTiming(item)
	if timing.status == protos.JobStatus_Pending ||
		timing.status == protos.JobStatus_Running {
		return "unknown"
	}
	if timing.status == protos.JobStatus_Completing {
		startTime, startOK := validAccountingTime(timing.startTime)
		endTime, endOK := validAccountingTime(timing.endTime)
		if !startOK || !endOK || !startTime.Before(time.Now()) ||
			!endTime.After(startTime) {
			return "unknown"
		}
		return endTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	if !isTerminalAccountingStatus(timing.status) {
		// Keep the historical predicted-end display for intermediate states
		// such as Suspended and Starting. Pending and Running returned above.
		startTime, startOK := validAccountingTime(timing.startTime)
		endTime, endOK := validAccountingTime(timing.endTime)
		if !startOK || !endOK || !startTime.Before(time.Now()) ||
			!endTime.After(startTime) {
			return "unknown"
		}
		return endTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	endTime, endOK := validAccountingTime(timing.endTime)
	if !endOK {
		return "unknown"
	}
	// A valid end timestamp is independently useful for accounting output.
	// Only reject a reversal when both timestamps are present and valid.
	if startTime, startOK := validAccountingTime(timing.startTime); startOK &&
		endTime.Before(startTime) {
		return "unknown"
	}
	return endTime.In(time.Local).Format("2006-01-02 15:04:05")
}

// ExitCode (e)
func ProcessExitCode(item *JobOrStep) string {
	exitCode := ""
	var code uint32

	if item.isStep {
		code = item.stepInfo.ExitCode
	} else {
		code = item.job.ExitCode
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
	return strconv.FormatBool(item.job.Held)
}

// JobID (j)
func ProcessJobID(item *JobOrStep) string {
	if item.isStep {
		return util.FormatStepId(item.job.JobId, item.job.ArrayTask, item.stepInfo.StepId)
	}
	return util.FormatJobId(item.job.JobId, item.job.ArrayTask)
}

// ArrayJobId
func ProcessArrayJobID(item *JobOrStep) string {
	if item.job.ArrayTask != nil {
		return strconv.FormatUint(uint64(item.job.ArrayTask.ArrayJobId), 10)
	}
	if item.job.ArraySpec != nil {
		return strconv.FormatUint(uint64(item.job.JobId), 10)
	}
	return ""
}

// ArrayTaskId
func ProcessArrayTaskID(item *JobOrStep) string {
	if item.job.ArrayTask == nil {
		return ""
	}
	return strconv.FormatUint(uint64(item.job.ArrayTask.TaskId), 10)
}

// ArraySpec
func ProcessArraySpec(item *JobOrStep) string {
	arraySpec := item.job.ArraySpec
	if arraySpec == nil {
		return ""
	}

	spec := strconv.FormatUint(uint64(arraySpec.Start), 10)
	if arraySpec.Start != arraySpec.End {
		spec = fmt.Sprintf("%s-%d", spec, arraySpec.End)
	}
	if arraySpec.Stride != nil && *arraySpec.Stride > 1 {
		spec = fmt.Sprintf("%s:%d", spec, *arraySpec.Stride)
	}
	if arraySpec.MaxConcurrent != nil && *arraySpec.MaxConcurrent > 0 {
		spec = fmt.Sprintf("%s%%%d", spec, *arraySpec.MaxConcurrent)
	}
	return spec
}

// Wckey (K)
func ProcessWckey(item *JobOrStep) string {
	if item.isStep {
		return ""
	}
	return item.job.Wckey
}

// Comment (k)
func ProcessComment(item *JobOrStep) string {
	var extraAttr string
	if item.isStep {
		extraAttr = item.stepInfo.ExtraAttr
	} else {
		extraAttr = item.job.ExtraAttr
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
	return item.job.GetCranedList()
}

// TimeLimit (l)
func ProcessTimeLimit(item *JobOrStep) string {
	var seconds int64
	if item.isStep {
		seconds = item.stepInfo.TimeLimit.Seconds
	} else {
		seconds = item.job.TimeLimit.Seconds
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
		totalMem = item.stepInfo.GetReqTotalResView().GetMemoryBytes()
		nodeNum = item.stepInfo.NodeNum
	} else {
		totalMem = item.job.GetReqTotalResView().GetMemoryBytes()
		nodeNum = item.job.NodeNum
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
		allocMem = item.stepInfo.GetAllocatedResView().GetMemoryBytes()
	} else {
		nodeNum = item.job.NodeNum
		allocMem = item.job.GetAllocatedResView().GetMemoryBytes()
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
	return strconv.FormatUint(uint64(item.job.NodeNum), 10)
}

// JobName (n)
func ProcessName(item *JobOrStep) string {
	if item.isStep {
		return item.stepInfo.Name
	}
	return item.job.Name
}

// Partition (P)
func ProcessPartition(item *JobOrStep) string {
	if item.isStep {
		// StepInfo doesn't have Partition field
		return ""
	}
	return item.job.Partition
}

// Priority (p)
func ProcessPriority(item *JobOrStep) string {
	if item.isStep {
		// StepInfo doesn't have Priority field
		return ""
	}
	return strconv.FormatUint(uint64(item.job.Priority), 10)
}

// Qos (q)
func ProcessQos(item *JobOrStep) string {
	if item.isStep {
		// StepInfo doesn't have Qos field
		return ""
	}
	return item.job.Qos
}

// Reason (R)
func ProcessReason(item *JobOrStep) string {
	if item.isStep {
		return ""
	}

	if item.job.Status == protos.JobStatus_Pending {
		return item.job.GetPendingReason()
	}
	return " "
}

// ReqNodes (r)
func ProcessReqNodes(item *JobOrStep) string {
	if item.isStep {
		return strings.Join(item.stepInfo.ReqNodes, ",")
	}
	return strings.Join(item.job.ReqNodes, ",")
}

// StartTime (S)
func ProcessStartTime(item *JobOrStep) string {
	return formatAccountingTime(getAccountingTiming(item).startTime, true)
}

// SubmitTime (s)
func ProcessSubmitTime(item *JobOrStep) string {
	var timestamp *timestamppb.Timestamp
	if item != nil {
		if item.isStep {
			if item.stepInfo != nil {
				timestamp = item.stepInfo.SubmitTime
			}
		} else if item.job != nil {
			timestamp = item.job.SubmitTime
		}
	}
	return formatAccountingTime(timestamp, false)
}

// JobType (T)
func ProcessJobType(item *JobOrStep) string {
	if item.isStep {
		return item.stepInfo.Type.String()
	}
	return item.job.Type.String()
}

// State (t)
func ProcessState(item *JobOrStep) string {
	if item.isStep {
		return item.stepInfo.Status.String()
	}
	return item.job.Status.String()
}

// UserName (U)
func ProcessUserName(item *JobOrStep) string {
	if item.isStep {
		// StepInfo doesn't have Username field
		return ""
	}
	return item.job.Username
}

// Uid (u)
func ProcessUid(item *JobOrStep) string {
	if item.isStep {
		return strconv.FormatUint(uint64(item.stepInfo.Uid), 10)
	}
	return strconv.FormatUint(uint64(item.job.Uid), 10)
}

// Exclusive (X)
func ProcessExclusive(item *JobOrStep) string {
	if item.isStep {
		// StepInfo doesn't have Exclusive field
		return ""
	}
	return strconv.FormatBool(item.job.Exclusive)
}

// ExcludeNodes (x)
func ProcessExcludeNodes(item *JobOrStep) string {
	if item.isStep {
		return strings.Join(item.stepInfo.ExcludeNodes, ",")
	}
	return strings.Join(item.job.ExcludeNodes, ",")
}

var fieldProcessors = map[string]FieldProcessor{
	// Group a
	"a":           {"Account", ProcessAccount},
	"account":     {"Account", ProcessAccount},
	"arrayjobid":  {"ArrayJobId", ProcessArrayJobID},
	"arrayspec":   {"ArraySpec", ProcessArraySpec},
	"arraytaskid": {"ArrayTaskId", ProcessArrayTaskID},

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

// FormatData formats job information according to a format string.
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
				"a/Account, ArrayJobId, ArraySpec, ArrayTaskId, C/ReqCpus, c/AllocCPUs, deadline/Deadline, D/ElapsedTime, E/EndTime, e/ExitCode, h/Held, j/JobID, K-Wckey, k/Comment, L/NodeList, l/TimeLimit,\n" +
				"M/ReqMemPerNode, m/AllocMemPerNode, N/NodeNum, n/JobName, P/Partition, p/Priority, q/Qos, r/ReqNodes, R/Reason, S/StartTime,\n" +
				"s/SubmitTime, T/JobType, t/State, U/UserName, u/Uid, X/Exclusive, x/ExcludeNodes.")
			os.Exit(util.ErrorInvalidFormat)
		}

		// Add header and process data
		tableOutputHeader = append(tableOutputHeader, strings.ToUpper(fieldProcessor.header))
		for j, item := range items {
			// Use unified processing for both jobs and steps
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
