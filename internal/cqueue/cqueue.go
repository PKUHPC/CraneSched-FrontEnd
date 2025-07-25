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
	"github.com/tidwall/gjson"
)

var (
	stub protos.CraneCtldClient
)

func QueryTasksInfo() (*protos.QueryTasksInfoReply, error) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	req := protos.QueryTasksInfoRequest{OptionIncludeCompletedTasks: false}
	var reply *protos.QueryTasksInfoReply

	if FlagFilterStates != "" {
		stateList, err := util.ParseInRamTaskStatusList(FlagFilterStates)
		if err != nil {
			return reply, &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: err.Error(),
			}
		}
		req.FilterTaskStates = stateList
	}

	if FlagSelf {
		cu, err := user.Current()
		if err != nil {
			return reply, &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Failed to get current username: %s.", err),
			}
		}
		req.FilterUsers = []string{cu.Username}
	}
	if FlagFilterJobNames != "" {
		filterJobNameList, err := util.ParseStringParamList(FlagFilterJobNames, ",")
		if err != nil {
			return reply, &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid job name list specified: %s.", err),
			}
		}
		req.FilterTaskNames = filterJobNameList
	}
	if FlagFilterUsers != "" {
		filterUserList, err := util.ParseStringParamList(FlagFilterUsers, ",")
		if err != nil {
			return reply, &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid user list specified: %s.", err),
			}
		}
		req.FilterUsers = filterUserList
	}
	if FlagFilterQos != "" {
		filterJobQosList, err := util.ParseStringParamList(FlagFilterQos, ",")
		if err != nil {
			return reply, &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid Qos list specified: %s.", err),
			}
		}
		req.FilterQos = filterJobQosList
	}
	if FlagFilterAccounts != "" {
		filterAccountList, err := util.ParseStringParamList(FlagFilterAccounts, ",")
		if err != nil {
			return reply, &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid account list specified: %s.", err),
			}
		}
		req.FilterAccounts = filterAccountList
	}
	if FlagFilterPartitions != "" {
		filterPartitionList, err := util.ParseStringParamList(FlagFilterPartitions, ",")
		if err != nil {
			return reply, &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid partition list specified: %s.", err),
			}
		}
		req.FilterPartitions = filterPartitionList
	}

	if FlagFilterJobIDs != "" {
		filterJobIdList, err := util.ParseJobIdList(FlagFilterJobIDs, ",")
		if err != nil {
			return reply, &util.CraneError{
				Code:    util.ErrorCmdArg,
				Message: fmt.Sprintf("Invalid job list specified: %s.", err),
			}
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
		return reply, &util.CraneError{Code: util.ErrorNetwork}
	}

	return reply, nil
}

func QueryTableOutput(reply *protos.QueryTasksInfoReply) error {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	var header []string
	tableData := make([][]string, len(reply.TaskInfoList))
	if FlagFull {
		header = []string{"JobId", "JobName", "UserName", "Partition",
			"Account", "NodeNum", "ReqCPUs", "ReqMemPerNode", "AllocCPUs", "AllocMemPerNode", "Status", "Time", "TimeLimit",
			"StartTime", "SubmitTime", "Type", "Qos", "Exclusive", "Held", "Priority", "NodeList/Reason"}
		for i := range reply.TaskInfoList {
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

			tableData[i] = []string{
				strconv.FormatUint(uint64(taskInfo.TaskId), 10),
				taskInfo.Name,
				taskInfo.Username,
				taskInfo.Partition,
				taskInfo.Account,
				strconv.FormatUint(uint64(taskInfo.NodeNum), 10),
				ProcessReqCPUs(taskInfo),
				ProcessReqMemPerNode(taskInfo),
				ProcessAllocCpus(taskInfo),
				ProcessAllocMemPerNode(taskInfo),
				taskInfo.Status.String(),
				timeElapsedStr,
				timeLimitStr,
				startTimeStr,
				submitTimeStr,
				reply.TaskInfoList[i].Type.String(),
				taskInfo.Qos,
				strconv.FormatBool(taskInfo.Exclusive),
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
	return nil
}

func Query() error {
	reply, err := QueryTasksInfo()
	if err != nil {
		return err
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return nil
		} else {
			return &util.CraneError{Code: util.ErrorBackend}
		}
	}

	return QueryTableOutput(reply)
}

// 'a' group
func ProcessAccount(task *protos.TaskInfo) string {
	return task.Account
}

// 'c' group
func ProcessAllocCpus(task *protos.TaskInfo) string {
	return strconv.FormatFloat(task.AllocatedResView.AllocatableRes.CpuCoreLimit, 'f', 2, 64)
}

// 'C' group
func ProcessReqCpuPerNode(task *protos.TaskInfo) string {
	return strconv.FormatFloat(task.ReqResView.AllocatableRes.CpuCoreLimit, 'f', 2, 64)
}

// 'e' group
func ProcessElapsedTime(task *protos.TaskInfo) string {
	if task.Status == protos.TaskStatus_Running {
		return util.SecondTimeFormat(task.ElapsedTime.Seconds)
	}
	return "-"
}

// 'h' group
func ProcessHeld(task *protos.TaskInfo) string {
	return strconv.FormatBool(task.Held)
}

// 'j' group
func ProcessJobId(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.TaskId), 10)
}

// 'k' group
func ProcessComment(task *protos.TaskInfo) string {
	if !gjson.Valid(task.ExtraAttr) {
		return ""
	}
	return gjson.Get(task.ExtraAttr, "comment").String()
}

// 'l' group
func ProcessTimeLimit(task *protos.TaskInfo) string {
	if task.TimeLimit.Seconds >= util.InvalidDuration().Seconds {
		return "unlimited"
	}
	return util.SecondTimeFormat(task.TimeLimit.Seconds)
}

func ProcessNodeList(task *protos.TaskInfo) string {
	return task.GetCranedList()
}

// 'm' group
func ProcessAllocMemPerNode(task *protos.TaskInfo) string {
	if task.NodeNum == 0 {
		return "0"
	}
	return util.FormatMemToMB(task.AllocatedResView.AllocatableRes.MemoryLimitBytes /
		uint64(task.NodeNum))
}

// 'M' group
func ProcessReqMemPerNode(task *protos.TaskInfo) string {
	return util.FormatMemToMB(task.ReqResView.AllocatableRes.MemoryLimitBytes)
}

// 'n' group
func ProcessName(task *protos.TaskInfo) string {
	return task.Name
}

func ProcessNodeNum(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.NodeNum), 10)
}

// 'o' group
func ProcessCommand(task *protos.TaskInfo) string {
	return task.CmdLine
}

// 'p' group
func ProcessPriority(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.Priority), 10)
}

func ProcessPartition(task *protos.TaskInfo) string {
	return task.Partition
}

// 'q' group
func ProcessQoS(task *protos.TaskInfo) string {
	return task.Qos
}

// 'Q' group
func ProcessReqCPUs(task *protos.TaskInfo) string {
	return strconv.FormatFloat(task.ReqResView.AllocatableRes.CpuCoreLimit*float64(task.NodeNum), 'f', 2, 64)
}

// 'r' group
func ProcessReqNodes(task *protos.TaskInfo) string {
	return strings.Join(task.ReqNodes, ",")
}

func ProcessReason(task *protos.TaskInfo) string {
	if task.Status == protos.TaskStatus_Pending {
		return task.GetPendingReason()
	}
	return " "
}

// 's' group
func ProcessSubmitTime(task *protos.TaskInfo) string {
	submitTime := task.SubmitTime.AsTime()
	if !submitTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
		return submitTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return "unknown"
}

func ProcessStartTime(task *protos.TaskInfo) string {
	startTime := task.StartTime.AsTime()
	if !startTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) && startTime.Before(time.Now()) {
		return startTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return "unknown"
}

// 't' group
func ProcessState(task *protos.TaskInfo) string {
	return task.Status.String()
}

func ProcessJobType(task *protos.TaskInfo) string {
	return task.Type.String()
}

// 'u' group
func ProcessUser(task *protos.TaskInfo) string {
	return task.Username
}

func ProcessUid(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.Uid), 10)
}

// 'x' group
func ProcessExcludeNodes(task *protos.TaskInfo) string {
	return strings.Join(task.ExcludeNodes, ",")
}

// 'X' group
func ProcessExclusive(task *protos.TaskInfo) string {
	return strconv.FormatBool(task.Exclusive)
}

type FieldProcessor struct {
	header  string
	process func(task *protos.TaskInfo) string
}

var fieldMap = map[string]FieldProcessor{
	// 'a' group
	"a":       {"Account", ProcessAccount},
	"account": {"Account", ProcessAccount},

	// 'c' group
	"c":         {"AllocCpus", ProcessAllocCpus},
	"alloccpus": {"AllocCpus", ProcessAllocCpus},
	"C":         {"ReqCpus", ProcessReqCPUs},
	"reqcpus":   {"ReqCpus", ProcessReqCPUs},

	// 'e' group
	"e":           {"ElapsedTime", ProcessElapsedTime},
	"elapsedtime": {"ElapsedTime", ProcessElapsedTime},

	// 'h' group
	"h":    {"Held", ProcessHeld},
	"held": {"Held", ProcessHeld},

	// 'j' group
	"j":     {"JobId", ProcessJobId},
	"jobid": {"JobId", ProcessJobId},

	// 'k' group
	"k":       {"Comment", ProcessComment},
	"comment": {"Comment", ProcessComment},

	// 'l' group
	"l":         {"TimeLimit", ProcessTimeLimit},
	"timelimit": {"TimeLimit", ProcessTimeLimit},
	"L":         {"NodeList(Reason)", ProcessNodeList},
	"nodelist":  {"NodeList(Reason)", ProcessNodeList},

	// 'm' group
	"m":               {"AllocMemPerNode", ProcessAllocMemPerNode},
	"allocmempernode": {"AllocMemPerNode", ProcessAllocMemPerNode},
	"M":               {"ReqMemPerNode", ProcessReqMemPerNode},
	"reqmempernode":   {"ReqMemPerNode", ProcessReqMemPerNode},

	// 'n' group
	"n":       {"Name", ProcessName},
	"name":    {"Name", ProcessName},
	"N":       {"NodeNum", ProcessNodeNum},
	"nodenum": {"NodeNum", ProcessNodeNum},

	"o":       {"Command", ProcessCommand},
	"command": {"Command", ProcessCommand},
	// 'p' group
	"p":         {"Priority", ProcessPriority},
	"priority":  {"Priority", ProcessPriority},
	"P":         {"Partition", ProcessPartition},
	"partition": {"Partition", ProcessPartition},

	// 'q' group
	"q":             {"QoS", ProcessQoS},
	"qos":           {"QoS", ProcessQoS},
	"Q":             {"ReqCpuPerNode", ProcessReqCpuPerNode},
	"reqcpupernode": {"ReqCpuPerNode", ProcessReqCpuPerNode},

	// 'r' group
	"r":        {"ReqNodes", ProcessReqNodes},
	"reqnodes": {"ReqNodes", ProcessReqNodes},
	"R":        {"Reason", ProcessReason},
	"reason":   {"Reason", ProcessReason},

	// 's' group
	"s":          {"SubmitTime", ProcessSubmitTime},
	"submittime": {"SubmitTime", ProcessSubmitTime},
	"S":          {"StartTime", ProcessStartTime},
	"starttime":  {"StartTime", ProcessStartTime},

	// 't' group
	"t":       {"State", ProcessState},
	"state":   {"State", ProcessState},
	"T":       {"JobType", ProcessJobType},
	"jobtype": {"JobType", ProcessJobType},

	// 'u' group
	"u":    {"User", ProcessUser},
	"user": {"User", ProcessUser},
	"U":    {"Uid", ProcessUid},
	"uid":  {"Uid", ProcessUid},

	// 'x' group
	"x":            {"ExcludeNodes", ProcessExcludeNodes},
	"excludenodes": {"ExcludeNodes", ProcessExcludeNodes},
	"X":            {"Exclusive", ProcessExclusive},
	"exclusive":    {"Exclusive", ProcessExclusive},
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
		field := FlagFormat[spec[4]:spec[5]]
		if len(field) > 1 {
			field = strings.ToLower(field)
		}

		//a-Account, c-AllocCPUs, C-ReqCpus, e-ElapsedTime, h-Held, j-JobID, l-TimeLimit, L-NodeList, k-Comment,
		//m-AllocMemPerNode, M-ReqMemPerNode, n-Name, N-NodeNum, p-Priority, P-Partition, q-Qos, Q-ReqCpuPerNode, r-ReqNodes,
		//R-Reason, s-SubmitTime, S-StartTime, t-State, T-JobType, u-User, U-Uid, x-ExcludeNodes, X-Exclusive.
		fieldProcessor, found := fieldMap[field]
		if !found {
			log.Errorf("Invalid format specifier or string : %s, string unfold case insensitive, reference:\n"+
				"a/Account, c/AllocCPUs, C/ReqCpus, e/ElapsedTime, h/Held, j/JobID, l/TimeLimit, L/NodeList, k/Comment,\n"+
				"m/AllocMemPerNode, M/ReqMemPerNode, n/Name, N/NodeNum, o/Command, p/Priority, P/Partition, q/Qos, Q/ReqCpuPerNode, r/ReqNodes,\n"+
				"R/Reason, s/SubmitTime, S/StartTime, t/State, T/JobType, u/User, U/Uid, x/ExcludeNodes, X/Exclusive.", field)
			os.Exit(util.ErrorInvalidFormat)
		}

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

func loopedQuery(iterate uint64) error {
	interval, err := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "Invalid time interval.",
		}
	}
	for {
		fmt.Println(time.Now().String()[0:19])
		err := Query()
		if err != nil {
			return err
		}
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
