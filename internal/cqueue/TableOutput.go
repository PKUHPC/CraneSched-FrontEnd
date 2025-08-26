package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"os"
	"strconv"
	"time"

	"github.com/olekukonko/tablewriter"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TableConfig struct {
	Header    []string
	RowMapper func(*protos.TaskInfo) []string
}

func FormatTime(t *timestamppb.Timestamp, fallback string) string {
	if t == nil || t.AsTime().Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
		return fallback
	}
	return t.AsTime().In(time.Local).Format("2006-01-02 15:04:05")
}

func GetElapsedTime(task *protos.TaskInfo) string {
	if task.Status == protos.TaskStatus_Running {
		return util.SecondTimeFormat(task.ElapsedTime.Seconds)
	}
	return "-"
}

func GetReasonInfo(task *protos.TaskInfo) string {
	if task.Status == protos.TaskStatus_Pending {
		return task.GetPendingReason()
	}
	return task.GetCranedList()
}

func FormatTimeLimit(seconds int64) string {
	if seconds >= util.InvalidDuration().Seconds {
		return "unlimited"
	}
	return util.SecondTimeFormat(seconds)
}

func GenerateTableConfig() TableConfig {
	if FlagFull {
		return TableConfig{
			Header: []string{
				"JobId", "JobName", "UserName", "Partition", "Account",
				"NodeNum", "ReqCPUs", "ReqMemPerNode", "AllocCPUs", "AllocMemPerNode",
				"Status", "Time", "TimeLimit", "StartTime", "SubmitTime",
				"Type", "Qos", "Exclusive", "Held", "Priority", "NodeList/Reason",
			},
			RowMapper: func(task *protos.TaskInfo) []string {
				return []string{
					strconv.FormatUint(uint64(task.TaskId), 10),
					task.Name,
					task.Username,
					task.Partition,
					task.Account,
					strconv.FormatUint(uint64(task.NodeNum), 10),
					ProcessReqCPUs(task),
					ProcessReqMemPerNode(task),
					ProcessAllocCpus(task),
					ProcessAllocMemPerNode(task),
					task.Status.String(),
					GetElapsedTime(task),
					FormatTimeLimit(task.TimeLimit.Seconds),
					FormatTime(task.StartTime, "unknown"),
					FormatTime(task.SubmitTime, "unknown"),
					task.Type.String(),
					task.Qos,
					strconv.FormatBool(task.Exclusive),
					strconv.FormatBool(task.Held),
					strconv.FormatUint(uint64(task.Priority), 10),
					GetReasonInfo(task),
				}
			},
		}
	} else {
		return TableConfig{
			Header: []string{
				"JobId", "Partition", "Name", "User", "Account",
				"Status", "Type", "Time", "TimeLimit", "Nodes", "NodeList/Reason",
			},
			RowMapper: func(task *protos.TaskInfo) []string {
				return []string{
					strconv.FormatUint(uint64(task.TaskId), 10),
					task.Partition,
					task.Name,
					task.Username,
					task.Account,
					task.Status.String(),
					task.Type.String(),
					GetElapsedTime(task),
					FormatTimeLimit(task.TimeLimit.Seconds),
					strconv.FormatUint(uint64(task.NodeNum), 10),
					GetReasonInfo(task),
				}
			},
		}
	}
}

func AppendDynamicColumns(config *TableConfig, reply *protos.QueryTasksInfoReply,
	tableData [][]string) ([][]string, []string) {
	header := config.Header
	if FlagStartTime {
		header = append(header, "StartTime")
		for i, task := range reply.TaskInfoList {
			cell := FormatTime(task.StartTime, "")
			tableData[i] = append(tableData[i], cell)
		}
	}
	if FlagFilterQos != "" {
		header = append(header, "QoS")
		for i, task := range reply.TaskInfoList {
			tableData[i] = append(tableData[i], task.Qos)
		}
	}
	return tableData, header
}

func QueryTableOutput(reply *protos.QueryTasksInfoReply) error {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)

	config := GenerateTableConfig()
	tableData := make([][]string, len(reply.TaskInfoList))

	for i, task := range reply.TaskInfoList {
		tableData[i] = config.RowMapper(task)
	}

	var header []string
	if !FlagFull {
		tableData, header = AppendDynamicColumns(&config, reply, tableData)
	} else {
		header = config.Header
	}

	if FlagFormat != "" {
		customHeader, customData := FormatData(reply)
		header, tableData = customHeader, customData
		table.SetAutoFormatHeaders(false)
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




// // FormatData formats the output data according to the format string.
// // The format string can accept specifiers in the form of %.<width><format character>.
// // Besides, it can contain prefix, padding and suffix strings, e.g.,
// // "prefix%j_xx%t_x%.5L(Suffix)"
// func FormatData(reply *protos.QueryTasksInfoReply) (header []string, tableData [][]string) {
// 	re := regexp.MustCompile(`%(\.\d+)?([a-zA-Z]+)`)
// 	specifiers := re.FindAllStringSubmatchIndex(FlagFormat, -1)
// 	if specifiers == nil {
// 		log.Errorln("Invalid format specifier.")
// 		os.Exit(util.ErrorInvalidFormat)
// 	}

// 	tableOutputWidth := make([]int, 0, len(specifiers))
// 	tableOutputHeader := make([]string, 0, len(specifiers))
// 	tableOutputCell := make([][]string, len(reply.TaskInfoList))

// 	// Get the prefix of the format string
// 	if specifiers[0][0] != 0 {
// 		prefix := FlagFormat[0:specifiers[0][0]]
// 		tableOutputWidth = append(tableOutputWidth, -1)
// 		tableOutputHeader = append(tableOutputHeader, prefix)
// 		for j := 0; j < len(reply.TaskInfoList); j++ {
// 			tableOutputCell[j] = append(tableOutputCell[j], prefix)
// 		}
// 	}

// 	for i, spec := range specifiers {
// 		// Get the padding string between specifiers
// 		if i > 0 && spec[0]-specifiers[i-1][1] > 0 {
// 			padding := FlagFormat[specifiers[i-1][1]:spec[0]]
// 			tableOutputWidth = append(tableOutputWidth, -1)
// 			tableOutputHeader = append(tableOutputHeader, padding)
// 			for j := 0; j < len(reply.TaskInfoList); j++ {
// 				tableOutputCell[j] = append(tableOutputCell[j], padding)
// 			}
// 		}

// 		// Parse width specifier
// 		if spec[2] == -1 {
// 			// w/o width specifier
// 			tableOutputWidth = append(tableOutputWidth, -1)
// 		} else {
// 			// with width specifier
// 			width, err := strconv.ParseUint(FlagFormat[spec[2]+1:spec[3]], 10, 32)
// 			if err != nil {
// 				log.Errorln("Invalid width specifier.")
// 				os.Exit(util.ErrorInvalidFormat)
// 			}
// 			tableOutputWidth = append(tableOutputWidth, int(width))
// 		}

// 		// Parse format specifier
// 		field := FlagFormat[spec[4]:spec[5]]
// 		if len(field) > 1 {
// 			field = strings.ToLower(field)
// 		}

// 		//a-Account, c-AllocCPUs, C-ReqCpus, e-ElapsedTime, h-Held, j-JobID, l-TimeLimit, L-NodeList, k-Comment,
// 		//m-AllocMemPerNode, M-ReqMemPerNode, n-Name, N-NodeNum, p-Priority, P-Partition, q-Qos, Q-ReqCpuPerNode, r-ReqNodes,
// 		//R-Reason, s-SubmitTime, S-StartTime, t-State, T-JobType, u-User, U-Uid, x-ExcludeNodes, X-Exclusive.
// 		fieldProcessor, found := fieldMap[field]
// 		if !found {
// 			log.Errorf("Invalid format specifier or string : %s, string unfold case insensitive, reference:\n"+
// 				"a/Account, c/AllocCPUs, C/ReqCpus, e/ElapsedTime, h/Held, j/JobID, l/TimeLimit, L/NodeList, k/Comment,\n"+
// 				"m/AllocMemPerNode, M/ReqMemPerNode, n/Name, N/NodeNum, o/Command, p/Priority, P/Partition, q/Qos, Q/ReqCpuPerNode, r/ReqNodes,\n"+
// 				"R/Reason, s/SubmitTime, S/StartTime, t/State, T/JobType, u/User, U/Uid, x/ExcludeNodes, X/Exclusive.", field)
// 			os.Exit(util.ErrorInvalidFormat)
// 		}

// 		tableOutputHeader = append(tableOutputHeader, strings.ToUpper(fieldProcessor.header))
// 		for j, task := range reply.TaskInfoList {
// 			tableOutputCell[j] = append(tableOutputCell[j], fieldProcessor.process(task))
// 		}
// 	}

// 	// Get the suffix of the format string
// 	if len(FlagFormat)-specifiers[len(specifiers)-1][1] > 0 {
// 		suffix := FlagFormat[specifiers[len(specifiers)-1][1]:]
// 		tableOutputWidth = append(tableOutputWidth, -1)
// 		tableOutputHeader = append(tableOutputHeader, suffix)
// 		for j := 0; j < len(reply.TaskInfoList); j++ {
// 			tableOutputCell[j] = append(tableOutputCell[j], suffix)
// 		}
// 	}

// 	return util.FormatTable(tableOutputWidth, tableOutputHeader, tableOutputCell)
// }

// func QueryTableOutput(reply *protos.QueryTasksInfoReply) error {
// 	table := tablewriter.NewWriter(os.Stdout)
// 	util.SetBorderlessTable(table)

// 	var header []string
// 	tableData := make([][]string, len(reply.TaskInfoList))
// 	if FlagFull {
// 		header = []string{"JobId", "JobName", "UserName", "Partition",
// 			"Account", "NodeNum", "ReqCPUs", "ReqMemPerNode", "AllocCPUs", "AllocMemPerNode", "Status", "Time", "TimeLimit",
// 			"StartTime", "SubmitTime", "Type", "Qos", "Exclusive", "Held", "Priority", "NodeList/Reason"}
// 		for i := range reply.TaskInfoList {
// 			taskInfo := reply.TaskInfoList[i]

// 			var timeElapsedStr string
// 			if reply.TaskInfoList[i].Status == protos.TaskStatus_Running {
// 				timeElapsedStr = util.SecondTimeFormat(reply.TaskInfoList[i].ElapsedTime.Seconds)
// 			} else {
// 				timeElapsedStr = "-"
// 			}

// 			var timeLimitStr string
// 			if taskInfo.TimeLimit.Seconds >= util.InvalidDuration().Seconds {
// 				timeLimitStr = "unlimited"
// 			} else {
// 				timeLimitStr = util.SecondTimeFormat(taskInfo.TimeLimit.Seconds)
// 			}

// 			startTimeStr := "unknown"
// 			startTime := taskInfo.StartTime.AsTime()
// 			if !startTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) &&
// 				startTime.Before(time.Now()) {
// 				startTimeStr = startTime.In(time.Local).Format("2006-01-02 15:04:05")
// 			}

// 			submitTimeStr := "unknown"
// 			submitTime := taskInfo.SubmitTime.AsTime()
// 			if !submitTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
// 				submitTimeStr = submitTime.In(time.Local).Format("2006-01-02 15:04:05")
// 			}

// 			var reasonOrListStr string
// 			if reply.TaskInfoList[i].Status == protos.TaskStatus_Pending {
// 				reasonOrListStr = reply.TaskInfoList[i].GetPendingReason()
// 			} else {
// 				reasonOrListStr = reply.TaskInfoList[i].GetCranedList()
// 			}

// 			tableData[i] = []string{
// 				strconv.FormatUint(uint64(taskInfo.TaskId), 10),
// 				taskInfo.Name,
// 				taskInfo.Username,
// 				taskInfo.Partition,
// 				taskInfo.Account,
// 				strconv.FormatUint(uint64(taskInfo.NodeNum), 10),
// 				ProcessReqCPUs(taskInfo),
// 				ProcessReqMemPerNode(taskInfo),
// 				ProcessAllocCpus(taskInfo),
// 				ProcessAllocMemPerNode(taskInfo),
// 				taskInfo.Status.String(),
// 				timeElapsedStr,
// 				timeLimitStr,
// 				startTimeStr,
// 				submitTimeStr,
// 				reply.TaskInfoList[i].Type.String(),
// 				taskInfo.Qos,
// 				strconv.FormatBool(taskInfo.Exclusive),
// 				strconv.FormatBool(taskInfo.Held),
// 				strconv.FormatUint(uint64(taskInfo.Priority), 10),
// 				reasonOrListStr}
// 		}
// 	} else {
// 		header = []string{"JobId", "Partition", "Name", "User",
// 			"Account", "Status", "Type", "Time", "TimeLimit", "Nodes", "NodeList/Reason"}

// 		for i := 0; i < len(reply.TaskInfoList); i++ {
// 			var timeLimitStr string
// 			if reply.TaskInfoList[i].TimeLimit.Seconds >= util.InvalidDuration().Seconds {
// 				timeLimitStr = "unlimited"
// 			} else {
// 				timeLimitStr = util.SecondTimeFormat(reply.TaskInfoList[i].TimeLimit.Seconds)
// 			}

// 			var timeElapsedStr string
// 			if reply.TaskInfoList[i].Status == protos.TaskStatus_Running {
// 				timeElapsedStr = util.SecondTimeFormat(reply.TaskInfoList[i].ElapsedTime.Seconds)
// 			} else {
// 				timeElapsedStr = "-"
// 			}

// 			var reasonOrListStr string
// 			if reply.TaskInfoList[i].Status == protos.TaskStatus_Pending {
// 				reasonOrListStr = reply.TaskInfoList[i].GetPendingReason()
// 			} else {
// 				reasonOrListStr = reply.TaskInfoList[i].GetCranedList()
// 			}

// 			tableData[i] = []string{
// 				strconv.FormatUint(uint64(reply.TaskInfoList[i].TaskId), 10),
// 				reply.TaskInfoList[i].Partition,
// 				reply.TaskInfoList[i].Name,
// 				reply.TaskInfoList[i].Username,
// 				reply.TaskInfoList[i].Account,
// 				reply.TaskInfoList[i].Status.String(),
// 				reply.TaskInfoList[i].Type.String(),
// 				timeElapsedStr,
// 				timeLimitStr,
// 				strconv.FormatUint(uint64(reply.TaskInfoList[i].NodeNum), 10),
// 				reasonOrListStr,
// 			}
// 		}

// 		if FlagFormat != "" {
// 			header, tableData = FormatData(reply)
// 			table.SetTablePadding("")
// 			table.SetAutoFormatHeaders(false)
// 		}

// 		if FlagStartTime {
// 			header = append(header, "StartTime")
// 			for i := 0; i < len(tableData); i++ {
// 				startTime := reply.TaskInfoList[i].StartTime
// 				if startTime.Seconds != 0 {
// 					tableData[i] = append(tableData[i],
// 						startTime.AsTime().In(time.Local).
// 							Format("2006-01-02 15:04:05"))
// 				} else {
// 					tableData[i] = append(tableData[i], "")
// 				}
// 			}
// 		}
// 		if FlagFilterQos != "" {
// 			header = append(header, "QoS")
// 			for i := 0; i < len(tableData); i++ {
// 				tableData[i] = append(tableData[i], reply.TaskInfoList[i].Qos)
// 			}
// 		}
// 	}

// 	if !FlagNoHeader {
// 		table.SetHeader(header)
// 	}

// 	if !FlagFull && FlagFormat == "" {
// 		util.TrimTable(&tableData)
// 	}

// 	table.AppendBulk(tableData)
// 	table.Render()
// 	return nil
// }
