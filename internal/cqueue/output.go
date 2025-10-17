package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
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
	if task.Status == protos.TaskStatus_Running && task.ElapsedTime != nil {
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
				"Type", "Qos", "Exclusive", "Held", "Priority", "NodeList/Reason", "Deadline",
			},
			RowMapper: func(task *protos.TaskInfo) []string {
				timeLimit := "-"
				if task.TimeLimit != nil {
					timeLimit = FormatTimeLimit(task.TimeLimit.Seconds)
				}
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
					timeLimit,
					FormatTime(task.StartTime, "unknown"),
					FormatTime(task.SubmitTime, "unknown"),
					task.Type.String(),
					task.Qos,
					strconv.FormatBool(task.Exclusive),
					strconv.FormatBool(task.Held),
					strconv.FormatUint(uint64(task.Priority), 10),
					GetReasonInfo(task),
					ProcessDeadline(task),
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
	if FlagDeadlineTime {
		header = append(header, "Deadline")
		for i, task := range reply.TaskInfoList {
			tableData[i] = append(tableData[i], ProcessDeadline(task))
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

func JsonOutput(reply *protos.QueryTasksInfoReply) error {
	fmt.Println(util.FmtJson.FormatReply(reply))
	if reply.GetOk() {
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, "JSON output failed")
	}
}
