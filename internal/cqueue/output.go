package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TableConfig struct {
	Header    []string
	RowMapper func(*protos.JobInfo) []string
}

func buildArraySummaryRow(header []string, info *util.ArraySummaryInfo) []string {
	row := make([]string, len(header))
	summaryId := fmt.Sprintf("%d_[%d-%d]", info.AnchorJobId, info.Start, info.End)

	for i, column := range header {
		switch column {
		case "JobId", "JOBID":
			row[i] = summaryId
		case "JobName", "Name":
			row[i] = info.Sample.Name
		case "User", "UserName":
			row[i] = info.Sample.Username
		case "Partition":
			row[i] = info.Sample.Partition
		case "Account":
			row[i] = info.Sample.Account
		case "Status", "State":
			row[i] = "ArraySummary"
		case "Type", "JobType":
			row[i] = info.Sample.Type.String()
		case "Time", "ElapsedTime":
			row[i] = "-"
		case "TimeLimit":
			row[i] = FormatTimeLimit(info.Sample.TimeLimit.Seconds)
		case "Nodes", "NodeNum":
			row[i] = strconv.Itoa(info.Count)
		case "NodeList/Reason", "NodeList":
			row[i] = fmt.Sprintf("ArrayTasks=%d", info.Count)
		case "QoS", "Qos":
			row[i] = info.Sample.Qos
		case "StartTime":
			row[i] = FormatTime(info.Sample.StartTime, "unknown")
		case "SubmitTime":
			row[i] = FormatTime(info.Sample.SubmitTime, "unknown")
		default:
			row[i] = "-"
		}
	}

	return row
}

func insertArraySummaryRows(header []string, rows [][]string, jobs []*protos.JobInfo) [][]string {
	return util.BuildArraySummaryRows(header, rows, jobs,
		func(job *protos.JobInfo) *protos.JobInfo { return job },
		buildArraySummaryRow,
	)
}

func FormatTime(t *timestamppb.Timestamp, fallback string) string {
	if t == nil || t.AsTime().Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
		return fallback
	}
	return t.AsTime().In(time.Local).Format("2006-01-02 15:04:05")
}

func GetElapsedTime(job *protos.JobInfo) string {
	if job.Status == protos.JobStatus_Running && job.ElapsedTime != nil {
		return util.SecondTimeFormat(job.ElapsedTime.Seconds)
	}
	return "-"
}

func GetReasonInfo(job *protos.JobInfo) string {
	if job.Status == protos.JobStatus_Pending {
		return job.GetPendingReason()
	}
	return job.GetCranedList()
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
			RowMapper: func(job *protos.JobInfo) []string {
				timeLimit := "-"
				if job.TimeLimit != nil {
					timeLimit = FormatTimeLimit(job.TimeLimit.Seconds)
				}
				return []string{
					formatJobIdForDisplay(job),
					job.Name,
					job.Username,
					job.Partition,
					job.Account,
					strconv.FormatUint(uint64(job.NodeNum), 10),
					ProcessReqCPUs(job),
					ProcessReqMemPerNode(job),
					ProcessAllocCpus(job),
					ProcessAllocMemPerNode(job),
					job.Status.String(),
					GetElapsedTime(job),
					timeLimit,
					FormatTime(job.StartTime, "unknown"),
					FormatTime(job.SubmitTime, "unknown"),
					job.Type.String(),
					job.Qos,
					strconv.FormatBool(job.Exclusive),
					strconv.FormatBool(job.Held),
					strconv.FormatUint(uint64(job.Priority), 10),
					GetReasonInfo(job),
					ProcessDeadline(job),
				}
			},
		}
	} else {
		return TableConfig{
			Header: []string{
				"JobId", "Partition", "Name", "User", "Account",
				"Status", "Type", "Time", "TimeLimit", "Nodes", "NodeList/Reason",
			},
			RowMapper: func(job *protos.JobInfo) []string {
				return []string{
					formatJobIdForDisplay(job),
					job.Partition,
					job.Name,
					job.Username,
					job.Account,
					job.Status.String(),
					job.Type.String(),
					GetElapsedTime(job),
					FormatTimeLimit(job.TimeLimit.Seconds),
					strconv.FormatUint(uint64(job.NodeNum), 10),
					GetReasonInfo(job),
				}
			},
		}
	}
}

func AppendDynamicColumns(config *TableConfig, reply *protos.QueryJobsInfoReply,
	tableData [][]string) ([][]string, []string) {
	header := config.Header
	if FlagStartTime {
		header = append(header, "StartTime")
		for i, job := range reply.JobInfoList {
			cell := FormatTime(job.StartTime, "")
			tableData[i] = append(tableData[i], cell)
		}
	}
	if FlagFilterQos != "" {
		header = append(header, "QoS")
		for i, job := range reply.JobInfoList {
			tableData[i] = append(tableData[i], job.Qos)
		}
	}
	if FlagDeadlineTime {
		header = append(header, "Deadline")
		for i, job := range reply.JobInfoList {
			tableData[i] = append(tableData[i], ProcessDeadline(job))
		}
	}
	if FlagFilterLicenses != "" {
		header = append(header, "Licenses")
		for i, job := range reply.JobInfoList {
			var parts []string
			for name, count := range job.LicensesCount {
				parts = append(parts, fmt.Sprintf("%s:%d", name, count))
			}
			licStr := strings.Join(parts, ",")
			tableData[i] = append(tableData[i], licStr)
		}
	}
	return tableData, header
}

func QueryTableOutput(reply *protos.QueryJobsInfoReply) error {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)

	config := GenerateTableConfig()
	tableData := make([][]string, len(reply.JobInfoList))

	for i, job := range reply.JobInfoList {
		tableData[i] = config.RowMapper(job)
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
	} else {
		tableData = insertArraySummaryRows(header, tableData, reply.JobInfoList)
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

func JsonOutput(reply *protos.QueryJobsInfoReply) error {
	fmt.Println(util.FmtJson.FormatReply(reply))
	if reply.GetOk() {
		return nil
	} else {
		return util.NewCraneErr(util.ErrorBackend, "JSON output failed")
	}
}
