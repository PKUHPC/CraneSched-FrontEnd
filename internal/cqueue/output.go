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

type arraySummaryInfo struct {
	anchorJobId uint32
	start       uint32
	end         uint32
	count       int
	sample      *protos.JobInfo
}

func makeArraySummaryGroupKey(job *protos.JobInfo) (string, bool) {
	if job.ArrayIndexStart == nil || job.ArrayIndexEnd == nil || job.ArrayTaskId == nil {
		return "", false
	}

	return fmt.Sprintf("%s|%s|%s|%s|%d|%d|%d",
		job.Account,
		job.Username,
		job.Partition,
		job.CmdLine,
		job.SubmitTime.GetSeconds(),
		*job.ArrayIndexStart,
		*job.ArrayIndexEnd,
	), true
}

func buildArraySummaryRow(header []string, info *arraySummaryInfo) []string {
	row := make([]string, len(header))
	summaryId := fmt.Sprintf("%d_[%d-%d]", info.anchorJobId, info.start, info.end)

	for i, column := range header {
		switch column {
		case "JobId", "JOBID":
			row[i] = summaryId
		case "JobName", "Name":
			row[i] = info.sample.Name
		case "User", "UserName":
			row[i] = info.sample.Username
		case "Partition":
			row[i] = info.sample.Partition
		case "Account":
			row[i] = info.sample.Account
		case "Status", "State":
			row[i] = "ArraySummary"
		case "Type", "JobType":
			row[i] = info.sample.Type.String()
		case "Time", "ElapsedTime":
			row[i] = "-"
		case "TimeLimit":
			row[i] = FormatTimeLimit(info.sample.TimeLimit.Seconds)
		case "Nodes", "NodeNum":
			row[i] = strconv.Itoa(info.count)
		case "NodeList/Reason", "NodeList":
			row[i] = fmt.Sprintf("ArrayTasks=%d", info.count)
		case "QoS", "Qos":
			row[i] = info.sample.Qos
		case "StartTime":
			row[i] = FormatTime(info.sample.StartTime, "unknown")
		case "SubmitTime":
			row[i] = FormatTime(info.sample.SubmitTime, "unknown")
		default:
			row[i] = "-"
		}
	}

	return row
}

func insertArraySummaryRows(header []string, rows [][]string, jobs []*protos.JobInfo) [][]string {
	if len(rows) != len(jobs) {
		return rows
	}

	groupByKey := make(map[string]*arraySummaryInfo)
	jobKeys := make([]string, len(jobs))

	for i, job := range jobs {
		key, ok := makeArraySummaryGroupKey(job)
		if !ok {
			continue
		}
		jobKeys[i] = key

		if _, ok := groupByKey[key]; !ok {
			groupByKey[key] = &arraySummaryInfo{
				anchorJobId: job.JobId,
				start:       *job.ArrayIndexStart,
				end:         *job.ArrayIndexEnd,
				count:       0,
				sample:      job,
			}
		}

		group := groupByKey[key]
		group.count++
		if job.JobId > group.anchorJobId {
			group.anchorJobId = job.JobId
		}
	}

	emitted := make(map[string]struct{})
	merged := make([][]string, 0, len(rows)+len(groupByKey))

	for i, row := range rows {
		key := jobKeys[i]
		if key != "" {
			if _, ok := emitted[key]; !ok {
				group := groupByKey[key]
				if group.count > 1 {
					merged = append(merged, buildArraySummaryRow(header, group))
				}
				emitted[key] = struct{}{}
			}
		}
		merged = append(merged, row)
	}

	return merged
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
				"Type", "Qos", "Exclusive", "Held", "Priority", "NodeList/Reason",
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
