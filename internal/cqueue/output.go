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
					strconv.FormatUint(uint64(job.JobId), 10),
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
					strconv.FormatUint(uint64(job.JobId), 10),
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
