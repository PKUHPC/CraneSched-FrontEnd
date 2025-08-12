package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"os"
	"strconv"
	"time"

	"github.com/olekukonko/tablewriter"
	"google.golang.org/protobuf/types/known/durationpb"
)


func QueryTableOutput(reply *protos.QueryTasksInfoReply) error {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	var header []string
	tableData := make([][]string, len(reply.TaskInfoList))
	if FlagFull {
		header = []string{"JobId", "JobName", "UserName", "Partition",
			"Account", "NodeNum", "ReqCPUs","ReqMemPerNode", "AllocCPUs", "AllocMemPerNode", "Status", "Time", "TimeLimit",
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
