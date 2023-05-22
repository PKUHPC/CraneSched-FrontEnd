package cacct

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	stub protos.CraneCtldClient
)

const (
	kCraneExitCodeBase = 256
)

// QueryJob will query all pending, running and completed tasks
func QueryJob() {
	request := protos.QueryTasksInfoRequest{OptionIncludeCompletedTasks: true}

	timeFormat := "2006-01-02T15:04:05"
	if FlagSetStartTime != "" {
		t, err := time.Parse(timeFormat, FlagSetStartTime)
		if err != nil {
			fmt.Println("Failed to parse the time string：", err)
			os.Exit(1)
		}
		request.FilterStartTime = timestamppb.New(t)
	}
	if FlagSetEndTime != "" {
		t, err := time.Parse(timeFormat, FlagSetEndTime)
		if err != nil {
			fmt.Println("Failed to parse the time string：", err)
			os.Exit(1)
		}
		request.FilterEndTime = timestamppb.New(t)
	}

	if FlagFilterAccounts != "" {
		filterAccountList := strings.Split(FlagFilterAccounts, ",")
		request.FilterAccounts = filterAccountList
	}

	if FlagFilterJobIDs != "" {
		filterJobIdList := strings.Split(FlagFilterJobIDs, ",")
		request.NumLimit = int32(len(filterJobIdList))
		var filterJobIdListInt []uint32
		for i := 0; i < len(filterJobIdList); i++ {
			id, err := strconv.ParseUint(filterJobIdList[i], 10, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid task id given: %s\n", filterJobIdList[i])
				os.Exit(1)
			}
			filterJobIdListInt = append(filterJobIdListInt, uint32(id))
		}
		request.FilterTaskIds = filterJobIdListInt
	}

	if FlagFilterUsers != "" {
		filterUserList := strings.Split(FlagFilterUsers, ",")
		request.FilterUsers = filterUserList
	}

	if FlagFilterJobNames != "" {
		filterJobNameList := strings.Split(FlagFilterJobNames, ",")
		request.FilterTaskNames = filterJobNameList
	}

	if FlagNumLimit != 0 {
		request.NumLimit = FlagNumLimit
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &request)
	if err != nil {
		panic("QueryJobsInPartition failed: " + err.Error())
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	header := []string{"TaskId", "TaskName", "Partition", "Account", "AllocCPUs", "State", "ExitCode"}

	tableData := make([][]string, len(reply.TaskInfoList))
	for i := 0; i < len(reply.TaskInfoList); i++ {
		exitCode := ""
		if reply.TaskInfoList[i].ExitCode >= kCraneExitCodeBase {
			exitCode = fmt.Sprintf("0:%d", reply.TaskInfoList[i].ExitCode-kCraneExitCodeBase)
		} else {
			exitCode = fmt.Sprintf("%d:0", reply.TaskInfoList[i].ExitCode)
		}
		tableData[i] = []string{
			strconv.FormatUint(uint64(reply.TaskInfoList[i].TaskId), 10),
			reply.TaskInfoList[i].Name,
			reply.TaskInfoList[i].Partition,
			reply.TaskInfoList[i].Account,
			strconv.FormatFloat(reply.TaskInfoList[i].AllocCpus, 'f', 2, 64),
			reply.TaskInfoList[i].Status.String(),
			exitCode}
	}

	if FlagFormat != "" {
		header, tableData = FormatData(reply)
	}

	if FlagSetStartTime != "" {
		header = append(header, "StartTime")
		for i := 0; i < len(tableData); i++ {
			tableData[i] = append(tableData[i], reply.TaskInfoList[i].StartTime.AsTime().String())
		}
	}

	if FlagSetEndTime != "" {
		header = append(header, "EndTime")
		for i := 0; i < len(tableData); i++ {
			tableData[i] = append(tableData[i], reply.TaskInfoList[i].EndTime.AsTime().String())
		}
	}

	if !FlagNoHeader {
		table.SetHeader(header)
	}

	// Get index of "TaskId" column
	idx := -1
	for i, val := range header {
		if val == "TaskId" {
			idx = i
			break
		}
	}

	// If "TaskId" column exists, sort all rows by descending order of "TaskId".
	if idx != -1 {
		less := func(i, j int) bool {
			x, _ := strconv.ParseUint(tableData[i][idx], 10, 32)
			y, _ := strconv.ParseUint(tableData[j][idx], 10, 32)
			return x > y
		}
		sort.Slice(tableData, less)
	}

	table.AppendBulk(tableData)
	table.Render()
}

func FormatData(reply *protos.QueryTasksInfoReply) (header []string, tableData [][]string) {
	formatTableData := make([][]string, len(reply.TaskInfoList))
	formatReq := strings.Split(FlagFormat, ",")
	tableOutputWidth := make([]int, len(formatReq))
	tableOutputHeader := make([]string, len(formatReq))
	for i := 0; i < len(formatReq); i++ {
		formatLines := strings.Split(formatReq[i], "%")
		if len(formatLines) > 2 {
			fmt.Println("Invalid format.")
			os.Exit(1)
		}
		if len(formatLines) == 2 {
			width, err := strconv.ParseUint(formatLines[1], 10, 32)
			if err != nil {
				if err != nil {
					fmt.Println("Invalid format.")
					os.Exit(1)
				}
			}
			tableOutputWidth[i] = int(width)
		} else {
			tableOutputWidth[i] = -1
		}
		tableOutputHeader[i] = formatLines[0]
		switch tableOutputHeader[i] {
		case "TaskId":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[j].TaskId), 10))
			}
		case "TaskName":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Name)
			}
		case "Partition":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Partition)
			}
		case "Account":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Account)
			}
		case "AllocCPUs":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					strconv.FormatFloat(reply.TaskInfoList[j].AllocCpus, 'f', 2, 64))
			}
		case "State":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Status.String())
			}
		case "ExitCode":
			for j := 0; j < len(reply.TaskInfoList); j++ {
				exitCode := ""
				if reply.TaskInfoList[j].ExitCode >= kCraneExitCodeBase {
					exitCode = fmt.Sprintf("0:%d", reply.TaskInfoList[j].ExitCode-kCraneExitCodeBase)
				} else {
					exitCode = fmt.Sprintf("%d:0", reply.TaskInfoList[j].ExitCode)
				}
				formatTableData[j] = append(formatTableData[j], exitCode)
			}
		default:
			fmt.Println("Invalid format.")
			os.Exit(1)
		}
	}
	return util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
}

func Preparation() {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
}
