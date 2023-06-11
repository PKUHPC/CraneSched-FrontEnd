package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	stub protos.CraneCtldClient
)

func Query() {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
	req := protos.QueryTasksInfoRequest{OptionIncludeCompletedTasks: false}

	var stateList []protos.TaskStatus
	if FlagFilterStates != "" {
		filterStateList := strings.Split(strings.ToLower(FlagFilterStates), ",")
		for i := 0; i < len(filterStateList); i++ {
			switch filterStateList[i] {
			case "r", "running":
				stateList = append(stateList, protos.TaskStatus_Running)
			case "p", "pending":
				stateList = append(stateList, protos.TaskStatus_Pending)
			case "cancelled":
				stateList = append(stateList, protos.TaskStatus_Cancelled)
			case "completing":
				stateList = append(stateList, protos.TaskStatus_Completing)
			default:
				util.Error("Invalid state given: %s\n", filterStateList[i])
			}
		}
		req.FilterTaskStates = stateList
	}

	if FlagFilterJobNames != "" {
		filterJobNameList := strings.Split(FlagFilterJobNames, ",")
		req.FilterTaskNames = filterJobNameList
	}
	if FlagFilterUsers != "" {
		filterUserList := strings.Split(FlagFilterUsers, ",")
		req.FilterUsers = filterUserList
	}
	if FlagFilterQos != "" {
		filterJobQosList := strings.Split(FlagFilterQos, ",")
		req.FilterQos = filterJobQosList
	}
	if FlagFilterAccounts != "" {
		filterAccountList := strings.Split(FlagFilterAccounts, ",")
		req.FilterAccounts = filterAccountList
	}
	if FlagFilterPartitions != "" {
		filterPartitionList := strings.Split(FlagFilterPartitions, ",")
		req.FilterPartitions = filterPartitionList
	}
	if FlagFilterJobIDs != "" {
		filterJobIdList := strings.Split(FlagFilterJobIDs, ",")
		req.NumLimit = int32(len(filterJobIdList))
		var filterJobIdListInt []uint32
		for i := 0; i < len(filterJobIdList); i++ {
			id, err := strconv.ParseUint(filterJobIdList[i], 10, 32)
			if err != nil {
				util.Error("Invalid job id given: %s\n", filterJobIdList[i])
			}
			filterJobIdListInt = append(filterJobIdListInt, uint32(id))
		}
		req.FilterTaskIds = filterJobIdListInt
	}

	if FlagNumLimit != 0 {
		req.NumLimit = FlagNumLimit
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &req)
	if err != nil {
		panic("QueryTasksInfo failed: " + err.Error())
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	header := []string{"JobId", "Name", "Status", "Partition", "User",
		"Account", "Type", "Nodes", "TimeLimit", "NodeList"}
	tableData := make([][]string, len(reply.TaskInfoList))
	for i := 0; i < len(reply.TaskInfoList); i++ {
		tableData[i] = []string{
			strconv.FormatUint(uint64(reply.TaskInfoList[i].TaskId), 10),
			reply.TaskInfoList[i].Name,
			reply.TaskInfoList[i].Status.String(),
			reply.TaskInfoList[i].Partition,
			reply.TaskInfoList[i].Username,
			reply.TaskInfoList[i].Account,
			reply.TaskInfoList[i].Type.String(),
			strconv.FormatUint(uint64(reply.TaskInfoList[i].NodeNum), 10),
			util.SecondTimeFormat(reply.TaskInfoList[i].TimeLimit.Seconds),
			reply.TaskInfoList[i].CranedList}
	}

	if FlagFormat != "" {
		header, tableData = FormatData(reply)
	}

	if FlagStartTime {
		header = append(header, "StartTime")
		for i := 0; i < len(tableData); i++ {
			tableData[i] = append(tableData[i], reply.TaskInfoList[i].StartTime.AsTime().String()[:20])
		}
	}
	if FlagFilterQos != "" {
		header = append(header, "QoS")
		for i := 0; i < len(tableData); i++ {
			tableData[i] = append(tableData[i], reply.TaskInfoList[i].Qos)
		}
	}

	if !FlagNoHeader {
		table.SetHeader(header)
	}

	// Get index of "JobId" column
	idx := -1
	for i, val := range header {
		if val == "JobId" {
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
	formatReq := strings.Split(FlagFormat, " ")
	tableOutputWidth := make([]int, len(formatReq))
	tableOutputHeader := make([]string, len(formatReq))
	for i := 0; i < len(formatReq); i++ {
		if formatReq[i][0] != '%' || len(formatReq[i]) < 2 {
			fmt.Println("Invalid format.")
			os.Exit(1)
		}
		if formatReq[i][1] == '.' {
			if len(formatReq[i]) < 4 {
				fmt.Println("Invalid format.")
				os.Exit(1)
			}
			width, err := strconv.ParseUint(formatReq[i][2:len(formatReq[i])-1], 10, 32)
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
		tableOutputHeader[i] = formatReq[i][len(formatReq[i])-1:]
		switch tableOutputHeader[i] {
		//j-TaskId, n-Name, t-State, p-Partition, u-User, a-Account, T-Type, I-NodeIndex,l-TimeLimit,N-Nodes
		case "j":
			tableOutputHeader[i] = "JobId"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[j].TaskId), 10))
			}
		case "n":
			tableOutputHeader[i] = "Name"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Name)
			}
		case "t":
			tableOutputHeader[i] = "Status"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Status.String())
			}
		case "p":
			tableOutputHeader[i] = "Partition"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Partition)
			}
		case "u":
			tableOutputHeader[i] = "User"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Username)
			}
		case "a":
			tableOutputHeader[i] = "Account"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Account)
			}
		case "T":
			tableOutputHeader[i] = "Type"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].Type.String())
			}
		case "I":
			tableOutputHeader[i] = "NodeList"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].CranedList)
			}
		case "l":
			tableOutputHeader[i] = "TimeLimit"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[i].TimeLimit.String())
			}
		case "N":
			tableOutputHeader[i] = "Nodes"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j],
					strconv.FormatUint(uint64(reply.TaskInfoList[i].NodeNum), 10))
			}
		default:
			fmt.Println("Invalid format, shorthand reference:\n" +
				"j-JobId, n-Name, t-State, p-Partition, u-User, a-Account, T-Type, N-NodeList")
			os.Exit(1)
		}
	}
	return util.FormatTable(tableOutputWidth, tableOutputHeader, formatTableData)
}

func loopedQuery(iterate uint64) {
	interval, _ := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	for {
		fmt.Println(time.Now().String()[0:19])
		Query()
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
