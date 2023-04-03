package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"os"
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
	req := protos.QueryTasksInfoRequest{QueryAll: false}

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
				fmt.Fprintf(os.Stderr, "Invalid state given: %s\n", filterStateList[i])
				os.Exit(1)
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
				fmt.Fprintf(os.Stderr, "Invalid task id given: %s\n", filterJobIdList[i])
				os.Exit(1)
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
	util.SetTableStyle(table)
	header := []string{"TaskId", "Name", "Status", "Partition", "User", "Account", "Type", "NodeIndex"}
	tableData := make([][]string, len(reply.TaskInfoList))
	for i := 0; i < len(reply.TaskInfoList); i++ {
		tableData = append(tableData, []string{
			strconv.FormatUint(uint64(reply.TaskInfoList[i].TaskId), 10),
			reply.TaskInfoList[i].Name,
			reply.TaskInfoList[i].Status.String(),
			reply.TaskInfoList[i].Partition,
			reply.TaskInfoList[i].UserName,
			reply.TaskInfoList[i].Account,
			reply.TaskInfoList[i].Type.String(),
			reply.TaskInfoList[i].CranedList})
	}

	if FlagFormat != "" {
		header, tableData = FormatData(reply)
	}

	if FlagStartTime {
		header = append(header, "StartTime")
		for i := 0; i < len(tableData); i++ {
			tableData[i] = append(tableData[i], reply.TaskInfoList[i].StartTime.AsTime().String())
		}
	}

	if !FlagNoHeader {
		table.SetHeader(header)
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
		//j-TaskId, n-Name, t-State, p-Partition, u-User, a-Account, T-Type, N-NodeIndex
		case "j":
			tableOutputHeader[i] = "TaskId"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], strconv.FormatUint(uint64(reply.TaskInfoList[j].TaskId), 10))
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
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].UserName)
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
		case "N":
			tableOutputHeader[i] = "NodeIndex"
			for j := 0; j < len(reply.TaskInfoList); j++ {
				formatTableData[j] = append(formatTableData[j], reply.TaskInfoList[j].CranedList)
			}
		default:
			fmt.Println("Invalid format, shorthand reference:\n" +
				"j-TaskId, n-Name, t-State, p-Partition, u-User, a-Account, T-Type, N-NodeIndex")
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
