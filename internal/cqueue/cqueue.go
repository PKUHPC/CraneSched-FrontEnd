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
	req := protos.QueryTasksInfoRequest{}

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
		filterJobIdList := strings.Split(FlagFilterPartitions, ",")
		var filterJobIdListInt []uint32
		for i := 0; i < len(filterJobIdList); i++ {
			id, err := strconv.ParseUint(filterJobIdList[i], 10, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid task id given: %s\n", filterJobIdList[i])
				os.Exit(1)
			}
			filterJobIdListInt[i] = uint32(id)
		}
		req.FilterTaskIds = filterJobIdListInt
	}

	reply, err := stub.QueryTasksInfo(context.Background(), &req)
	if err != nil {
		panic("QueryTasksInfo failed: " + err.Error())
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetTablePadding("\t")
	table.SetHeaderLine(false)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetNoWhiteSpace(true)
	if !FlagNoHeader {
		if FlagStartTime {
			table.SetHeader([]string{"TaskId", "Name", "State", "Partition", "User", "Account", "Type", "Status", "StartTime", "NodeIndex"})
		} else {
			table.SetHeader([]string{"TaskId", "Name", "State", "Partition", "User", "Account", "Type", "Status", "NodeIndex"})
		}
	}

	tableData := make([][]string, len(reply.TaskInfoList))
	for i := 0; i < len(reply.TaskInfoList); i++ {
		if FlagStartTime {
			tableData = append(tableData, []string{
				strconv.FormatUint(uint64(reply.TaskInfoList[i].TaskId), 10),
				reply.TaskInfoList[i].Name,
				reply.TaskInfoList[i].Status.String(),
				reply.TaskInfoList[i].Partition,
				reply.TaskInfoList[i].UserName,
				reply.TaskInfoList[i].Account,
				reply.TaskInfoList[i].Type.String(),
				reply.TaskInfoList[i].Status.String(),
				reply.TaskInfoList[i].StartTime.String(),
				reply.TaskInfoList[i].CranedList})
		} else {
			tableData = append(tableData, []string{
				strconv.FormatUint(uint64(reply.TaskInfoList[i].TaskId), 10),
				reply.TaskInfoList[i].Name,
				reply.TaskInfoList[i].Status.String(),
				reply.TaskInfoList[i].Partition,
				reply.TaskInfoList[i].UserName,
				reply.TaskInfoList[i].Account,
				reply.TaskInfoList[i].Type.String(),
				reply.TaskInfoList[i].Status.String(),
				reply.TaskInfoList[i].CranedList})
		}
	}

	table.AppendBulk(tableData)
	table.Render()
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
