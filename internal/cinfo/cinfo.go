package cinfo

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

func cinfoFunc() {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)

	req := &protos.QueryClusterInfoRequest{
		FilterOnlyDownNodes:       FlagFilterDownOnly,
		FilterOnlyRespondingNodes: FlagFilterRespondingOnly,
	}

	var stateList []protos.CranedState
	if FlagFilterCranedStates != "" {
		filterCranedStateList := strings.Split(strings.ToLower(FlagFilterCranedStates), ",")
		for i := 0; i < len(filterCranedStateList); i++ {
			switch filterCranedStateList[i] {
			case "idle":
				stateList = append(stateList, protos.CranedState_CRANE_IDLE)
			case "mix":
				stateList = append(stateList, protos.CranedState_CRANE_MIX)
			case "alloc":
				stateList = append(stateList, protos.CranedState_CRANE_ALLOC)
			case "down":
				stateList = append(stateList, protos.CranedState_CRANE_DOWN)
			default:
				util.Error("Invalid state given: %s\n", filterCranedStateList[i])
			}
		}
		req.FilterCranedStates = stateList
	}

	if FlagFilterPartitions != "" {
		filterPartitionList := strings.Split(FlagFilterPartitions, ",")
		req.FilterPartitions = filterPartitionList
	}

	if FlagFilterNodes != "" {
		filterNodeList := strings.Split(FlagFilterNodes, ",")
		req.FilterNodes = filterNodeList
	}

	reply, err := stub.QueryClusterInfo(context.Background(), req)
	if err != nil {
		panic("QueryClusterInfo failed: " + err.Error())
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
	var tableData [][]string
	table.SetHeader([]string{"PARTITION", "AVAIL", "TIMELIMIT", "NODES", "STATE", "NODELIST"})
	for _, partitionCraned := range reply.Partitions {
		for _, commonCranedStateList := range partitionCraned.CranedLists {
			if commonCranedStateList.Count > 0 {
				tableData = append(tableData, []string{
					partitionCraned.Name,
					strings.ToLower(partitionCraned.State.String()[10:]),
					"infinite",
					strconv.FormatUint(uint64(commonCranedStateList.Count), 10),
					strings.ToLower(commonCranedStateList.State.String()[6:]),
					commonCranedStateList.CranedListRegex,
				})
			}
		}
	}
	table.AppendBulk(tableData)
	if len(tableData) == 0 {
		fmt.Printf("No partition is available.\n")
	} else {
		table.Render()
	}
}

func loopedQuery(iterate uint64) {
	interval, _ := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	for {
		fmt.Println(time.Now().String()[0:19])
		cinfoFunc()
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
