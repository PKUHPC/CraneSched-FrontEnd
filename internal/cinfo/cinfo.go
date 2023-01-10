package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"strconv"
	"strings"
	"time"
)

func cinfoFun() {

	config := util.ParseConfig()

	serverAddr := fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("Cannot connect to CraneCtld: " + err.Error())
	}
	stub := protos.NewCraneCtldClient(conn)
	req := &protos.QueryClusterInfoRequest{
		QueryDownNodes:       dead,
		QueryRespondingNodes: responding,
	}

	partitionList := strings.Split(partitions, ",")
	nodeList := strings.Split(nodes, ",")
	stateReq := strings.Split(strings.ToLower(states), ",")
	var stateList []protos.CranedState
	if states != "" {
		for i := 0; i < len(stateReq); i++ {
			switch stateReq[i] {
			case "idle":
				stateList = append(stateList, 0)
			case "mix":
				stateList = append(stateList, 1)
			case "alloc":
				stateList = append(stateList, 2)
			case "down":
				stateList = append(stateList, 3)
			default:
				fmt.Println("Invalid states specified.")
				os.Exit(1)
			}
		}
	}

	if partitions != "" {
		req.Partitions = partitionList
	}
	if nodes != "" {
		req.Nodes = nodeList
	}
	if states != "" {
		req.States = stateList
	}

	reply, err := stub.QueryClusterInfo(context.Background(), req)
	if err != nil {
		panic("QueryClusterInfo failed: " + err.Error())
	}
	//fmt.Println(reply.Num)

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
	for _, partitionCraned := range reply.PartitionCraned {
		for _, commonCranedStateList := range partitionCraned.CommonCranedStateList {
			if commonCranedStateList.CranedNum > 0 {
				tableData = append(tableData, []string{
					partitionCraned.Name,
					strings.ToLower(partitionCraned.State.String()[10:]),
					"infinite",
					strconv.FormatUint(uint64(commonCranedStateList.CranedNum), 10),
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

func IterateQuery(iterate uint64) {
	iter, _ := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	for {
		fmt.Println(time.Now().String()[0:19])
		cinfoFun()
		time.Sleep(time.Duration(iter.Nanoseconds()))
		fmt.Println()
	}
}
