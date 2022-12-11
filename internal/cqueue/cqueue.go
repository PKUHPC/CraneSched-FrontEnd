package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"github.com/olekukonko/tablewriter"
	"os"
	"strconv"
)

var (
	stub protos.CraneCtldClient
)

func Query(partition string) {
	request := protos.QueryTasksInfoRequest{
		Partition: partition,
		TaskId:    -1,
	}
	reply, err := stub.QueryTasksInfo(context.Background(), &request)
	if err != nil {
		panic("QueryJobsInPartition failed: " + err.Error())
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

	table.SetHeader([]string{"TaskId", "Type", "Status", "NodeIndex"})

	tableData := make([][]string, len(reply.TaskInfoList))
	for i := 0; i < len(reply.TaskInfoList); i++ {
		tableData = append(tableData, []string{
			strconv.FormatUint(uint64(reply.TaskInfoList[i].TaskId), 10),
			reply.TaskInfoList[i].Type.String(),
			reply.TaskInfoList[i].Status.String(),
			reply.TaskInfoList[i].CranedList})
	}

	table.AppendBulk(tableData)
	table.Render()
}

func Preparation() {
	config := util.ParseConfig()
	stub = util.GetStubToCtldByConfig(config)
}
