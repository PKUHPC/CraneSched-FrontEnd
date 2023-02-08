package cacct

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

func QueryJob() {
	request := protos.QueryTasksInfoRequest{
		NumLimit: 100,
		TaskId:   -1,
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

	table.SetHeader([]string{"TaskId", "TaskName", "Partition", "Account", "AllocCPUs", "State", "ExitCode"})

	tableData := make([][]string, len(reply.TaskInfoList))
	for i := 0; i < len(reply.TaskInfoList); i++ {
		tableData = append(tableData, []string{
			strconv.FormatUint(uint64(reply.TaskInfoList[i].TaskId), 10),
			reply.TaskInfoList[i].Name,
			reply.TaskInfoList[i].Partition,
			reply.TaskInfoList[i].Account,
			strconv.FormatFloat(reply.TaskInfoList[i].Alloc_CPUs, 'f', 2, 64),
			reply.TaskInfoList[i].Status.String(),
			reply.TaskInfoList[i].ExitCode})
	}

	table.AppendBulk(tableData)
	table.Render()
}

func Preparation() {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)
}