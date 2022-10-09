package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"strconv"
)

func Query(serverAddr string, partition string, findAll bool) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("Cannot connect to CraneCtld: " + err.Error())
	}

	stub := protos.NewCraneCtldClient(conn)

	request := protos.QueryJobsInPartitionRequest{
		Partition: partition,
		FindAll:   findAll,
	}
	reply, err := stub.QueryJobsInPartition(context.Background(), &request)
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

	tableData := make([][]string, len(reply.TaskMetas))
	for i := 0; i < len(reply.TaskMetas); i++ {
		tableData = append(tableData, []string{
			strconv.FormatUint(uint64(reply.TaskIds[i]), 10),
			reply.TaskMetas[i].Type.String(),
			reply.TaskStatus[i].String(),
			reply.AllocatedCraneds[i]})
	}

	table.AppendBulk(tableData)
	table.Render()
}
