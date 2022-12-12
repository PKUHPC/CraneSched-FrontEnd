package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	stub protos.CraneCtldClient
)

func DefaultQuery() {
	request := protos.QueryJobsInPartitionRequest{}
	reply, err := stub.QueryJobsInPartition(context.Background(), &request)
	if err != nil {
		panic("QueryJobsInPartition failed: " + err.Error())
	}

	table := tablewriter.NewWriter(os.Stdout)
	TableInit(table)
	TableDefaultPrint(table, reply)
}

func IterateQuery(iterate uint64) {
	iter, _ := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	for {
		fmt.Println(time.Now().String()[0:19])
		DefaultQuery()
		time.Sleep(time.Duration(iter.Nanoseconds()))
	}
}

func FilterQuery() {
	request := protos.QueryJobsInPartitionRequest{}

	if taskName != "" {
		///Query the specified task names
		nameList := strings.Split(taskName, ",")
		request.TaskNames = nameList
	} else if partition != "" {
		///Query the specified partitions
		partitionList := strings.Split(partition, ",")
		request.Partitions = partitionList
	} else if taskId != "" {
		///Query the specified task ids
		split := strings.Split(taskId, ",")
		var taskIdList []uint32
		for i := 0; i < len(split); i++ {
			task, err := strconv.ParseUint(split[i], 10, 32)
			if err != nil {
				fmt.Printf("Invalid task ID: %s\n", split[i])
				os.Exit(1)
			}
			taskIdList = append(taskIdList, uint32(task))
		}
		request.TaskIds = taskIdList
	} else if states != "" {
		///Query a specified status
		statesList := strings.Split(states, ",")
		var status []protos.TaskStatus
		for i := 0; i < len(statesList); i++ {
			switch statesList[i] {
			case "Pending", "pending", "PD":
				status = append(status, protos.TaskStatus_Pending)
			case "Running", "running", "R":
				status = append(status, protos.TaskStatus_Running)
			case "finished", "Finished":
				status = append(status, protos.TaskStatus_Finished)
			case "Failed", "failed":
				status = append(status, protos.TaskStatus_Failed)
			case "completing", "Completing":
				status = append(status, protos.TaskStatus_Completing)
			case "cancelled", "Cancelled":
				status = append(status, protos.TaskStatus_Cancelled)
			default:
				fmt.Printf("Invaid task status: %s\n", states)
				os.Exit(1)
			}
		}
		request.TaskStatus = status
	}

	reply, err := stub.QueryJobsInPartition(context.Background(), &request)
	if err != nil {
		panic("QueryJobsInPartition failed: " + err.Error())
	}

	if taskName != "" && len(reply.TaskMetas) == 0 {
		fmt.Printf("Invaid task name specified")
	} else if partition != "" && len(reply.TaskMetas) == 0 {
		fmt.Printf("Invaid partition specified")
	} else if taskId != "" && len(reply.TaskMetas) == 0 {
		fmt.Printf("Invaid task id specified")
	} else if states != "" && len(reply.TaskMetas) == 0 {
		fmt.Printf("Invaid states specified")
	} else {
		table := tablewriter.NewWriter(os.Stdout)
		TableInit(table)
		TableDefaultPrint(table, reply)
	}
}

func FormatQuery(format string) {
	request := protos.QueryJobsInPartitionRequest{}
	reply, err := stub.QueryJobsInPartition(context.Background(), &request)
	if err != nil {
		panic("QueryJobsInPartition failed: " + err.Error())
	}
	table := tablewriter.NewWriter(os.Stdout)
	TableInit(table)
	split := strings.Fields(format)
	var header []string
	tableData := make([][]string, len(reply.TaskMetas))
	reg := regexp.MustCompile(`^%\.?[0-9]*[NjTtiP]$`)
	regNum := regexp.MustCompile(`[0-9]+`)
	for i := 0; i < len(split); i++ {
		if reg.MatchString(split[i]) {
			switch split[i][len(split[i])-1:] {
			case "i": ///column TaskId
				var idStr []string
				for j := 0; j < len(reply.TaskMetas); j++ {
					idStr = append(idStr, strconv.FormatUint(uint64(reply.TaskIds[j]), 10))
				}
				header, tableData = SetColumWidth(regNum.FindString(split[i]), 15,
					tableData, header, "TaskId", idStr, reply)
			case "j": ///column TaskName
				header, tableData = SetColumWidth(regNum.FindString(split[i]), 20,
					tableData, header, "TaskName", reply.TaskNames, reply)
			case "T": ///column Type
				var typeStr []string
				for j := 0; j < len(reply.TaskMetas); j++ {
					typeStr = append(typeStr, reply.TaskMetas[j].Type.String())
				}
				header, tableData = SetColumWidth(regNum.FindString(split[i]), 15,
					tableData, header, "Type", typeStr, reply)
			case "t": ///column Status
				var statusStr []string
				for j := 0; j < len(reply.TaskMetas); j++ {
					statusStr = append(statusStr, reply.TaskStatus[j].String())
				}
				header, tableData = SetColumWidth(regNum.FindString(split[i]), 15,
					tableData, header, "Status", statusStr, reply)
			case "N": ///column NodeIndex
				header, tableData = SetColumWidth(regNum.FindString(split[i]), 50,
					tableData, header, "NodeIndex", reply.AllocatedCraneds, reply)
			case "P": ///column Partition
				header, tableData = SetColumWidth(regNum.FindString(split[i]), 15,
					tableData, header, "Partition", reply.TaskPartitions, reply)
			}
		} else {
			fmt.Printf("Unrecognized option: %s\n", split[i])
			os.Exit(1)
		}
	}
	if !noheader {
		table.SetHeader(header)
	}
	table.AppendBulk(tableData)
	table.Render()
}
func SetColumWidth(setWidth string, limit uint32, tableData [][]string, header []string,
	setHeader string, content []string, reply *protos.QueryJobsInPartitionReply) ([]string, [][]string) {
	if setWidth != "" {
		convInt64, _ := strconv.ParseUint(setWidth, 10, 32)
		colWidth := uint32(convInt64)
		if colWidth > limit {
			fmt.Println("The width of the " + setHeader +
				"column exceeds the limit, please enter a number between 1 and " +
				strconv.FormatUint(uint64(limit), 10) + ".")
			os.Exit(1)
		} else {
			header = append(header, setHeader)
			for j := 0; j < len(reply.TaskMetas); j++ {
				tableData[j] = append(tableData[j], FormatString(content[j], int(colWidth)))
			}
		}
	} else {
		header = append(header, setHeader)
		for j := 0; j < len(reply.TaskMetas); j++ {
			if uint32(len(content)) > limit {
				tableData[j] = append(tableData[j], FormatString(content[j], int(limit)))
			} else {
				tableData[j] = append(tableData[j], content[j])
			}
		}
	}
	return header, tableData
}

func FormatString(str string, setLen int) string {
	space := ""
	if len(str) <= setLen {
		for i := 0; i < (setLen - len(str)); i++ {
			space += " "
		}
		return str + space
	} else {
		return str[len(str)-setLen:]
	}
}

func TableInit(table *tablewriter.Table) {
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
}
func TableDefaultPrint(table *tablewriter.Table, reply *protos.QueryJobsInPartitionReply) {
	tableData := make([][]string, len(reply.TaskMetas))
	if !noheader {
		table.SetHeader([]string{"TaskId", "TaskName", "Type", "Status", "Partition", "NodeIndex"})
	}
	for i := 0; i < len(reply.TaskMetas); i++ {
		tableData = append(tableData, []string{
			strconv.FormatUint(uint64(reply.TaskIds[i]), 10),
			reply.TaskNames[i],
			reply.TaskMetas[i].Type.String(),
			reply.TaskStatus[i].String(),
			reply.TaskPartitions[i],
			reply.AllocatedCraneds[i],
		})
	}
	table.AppendBulk(tableData)
	table.Render()
}
func Init() {
	config := util.ParseConfig()

	serverAddr := fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("Cannot connect to CraneCtld: " + err.Error())
	}
	stub = protos.NewCraneCtldClient(conn)

}
