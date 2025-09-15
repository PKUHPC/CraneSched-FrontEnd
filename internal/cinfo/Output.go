package cinfo  

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
)

func GetInvalidMeg(partitionCraned *protos.TrimmedPartitionInfo) ([]string) {
	return []string {
		partitionCraned.Name,
		strings.ToLower(partitionCraned.State.String()[10:]),
		"0",
		"n/a",
		"",
	}
}

func BuildStateString(cranedList *protos.TrimmedPartitionInfo_TrimmedCranedInfo) string {
    stateStr := strings.ToLower(cranedList.ResourceState.String()[6:])

    if cranedList.ControlState != protos.CranedControlState_CRANE_NONE {
        controlState := strings.ToLower(cranedList.ControlState.String()[6:])
        stateStr += "(" + controlState + ")"
    }

    powerStateSuffix := "[" + strings.ToLower(cranedList.PowerState.String()[6:]) + "]"
    if cranedList.ResourceState == protos.CranedResourceState_CRANE_DOWN &&
         (cranedList.PowerState == protos.CranedPowerState_CRANE_POWER_IDLE ||
          cranedList.PowerState == protos.CranedPowerState_CRANE_POWER_ACTIVE) {
        powerStateSuffix = "[failed]"
    }
    stateStr += powerStateSuffix

    return stateStr
}

func CreateValidPartitionRow(partition *protos.TrimmedPartitionInfo, cranedList *protos.TrimmedPartitionInfo_TrimmedCranedInfo) []string {
    stateStr := BuildStateString(cranedList)
    return []string{
        partition.Name,
        strings.ToLower(partition.State.String()[10:]),
        strconv.FormatUint(uint64(cranedList.Count), 10),
        stateStr,
        cranedList.CranedListRegex,
    }
}

func PartitionDeal(partitionCraned *protos.TrimmedPartitionInfo,tableData *[][]string) bool {
	hasValidCraned := false
    for _, cranedList := range partitionCraned.CranedLists {
        if cranedList.Count > 0 {
            hasValidCraned = true
            *tableData = append(*tableData, CreateValidPartitionRow(partitionCraned, cranedList))
        }
    }
    return hasValidCraned
}

func FindtableDataByReply(reply *protos.QueryClusterInfoReply) ([][]string) {
	var partitionInValid [][]string
	var tableData [][]string

	for _,partitionCraned := range reply.Partitions {
		if hasValid := PartitionDeal(partitionCraned,&tableData); !hasValid {
			partitionInValid = append(partitionInValid,GetInvalidMeg(partitionCraned))
		}
	}

	tableData = append(tableData, partitionInValid...)
	return tableData
}

func FillTable(reply *protos.QueryClusterInfoReply ,table *tablewriter.Table) error{
	header := []string{"PARTITION", "AVAIL", "NODES", "STATE", "NODELIST"}
	var err error
	tableData := FindtableDataByReply(reply)
	if FlagFormat != "" {
		header, tableData, err = FormatData(reply)
		if err != nil {
			return err
		}
		table.SetTablePadding("")
		table.SetAutoFormatHeaders(false)
	}

	table.AppendBulk(tableData)
	if !FlagNoHeader {
		table.SetHeader(header)
	}
	if len(tableData) == 0 {
		log.Info("No matching partitions were found for the given filter.")
	} else {
		table.Render()
	}

	return nil
}

func GetFoundedNodes(reply *protos.QueryClusterInfoReply) map[string]bool {
	replyNodes := ""
	for _, partitionCraned := range reply.Partitions {
		for _, commonCranedStateList := range partitionCraned.CranedLists {
			if commonCranedStateList.Count > 0 {
				if replyNodes != "" {
					replyNodes += ","
				}
				replyNodes += commonCranedStateList.CranedListRegex
			}
		}
	}
	replyNodes_, _ := util.ParseHostList(replyNodes)

	foundedNodes := make(map[string]bool)
	for _, node := range replyNodes_ {
		foundedNodes[node] = true
	}
	return foundedNodes
}

func GetNodeList() []string {
	var nodeList []string
	for _, node := range FlagFilterNodes {
		if node == "" {
			log.Warn("Empty node name is ignored.")
			continue
		}
		nodeList = append(nodeList, node)
	}
	return nodeList
}

func ExtraDealNodeList(reply *protos.QueryClusterInfoReply) {
	var redList []string
	requestedNodes_, _ := util.ParseHostList(strings.Join(GetNodeList(), ","))
	for _, node := range requestedNodes_ {
		if _, exist := GetFoundedNodes(reply)[node]; !exist {
			redList = append(redList, node)
		}
	}
	if len(redList) > 0 {
		log.Infof("Requested nodes do not exist or do not meet the given filter condition: %s.",
			util.HostNameListToStr(redList))
	}
}

func QueryTableOutput(reply *protos.QueryClusterInfoReply) error {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)

	FillTable(reply,table)

	if len(FlagFilterNodes) != 0 {
		ExtraDealNodeList(reply)
	}
	return nil
}

func JsonOutput(reply * protos.QueryClusterInfoReply) error{
	fmt.Println(util.FmtJson.FormatReply(reply))
	if reply.GetOk() {
		return nil
	} else {
		return &util.CraneError{Code: util.ErrorBackend}
	}
}
