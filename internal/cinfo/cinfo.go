/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
)

// Define the flattened data structure
type FlattenedData struct {
	PartitionName   string
	Avail           string
	CranedListRegex string
	ResourceState   string
	ControlState    string
	CranedListCount uint64
}

// Flatten the nested structure into a one-dimensional array
func FlattenReplyData(reply *protos.QueryClusterInfoReply) []FlattenedData {
	var flattened []FlattenedData
	for _, partitionCraned := range reply.Partitions {
		for _, commonCranedStateList := range partitionCraned.CranedLists {
			if commonCranedStateList.Count > 0 {
				flattened = append(flattened, FlattenedData{
					PartitionName:   partitionCraned.Name,
					Avail:           strings.ToLower(partitionCraned.State.String()[10:]),
					CranedListRegex: commonCranedStateList.CranedListRegex,
					ResourceState:   strings.ToLower(commonCranedStateList.ResourceState.String()[6:]),
					ControlState:    strings.ToLower(commonCranedStateList.ControlState.String()[6:]),
					CranedListCount: uint64(commonCranedStateList.Count),
				})
			}
		}
	}
	return flattened
}

type FieldProcessor struct {
	header  string
	process func(flattened []FlattenedData, tableOutputCell [][]string)
}

var fieldMap = map[string]FieldProcessor{
	"p":         {"Partition", ProcessPartition},
	"partition": {"Partition", ProcessPartition},
	"a":         {"Avail", ProcessAvail},
	"avail":     {"Avail", ProcessAvail},
	"n":         {"Nodes", ProcessNodes},
	"nodes":     {"Nodes", ProcessNodes},
	"s":         {"State", ProcessState},
	"state":     {"State", ProcessState},
	"l":         {"NodeList", ProcessNodeList},
	"nodelist":  {"NodeList", ProcessNodeList},
}

// / Partition
func ProcessPartition(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], data.PartitionName)
	}
}

// Avail
func ProcessAvail(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], data.Avail)
	}
}

// Nodes
func ProcessNodes(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], strconv.FormatUint(data.CranedListCount, 10))
	}
}

// State
func ProcessState(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		stateStr := data.ResourceState
		if data.ControlState != "none" {
			stateStr += "(" + data.ControlState + ")"
		}
		tableOutputCell[idx] = append(tableOutputCell[idx], stateStr)
	}
}

// NodeList
func ProcessNodeList(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], data.CranedListRegex)
	}
}

func FormatData(reply *protos.QueryClusterInfoReply) (header []string, tableData [][]string) {
	re := regexp.MustCompile(`%(\.\d+)?([a-zA-Z]+)`)
	specifiers := re.FindAllStringSubmatchIndex(FlagFormat, -1)
	if specifiers == nil {
		log.Errorln("Invalid format specifier.")
		os.Exit(util.ErrorInvalidFormat)
	}

	tableOutputWidth := make([]int, 0, len(specifiers))
	tableOutputHeader := make([]string, 0, len(specifiers))
	flattened := FlattenReplyData(reply)
	tableLen := len(flattened)
	tableOutputCell := make([][]string, tableLen)

	// Get the prefix of the format string
	if specifiers[0][0] != 0 {
		prefix := FlagFormat[0:specifiers[0][0]]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, prefix)
		for j := 0; j < tableLen; j++ {
			tableOutputCell[j] = append(tableOutputCell[j], prefix)
		}
	}

	for i, spec := range specifiers {
		// Get the padding string between specifiers
		if i > 0 && spec[0]-specifiers[i-1][1] > 0 {
			padding := FlagFormat[specifiers[i-1][1]:spec[0]]
			tableOutputWidth = append(tableOutputWidth, -1)
			tableOutputHeader = append(tableOutputHeader, padding)
			for j := 0; j < tableLen; j++ {
				tableOutputCell[j] = append(tableOutputCell[j], padding)
			}
		}
		// Parse width specifier
		if spec[2] == -1 {
			// w/o width specifier
			tableOutputWidth = append(tableOutputWidth, -1)
		} else {
			// with width specifier
			width, err := strconv.ParseUint(FlagFormat[spec[2]+1:spec[3]], 10, 32)
			if err != nil {
				log.Errorln("Invalid width specifier.")
				os.Exit(util.ErrorInvalidFormat)
			}
			tableOutputWidth = append(tableOutputWidth, int(width))
		}

		// Parse format specifier
		field := FlagFormat[spec[4]:spec[5]]
		if len(field) > 1 {
			field = strings.ToLower(field)
		}

		if processor, exists := fieldMap[field]; exists {
			tableOutputHeader = append(tableOutputHeader, strings.ToUpper(processor.header))
			processor.process(flattened, tableOutputCell)
		} else {
			log.Errorf("Invalid format specifier or string: %s, string unfold case insensitive, reference:\n"+
				"p/Partition, a/Avail, n/Nodes, s/State, l/NodeList.", field)
			os.Exit(util.ErrorInvalidFormat)
		}
	}
	// Get the suffix of the format string
	if len(FlagFormat)-specifiers[len(specifiers)-1][1] > 0 {
		suffix := FlagFormat[specifiers[len(specifiers)-1][1]:]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, suffix)
		for j := 0; j < tableLen; j++ {
			tableOutputCell[j] = append(tableOutputCell[j], suffix)
		}
	}
	return util.FormatTable(tableOutputWidth, tableOutputHeader, tableOutputCell)
}

func Query() util.CraneCmdError {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)

	var resourceStateList []protos.CranedResourceState
	var controlStateList []protos.CranedControlState
	for i := 0; i < len(FlagFilterCranedStates); i++ {
		switch strings.ToLower(FlagFilterCranedStates[i]) {
		case "idle":
			resourceStateList = append(resourceStateList, protos.CranedResourceState_CRANE_IDLE)
		case "mix":
			resourceStateList = append(resourceStateList, protos.CranedResourceState_CRANE_MIX)
		case "alloc":
			resourceStateList = append(resourceStateList, protos.CranedResourceState_CRANE_ALLOC)
		case "down":
			resourceStateList = append(resourceStateList, protos.CranedResourceState_CRANE_DOWN)
		case "none":
			controlStateList = append(controlStateList, protos.CranedControlState_CRANE_NONE)
		case "drain":
			controlStateList = append(controlStateList, protos.CranedControlState_CRANE_DRAIN)
		default:
			log.Errorf("Invalid state given: %s.\n", FlagFilterCranedStates[i])
			return util.ErrorCmdArg
		}
	}
	if len(resourceStateList) == 0 {
		if FlagFilterRespondingOnly {
			resourceStateList = append(resourceStateList, protos.CranedResourceState_CRANE_IDLE, protos.CranedResourceState_CRANE_MIX, protos.CranedResourceState_CRANE_ALLOC)
		} else if FlagFilterDownOnly {
			resourceStateList = append(resourceStateList, protos.CranedResourceState_CRANE_DOWN)
		} else {
			resourceStateList = append(resourceStateList, protos.CranedResourceState_CRANE_IDLE, protos.CranedResourceState_CRANE_MIX, protos.CranedResourceState_CRANE_ALLOC, protos.CranedResourceState_CRANE_DOWN)
		}
	}
	if len(controlStateList) == 0 {
		controlStateList = append(controlStateList, protos.CranedControlState_CRANE_NONE, protos.CranedControlState_CRANE_DRAIN)
	}

	var nodeList []string
	if len(FlagFilterNodes) != 0 {
		for _, node := range FlagFilterNodes {
			if node == "" {
				log.Warn("Empty node name is ignored.")
				continue
			}
			nodeList = append(nodeList, node)
		}
	}

	var partList []string
	if len(FlagFilterPartitions) != 0 {
		for _, part := range FlagFilterPartitions {
			if part == "" {
				log.Warn("Empty partition name is ignored.")
				continue
			}
			partList = append(partList, part)
		}
	}

	req := &protos.QueryClusterInfoRequest{
		FilterPartitions:           partList,
		FilterNodes:                nodeList,
		FilterCranedResourceStates: resourceStateList,
		FilterCranedControlStates:  controlStateList,
	}

	reply, err := stub.QueryClusterInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query cluster information")
		id := util.QueryLeaderFromCtld(config)
		if id >= 0 {
			util.UpdateLeaderIdToFile(id)
			log.Printf("Leader ID was changed to %d, please try again!\n", id)
		} else {
			log.Errorln("Failed to query current leader ID, broken backend.")
		}
		return util.ErrorNetwork
	}
	if !reply.GetOk() {
		if reply.GetCurLeaderId() >= 0 {
			log.Printf("Leader id was changed to %d, caching the value, please try again!\n", reply.GetCurLeaderId())
			util.UpdateLeaderIdToFile(int(reply.GetCurLeaderId()))
		} else if reply.GetCurLeaderId() == -1 {
			log.Errorf("Leader id equals -1")
		}
		return util.ErrorBackend
	}

	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		if reply.GetOk() {
			return util.ErrorSuccess
		} else {
			return util.ErrorBackend
		}
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
	header := []string{"PARTITION", "AVAIL", "NODES", "STATE", "NODELIST"}
	var tableData [][]string
	for _, partitionCraned := range reply.Partitions {
		for _, commonCranedStateList := range partitionCraned.CranedLists {
			if commonCranedStateList.Count > 0 {
				stateStr := strings.ToLower(commonCranedStateList.ResourceState.String()[6:])
				if commonCranedStateList.ControlState != protos.CranedControlState_CRANE_NONE {
					stateStr += "(" + strings.ToLower(commonCranedStateList.ControlState.String()[6:]) + ")"
				}
				tableData = append(tableData, []string{
					partitionCraned.Name,
					strings.ToLower(partitionCraned.State.String()[10:]),
					strconv.FormatUint(uint64(commonCranedStateList.Count), 10),
					stateStr,
					commonCranedStateList.CranedListRegex,
				})
			}
		}
	}

	if FlagFormat != "" {
		header, tableData = FormatData(reply)
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

	if len(nodeList) != 0 {
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
		requestedNodes_, _ := util.ParseHostList(strings.Join(nodeList, ","))

		foundedNodes := make(map[string]bool)
		for _, node := range replyNodes_ {
			foundedNodes[node] = true
		}

		var redList []string
		for _, node := range requestedNodes_ {
			if _, exist := foundedNodes[node]; !exist {
				redList = append(redList, node)
			}
		}

		if len(redList) > 0 {
			log.Infof("Requested nodes do not exist or do not meet the given filter condition: %s.", util.HostNameListToStr(redList))
		}
	}
	return util.ErrorSuccess
}

func loopedQuery(iterate uint64) util.CraneCmdError {
	interval, err := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	if err != nil {
		log.Error(err)
		return util.ErrorCmdArg
	}
	for {
		fmt.Println(time.Now().String()[0:19])
		err := Query()
		if err != util.ErrorSuccess {
			return err
		}
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
