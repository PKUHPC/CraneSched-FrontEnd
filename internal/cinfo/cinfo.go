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
	"strconv"
	"strings"
	"time"
	"regexp"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
)

func FormatData(reply *protos.QueryClusterInfoReply) (header []string, tableData [][]string) {
	re := regexp.MustCompile(`%(\.\d+)?([a-zA-Z]+)`)
	specifiers := re.FindAllStringSubmatchIndex(FlagFormat, -1)
	if specifiers == nil {
		log.Errorln("Invalid format specifier.")
		os.Exit(util.ErrorInvalidFormat)
	}
	tableOutputWidth := make([]int, 0, len(specifiers))
	tableOutputHeader := make([]string, 0, len(specifiers))
	tableLen := 0
	if len(reply.Partitions ) > 0 && reply.Partitions[0].CranedLists[0].Count > 0{
		tableLen = len(reply.Partitions) * int(reply.Partitions[0].CranedLists[0].Count)
	}
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
		header := ""
		field := FlagFormat[spec[4]:spec[5]]
		if len(field) > 1 {
			field = strings.ToLower(field)
		}
		switch field {
		// a-Account, c-AllocCPUs, e-ExitCode, j-JobId, n-JobName, P-Partition, t-State, u-Uid
		// l-TimeLimit, S-StartTime, E-EndTime, D-ElapsedTime s-SubmitTime, N-NodeNum, U-UserName q-Qos,
		// r-ReqNodes, x-ExcludeNodes, h-Held, p-Priority, L-NodeList, T-JobType, m-MemPerNode, R-Reason
		case "p", "partition":
			header = "Partition"
			// for j := 0; j < tableLen; j++ {
			// 	tableOutputCell[j] = append(tableOutputCell[j], reply.Partitions[j].Name)
			// }
			for i, partitionCraned := range reply.Partitions {
				for j, commonCranedStateList := range partitionCraned.CranedLists {
					cranedStateListCount := commonCranedStateList.Count
					if  cranedStateListCount > 0 {
						tableIdx := uint32(i) * cranedStateListCount + uint32(j)
						tableOutputCell[tableIdx] = append(tableOutputCell[tableIdx], partitionCraned.Name)
					}
				}
			}
		case "a", "avail":
			header = "Avail"
			for i, partitionCraned := range reply.Partitions {
				for j, commonCranedStateList := range partitionCraned.CranedLists {
					cranedStateListCount := commonCranedStateList.Count
					if  cranedStateListCount > 0 {
						tableIdx := uint32(i) * cranedStateListCount + uint32(j)
						tableOutputCell[tableIdx] = append(tableOutputCell[tableIdx],
							strings.ToLower(partitionCraned.State.String()[10:]))
					}
				}
			}
		case "n", "nodes":
			header = "Nodes"
			for i, partitionCraned := range reply.Partitions {
				for j, commonCranedStateList := range partitionCraned.CranedLists {
					cranedStateListCount := commonCranedStateList.Count
					if  cranedStateListCount > 0 {
						tableIdx := uint32(i) * cranedStateListCount + uint32(j)
						tableOutputCell[tableIdx] = append(tableOutputCell[tableIdx],
							strconv.FormatUint(uint64(commonCranedStateList.Count), 10))
					}
				}
			}
		case "s", "state":
			header = "State"
			for i, partitionCraned := range reply.Partitions {
				for j, commonCranedStateList := range partitionCraned.CranedLists {
					cranedStateListCount := commonCranedStateList.Count
					if cranedStateListCount > 0 {
						stateStr := strings.ToLower(commonCranedStateList.ResourceState.String()[6:])
						if commonCranedStateList.ControlState != protos.CranedControlState_CRANE_NONE {
							stateStr += "(" + strings.ToLower(commonCranedStateList.ControlState.String()[6:]) + ")"
						}
						tableIdx := uint32(i) * cranedStateListCount + uint32(j)
						tableOutputCell[tableIdx] = append(tableOutputCell[tableIdx],  stateStr)
					}
				}
			}
		case "l", "nodelist":
			header = "NodeList"
			for i, partitionCraned := range reply.Partitions {
				for j, commonCranedStateList := range partitionCraned.CranedLists {
					cranedStateListCount := commonCranedStateList.Count
					if cranedStateListCount > 0 {
						tableIdx := uint32(i) * cranedStateListCount + uint32(j)
						tableOutputCell[tableIdx] = append(tableOutputCell[tableIdx],  commonCranedStateList.CranedListRegex)
					}
				}
			}
		default:
			log.Errorln("Invalid format specifier or string, string unfold case insensitive, reference:\n" +		
			"p/Partition, a/Avail, n/Nodes, j/State, l/NodeList.")
			os.Exit(util.ErrorInvalidFormat)
		}
		tableOutputHeader = append(tableOutputHeader, strings.ToUpper(header))
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
		return util.ErrorNetwork
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
			log.Infof("Requested nodes do not exist or do not meet the given filter condition: %s.",
				util.HostNameListToStr(redList))
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
