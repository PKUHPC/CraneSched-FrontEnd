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

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
)

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
