/**
 * Copyright (c) 2023 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * CraneSched is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of
 * the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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

func cinfoFunc() util.CraneCmdError {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)

	req := &protos.QueryClusterInfoRequest{
		FilterPartitions: FlagFilterPartitions,
		FilterNodes:      FlagFilterNodes,
	}

	var stateList []protos.CranedState
	if len(FlagFilterCranedStates) != 0 {
		for i := 0; i < len(FlagFilterCranedStates); i++ {
			switch strings.ToLower(FlagFilterCranedStates[i]) {
			case "idle":
				stateList = append(stateList, protos.CranedState_CRANE_IDLE)
			case "mix":
				stateList = append(stateList, protos.CranedState_CRANE_MIX)
			case "alloc":
				stateList = append(stateList, protos.CranedState_CRANE_ALLOC)
			case "down":
				stateList = append(stateList, protos.CranedState_CRANE_DOWN)
			case "drain":
				stateList = append(stateList, protos.CranedState_CRANE_DRAIN)
			default:
				log.Errorf("Invalid state given: %s\n", FlagFilterCranedStates[i])
				return util.ErrorCmdArg
			}
		}
	} else if FlagFilterRespondingOnly {
		stateList = append(stateList, protos.CranedState_CRANE_IDLE, protos.CranedState_CRANE_MIX, protos.CranedState_CRANE_ALLOC)
	} else if FlagFilterDownOnly {
		stateList = append(stateList, protos.CranedState_CRANE_DOWN)
	} else {
		stateList = append(stateList, protos.CranedState_CRANE_IDLE, protos.CranedState_CRANE_MIX, protos.CranedState_CRANE_ALLOC, protos.CranedState_CRANE_DOWN, protos.CranedState_CRANE_DRAIN)
	}

	req.FilterCranedStates = stateList

	reply, err := stub.QueryClusterInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query cluster information")
		return util.ErrorNetwork
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)
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
		fmt.Println("No partition is available.")
	} else {
		table.Render()
	}
	if len(FlagFilterNodes) != 0 {
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
		requestedNodes_, _ := util.ParseHostList(strings.Join(FlagFilterNodes, ","))

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
			println("The following nodes do not exist: " + util.HostNameListToStr(redList))
		}
	}
	return util.ErrorSuccess
}

func loopedQuery(iterate uint64) util.CraneCmdError {
	interval, _ := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	for {
		fmt.Println(time.Now().String()[0:19])
		err := cinfoFunc()
		if err != util.ErrorSuccess {
			return err
		}
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
