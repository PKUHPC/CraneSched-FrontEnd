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
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"time"
)

func cinfoFunc() {
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
				log.Fatalf("Invalid state given: %s\n", FlagFilterCranedStates[i])
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
		fmt.Printf("No partition is available.\n")
	} else {
		table.Render()
	}
}

func loopedQuery(iterate uint64) {
	interval, _ := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	for {
		fmt.Println(time.Now().String()[0:19])
		cinfoFunc()
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
