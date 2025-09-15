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

package creport

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"os"
	//"os/user"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	//log "github.com/sirupsen/logrus"

	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	stub protos.CraneCtldClient
)

func QueryAccountUserSummaryItem() error {
	req := &protos.QueryAccountUserSummaryItemRequest{
		Account:  FlagFilterAccounts,
		Username: FlagFilterUsers,
	}
	var start_time, end_time time.Time
	var err error
	start_time, err = util.ParseTime(FlagFilterStartTime)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Failed to parse the StartTime filter: %s.", err),
		}
	}
	end_time, err = util.ParseTime(FlagFilterEndTime)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Failed to parse the EndTime filter: %s.", err),
		}
	}
	if !util.CheckCreportOutType(FlagOutType) {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid argument: invalid: --time/-t"),
		}
	}
	req.StartTime = timestamppb.New(start_time)
	req.EndTime = timestamppb.New(end_time)

	rpcStart := time.Now()
	reply, err := stub.QueryAccountUserSummaryItem(context.Background(), req)
	rpcElapsed := time.Since(rpcStart)
	fmt.Printf("[QueryAccountUserSummaryItem] QueryAccountUserSummaryItem RPC used %d ms\n", rpcElapsed.Milliseconds())
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query job info")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	for _, item := range reply.Items {
		fmt.Printf("Account: %s, Username: %s, CPU Time: %d, CPU Alloc: %d, job_count: %d\n",
			item.Account, item.Username, item.TotalCpuTime, item.TotalCpuAlloc, item.TotalCount)
	}

	PrintAccountUserList(reply.Items, reply.GetCluster(), start_time, end_time, FlagOutType)

	return nil
}

func PrintAccountUserList(accountUserList []*protos.AccountUserSummaryItem, cluster string, startTime, endTime time.Time, outType string) {
	if len(accountUserList) == 0 {
		return
	}

	sort.Slice(accountUserList, func(i, j int) bool {
		if accountUserList[i].Account < accountUserList[j].Account {
			return true
		}
		if accountUserList[i].Account > accountUserList[j].Account {
			return false
		}
		return accountUserList[i].Username < accountUserList[j].Username
	})

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	fmt.Println(strings.Repeat("-", 100))
	if totalSecs > 0 {
		fmt.Printf("Cluster/Account/User Utilization %s - %s (%d secs)\n",
			startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}

	util.ReportUsageType(outType)

	var divisor int64
	switch outType {
	case "Seconds":
		divisor = 1
	case "Minutes":
		divisor = 60
	case "Hours":
		divisor = 3600
	default:
		divisor = 1
	}

	header := []string{"Cluster", "Account", "User", "Proper_name", "Used", "Energy"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(accountUserList))

	var notFoundUsers []string
	for _, item := range accountUserList {
		// usr, err := user.Lookup(item.Username)
		// if err != nil {
		// 	notFoundUsers = append(notFoundUsers, fmt.Sprintf("User %s not found: %v", item.Username, err))
		// 	continue
		// }
		tableData = append(tableData, []string{
			cluster,
			item.Account,
			item.Username,
			item.Username,//usr.Name,
			strconv.FormatInt(item.TotalCpuTime/divisor, 10),
			"0",
		})
	}
	table.AppendBulk(tableData)
	table.Render()

	for _, msg := range notFoundUsers {
		fmt.Println(msg)
	}
}
