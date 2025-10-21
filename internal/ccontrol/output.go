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

package ccontrol

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func formatDeviceMap(data *protos.DeviceMap) string {
	if data == nil {
		return "None"
	}
	var kvStrings []string
	for deviceName, typeCountMap := range data.NameTypeMap {
		var typeCountPairs []string
		for deviceType, count := range typeCountMap.TypeCountMap {
			if count != 0 {
				typeCountPairs = append(typeCountPairs, fmt.Sprintf("%s:%d", deviceType, count))
			}
		}
		if typeCountMap.Total != 0 {
			typeCountPairs = append(typeCountPairs, strconv.FormatUint(typeCountMap.Total, 10))
		}
		for _, typeCountPair := range typeCountPairs {
			kvStrings = append(kvStrings, fmt.Sprintf("%s:%s", deviceName, typeCountPair))
		}
	}
	if len(kvStrings) == 0 {
		return "None"
	}
	kvString := strings.Join(kvStrings, ", ")

	return kvString
}
func formatDedicatedResource(data *protos.DedicatedResourceInNode) string {
	if data == nil {
		return "None"
	}

	var kvStrings []string
	for deviceName, typeCountMap := range data.NameTypeMap {
		var typeCountPairs []string
		for deviceType, slots := range typeCountMap.TypeSlotsMap {
			slotsSize := len(slots.Slots)
			if slotsSize != 0 {
				typeCountPairs = append(typeCountPairs, fmt.Sprintf("%s:%d", deviceType, slotsSize))
			}
		}
		for _, typeCountPair := range typeCountPairs {
			kvStrings = append(kvStrings, fmt.Sprintf("%s:%s", deviceName, typeCountPair))
		}
	}
	if len(kvStrings) == 0 {
		return "None"
	}
	kvString := strings.Join(kvStrings, ", ")

	return kvString
}
func formatAllowedAccounts(allowedAccounts []string) string {
	if len(allowedAccounts) == 0 {
		return "ALL"
	}
	return strings.Join(allowedAccounts, ",")
}
func formatDeniedAccounts(deniedAccounts []string) string {
	if len(deniedAccounts) == 0 {
		return "None"
	}

	return strings.Join(deniedAccounts, ",")
}

func getCranedNodes(nodeName string) ([]*protos.CranedInfo, error) {
	req := &protos.QueryCranedInfoRequest{CranedName: nodeName}
	reply, err := stub.QueryCranedInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show nodes")
		return nil, &util.CraneError{Code: util.ErrorNetwork}
	}
	return reply.CranedInfoList, nil
}

func handleNodesEmptyResult(nodeName string, queryAll bool) error {
	if queryAll {
		fmt.Println("No node is available.")
		return nil
	}
	return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Node %s not found.", nodeName))
}

func outputNodes(nodes []*protos.CranedInfo) error {
	for _, node := range nodes {
		if err := printNodeDetails(node); err != nil {
			return err
		}
	}
	return nil
}

func printNodeDetails(node *protos.CranedInfo) error {
	stateStr := formatNodeState(node)
	cranedVersion := "unknown"
	if len(node.CranedVersion) > 0 {
		cranedVersion = node.CranedVersion
	}
	cranedOs := "unknown"
	if len(node.SystemDesc) > 2 {
		cranedOs = node.SystemDesc
	}
	timeInfo := formatNodeTimes(node)
	cpuInfo := formatCpuInfo(node)
	memInfo := formatMemInfo(node)
	gresInfo := formatGresInfo(node)

	fmt.Printf(
		"NodeName=%v State=%v %s\n"+
			"\t%s\n"+
			"\t%s\n"+
			"\tPartition=%s RunningJob=%d Version=%s\n"+
			"\tOs=%s\n"+
			"\tBootTime=%s CranedStartTime=%s\n"+
			"\tLastBusyTime=%s\n",
		node.Hostname, stateStr, cpuInfo,
		memInfo,
		gresInfo,
		strings.Join(node.PartitionNames, ","), node.RunningTaskNum, cranedVersion,
		cranedOs,
		timeInfo.bootTime, timeInfo.startTime, timeInfo.lastBusyTime,
	)

	return nil
}

type nodeTimes struct {
	bootTime     string
	startTime    string
	lastBusyTime string
}

func formatNodeTimes(node *protos.CranedInfo) nodeTimes {
	formatTime := func(t *timestamppb.Timestamp) string {
		if t == nil || t.AsTime().Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
			return "unknown"
		}
		return t.AsTime().In(time.Local).Format("2006-01-02 15:04:05")
	}
	return nodeTimes{
		bootTime:     formatTime(node.SystemBootTime),
		startTime:    formatTime(node.CranedStartTime),
		lastBusyTime: formatTime(node.LastBusyTime),
	}
}

func formatNodeState(node *protos.CranedInfo) string {
	stateStr := strings.ToLower(node.ResourceState.String()[6:])
	if node.ControlState != protos.CranedControlState_CRANE_NONE {
		stateStr += "(" + strings.ToLower(node.ControlState.String()[6:]) + ")"
	}
	stateStr += "[" + strings.ToLower(node.PowerState.String()[6:]+"]")
	return stateStr
}

func formatCpuInfo(node *protos.CranedInfo) string {
	return fmt.Sprintf("CPU=%.2f AllocCPU=%.2f FreeCPU=%.2f",
		node.ResTotal.AllocatableResInNode.CpuCoreLimit,
		math.Abs(node.ResAlloc.AllocatableResInNode.CpuCoreLimit),
		math.Abs(node.ResAvail.AllocatableResInNode.CpuCoreLimit),
	)
}
func formatMemInfo(node *protos.CranedInfo) string {
	return fmt.Sprintf("RealMemory=%s AllocMem=%s FreeMem=%s",
		util.FormatMemToMB(node.ResTotal.AllocatableResInNode.MemoryLimitBytes),
		util.FormatMemToMB(node.ResAlloc.AllocatableResInNode.MemoryLimitBytes),
		util.FormatMemToMB(node.ResAvail.AllocatableResInNode.MemoryLimitBytes),
	)
}
func formatGresInfo(node *protos.CranedInfo) string {
	return fmt.Sprintf("Gres=%s AllocGres=%s FreeGres=%s",
		formatDedicatedResource(node.ResTotal.GetDedicatedResInNode()),
		formatDedicatedResource(node.ResAlloc.GetDedicatedResInNode()),
		formatDedicatedResource(node.ResAvail.GetDedicatedResInNode()),
	)
}

func ShowNodes(nodeName string, queryAll bool) error {
	nodes, err := getCranedNodes(nodeName)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return handleNodesEmptyResult(nodeName, queryAll)
	}
	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(&protos.QueryCranedInfoReply{
			CranedInfoList: nodes,
		}))
		return nil
	}
	return outputNodes(nodes)
}

func getPartitionInfo(partitionName string) ([]*protos.PartitionInfo, error) {
	req := &protos.QueryPartitionInfoRequest{PartitionName: partitionName}
	reply, err := stub.QueryPartitionInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show partitions")
		return nil, &util.CraneError{Code: util.ErrorNetwork}
	}
	return reply.PartitionInfoList, nil
}
func handleEmptyPartitionResult(partitionName string, queryAll bool) error {
	if queryAll {
		fmt.Println("No partition is available.")
		return nil
	}
	return util.NewCraneErr(util.ErrorBackend, fmt.Sprintf("Partition %s not found.", partitionName))
}

func outputPartitionJson(partitions []*protos.PartitionInfo) error {
	fmt.Println(util.FmtJson.FormatReply(&protos.QueryPartitionInfoReply{
		PartitionInfoList: partitions,
	}))
	return nil
}
func outputPartitions(partitions []*protos.PartitionInfo) error {
	for _, partition := range partitions {
		if err := printPartitionDetails(partition); err != nil {
			return err
		}
	}
	return nil
}

func printPartitionDetails(partition *protos.PartitionInfo) error {
	accountsInfo := formatAccountsInfo(partition)
	nodesInfo := fmt.Sprintf("TotalNodes=%d AliveNodes=%d", partition.TotalNodes, partition.AliveNodes)
	cpuInfo := formatCpuResources(partition)
	memInfo := formatMemoryResources(partition)
	gresInfo := formatGresResources(partition)

	memLimits := fmt.Sprintf("DefaultMemPerCPU=%s MaxMemPerCPU=%s",
		util.FormatMemToMB(partition.DefaultMemPerCpu),
		util.FormatMemToMB(partition.MaxMemPerCpu))
	fmt.Printf("PartitionName=%v State=%v\n"+
		"\t%s\n"+
		"\t%s\n"+
		"\t%s\n"+
		"\t%s\n"+
		"\t%s\n"+
		"\t%s\n"+
		"\tHostList=%v\n\n",
		partition.Name, partition.State.String()[10:],
		accountsInfo,
		nodesInfo,
		cpuInfo,
		memInfo,
		gresInfo,
		memLimits,
		partition.Hostlist)

	return nil
}

func formatAccountsInfo(partition *protos.PartitionInfo) string {
	allowed := formatAllowedAccounts(partition.AllowedAccounts)
	denied := formatDeniedAccounts(partition.DeniedAccounts)
	return fmt.Sprintf("AllowedAccounts=%s DeniedAccounts=%s", allowed, denied)
}

func formatCpuResources(partition *protos.PartitionInfo) string {
	total := math.Abs(partition.ResTotal.AllocatableRes.CpuCoreLimit)
	avail := math.Abs(partition.ResAvail.AllocatableRes.CpuCoreLimit)
	alloc := math.Abs(partition.ResAlloc.AllocatableRes.CpuCoreLimit)
	return fmt.Sprintf("TotalCPU=%.2f AvailCPU=%.2f AllocCPU=%.2f", total, avail, alloc)
}
func formatMemoryResources(partition *protos.PartitionInfo) string {
	total := util.FormatMemToMB(partition.ResTotal.AllocatableRes.MemoryLimitBytes)
	avail := util.FormatMemToMB(partition.ResAvail.AllocatableRes.MemoryLimitBytes)
	alloc := util.FormatMemToMB(partition.ResAlloc.AllocatableRes.MemoryLimitBytes)
	return fmt.Sprintf("TotalMem=%s AvailMem=%s AllocMem=%s", total, avail, alloc)
}
func formatGresResources(partition *protos.PartitionInfo) string {
	total := formatDeviceMap(partition.ResTotal.GetDeviceMap())
	avail := formatDeviceMap(partition.ResAvail.GetDeviceMap())
	alloc := formatDeviceMap(partition.ResAlloc.GetDeviceMap())
	return fmt.Sprintf("TotalGres=%s AvailGres=%s AllocGres=%s", total, avail, alloc)
}

func ShowPartitions(partitionName string, queryAll bool) error {
	partitions, err := getPartitionInfo(partitionName)
	if err != nil {
		return err
	}
	if len(partitions) == 0 {
		return handleEmptyPartitionResult(partitionName, queryAll)
	}
	if FlagJson {
		return outputPartitionJson(partitions)
	}
	return outputPartitions(partitions)
}
