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
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/user"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"time"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

// show Config
func PrintFlattenYAML(prefix string, m interface{}) {
	switch v := m.(type) {
	case map[string]interface{}:
		for key, value := range v {
			newPrefix := key
			if prefix != "" {
				newPrefix = prefix + "." + key
			}
			PrintFlattenYAML(newPrefix, value)
		}
	case []interface{}:
		for i, value := range v {
			PrintFlattenYAML(fmt.Sprintf("%s.%d", prefix, i), value)
		}
	default:
		fmt.Printf("%s = %v\n", prefix, v)
	}
}
func ShowConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Errorf("Failed to read configuration file: %s", err)
		return &util.CraneError{Code: util.ErrorCmdArg}
	}

	var config map[string]interface{}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Errorf("Failed to unmarshal yaml configuration file: %s", err)
		return &util.CraneError{Code: util.ErrorCmdArg}
	}
	if FlagJson {
		output, _ := json.Marshal(config)
		fmt.Println(string(output))
	} else {
		PrintFlattenYAML("", config)
	}

	return nil
}

// show Nodes
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

func getCranedNodesReply(nodeName string) (*protos.QueryCranedInfoReply, error) {
	req := &protos.QueryCranedInfoRequest{CranedName: nodeName}
	reply, err := stub.QueryCranedInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show nodes")
		return nil, &util.CraneError{Code: util.ErrorNetwork}
	}
	return reply, nil
}
func handleNodesEmptyResult(nodeName string, queryAll bool) error {
	if queryAll {
		fmt.Println("No node is available.")
		return nil
	}
	log.Errorf("Node %s not found.", nodeName)
	return &util.CraneError{Code: util.ErrorBackend}
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
	// time/cpu/mem/gres
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
	stateStr += "[" + strings.ToLower(node.PowerState.String()[6:]) + "]"
	return stateStr
}

// Cpu
func formatCpuInfo(node *protos.CranedInfo) string {
	return fmt.Sprintf("CPU=%.2f AllocCPU=%.2f FreeCPU=%.2f",
		node.ResTotal.AllocatableResInNode.CpuCoreLimit,
		math.Abs(node.ResAlloc.AllocatableResInNode.CpuCoreLimit),
		math.Abs(node.ResAvail.AllocatableResInNode.CpuCoreLimit),
	)
}

// Mem
func formatMemInfo(node *protos.CranedInfo) string {
	return fmt.Sprintf("RealMemory=%s AllocMem=%s FreeMem=%s",
		util.FormatMemToMB(node.ResTotal.AllocatableResInNode.MemoryLimitBytes),
		util.FormatMemToMB(node.ResAlloc.AllocatableResInNode.MemoryLimitBytes),
		util.FormatMemToMB(node.ResAvail.AllocatableResInNode.MemoryLimitBytes),
	)
}

// Gres
func formatGresInfo(node *protos.CranedInfo) string {
	return fmt.Sprintf("Gres=%s AllocGres=%s FreeGres=%s",
		formatDedicatedResource(node.ResTotal.GetDedicatedResInNode()),
		formatDedicatedResource(node.ResAlloc.GetDedicatedResInNode()),
		formatDedicatedResource(node.ResAvail.GetDedicatedResInNode()),
	)
}

func ShowNodes(nodeName string, queryAll bool) error {
	reply, err := getCranedNodesReply(nodeName)
	if err != nil {
		return err
	}
	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return nil
	}
	if len(reply.CranedInfoList) == 0 {
		return handleNodesEmptyResult(nodeName, queryAll)
	}
	return outputNodes(reply.CranedInfoList)
}

// show Partitions
func getPartitionInfoReply(partitionName string) (*protos.QueryPartitionInfoReply, error) {
	req := &protos.QueryPartitionInfoRequest{PartitionName: partitionName}
	reply, err := stub.QueryPartitionInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show partitions")
		return nil, &util.CraneError{Code: util.ErrorNetwork}
	}
	return reply, nil
}
func handleEmptyPartitionResult(partitionName string, queryAll bool) error {
	if queryAll {
		fmt.Println("No partition is available.")
		return nil
	}
	log.Errorf("Partition %s not found.", partitionName)
	return &util.CraneError{Code: util.ErrorBackend}
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
	reply, err := getPartitionInfoReply(partitionName)
	if err != nil {
		return err
	}
	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return nil
	}
	if len(reply.PartitionInfoList) == 0 {
		return handleEmptyPartitionResult(partitionName, queryAll)
	}
	return outputPartitions(reply.PartitionInfoList)
}

// show Reservation
func getReservationInfoReply(reservationName string) (*protos.QueryReservationInfoReply, error) {
	req := &protos.QueryReservationInfoRequest{
		Uid:             uint32(os.Getuid()),
		ReservationName: reservationName,
	}
	reply, err := stub.QueryReservationInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show reservations")
		return nil, &util.CraneError{Code: util.ErrorNetwork}
	}
	if !reply.GetOk() {
		log.Errorf("Failed to retrieve reservation info: %s", reply.GetReason())
		return nil, &util.CraneError{Code: util.ErrorBackend}
	}
	return reply, nil
}
func handleEmptyReservationResult(reservationName string, queryAll bool) error {
	if queryAll {
		fmt.Println("No reservation is available.")
		return nil
	}
	log.Errorf("Reservation %s not found.", reservationName)
	return &util.CraneError{Code: util.ErrorBackend}
}
func outputReservations(reservations []*protos.ReservationInfo) error {
	for _, res := range reservations {
		fmt.Println(formatReservationDetails(res))
	}
	return nil
}

// strings.Builder
func formatReservationDetails(res *protos.ReservationInfo) string {
	var buf strings.Builder

	buf.WriteString(fmt.Sprintf("ReservationName=%v\n", res.ReservationName))
	buf.WriteString(fmt.Sprintf("\tStartTime=%v Duration=%v\n",
		res.StartTime.AsTime().In(time.Local).Format("2006-01-02 15:04:05"),
		res.Duration.AsDuration().String()))
	buf.WriteString(formatReservationTarget(res))
	buf.WriteString(formatReservationAccounts(res))
	buf.WriteString(formatReservationResources(res))

	return buf.String()
}

func formatReservationTarget(res *protos.ReservationInfo) string {
	if res.Partition != "" && res.CranedRegex != "" {
		return fmt.Sprintf("\tPartition=%v CranedRegex=%v\n", res.Partition, res.CranedRegex)
	} else if res.Partition != "" {
		return fmt.Sprintf("\tPartition=%v\n", res.Partition)
	} else if res.CranedRegex != "" {
		return fmt.Sprintf("\tCranedRegex=%v\n", res.CranedRegex)
	}
	return ""
}
func formatReservationAccounts(res *protos.ReservationInfo) string {
	var buf strings.Builder
	if len(res.AllowedAccounts) > 0 {
		buf.WriteString(fmt.Sprintf("\tAllowedAccounts=%s\n", strings.Join(res.AllowedAccounts, ",")))
	}
	if len(res.DeniedAccounts) > 0 {
		buf.WriteString(fmt.Sprintf("\tDeniedAccounts=%s\n", strings.Join(res.DeniedAccounts, ",")))
	}
	if len(res.AllowedUsers) > 0 {
		buf.WriteString(fmt.Sprintf("\tAllowedUsers=%s\n", strings.Join(res.AllowedUsers, ",")))
	}
	if len(res.DeniedUsers) > 0 {
		buf.WriteString(fmt.Sprintf("\tDeniedUsers=%s\n", strings.Join(res.DeniedUsers, ",")))
	}
	return buf.String()
}
func formatReservationResources(res *protos.ReservationInfo) string {
	var buf strings.Builder
	// CPU
	buf.WriteString(fmt.Sprintf("\tTotalCPU=%.2f AvailCPU=%.2f AllocCPU=%.2f\n",
		math.Abs(res.ResTotal.AllocatableRes.CpuCoreLimit),
		math.Abs(res.ResAvail.AllocatableRes.CpuCoreLimit),
		math.Abs(res.ResAlloc.AllocatableRes.CpuCoreLimit)))

	// mem
	buf.WriteString(fmt.Sprintf("\tTotalMem=%s AvailMem=%s AllocMem=%s\n",
		util.FormatMemToMB(res.ResTotal.AllocatableRes.MemoryLimitBytes),
		util.FormatMemToMB(res.ResAvail.AllocatableRes.MemoryLimitBytes),
		util.FormatMemToMB(res.ResAlloc.AllocatableRes.MemoryLimitBytes)))

	// Gres
	buf.WriteString(fmt.Sprintf("\tTotalGres=%s AvailGres=%s AllocGres=%s\n",
		formatDeviceMap(res.ResTotal.GetDeviceMap()),
		formatDeviceMap(res.ResAvail.GetDeviceMap()),
		formatDeviceMap(res.ResAlloc.GetDeviceMap())))

	return buf.String()
}

func ShowReservations(reservationName string, queryAll bool) error {
	reply, err := getReservationInfoReply(reservationName)
	if err != nil {
		return err
	}
	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return nil
	}
	if len(reply.ReservationInfoList) == 0 {
		return handleEmptyReservationResult(reservationName, queryAll)
	}
	return outputReservations(reply.ReservationInfoList)
}

// show Jobs
func parseJobIds(jobIds string, queryAll bool) ([]uint32, error) {
	if queryAll {
		return nil, nil
	}
	jobIdList, err := util.ParseJobIdList(jobIds, ",")
	if err != nil {
		log.Errorf("Invalid job list specified: %s.", err)
		return nil, &util.CraneError{Code: util.ErrorCmdArg}
	}
	return jobIdList, nil
}

func getTaskInfoReply(jobIdList []uint32) (*protos.QueryTasksInfoReply, error) {
	req := &protos.QueryTasksInfoRequest{FilterTaskIds: jobIdList}
	reply, err := stub.QueryTasksInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to show jobs")
		return nil, &util.CraneError{Code: util.ErrorNetwork}
	}

	if !reply.GetOk() {
		log.Errorf("Failed to retrieve information for job %v", jobIdList)
		return nil, &util.CraneError{Code: util.ErrorBackend}
	}
	return reply, nil
}

func handleEmptyJobResult(jobIdList []uint32, queryAll bool) error {
	if queryAll {
		fmt.Println("No job is running.")
		return nil
	}

	jobIdListString := util.ConvertSliceToString(jobIdList, ", ")
	fmt.Printf("Job %s is not running.\n", jobIdListString)
	return nil
}

func outputJobs(tasks []*protos.TaskInfo, requestedIds []uint32) error {
	// Track if any job requested is not returned
	printed := make(map[uint32]bool)
	for _, task := range tasks {
		if err := printJobDetails(task); err != nil {
			return err
		}
		printed[task.TaskId] = true
	}
	return checkMissingJobs(requestedIds, printed)
}

func printJobDetails(task *protos.TaskInfo) error {
	// id/name
	fmt.Printf("JobId=%v JobName=%v\n", task.TaskId, task.Name)
	// user / group
	userInfo, err := getUserGroupInfo(task)
	if err != nil {
		log.Errorf("Failed to get user/group info: %s", err)
		return &util.CraneError{Code: util.ErrorGeneric}
	}
	fmt.Printf("\tUser=%s(%d) GroupId=%s(%d) Account=%v\n", userInfo.username, task.Uid, userInfo.groupname, task.Gid, task.Account)

	// time
	timeInfo := formatJobTimes(task)
	fmt.Printf("\tJobState=%v RunTime=%v TimeLimit=%s SubmitTime=%v\n"+
		"\tStartTime=%v EndTime=%v Partition=%v NodeList=%v ExecutionHost=%v\n",
		task.Status.String(), timeInfo.runTime, timeInfo.timeLimit, timeInfo.submitTime,
		timeInfo.startTime, timeInfo.endTime, task.Partition,
		formatHostNameStr(task.GetCranedList()),
		formatHostNameStr(util.HostNameListToStr(task.GetExecutionNode())))

	// cmd / work
	fmt.Printf("\tCmdLine=\"%v\" Workdir=%v\n", task.CmdLine, task.Cwd)
	// resource
	printResourceRequests(task)

	fmt.Printf("\tReqNodeList=%v ExecludeNodeList=%v\n"+
		"\tExclusive=%v Comment=%v\n",
		formatHostNameStr(util.HostNameListToStr(task.GetReqNodes())),
		formatHostNameStr(util.HostNameListToStr(task.GetExcludeNodes())),
		strconv.FormatBool(task.Exclusive), getJobExtraAttr(task.ExtraAttr, "comment"))

	//  mail
	printMailNotification(task.ExtraAttr)

	return nil
}

type userGroupInfo struct {
	username  string
	groupname string
}

func getUserGroupInfo(task *protos.TaskInfo) (userGroupInfo, error) {
	craneUser, err := user.LookupId(strconv.Itoa(int(task.Uid)))
	if err != nil {
		log.Errorf("Failed to get username for UID %d: %s", task.Uid, err)
		return userGroupInfo{}, &util.CraneError{Code: util.ErrorGeneric}
	}

	group, err := user.LookupGroupId(strconv.Itoa(int(task.Gid)))
	if err != nil {
		log.Errorf("Failed to get groupname for GID %d: %s", task.Gid, err)
		return userGroupInfo{}, &util.CraneError{Code: util.ErrorGeneric}
	}

	return userGroupInfo{
		username:  craneUser.Username,
		groupname: group.Name,
	}, nil
}

type jobTimeInfo struct {
	submitTime string
	startTime  string
	endTime    string
	runTime    string
	timeLimit  string
}

func GetElapsedTime(task *protos.TaskInfo) string {
	if task.Status == protos.TaskStatus_Running && task.ElapsedTime != nil {
		return util.SecondTimeFormat(task.ElapsedTime.Seconds)
	}
	return "unknown"
}

func formatJobTimes(task *protos.TaskInfo) jobTimeInfo {
	// formatTime for submit and start
	formatTime := func(t time.Time) string {
		if t.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
			return "unknown"
		}
		return t.In(time.Local).Format("2006-01-02 15:04:05")
	}
	// submit time
	submitTime := formatTime(task.SubmitTime.AsTime())
	// start time
	startTime := formatTime(task.StartTime.AsTime())
	// end time
	endTime := "unknown"
	timeEnd := task.EndTime.AsTime()
	if !timeEnd.Before(task.StartTime.AsTime()) && timeEnd.Unix() < util.MaxJobTimeStamp {
		endTime = timeEnd.In(time.Local).Format("2006-01-02 15:04:05")
	}
	// time limit
	timeLimitStr := "unlimited"
	if task.TimeLimit.Seconds < util.MaxJobTimeLimit {
		timeLimitStr = util.SecondTimeFormat(task.TimeLimit.Seconds)
	}
	// runTime
	// runTimeStr := "unknown"
	runTimeStr := GetElapsedTime(task)

	return jobTimeInfo{
		submitTime: submitTime,
		startTime:  startTime,
		endTime:    endTime,
		runTime:    runTimeStr,
		timeLimit:  timeLimitStr,
	}
}

func printResourceRequests(task *protos.TaskInfo) {
	// Priority / QoS
	fmt.Printf("\tPriority=%v Qos=%v CpusPerTask=%v MemPerNode=%v\n",
		task.Priority, task.Qos,
		task.ReqResView.AllocatableRes.CpuCoreLimit,
		util.FormatMemToMB(task.ReqResView.AllocatableRes.MemoryLimitBytes))
	// ReqRes
	fmt.Printf("\tReqRes:node=%d cpu=%.2f mem=%v gres=%s\n",
		task.NodeNum,
		task.ReqResView.AllocatableRes.CpuCoreLimit*float64(task.NodeNum),
		util.FormatMemToMB(task.ReqResView.AllocatableRes.MemoryLimitBytes*uint64(task.NodeNum)),
		formatDeviceMap(task.ReqResView.DeviceMap))
	// AllocRes
	if task.Status == protos.TaskStatus_Running {
		fmt.Printf("\tAllocRes:node=%d cpu=%.2f mem=%v gres=%s\n",
			task.NodeNum,
			task.AllocatedResView.AllocatableRes.CpuCoreLimit,
			util.FormatMemToMB(task.AllocatedResView.AllocatableRes.MemoryLimitBytes),
			formatDeviceMap(task.AllocatedResView.DeviceMap))
	}
}

func getJobExtraAttr(extraAttr, key string) string {
	if !gjson.Valid(extraAttr) {
		return ""
	}
	return gjson.Get(extraAttr, key).String()
}

func printMailNotification(extraAttr string) {
	mailUser := getJobExtraAttr(extraAttr, "mail.user")
	mailType := getJobExtraAttr(extraAttr, "mail.type")

	if mailUser != "" || mailType != "" {
		fmt.Printf("\tMailUser=%v MailType=%v\n", mailUser, mailType)
	}
}

func formatHostNameStr(hosts string) string {
	if hosts == "" {
		return "None"
	}
	return hosts
}

// If any job is requested but not returned, remind the user
func checkMissingJobs(requestedIds []uint32, printed map[uint32]bool) error {
	if len(requestedIds) == 0 {
		return nil
	}
	missingJobs := []uint32{}
	for _, id := range requestedIds {
		if !printed[id] {
			missingJobs = append(missingJobs, id)
		}
	}
	if len(missingJobs) > 0 {
		missingList := util.ConvertSliceToString(missingJobs, ", ")
		fmt.Printf("Job %s is not running.\n", missingList)
	}

	return nil
}
func ShowJobs(jobIds string, queryAll bool) error {
	jobIdList, err := parseJobIds(jobIds, queryAll)
	if err != nil {
		return err
	}
	reply, err := getTaskInfoReply(jobIdList)
	if err != nil {
		return err
	}
	if FlagJson {
		fmt.Println(util.FmtJson.FormatReply(reply))
		return nil
	}
	if len(reply.TaskInfoList) == 0 {
		return handleEmptyJobResult(jobIdList, queryAll)
	}
	return outputJobs(reply.TaskInfoList, jobIdList)
}
