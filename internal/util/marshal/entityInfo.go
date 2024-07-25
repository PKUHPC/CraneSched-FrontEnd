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

package marshal

import "CraneFrontEnd/generated/protos"

type TasksInfo struct {
	Tasks []Task `json:"tasks"`
}

func (data *TasksInfo) Load(reply *protos.QueryTasksInfoReply) {
	for _, info := range reply.TaskInfoList {
		dat := Task{}
		dat.Load(info)
		data.Tasks = append(data.Tasks, dat)
	}
}

type Task struct {
	Name             string  `json:"name"`
	TaskId           uint32  `json:"task_id"`
	Command          string  `json:"command"`
	Account          string  `json:"account"`
	UserName         string  `json:"user_name"`
	UserId           uint32  `json:"user_id"`
	GroupId          uint32  `json:"group_id"`
	Priority         uint32  `json:"priority"`
	Qos              string  `json:"qos"`
	Partition        string  `json:"partition"`
	NodeCount        uint32  `json:"node_count"`
	Cpus             float64 `json:"cpus"`
	SubmitTIme       int64   `json:"submit_time"`
	StartTime        int64   `json:"start_time"`
	EndTime          int64   `json:"end_time"`
	ElapsedTime      *int64  `json:"elapsed_time"`
	ExitCode         uint32  `json:"exit_code"`
	WorkingDirectory string  `json:"working_directory"`
	State            string  `json:"state"`

	NodeList      *string `json:"node_list"`
	PendingReason *string `json:"pending_reason"`
}

func (t *Task) Load(i *protos.TaskInfo) {
	t.Name = i.Name
	t.TaskId = i.TaskId
	t.Command = i.CmdLine
	t.Account = i.Account
	t.UserName = i.Username
	t.UserId = i.Uid
	t.GroupId = i.Gid
	t.Priority = i.Priority
	t.Qos = i.Qos
	t.Partition = i.Partition
	t.NodeCount = i.NodeNum
	t.Cpus = i.AllocCpu
	t.SubmitTIme = i.SubmitTime.Seconds
	t.StartTime = i.StartTime.Seconds
	t.EndTime = i.EndTime.Seconds
	if i.Status == protos.TaskStatus_Running {
		t.ElapsedTime = &i.ElapsedTime.Seconds
	}
	t.ExitCode = i.ExitCode
	t.WorkingDirectory = i.Cwd
	t.State = i.Status.String()

	if i.Status == protos.TaskStatus_Pending {
		reason := i.GetPendingReason()
		t.PendingReason = &reason
	} else {
		nodelist := i.GetCranedList()
		t.NodeList = &nodelist
	}
}

type PartitionInfo struct {
	Partitions []Partition `json:"partitions"`
}

func (data *PartitionInfo) Load(reply *protos.QueryPartitionInfoReply) {
	for _, info := range reply.PartitionInfo {
		dat := Partition{}
		dat.Load(info)
		data.Partitions = append(data.Partitions, dat)
	}
}

type Partition struct {
	Name     string             `json:"name"`
	Hostlist string             `json:"host_list"`
	Nodes    map[string]uint32  `json:"nodes"`
	Cpu      map[string]float64 `json:"cpu"`
	Mem      map[string]uint64  `json:"mem"`
	State    string             `json:"state"`
}

func (p *Partition) Load(i *protos.PartitionInfo) {
	p.Name = i.Name
	p.Hostlist = i.Hostlist
	p.Nodes = map[string]uint32{
		"total": i.TotalNodes,
		"alive": i.AliveNodes,
	}
	p.Cpu = map[string]float64{
		"total": i.TotalCpu,
		"avail": i.AvailCpu,
		"alloc": i.AllocCpu,
	}
	p.Mem = map[string]uint64{
		"total": i.TotalMem,
		"avail": i.AvailMem,
		"alloc": i.AllocMem,
	}
	p.State = i.State.String()
}

type NodeInfo struct {
	Nodes []Node `json:"nodes"`
}

func (data *NodeInfo) Load(reply *protos.QueryCranedInfoReply) {
	for _, info := range reply.CranedInfoList {
		dat := Node{}
		dat.Load(info)
		data.Nodes = append(data.Nodes, dat)
	}
}

type Node struct {
	Hostname      string             `json:"hostname"`
	TaskNum       uint32             `json:"task_num"`
	Cpu           map[string]float64 `json:"cpu"`
	Mem           map[string]uint64  `json:"mem"`
	Partitions    []string           `json:"partitions"`
	ResourceState string             `json:"resource_state"`
	ControlState  string             `json:"control_state"`
}

func (n *Node) Load(i *protos.CranedInfo) {
	n.Hostname = i.Hostname
	n.TaskNum = i.RunningTaskNum
	n.Cpu = map[string]float64{
		"total": i.Cpu,
		"avail": i.FreeCpu,
		"alloc": i.AllocCpu,
	}
	n.Mem = map[string]uint64{
		"total": i.RealMem,
		"avail": i.FreeMem,
		"alloc": i.AllocMem,
	}
	n.Partitions = i.PartitionNames
	n.ResourceState = i.ResourceState.String()
	n.ControlState = i.ControlState.String()
}

type ClusterInfo struct {
	Partitions []TrimmedPartition `json:"partitions"`
}

func (data *ClusterInfo) Load(reply *protos.QueryClusterInfoReply) {
	for _, info := range reply.Partitions {
		dat := TrimmedPartition{}
		dat.Load(info)
		data.Partitions = append(data.Partitions, dat)
	}
}

type TrimmedPartition struct {
	Name        string          `json:"name"`
	State       string          `json:"state"`
	CranedLists []TrimmedCraned `json:"craned_lists"`
}

func (data *TrimmedPartition) Load(i *protos.TrimmedPartitionInfo) {
	data.Name = i.Name
	data.State = i.State.String()
	for _, info := range i.CranedLists {
		dat := TrimmedCraned{}
		dat.Load(info)
		data.CranedLists = append(data.CranedLists, dat)
	}
}

type TrimmedCraned struct {
	ResourceState   string `json:"resource_state"`
	ControlState    string `json:"control_state"`
	Count           uint32 `json:"count"`
	CranedListRegex string `json:"craned_list_regex"`
}

func (t *TrimmedCraned) Load(i *protos.TrimmedPartitionInfo_TrimmedCranedInfo) {
	t.ResourceState = i.ResourceState.String()
	t.ControlState = i.ControlState.String()
	t.Count = i.Count
	t.CranedListRegex = i.CranedListRegex
}
