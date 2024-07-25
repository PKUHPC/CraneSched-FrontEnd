package util

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util/marshal"
	"encoding/json"
)

type Formatter interface {
	FormatQueryTasksInfoReply(*protos.QueryTasksInfoReply) string
	FormatQueryPartitionInfoReply(*protos.QueryPartitionInfoReply) string
	FormatQueryCranedInfoReply(*protos.QueryCranedInfoReply) string
	FormatQueryClusterInfoReply(*protos.QueryClusterInfoReply) string

	FormatModifyTaskReply(*protos.ModifyTaskReply) string
	FormatModifyCranedStateReply(*protos.ModifyCranedStateReply) string
	FormatSubmitBatchTaskReply(*protos.SubmitBatchTaskReply) string
	FormatSubmitBatchTasksReply(*protos.SubmitBatchTasksReply) string
	FormatCancelTaskReply(*protos.CancelTaskReply) string
}

type formatterJSON struct {
}

func (f *formatterJSON) FormatQueryTasksInfoReply(reply *protos.QueryTasksInfoReply) string {
	data := marshal.TasksInfo{}
	data.Load(reply)
	output, _ := json.Marshal(data)
	return string(output)
}

func (f *formatterJSON) FormatQueryPartitionInfoReply(reply *protos.QueryPartitionInfoReply) string {
	data := marshal.PartitionInfo{}
	data.Load(reply)
	output, _ := json.Marshal(data)
	return string(output)
}

func (f *formatterJSON) FormatQueryNodeInfoReply(reply *protos.QueryCranedInfoReply) string {
	data := marshal.NodeInfo{}
	data.Load(reply)
	output, _ := json.Marshal(data)
	return string(output)
}

func (f *formatterJSON) FormatQueryClusterInfoReply(reply *protos.QueryClusterInfoReply) string {
	data := marshal.ClusterInfo{}
	data.Load(reply)
	output, _ := json.Marshal(data)
	return string(output)
}

func (f *formatterJSON) FormatModifyTaskReply(reply *protos.ModifyTaskReply) string {
	data := marshal.ModifyInfo{}
	data.Ok = reply.Ok
	data.Reason = reply.Reason
	output, _ := json.Marshal(data)
	return string(output)
}

func (f *formatterJSON) FormatModifyCranedStateReply(reply *protos.ModifyCranedStateReply) string {
	data := marshal.ModifyInfo{}
	data.Ok = reply.Ok
	data.Reason = reply.Reason
	output, _ := json.Marshal(data)
	return string(output)
}

func (f *formatterJSON) FormatSubmitBatchTaskReply(reply *protos.SubmitBatchTaskReply) string {
	data := marshal.SubmitBatchTaskInfo{}
	data.Load(reply)
	output, _ := json.Marshal(data)
	return string(output)
}

func (f *formatterJSON) FormatSubmitBatchTasksReply(reply *protos.SubmitBatchTasksReply) string {
	data := marshal.SubmitBatchTasksInfo{}
	data.Load(reply)
	output, _ := json.Marshal(data)
	return string(output)
}

func (f *formatterJSON) FormatCancelTaskReply(reply *protos.CancelTaskReply) string {
	data := marshal.CancelInfo{}
	data.Load(reply)
	output, _ := json.Marshal(data)
	return string(output)
}

var FormatterJSON = formatterJSON{}
