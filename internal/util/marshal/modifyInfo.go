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

type ModifyInfo struct {
	Ok     bool   `json:"ok"`
	Reason string `json:"reason"`
}

type SubmitBatchTaskInfo struct {
	Ok     bool   `json:"ok"`
	TaskId uint32 `json:"task_id"`
	Reason string `json:"reason"`
}

func (info *SubmitBatchTaskInfo) Load(reply *protos.SubmitBatchTaskReply) {
	info.Ok = reply.Ok
	info.TaskId = reply.GetTaskId()
	info.Reason = reply.GetReason()
}

type SubmitBatchTasksInfo struct {
	TaskIdList []uint32 `json:"task_id_list"`
	ReasonList []string `json:"reason_list"`
}

func (info *SubmitBatchTasksInfo) Load(reply *protos.SubmitBatchTasksReply) {
	info.TaskIdList = reply.TaskIdList
	info.ReasonList = reply.ReasonList
}

type CancelInfo struct {
	CancelledTasks      []uint32 `json:"cancelled_tasks"`
	NotCancelledTasks   []uint32 `json:"not_cancelled_tasks"`
	NotCancelledReasons []string `json:"not_cancelled_reason"`
}

func (info *CancelInfo) Load(reply *protos.CancelTaskReply) {
	info.CancelledTasks = reply.CancelledTasks
	info.NotCancelledTasks = reply.NotCancelledTasks
	info.NotCancelledReasons = reply.NotCancelledReasons
}
