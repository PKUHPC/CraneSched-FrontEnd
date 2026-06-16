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

package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"strconv"
	"time"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

var (
	stub protos.CraneCtldClient
)

func FillReqByCobraFlags() (*protos.QueryJobsInfoRequest, error) {
	req := protos.QueryJobsInfoRequest{OptionIncludeCompletedJobs: false}

	processors := []FilterProcessor{
		&StatesFilterProcessor{},
		&SelfProcessor{},
		&JobNamesProcessor{},
		&UserFilterProcessor{},
		&QosProcessor{},
		&AccountProcessor{},
		&PartitionsProcessor{},
		&JobIDsProcessor{},
		&StepIDsProcessor{},
		&LicensesProcessor{},
		&NodeNamesProcessor{},
	}

	for _, p := range processors {
		if err := p.Process(&req); err != nil {
			return nil, err
		}
	}

	if FlagNumLimit != 0 {
		req.NumLimit = FlagNumLimit
	}

	return &req, nil
}

func QueryJobsInfo() (*protos.QueryJobsInfoReply, error) {
	reply, _, err := queryJobsInfoFromFlags()
	return reply, err
}

func queryJobsInfoFromFlags() (*protos.QueryJobsInfoReply, *protos.QueryJobsInfoRequest, error) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub = util.GetStubToCtldByConfig(config)

	req, err := FillReqByCobraFlags()
	if err != nil {
		return &protos.QueryJobsInfoReply{}, nil, err
	}

	reply, err := queryJobsInfo(req)
	return reply, req, err
}

func queryJobsInfo(req *protos.QueryJobsInfoRequest) (*protos.QueryJobsInfoReply, error) {
	// set 10 seconds limit
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reply, err := stub.QueryJobsInfo(ctx, req)
	if err != nil {
		if rpcErr, ok := grpcstatus.FromError(err); ok {
			switch rpcErr.Code() {
			case grpccodes.DeadlineExceeded:
				util.GrpcErrorPrintf(err, "Query time out, due to too many jobs")
				return nil, util.NewCraneErr(util.ErrorBackend,
					"Query timed out due to large number of jobs. Please try with a smaller scope or use filters.",
				)
			case grpccodes.ResourceExhausted:
				util.GrpcErrorPrintf(err, "Response too large")
				return nil, util.NewCraneErr(util.ErrorNetwork,
					"Response too large for gRPC. Please reduce the query scope or avoid using -m with huge values.")
			default:
				util.GrpcErrorPrintf(err, "Failed to query job queue")
				return nil, &util.CraneError{Code: util.ErrorNetwork}
			}
		} else {
			util.GrpcErrorPrintf(err, "Failed to query job queue, grpcstatus get error failed.")
			return nil, &util.CraneError{Code: util.ErrorNetwork}
		}
	}
	return reply, nil
}

func Query() error {
	reply, req, err := queryJobsInfoFromFlags()
	if err != nil {
		return err
	}
	if err := reportMissingArrayTaskReason(req, reply); err != nil {
		return err
	}

	if FlagJson {
		return JsonOutput(reply)
	} else if FlagStep {
		return QueryStepsTableOutput(reply)
	} else {
		return QueryTableOutput(reply)
	}
}

func reportMissingArrayTaskReason(req *protos.QueryJobsInfoRequest, reply *protos.QueryJobsInfoReply) error {
	if req == nil || reply == nil || len(reply.GetJobInfoList()) > 0 {
		return nil
	}

	missingSelectors := missingArrayTaskSelectors(req.GetFilterJobIds(), reply.GetJobInfoList())
	if len(missingSelectors) == 0 {
		return nil
	}

	reason, err := queryMissingArrayTaskReason(missingSelectors)
	if err != nil || reason == "" {
		return err
	}
	return util.NewCraneErr(util.ErrorBackend, reason)
}

func missingArrayTaskSelectors(selectors []*protos.JobIdSelector, jobs []*protos.JobInfo) []*protos.JobIdSelector {
	if len(selectors) == 0 {
		return nil
	}

	returned := make(map[string]bool, len(jobs))
	for _, job := range jobs {
		if job == nil || job.GetArrayTask() == nil {
			continue
		}
		arrayTask := job.GetArrayTask()
		returned[arrayTaskSelectorKey(arrayTask.GetArrayJobId(), arrayTask.GetTaskId())] = true
	}

	var missing []*protos.JobIdSelector
	for _, selector := range selectors {
		if selector == nil || selector.ArrayTaskId == nil {
			continue
		}
		key := arrayTaskSelectorKey(selector.GetJobId(), selector.GetArrayTaskId())
		if !returned[key] {
			missing = append(missing, selector)
		}
	}
	return missing
}

func queryMissingArrayTaskReason(missingSelectors []*protos.JobIdSelector) (string, error) {
	unmaterializedSelectors, err := unmaterializedArrayTaskSelectors(missingSelectors)
	if err != nil || len(unmaterializedSelectors) == 0 {
		return "", err
	}

	parentSelectors := make([]*protos.JobIdSelector, 0, len(unmaterializedSelectors))
	seenParents := make(map[uint32]bool, len(unmaterializedSelectors))
	for _, selector := range unmaterializedSelectors {
		parentID := selector.GetJobId()
		if seenParents[parentID] {
			continue
		}
		seenParents[parentID] = true
		parentSelectors = append(parentSelectors, &protos.JobIdSelector{JobId: parentID})
	}
	if len(parentSelectors) == 0 {
		return "", nil
	}

	parentReq := &protos.QueryJobsInfoRequest{
		FilterJobIds:               parentSelectors,
		NumLimit:                   uint32(len(parentSelectors)),
		OptionIncludeCompletedJobs: true,
	}

	parentReply, err := queryJobsInfo(parentReq)
	if err != nil {
		return "", err
	}

	parentByID := make(map[uint32]*protos.JobInfo, len(parentReply.GetJobInfoList()))
	for _, parent := range parentReply.GetJobInfoList() {
		if parent == nil || parent.GetArraySpec() == nil {
			continue
		}
		parentByID[parent.GetJobId()] = parent
	}

	for _, selector := range unmaterializedSelectors {
		parent := parentByID[selector.GetJobId()]
		if parent == nil || !arraySpecContainsTask(parent.GetArraySpec(), selector.GetArrayTaskId()) {
			continue
		}

		displayID := util.FormatJobIdFromArrayTaskId(selector.GetJobId(), selector.ArrayTaskId)
		return fmt.Sprintf("Array task %s has not been materialized yet", displayID), nil
	}

	return "", nil
}

func unmaterializedArrayTaskSelectors(selectors []*protos.JobIdSelector) ([]*protos.JobIdSelector, error) {
	req := &protos.QueryJobsInfoRequest{
		FilterJobIds:               selectors,
		NumLimit:                   uint32(len(selectors)),
		OptionIncludeCompletedJobs: true,
	}

	reply, err := queryJobsInfo(req)
	if err != nil {
		return nil, err
	}
	return missingArrayTaskSelectors(selectors, reply.GetJobInfoList()), nil
}

func arraySpecContainsTask(arraySpec *protos.ArraySpec, taskID uint32) bool {
	if arraySpec == nil || taskID < arraySpec.GetStart() || taskID > arraySpec.GetEnd() {
		return false
	}

	stride := arraySpec.GetStride()
	if stride == 0 {
		stride = 1
	}
	return (taskID-arraySpec.GetStart())%stride == 0
}

func arrayTaskSelectorKey(jobID uint32, arrayTaskID uint32) string {
	return strconv.FormatUint(uint64(jobID), 10) + "_" + strconv.FormatUint(uint64(arrayTaskID), 10)
}

func loopedQuery(iterate uint64) error {
	interval, err := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, "Invalid time interval.")
	}
	return loopedSubQuery(interval)
}

func loopedSubQuery(interval time.Duration) error {
	for {
		fmt.Println(time.Now().String()[0:19])
		if err := Query(); err != nil {
			return err
		}
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
