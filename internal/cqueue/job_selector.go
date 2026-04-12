package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
)

var (
	jobIdSelectors        []util.JobIdSelector
	hasArrayTaskSelectors bool
	allJobSelections      map[uint32]struct{}
	arrayTaskIdSelections map[uint32]map[uint32]struct{}
)

func resetJobIdSelectors() {
	jobIdSelectors = nil
	hasArrayTaskSelectors = false
	allJobSelections = make(map[uint32]struct{})
	arrayTaskIdSelections = make(map[uint32]map[uint32]struct{})
}

func configureJobIdSelectors(selectors []util.JobIdSelector) error {
	resetJobIdSelectors()

	for _, selector := range selectors {
		if selector.StepId != nil {
			return fmt.Errorf("step selector is unsupported in cqueue --job: use --step for step queries")
		}

		jobIdSelectors = append(jobIdSelectors, selector)
		if selector.ArrayTaskId == nil {
			allJobSelections[selector.JobId] = struct{}{}
			continue
		}

		hasArrayTaskSelectors = true
		if _, ok := arrayTaskIdSelections[selector.JobId]; !ok {
			arrayTaskIdSelections[selector.JobId] = make(map[uint32]struct{})
		}
		arrayTaskIdSelections[selector.JobId][*selector.ArrayTaskId] = struct{}{}
	}

	return nil
}

func buildFilterIdsFromJobSelectors() map[uint32]*protos.JobStepIds {
	filterIds := make(map[uint32]*protos.JobStepIds)
	for _, selector := range jobIdSelectors {
		if _, ok := filterIds[selector.JobId]; !ok {
			filterIds[selector.JobId] = nil
		}
	}
	return filterIds
}

func buildFilterArrayTaskIdsFromJobSelectors() map[uint32]*protos.ArrayTaskIds {
	if !hasArrayTaskSelectors {
		return nil
	}
	result := make(map[uint32]*protos.ArrayTaskIds)
	for jobId, taskSet := range arrayTaskIdSelections {
		taskIds := &protos.ArrayTaskIds{}
		for taskId := range taskSet {
			taskIds.ArrayTaskIds = append(taskIds.ArrayTaskIds, taskId)
		}
		result[jobId] = taskIds
	}
	return result
}

func applyArrayAwareJobFilter(reply *protos.QueryJobsInfoReply) {
	if !hasArrayTaskSelectors {
		return
	}

	filtered := make([]*protos.JobInfo, 0, len(reply.JobInfoList))
	for _, job := range reply.JobInfoList {
		logicalJobId := util.ResolveArrayJobId(job.JobId, job.ArrayJobId)
		if _, ok := allJobSelections[logicalJobId]; ok {
			filtered = append(filtered, job)
			continue
		}

		selectedTasks, ok := arrayTaskIdSelections[logicalJobId]
		if !ok || job.ArrayTaskId == nil {
			continue
		}

		if _, ok := selectedTasks[*job.ArrayTaskId]; ok {
			filtered = append(filtered, job)
		}
	}

	reply.JobInfoList = filtered
}

func formatJobIdForDisplay(job *protos.JobInfo) string {
	return util.FormatJobIdWithArray(
		util.ResolveArrayJobId(job.JobId, job.ArrayJobId), job.ArrayTaskId)
}

func formatStepIdForDisplay(stepInfo *protos.StepInfo, parentJob *protos.JobInfo) string {
	return util.FormatStepIdWithArray(
		util.ResolveArrayJobId(stepInfo.JobId, parentJob.ArrayJobId),
		parentJob.ArrayTaskId,
		stepInfo.StepId,
	)
}
