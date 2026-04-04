package cacct

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"sort"
)

var (
	jobIdSelectors        []util.JobIdSelector
	hasArrayTaskSelectors bool
)

func resetJobIdSelectors() {
	jobIdSelectors = nil
	hasArrayTaskSelectors = false
}

func configureJobIdSelectors(selectors []util.JobIdSelector) {
	resetJobIdSelectors()
	jobIdSelectors = append(jobIdSelectors, selectors...)
	for _, selector := range selectors {
		if selector.ArrayTaskId != nil {
			hasArrayTaskSelectors = true
			break
		}
	}
}

func buildFilterIdsFromSelectors() map[uint32]*protos.JobStepIds {
	type stepBucket struct {
		all   bool
		steps map[uint32]struct{}
	}

	buckets := make(map[uint32]*stepBucket)
	for _, selector := range jobIdSelectors {
		bucket, ok := buckets[selector.JobId]
		if !ok {
			bucket = &stepBucket{steps: make(map[uint32]struct{})}
			buckets[selector.JobId] = bucket
		}

		if selector.StepId == nil {
			bucket.all = true
			continue
		}

		if !bucket.all {
			bucket.steps[*selector.StepId] = struct{}{}
		}
	}

	filterIds := make(map[uint32]*protos.JobStepIds)
	for jobId, bucket := range buckets {
		if bucket.all {
			filterIds[jobId] = &protos.JobStepIds{Steps: []uint32{}}
			continue
		}

		steps := make([]uint32, 0, len(bucket.steps))
		for stepId := range bucket.steps {
			steps = append(steps, stepId)
		}
		sort.Slice(steps, func(i, j int) bool { return steps[i] < steps[j] })
		filterIds[jobId] = &protos.JobStepIds{Steps: steps}
	}

	return filterIds
}

func applyArrayAwareItemFilter(items []*JobOrStep) []*JobOrStep {
	if !hasArrayTaskSelectors {
		return items
	}

	filtered := make([]*JobOrStep, 0, len(items))
	for _, item := range items {
		if item.job == nil {
			continue
		}

		matched := false
		for _, selector := range jobIdSelectors {
			if item.job.JobId != selector.JobId {
				continue
			}

			if selector.ArrayTaskId != nil {
				if item.job.ArrayTaskId == nil || *item.job.ArrayTaskId != *selector.ArrayTaskId {
					continue
				}
			}

			if selector.StepId != nil {
				if !item.isStep || item.stepInfo.StepId != *selector.StepId {
					continue
				}
			}

			matched = true
			break
		}

		if matched {
			filtered = append(filtered, item)
		}
	}

	return filtered
}
