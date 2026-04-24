package util

import (
	"CraneFrontEnd/generated/protos"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var jobIdSelectorPattern = regexp.MustCompile(`^(\d+)(?:_(\d+))?(?:\.(\d+))?$`)

// JobIdSelector describes a selector token accepted by job query flags.
// Supported forms: jobid, jobid_arraytaskid, jobid.stepid, jobid_arraytaskid.stepid.
type JobIdSelector struct {
	JobId       uint32
	ArrayTaskId *uint32
	StepId      *uint32
}

func ResolveArrayJobId(jobId uint32, arrayJobId *uint32) uint32 {
	if arrayJobId != nil {
		return *arrayJobId
	}
	return jobId
}

func JobArrayIdentity(job *protos.JobInfo) *protos.ArrayTaskIdentity {
	if job == nil {
		return nil
	}
	return job.ArrayTask
}

func JobArrayJobId(job *protos.JobInfo) *uint32 {
	if identity := JobArrayIdentity(job); identity != nil {
		return &identity.ArrayJobId
	}
	return nil
}

func JobArrayTaskId(job *protos.JobInfo) *uint32 {
	if identity := JobArrayIdentity(job); identity != nil {
		return &identity.TaskId
	}
	return nil
}

func FormatJobIdWithArray(jobId uint32, arrayTaskId *uint32) string {
	if arrayTaskId != nil {
		return fmt.Sprintf("%d_%d", jobId, *arrayTaskId)
	}
	return strconv.FormatUint(uint64(jobId), 10)
}

func FormatStepIdWithArray(jobId uint32, arrayTaskId *uint32, stepId uint32) string {
	return fmt.Sprintf("%s.%d", FormatJobIdWithArray(jobId, arrayTaskId), stepId)
}

func ParseJobIdSelector(token string) (JobIdSelector, error) {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return JobIdSelector{}, fmt.Errorf("empty selector")
	}

	match := jobIdSelectorPattern.FindStringSubmatch(trimmed)
	if match == nil {
		return JobIdSelector{}, fmt.Errorf("invalid selector \"%s\"", token)
	}

	jobId, err := strconv.ParseUint(match[1], 10, 32)
	if err != nil || jobId == 0 {
		return JobIdSelector{}, fmt.Errorf("invalid job id in selector \"%s\"", token)
	}

	selector := JobIdSelector{JobId: uint32(jobId)}

	if match[2] != "" {
		arrayTaskId, err := strconv.ParseUint(match[2], 10, 32)
		if err != nil {
			return JobIdSelector{}, fmt.Errorf("invalid array task id in selector \"%s\"", token)
		}
		arrayTaskId32 := uint32(arrayTaskId)
		selector.ArrayTaskId = &arrayTaskId32
	}

	if match[3] != "" {
		stepId, err := strconv.ParseUint(match[3], 10, 32)
		if err != nil {
			return JobIdSelector{}, fmt.Errorf("invalid step id in selector \"%s\"", token)
		}
		stepId32 := uint32(stepId)
		selector.StepId = &stepId32
	}

	return selector, nil
}

func ParseJobIdSelectorList(selectorList string, splitStr string) ([]JobIdSelector, error) {
	selectors := make([]JobIdSelector, 0)

	for token := range strings.SplitSeq(selectorList, splitStr) {
		selector, err := ParseJobIdSelector(token)
		if err != nil {
			return nil, err
		}
		selectors = append(selectors, selector)
	}

	return selectors, nil
}
