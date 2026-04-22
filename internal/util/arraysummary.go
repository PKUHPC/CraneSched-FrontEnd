package util

import (
	"CraneFrontEnd/generated/protos"
	"strconv"
)

// ArraySummaryInfo groups consecutive array task rows for summary display.
type ArraySummaryInfo struct {
	AnchorJobId uint32
	Start       uint32
	End         uint32
	Count       int
	Sample      *protos.JobInfo
}

// MakeArraySummaryGroupKey returns a grouping key for array tasks.
// Returns ("", false) if the job is not an array child.
func MakeArraySummaryGroupKey(job *protos.JobInfo) (string, bool) {
	if job.ArrayJobId == nil || job.ArrayTaskId == nil {
		return "", false
	}
	return strconv.FormatUint(uint64(*job.ArrayJobId), 10), true
}

// BuildArraySummaryRows inserts summary rows for array task groups into the
// output. buildSummaryRow is called to format each summary row from an
// ArraySummaryInfo. getJobInfo extracts the *protos.JobInfo from each item.
func BuildArraySummaryRows[T any](
	header []string,
	rows [][]string,
	items []T,
	getJobInfo func(T) *protos.JobInfo,
	buildSummaryRow func([]string, *ArraySummaryInfo) []string,
) [][]string {
	if len(rows) != len(items) {
		return rows
	}

	groupByKey := make(map[string]*ArraySummaryInfo)
	itemKeys := make([]string, len(items))

	for i, item := range items {
		job := getJobInfo(item)
		if job == nil {
			continue
		}
		key, ok := MakeArraySummaryGroupKey(job)
		if !ok {
			continue
		}
		itemKeys[i] = key

		if _, ok := groupByKey[key]; !ok {
			groupByKey[key] = &ArraySummaryInfo{
				AnchorJobId: ResolveArrayJobId(job.JobId, job.ArrayJobId),
				Start:       *job.ArrayTaskId,
				End:         *job.ArrayTaskId,
				Count:       0,
				Sample:      job,
			}
		}

		group := groupByKey[key]
		group.Count++
		if *job.ArrayTaskId < group.Start {
			group.Start = *job.ArrayTaskId
		}
		if *job.ArrayTaskId > group.End {
			group.End = *job.ArrayTaskId
		}
	}

	emitted := make(map[string]struct{})
	merged := make([][]string, 0, len(rows)+len(groupByKey))

	for i, row := range rows {
		key := itemKeys[i]
		if key != "" {
			if _, ok := emitted[key]; !ok {
				group := groupByKey[key]
				if group.Count > 1 {
					merged = append(merged, buildSummaryRow(header, group))
				}
				emitted[key] = struct{}{}
			}
		}
		merged = append(merged, row)
	}

	return merged
}
