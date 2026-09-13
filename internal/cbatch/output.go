package cbatch

import (
	"CraneFrontEnd/internal/util"
	"fmt"
	"strconv"
	"strings"
)

// formatJobSubmissionOutput keeps cbatch's native output separate from the
// output contract exposed by the command wrappers.
func formatJobSubmissionOutput(jobIDs []uint32) string {
	if len(jobIDs) == 0 {
		return ""
	}

	if util.IsSlurmOutputMode() {
		lines := make([]string, 0, len(jobIDs))
		for _, jobID := range jobIDs {
			lines = append(lines, fmt.Sprintf("Submitted batch job %d", jobID))
		}
		return strings.Join(lines, "\n") + "\n"
	}

	if len(jobIDs) == 1 {
		return fmt.Sprintf("Job id allocated: %d.\n", jobIDs[0])
	}

	ids := make([]string, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		ids = append(ids, strconv.FormatUint(uint64(jobID), 10))
	}
	return fmt.Sprintf("Job id allocated: %s.\n", strings.Join(ids, ", "))
}
