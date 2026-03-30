package cbatch

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ParseArrayExpr parses a Slurm-compatible array index expression.
// Supported formats: "0-31", "1,3,5,7", "1-7:2", "0-3,10,20-25:5"
// Returns sorted and deduplicated index list.
func ParseArrayExpr(expr string) ([]uint32, error) {
	var result []uint32
	parts := strings.Split(expr, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if strings.Contains(part, "-") {
			// Range form: N-M or N-M:S
			rangeAndStep := strings.SplitN(part, ":", 2)
			step := uint64(1)
			if len(rangeAndStep) > 1 {
				var err error
				step, err = strconv.ParseUint(rangeAndStep[1], 10, 32)
				if err != nil || step == 0 {
					return nil, fmt.Errorf("invalid step in '%s'", part)
				}
			}

			bounds := strings.SplitN(rangeAndStep[0], "-", 2)
			start, err1 := strconv.ParseUint(bounds[0], 10, 32)
			end, err2 := strconv.ParseUint(bounds[1], 10, 32)
			if err1 != nil || err2 != nil || start > end {
				return nil, fmt.Errorf("invalid range in '%s'", part)
			}

			for i := start; i <= end; i += step {
				result = append(result, uint32(i))
			}
		} else {
			// Single number
			val, err := strconv.ParseUint(part, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid index '%s'", part)
			}
			result = append(result, uint32(val))
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("empty array expression")
	}

	// Sort and deduplicate
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	deduped := result[:1]
	for i := 1; i < len(result); i++ {
		if result[i] != result[i-1] {
			deduped = append(deduped, result[i])
		}
	}
	return deduped, nil
}
