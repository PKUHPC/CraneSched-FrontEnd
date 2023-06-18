package util

import (
	"fmt"
	"github.com/golang/protobuf/ptypes/duration"
	"regexp"
	"strconv"
)

func ParseDuration(time string, duration *duration.Duration) bool {
	re := regexp.MustCompile(`(.*):(.*):(.*)`)
	result := re.FindAllStringSubmatch(time, -1)
	if result == nil || len(result) != 1 {
		return false
	}
	hh, err := strconv.ParseUint(result[0][1], 10, 32)
	if err != nil {
		return false
	}
	mm, err := strconv.ParseUint(result[0][2], 10, 32)
	if err != nil {
		return false
	}
	ss, err := strconv.ParseUint(result[0][3], 10, 32)
	if err != nil {
		return false
	}

	duration.Seconds = int64(60*60*hh + 60*mm + ss)
	return true
}

func SecondTimeFormat(second int64) string {
	timeFormat := ""
	dd := second / 24 / 3600
	second %= 24 * 3600
	hh := second / 3600
	second %= 3600
	mm := second / 60
	ss := second % 60
	if dd > 0 {
		timeFormat = fmt.Sprintf("%d-%02d:%02d:%02d", dd, hh, mm, ss)
	} else {
		timeFormat = fmt.Sprintf("%02d:%02d:%02d", hh, mm, ss)
	}
	return timeFormat
}
