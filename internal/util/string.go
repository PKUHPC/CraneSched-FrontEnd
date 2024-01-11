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

package util

import (
	"fmt"
	"github.com/golang/protobuf/ptypes/duration"
	"regexp"
	"strconv"
	"strings"
)

func ParseMemStringAsByte(mem string) (uint64, error) {
	re := regexp.MustCompile(`([0-9]+(\.?[0-9]+)?)([MmGgKk])`)
	result := re.FindAllStringSubmatch(mem, -1)
	if result == nil || len(result) != 1 {
		return 0, fmt.Errorf("invalid memory format")
	}
	sz, err := strconv.ParseFloat(result[0][1], 10)
	if err != nil {
		return 0, err
	}
	switch result[0][len(result[0])-1] {
	case "M", "m":
		return uint64(1024 * 1024 * sz), nil
	case "G", "g":
		return uint64(1024 * 1024 * 1024 * sz), nil
	case "K", "k":
		return uint64(1024 * sz), nil
	}
	return uint64(sz), nil
}

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

func ParseGres(gres string) map[string]map[string]uint64 {
	result := make(map[string]map[string]uint64)
	if gres == "" {
		return result
	}
	gresList := strings.Split(gres, ",")
	for _, g := range gresList {
		parts := strings.Split(g, ":")
		name := parts[0]
		var gresType string
		if len(parts) == 2 {
			gresType = "UNKNOWN"
		} else if len(parts) == 3 {
			gresType = parts[1]
		} else {
			fmt.Printf("Error parsing gresType of: %s\n", g)
		}
		count, err := strconv.ParseUint(parts[len(parts)-1], 10, 64)
		if err != nil {
			fmt.Printf("Error parsing count for %s: %v\n", name, err)
			continue
		}

		if _, ok := result[name]; !ok {
			result[name] = make(map[string]uint64)
		}
		result[name][gresType] = count
	}

	return result
}
