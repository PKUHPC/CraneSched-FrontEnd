/**
 * Copyright (c) 2025 Peking University and Peking University
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

package creport

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

var validCreportTypes = map[string]string{
	"SecPer":  "Seconds/Percentage of Total",
	"MinPer":  "Minutes/Percentage of Total",
	"HourPer": "Hours/Percentage of Total",
	"seconds": "Seconds",
	"minutes": "Minutes",
	"hours":   "Hours",
	"percent": "Percentage of Total",
}

func CheckCreportOutType(outType string) bool {
	_, ok := validCreportTypes[outType]
	return ok
}

func ReportUsageDivisor(outType string) float64 {
	switch outType {
	case "seconds":
		return 1
	case "minutes":
		return 60
	case "hours":
		return 3600
	default:
		return 1
	}
}

func PrintUsageTypeInfo(outType string, isJobSize, printCount bool) {
	if isJobSize {
		if printCount {
			fmt.Println("Units are in number of jobs range")
		} else {
			if suffix, ok := validCreportTypes[outType]; ok {
				fmt.Printf("Time reported in %s\n", suffix)
			}
		}
	} else {
		if suffix, ok := validCreportTypes[outType]; ok {
			fmt.Printf("Usage reported in CPU %s\n", suffix)
		}
	}
}

func PrintAsJsonToStdout(outputList interface{}) {
	encodedJson, err := json.Marshal(outputList)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to encode json: %v\n", err)
		return
	}
	fmt.Println(string(encodedJson))
}

func ParseAndSortJobSizeList(input string) ([]uint32, error) {
	segments := strings.Split(input, ",")
	uniqueNumbers := make(map[uint32]struct{})
	for index, segment := range segments {
		trimmed := strings.TrimSpace(segment)
		if trimmed == "" {
			return nil, fmt.Errorf("empty value detected at position %d", index+1)
		}
		number, err := strconv.Atoi(trimmed)
		if err != nil {
			return nil, fmt.Errorf("invalid number '%s' at position %d", trimmed, index+1)
		}
		if number < 0 {
			return nil, fmt.Errorf("negative number '%s' at position %d", trimmed, index+1)
		}
		uniqueNumbers[uint32(number)] = struct{}{}
	}

	resultList := make([]uint32, 0, len(uniqueNumbers))
	for number := range uniqueNumbers {
		resultList = append(resultList, number)
	}
	sort.Slice(resultList, func(i, j int) bool { return resultList[i] < resultList[j] })

	if len(resultList) == 0 {
		return nil, fmt.Errorf("no valid job size thresholds provided")
	}

	return resultList, nil
}
