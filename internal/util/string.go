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
	"CraneFrontEnd/generated/protos"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

func ParseConfig(configFilePath string) *Config {
	confFile, err := os.ReadFile(configFilePath)
	if err != nil {
		log.Errorf("Failed to read config file %s: %v", configFilePath, err)
		os.Exit(ErrorCmdArg)
	}

	config := &Config{}
	err = yaml.Unmarshal(confFile, config)
	if err != nil {
		log.Errorf("Failed to read config file %s: %v", configFilePath, err)
		os.Exit(ErrorCmdArg)
	}

	if config.CraneBaseDir == "" {
		config.CraneBaseDir = DefaultCraneBaseDir
	}

	if config.CranedCforedSockPath == "" {
		config.CranedCforedSockPath = filepath.Join(config.CraneBaseDir, DefaultCforedSocketPath)
	} else {
		config.CranedCforedSockPath = filepath.Join(config.CraneBaseDir, config.CranedCforedSockPath)
	}

	return config
}

func ParseMemStringAsByte(mem string) (uint64, error) {
	re := regexp.MustCompile(`^([0-9]+(\.?[0-9]*))([MmGgKkB]?)$`)
	result := re.FindAllStringSubmatch(mem, -1)
	if result == nil || len(result) != 1 {
		return 0, fmt.Errorf("invalid memory format")
	}
	sz, err := ParseFloatWithPrecision(result[0][1], 10)
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
	case "B":
		return uint64(sz), nil
	}
	// default unit is MB
	return uint64(1024 * 1024 * sz), nil
}

func ParseInterval(interval string, intervalpb *protos.TimeInterval) (err error) {
	if !strings.Contains(interval, "~") {
		err = fmt.Errorf("'~' cannot be omitted")
		return
	}

	split := strings.Split(interval, "~")
	if len(split) > 2 {
		err = fmt.Errorf("too many '~' found")
		return
	}

	var tl, tr time.Time
	if split[0] != "" {
		tl, err = ParseTime(strings.TrimSpace(split[0]))
		if err != nil {
			return
		}
		intervalpb.LowerBound = timestamppb.New(tl)
	}
	if len(split) == 2 && split[1] != "" {
		tr, err = ParseTime(strings.TrimSpace(split[1]))
		if err != nil {
			return
		}
		if tr.Before(tl) {
			err = fmt.Errorf("%v is earlier than %v", tr, tl)
			return
		}
		intervalpb.UpperBound = timestamppb.New(tr)
	}
	return
}

func ParseDuration(time string, duration *durationpb.Duration) bool {
	re := regexp.MustCompile(`((.*)-)?(.*):(.*):(.*)`)
	result := re.FindAllStringSubmatch(time, -1)
	if result == nil || len(result) != 1 {
		return false
	}
	var dd uint64 = 0
	if result[0][1] != "" {
		day, err := strconv.ParseUint(result[0][2], 10, 32)
		if err != nil {
			return false
		}
		dd = day
	}
	hh, err := strconv.ParseUint(result[0][3], 10, 32)
	if err != nil {
		return false
	}
	mm, err := strconv.ParseUint(result[0][4], 10, 32)
	if err != nil {
		return false
	}
	ss, err := strconv.ParseUint(result[0][5], 10, 32)
	if err != nil {
		return false
	}

	duration.Seconds = int64(24*60*60*dd + 60*60*hh + 60*mm + ss)
	return true
}

func ParseTime(ts string) (time.Time, error) {
	// Use regex to check if HH:MM:SS exists
	// This is required as Golang permits `2:03:14` but denies `2:3:14`,
	// which is undesired.
	re := regexp.MustCompile(`(\d{2}:\d{2}:\d{2})`)
	if !re.MatchString(ts) {
		return time.Time{}, fmt.Errorf("invalid time format")
	}

	// Try to parse the timezone at first
	layout := time.RFC3339
	parsed, err := time.Parse(layout, ts)
	if err == nil {
		return parsed, nil
	}

	// Fallback to the short layout, assuming local timezone
	layoutShort := time.RFC3339[:19]
	parsed, err = time.ParseInLocation(layoutShort, ts, time.Local)
	return parsed, err
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

// Parses a string containing a float number with a given precision.
func ParseFloatWithPrecision(val string, decimalPlaces int) (float64, error) {
	num, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, err
	}

	shift := math.Pow(10, float64(decimalPlaces))
	return math.Floor(num*shift) / shift, nil
}

func CheckTaskExtraAttr(attr string) bool {
	return gjson.Valid(attr)
}

// Merge two JSON strings.
// If there are overlapping keys, values from the second JSON take precedence.
func AmendTaskExtraAttr(origin, new string) string {
	result := gjson.Parse(new)
	result.ForEach(func(key, value gjson.Result) bool {
		var err error
		// Use sjson to set/override the value in the first JSON
		origin, err = sjson.Set(origin, key.String(), value.Value())
		return err == nil
	})

	return origin
}

func CheckMailType(mailtype string) bool {
	return mailtype == "NONE" ||
		mailtype == "BEGIN" ||
		mailtype == "END" ||
		mailtype == "FAIL" ||
		mailtype == "ALL"
}

// CheckNodeList check if the node list is comma separated node names.
// The node name should contain only letters and numbers, and start with a letter, end with a number.
func CheckNodeList(nodeStr string) bool {
	nameStr := strings.ReplaceAll(nodeStr, " ", "")
	if nameStr == "" {
		return true
	}
	re := regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9]*[0-9])(,([a-zA-Z][a-zA-Z0-9]*[0-9]))*$`)
	return re.MatchString(nameStr)
}

// CheckFileLength check if the file length is within the limit.
func CheckFileLength(filepath string) error {
	if len(path.Base(filepath)) > MaxJobFileNameLength {
		return fmt.Errorf("file name length exceeds %v characters", MaxJobFileNameLength)
	}

	if len(filepath) > MaxJobFilePathLengthForUnix {
		return fmt.Errorf("file path length exceeds %v characters", MaxJobFilePathLengthForUnix)
	}

	// Special case for Windows
	if runtime.GOOS == "windows" && len(filepath) > MaxJobFilePathLengthForWindows {
		return fmt.Errorf("file path length exceeds %v characters on Windows", MaxJobFilePathLengthForWindows)
	}

	return nil
}

func ParseHostList(hostStr string) ([]string, bool) {
	nameStr := strings.ReplaceAll(hostStr, " ", "")
	nameStr += ","

	var nameMeta string
	var strList []string
	var charQueue string

	for _, c := range nameStr {
		if c == '[' {
			if charQueue == "" {
				charQueue = string(c)
			} else {
				log.Errorln("Illegal node name string format: duplicate brackets")
				return nil, false
			}
		} else if c == ']' {
			if charQueue == "" {
				log.Errorln("Illegal node name string format: isolated bracket")
				return nil, false
			} else {
				nameMeta += charQueue
				nameMeta += string(c)
				charQueue = ""
			}
		} else if c == ',' {
			if charQueue == "" {
				strList = append(strList, nameMeta)
				nameMeta = ""
			} else {
				charQueue += string(c)
			}
		} else {
			if charQueue == "" {
				nameMeta += string(c)
			} else {
				charQueue += string(c)
			}
		}
	}
	if charQueue != "" {
		log.Errorln("Illegal node name string format: isolated bracket")
		return nil, false
	}

	regex := regexp.MustCompile(`.*\[(.*)\](\..*)*$`)
	var hostList []string

	for _, str := range strList {
		strS := strings.TrimSpace(str)
		if !regex.MatchString(strS) {
			hostList = append(hostList, strS)
		} else {
			nodes, ok := ParseNodeList(strS)
			if !ok {
				return nil, false
			}
			hostList = append(hostList, nodes...)
		}
	}
	return hostList, true
}

func ParseNodeList(nodeStr string) ([]string, bool) {
	bracketsRegex := regexp.MustCompile(`.*\[(.*)\]`)
	numRegex := regexp.MustCompile(`^\d+$`)
	scopeRegex := regexp.MustCompile(`^(\d+)-(\d+)$`)

	if !bracketsRegex.MatchString(nodeStr) {
		return nil, false
	}

	unitStrList := strings.Split(nodeStr, "]")
	endStr := unitStrList[len(unitStrList)-1]
	unitStrList = unitStrList[:len(unitStrList)-1]
	resList := []string{""}

	for _, str := range unitStrList {
		nodeNum := strings.FieldsFunc(str, func(r rune) bool {
			return r == '[' || r == ','
		})
		unitList := []string{}
		headStr := nodeNum[0]

		for _, numStr := range nodeNum[1:] {
			if numRegex.MatchString(numStr) {
				unitList = append(unitList, fmt.Sprintf("%s%s", headStr, numStr))
			} else if scopeRegex.MatchString(numStr) {
				locIndex := scopeRegex.FindStringSubmatch(numStr)
				start, err1 := strconv.Atoi(locIndex[1])
				end, err2 := strconv.Atoi(locIndex[2])
				if err1 != nil || err2 != nil {
					return nil, false
				}
				width := len(locIndex[1])
				for j := start; j <= end; j++ {
					sNum := fmt.Sprintf("%0*d", width, j)
					unitList = append(unitList, fmt.Sprintf("%s%s", headStr, sNum))
				}
			} else {
				return nil, false // Format error
			}
		}

		tempList := []string{}
		for _, left := range resList {
			for _, right := range unitList {
				tempList = append(tempList, left+right)
			}
		}
		resList = tempList
	}

	if endStr != "" {
		for i := range resList {
			resList[i] += endStr
		}
	}

	return resList, true
}

func InvalidDuration() *durationpb.Duration {
	return &durationpb.Duration{
		Seconds: 315576000000,
		Nanos:   0,
	}
}

func HostNameListToStr(hostList []string) string {
	sourceList := hostList

	for {
		resList, res := HostNameListToStr_(sourceList)
		if res {
			sort.Strings(resList)
			hostNameStr := strings.Join(resList, ",")
			return RemoveBracketsWithoutDashOrComma(hostNameStr)
		}
		sourceList = resList
	}
}

func HostNameListToStr_(hostList []string) ([]string, bool) {
	hostMap := make(map[string][]string)
	resList := []string{}
	res := true

	sz := len(hostList)
	if sz == 0 {
		return resList, true
	} else if sz == 1 {
		resList = append(resList, hostList[0])
		return resList, true
	}

	for _, host := range hostList {
		if host == "" {
			continue
		}

		start, end, found := FoundFirstNumberWithoutBrackets(host)
		if found {
			res = false
			numStr := host[start:end]
			headStr := host[:start]
			tailStr := host[end:]
			keyStr := fmt.Sprintf("%s<%s", headStr, tailStr)

			if _, ok := hostMap[keyStr]; !ok {
				hostMap[keyStr] = []string{}
			}
			hostMap[keyStr] = append(hostMap[keyStr], numStr)
		} else {
			resList = append(resList, host)
		}
	}

	if res {
		return resList, true
	}

	for key, nums := range hostMap {
		delimiterPos := strings.Index(key, "<")
		hostNameStr := key[:delimiterPos] + "["

		sort.Slice(nums, func(i, j int) bool {
			a, b := nums[i], nums[j]
			if len(a) != len(b) {
				return len(a) < len(b)
			}
			ai, _ := strconv.Atoi(a)
			bi, _ := strconv.Atoi(b)
			return ai < bi
		})

		nums = unique(nums)

		first, last := -1, -1
		firstStr, lastStr := "", ""

		for _, numStr := range nums {
			num, _ := strconv.Atoi(numStr)
			if first == -1 {
				first, last = num, num
				firstStr, lastStr = numStr, numStr
			} else if num == last+1 {
				last = num
				lastStr = numStr
			} else {
				if first == last {
					hostNameStr += firstStr
				} else {
					hostNameStr += fmt.Sprintf("%s-%s", firstStr, lastStr)
				}
				hostNameStr += ","
				first, last = num, num
				firstStr, lastStr = numStr, numStr
			}
		}

		if first == last {
			hostNameStr += firstStr
		} else {
			hostNameStr += fmt.Sprintf("%s-%s", firstStr, lastStr)
		}

		hostNameStr += "]" + key[delimiterPos+1:]
		resList = append(resList, hostNameStr)
	}

	return resList, res
}

func FoundFirstNumberWithoutBrackets(input string) (int, int, bool) {
	start, end := -1, -1
	opens := 0

	for i, c := range input {
		switch c {
		case '[':
			opens++
		case ']':
			opens--
		default:
			if opens == 0 {
				if start == -1 && c >= '0' && c <= '9' {
					start = i
				} else if start != -1 && (c < '0' || c > '9') {
					end = i
					return start, end, true
				}
			}
		}
	}

	if start != -1 {
		return start, len(input), true
	}
	return start, end, false
}

func unique(nums []string) []string {
	if len(nums) == 0 {
		return nums
	}
	j := 0
	for i := 1; i < len(nums); i++ {
		if nums[j] != nums[i] {
			j++
			nums[j] = nums[i]
		}
	}
	return nums[:j+1]
}

func RemoveBracketsWithoutDashOrComma(input string) string {
	output := input
	leftBracketPos := strings.Index(output, "[")

	for leftBracketPos != -1 {
		rightBracketPos := strings.Index(output[leftBracketPos:], "]")
		if rightBracketPos == -1 {
			break
		}
		rightBracketPos += leftBracketPos
		betweenBrackets := output[leftBracketPos+1 : rightBracketPos]
		if !strings.Contains(betweenBrackets, "-") && !strings.Contains(betweenBrackets, ",") {
			output = output[:rightBracketPos] + output[rightBracketPos+1:]
			output = output[:leftBracketPos] + output[leftBracketPos+1:]
		} else {
			leftBracketPos = rightBracketPos + 1
		}
		leftBracketPos = strings.Index(output[leftBracketPos:], "[")
		if leftBracketPos != -1 {
			leftBracketPos += leftBracketPos
		}
	}
	return output
}

func ParseGres(gres string) *protos.DeviceMap {
	result := &protos.DeviceMap{NameTypeMap: make(map[string]*protos.TypeCountMap)}
	if gres == "" {
		return result
	}
	gresList := strings.Split(gres, ",")
	for _, g := range gresList {
		parts := strings.Split(g, ":")
		name := parts[0]
		if len(parts) == 2 {
			gresNameCount, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				log.Errorf("Error parsing gres count: %s\n", g)
			}
			if gresNameCount == 0 {
				continue
			}
			if _, exist := result.NameTypeMap[name]; !exist {
				result.NameTypeMap[name] = &protos.TypeCountMap{TypeCountMap: make(map[string]uint64), Total: gresNameCount}
			} else {
				result.NameTypeMap[name].Total += gresNameCount
			}
		} else if len(parts) == 3 {
			gresType := parts[1]
			count, err := strconv.ParseUint(parts[2], 10, 64)
			if err != nil {
				fmt.Printf("Error parsing count for %s: %v\n", name, err)
				continue
			}
			if count == 0 {
				continue
			}
			if _, exist := result.NameTypeMap[name]; !exist {
				typeCountMap := make(map[string]uint64)
				typeCountMap[gresType] = count
				result.NameTypeMap[name] = &protos.TypeCountMap{TypeCountMap: typeCountMap, Total: 0}
			} else {
				result.NameTypeMap[name].TypeCountMap[gresType] = count
			}
		} else {
			log.Errorf("Error parsing gres : %s\n", g)
		}
	}

	return result
}

func ParseTaskStatusName(state string) (protos.TaskStatus, error) {
	state = strings.ToLower(state)
	switch state {
	case "pending", "p":
		return protos.TaskStatus_Pending, nil
	case "running", "r":
		return protos.TaskStatus_Running, nil
	case "completed", "c":
		return protos.TaskStatus_Completed, nil
	case "failed", "f":
		return protos.TaskStatus_Failed, nil
	case "tle", "time-limit-exceeded", "timelimitexceeded", "t":
		return protos.TaskStatus_ExceedTimeLimit, nil
	case "canceled", "cancelled", "x":
		return protos.TaskStatus_Cancelled, nil
	case "all":
		return protos.TaskStatus_Invalid, nil
	default:
		return protos.TaskStatus_Invalid, fmt.Errorf("unknown state: %s", state)
	}
}

func ParseTaskStatusList(statesStr string) ([]protos.TaskStatus, error) {
	var stateSet = make(map[protos.TaskStatus]bool)
	filterStateList := strings.Split(statesStr, ",")
	for i := 0; i < len(filterStateList); i++ {
		state, err := ParseTaskStatusName(filterStateList[i])
		if err != nil {
			return nil, err
		}
		stateSet[state] = true
	}
	if _, exists := stateSet[protos.TaskStatus_Invalid]; !exists && len(stateSet) < len(protos.TaskStatus_name)-1 {
		var stateList []protos.TaskStatus
		for state := range stateSet {
			stateList = append(stateList, state)
		}
		return stateList, nil
	}
	return []protos.TaskStatus{}, nil
}

func ParseInRamTaskStatusList(statesStr string) ([]protos.TaskStatus, error) {
	var stateSet = make(map[protos.TaskStatus]bool)
	filterStateList := strings.Split(statesStr, ",")
	for i := 0; i < len(filterStateList); i++ {
		state, err := ParseTaskStatusName(filterStateList[i])
		if err != nil {
			return nil, err
		}
		if state != protos.TaskStatus_Invalid && state != protos.TaskStatus_Pending && state != protos.TaskStatus_Running {
			return nil, fmt.Errorf("unsupported state: %s", filterStateList[i])
		}
		stateSet[state] = true
	}
	if len(stateSet) == 1 {
		for state := range stateSet {
			if state == protos.TaskStatus_Invalid {
				return []protos.TaskStatus{}, nil
			} else {
				return []protos.TaskStatus{state}, nil
			}
		}
	}
	return []protos.TaskStatus{}, nil
}

func ParseParameterList(parameters string, splitStr string) ([]string, error) {
	parameterList := strings.Split(parameters, splitStr)
	for i := 0; i < len(parameterList); i++ {
		if parameterList[i] == "" {
			return nil, fmt.Errorf("unsupported parameter: %s.", parameterList[i])
		}
	}

	return parameterList, nil
}

func ParseJobIdList(jobIds string, splitStr string) ([]uint32, error) {
	filterJobIdList := strings.Split(jobIds, splitStr)
	var taskIds []uint32
	for i := 0; i < len(filterJobIdList); i++ {
		jobId, err := strconv.ParseUint(filterJobIdList[i], 10, 32)
		if err != nil || jobId == 0 {
			return nil, fmt.Errorf("Invalid job id given: %s.\n", filterJobIdList[i])
		}
		taskIds = append(taskIds, uint32(jobId))
	}

	return taskIds, nil
}
