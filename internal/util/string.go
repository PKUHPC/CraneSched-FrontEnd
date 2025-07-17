/**
 * Copyright (c) 2024 Peking University and Peking University
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

// TODO: Refactor this to return ErrCodes instead of exiting.
func ParseConfig(configFilePath string) *Config {
	confFile, err := os.ReadFile(configFilePath)
	if err != nil {
		log.Errorf("Failed to read config file %s: %v", configFilePath, err)
		os.Exit(ErrorCmdArg)
	}

	config := &Config{}
	err = yaml.Unmarshal(confFile, config)
	if err != nil {
		log.Errorf("Config syntax error in %s: %v", configFilePath, err)
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

	if config.CforedLogDir == "" {
		config.CforedLogDir = filepath.Join(config.CraneBaseDir, DefaultCforedLogDir)
	} else {
		config.CforedLogDir = filepath.Join(config.CraneBaseDir, config.CforedLogDir)
	}

	if config.CraneCtldForInternalListenPort == "" {
		log.Errorf("CraneCtldForInternalListenPort is not set in config file %s", configFilePath)
		os.Exit(ErrorGeneric)
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

func ParseDurationStrToSeconds(duration string) (int64, error) {
	re := regexp.MustCompile(`^((\d+)-)?(\d+):(\d+):(\d+)$`)
	result := re.FindStringSubmatch(duration)
	if result == nil {
		return 0, fmt.Errorf("invalid duration format: %s", duration)
	}

	var dd uint64 = 0
	if result[1] != "" {
		day, err := strconv.ParseUint(result[2], 10, 32)
		if err != nil {
			return 0, fmt.Errorf("invalid day format: %s", result[2])
		}
		dd = day
	}

	hh, err := strconv.ParseUint(result[3], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid hour format: %s", result[3])
	}

	mm, err := strconv.ParseUint(result[4], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid minute format: %s", result[4])
	}

	ss, err := strconv.ParseUint(result[5], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid second format: %s", result[5])
	}

	seconds := int64(24*60*60*dd + 60*60*hh + 60*mm + ss)
	return seconds, nil
}

func ParseRelativeTime(ts string) (int64, error) {
	// handle now+1hour now-2week etc.
	re := regexp.MustCompile(`^(\d+)([a-zA-Z]*)$`)
	if result := re.FindStringSubmatch(ts); result != nil {
		value, err := strconv.ParseInt(result[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration: %v", ts)
		}
		unit := strings.ToLower(result[2])
		switch unit {
		case "", "s", "sec", "second", "seconds":
			return value, nil
		case "m", "min", "minute", "minutes":
			return value * 60, nil
		case "h", "hour", "hours":
			return value * 3600, nil
		case "d", "day", "days":
			return value * 24 * 3600, nil
		case "w", "week", "weeks":
			return value * 7 * 24 * 3600, nil
		default:
			return 0, fmt.Errorf("invalid duration: %v", ts)
		}
	}
	// handle now+12:20:12
	return ParseDurationStrToSeconds(ts)
}

func ParseDayTime(day string) (time.Time, error) {
	now := time.Now()
	// handle time of day (HH:MM[:SS] am pm also available)
	// e.g. 09:00am 16:30
	re := regexp.MustCompile(`^(\d{2}):(\d{2})(?::(\d{2}))?(am|pm)?$`)
	if result := re.FindStringSubmatch(day); result != nil {
		hh, err := strconv.ParseUint(result[1], 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid hour format: %s", result[1])
		}
		if hh > 12 && result[4] == "am" {
			return time.Time{}, fmt.Errorf("invalid time format: %v", day)
		}
		if hh == 12 && result[4] == "am" {
			hh = 0
		}
		if hh != 12 && result[4] == "pm" {
			hh += 12
		}
		if hh >= 24 {
			return time.Time{}, fmt.Errorf("invalid hour format: %s", result[1])
		}
		mm, err := strconv.ParseUint(result[2], 10, 64)
		if err != nil || mm >= 60 {
			return time.Time{}, fmt.Errorf("invalid hour format: %s", result[1])
		}
		var ss uint64 = 0
		var err1 error
		if result[3] != "" {
			ss, err1 = strconv.ParseUint(result[3], 10, 64)
			if err1 != nil || ss >= 60 {
				return time.Time{}, fmt.Errorf("invalid second format: %s", result[3])
			}
		}
		t := time.Date(now.Year(), now.Month(), now.Day(), int(hh), int(mm), int(ss), 0, now.Location())
		if t.Before(now) {
			t = t.Add(24 * time.Hour)
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid time format: %v", day)
}

func ParseDate(year string, month string, day string) (time.Time, error) {
	// note: if the date is before now, the return date's year will plus one
	now := time.Now()
	var yy uint64
	if year == "" {
		yy = uint64(now.Year())
	} else {
		var err error
		yy, err = strconv.ParseUint(year, 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid year format: %s", year)
		}
	}
	if len(year) == 2 {
		yy += 2000
	}
	mm, err := strconv.ParseUint(month, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid month format: %s", month)

	}
	dd, err := strconv.ParseUint(day, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid day format: %s", day)
	}
	t := time.Date(int(yy), time.Month(mm), int(dd), 0, 0, 0, 0, now.Location())
	if t.Month() != time.Month(mm) || t.Day() != int(dd) {
		return time.Time{}, fmt.Errorf("invalid date format: %04d-%02d-%02d", yy, mm, dd)
	}
	if year == "" && t.Before(now) {
		t = t.AddDate(1, 0, 0)
	}
	return t, nil
}

func ParseDateTime(date string) (time.Time, error) {
	var re *regexp.Regexp
	// handle YYYY-MM-DD
	re = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})$`)
	if result := re.FindStringSubmatch(date); result != nil {
		t, err := ParseDate(result[1], result[2], result[3])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid date format: %v", date)
		}
		return t, nil
	}
	// handle MM/DD[/YY]
	re = regexp.MustCompile(`^(\d{2})/(\d{2})(?:/(\d{2}))?$`)
	if result := re.FindStringSubmatch(date); result != nil {
		t, err := ParseDate(result[3], result[1], result[2])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid date format: %v", date)
		}
		return t, nil
	}
	// handle MMDD[YY]
	re = regexp.MustCompile(`^(\d{2})(\d{2})(\d{2})?$`)
	if result := re.FindStringSubmatch(date); result != nil {
		t, err := ParseDate(result[3], result[1], result[2])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid date format: %v", date)
		}
		return t, nil
	}
	// handle YYYY-MM-DD[THH:MM[:SS]]
	re = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})(?:T(.+))?$`)
	if result := re.FindStringSubmatch(date); result != nil {
		t, err := ParseDate(result[1], result[2], result[3])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid date format: %v", date)
		}
		if result[4] != "" {
			dayTime, err := ParseDayTime(result[4])
			if err != nil {
				return time.Time{}, fmt.Errorf("invalid date format: %v", date)
			}
			hh, mm, ss := dayTime.Clock()
			t = t.Add(time.Duration(hh)*time.Hour +
				time.Duration(mm)*time.Minute +
				time.Duration(ss)*time.Second)
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid date format: %v", date)
}

func ParseKeywordTime(keyword string) (time.Time, error) {
	// note: donot support tomorrowTnoon
	// only handle single keyword
	now := time.Now()
	t := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	parseWeekday := func(offset int) (time.Time, error) {
		offset = (offset - int(now.Weekday()) + 7) % 7
		if offset == 0 {
			offset = 7
		}
		return t.AddDate(0, 0, offset), nil
	}
	switch keyword {
	case "midnight":
		return t, nil
	case "elevenses":
		return t.Add(11 * time.Hour), nil
	case "noon":
		return t.Add(12 * time.Hour), nil
	case "fika":
		return t.Add(15 * time.Hour), nil
	case "teatime":
		return t.Add(16 * time.Hour), nil
	case "today":
		return t, nil
	case "tomorrow":
		return t.AddDate(0, 0, 1), nil
	case "sunday":
		return parseWeekday(int(time.Sunday))
	case "monday":
		return parseWeekday(int(time.Monday))
	case "tuesday":
		return parseWeekday(int(time.Tuesday))
	case "wednesday":
		return parseWeekday(int(time.Wednesday))
	case "thursday":
		return parseWeekday(int(time.Thursday))
	case "friday":
		return parseWeekday(int(time.Friday))
	case "saturday":
		return parseWeekday(int(time.Saturday))
	}
	return time.Time{}, fmt.Errorf("invalid keyword time: %s", keyword)
}

func ParseTime(ts string) (time.Time, error) {
	if strings.HasPrefix(ts, "now") {
		t := time.Time{}
		if ts == "now" {
			t = time.Now()
		} else if ts[3] == '+' {
			// '+' adds offset to Now()
			seconds, err := ParseRelativeTime(ts[4:])
			if err != nil {
				return t, fmt.Errorf("invalid duration '%v'", ts[4:])
			}
			t = time.Now().Add(time.Duration(seconds) * time.Second)
		} else if ts[3] == '-' {
			// '-' subtracts offset from Now()
			seconds, err := ParseRelativeTime(ts[4:])
			if err != nil {
				return t, fmt.Errorf("invalid duration '%v'", ts[4:])
			}
			t = time.Now().Add(-1 * time.Duration(seconds) * time.Second)
		} else {
			return t, fmt.Errorf("invalid time format")
		}
		return t.Round(0), nil
	}

	dayTime, err := ParseDayTime(ts)
	if err == nil {
		return dayTime, nil
	}

	dateTime, err := ParseDateTime(ts)
	if err == nil {
		return dateTime, nil
	}

	keywordTime, err := ParseKeywordTime(ts)
	if err == nil {
		return keywordTime, nil
	}

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

var allowedMailTypes = map[string]struct{}{
	"NONE":      {},
	"BEGIN":     {},
	"END":       {},
	"FAIL":      {},
	"TIMELIMIT": {},
	"OOM":       {},
	"ALL":       {},
}

func CheckMailType(mailtype string) bool {
	parts := strings.Split(mailtype, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if _, ok := allowedMailTypes[part]; !ok {
			return false
		}
	}
	return true
}

// CheckNodeList check if the node list is comma separated node names.
// The node name should contain only letters and numbers, and start with a letter, end with a number.
func CheckNodeList(nodeStr string) bool {
	nameStr := strings.ReplaceAll(nodeStr, " ", "")
	if nameStr == "" {
		return true
	}
	ValidHostnameRegex := "(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9-]*[A-Za-z0-9])"
	ValidHostListRegex := "^(" + ValidHostnameRegex + ")(," + ValidHostnameRegex + ")*$"
	re := regexp.MustCompile(ValidHostListRegex)
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

func CheckJobNameLength(name string) error {
	if len(name) > MaxJobNameLength {
		return fmt.Errorf("name is too long (up to %v)", MaxJobNameLength)
	}
	return nil
}

func CheckTaskArgs(task *protos.TaskToCtld) error {
	if err := CheckJobNameLength(task.Name); err != nil {
		return err
	}
	if task.CpusPerTask <= 0 {
		return fmt.Errorf("--cpus-per-task must > 0")
	}
	if task.NtasksPerNode <= 0 {
		return fmt.Errorf("--ntasks-per-node must > 0")
	}
	if task.NodeNum <= 0 {
		return fmt.Errorf("--nodes must > 0")
	}
	if task.TimeLimit.AsDuration() <= 0 {
		return fmt.Errorf("--time must > 0")
	}
	if !CheckNodeList(task.Nodelist) {
		return fmt.Errorf("invalid format for --nodelist")
	}
	if !CheckNodeList(task.Excludes) {
		return fmt.Errorf("invalid format for --exclude")
	}
	if task.ReqResources.AllocatableRes.CpuCoreLimit > 1e6 {
		return fmt.Errorf("requesting too many CPUs: %f", task.ReqResources.AllocatableRes.CpuCoreLimit)
	}
	if task.ExtraAttr != "" {
		// Check attrs in task.ExtraAttr, e.g., mail.type, mail.user
		mailtype := gjson.Get(task.ExtraAttr, "mail.type")
		mailuser := gjson.Get(task.ExtraAttr, "mail.user")
		if mailtype.Exists() != mailuser.Exists() {
			return fmt.Errorf("incomplete mail arguments")
		}
		if mailtype.Exists() && !CheckMailType(mailtype.String()) {
			return fmt.Errorf("invalid --mail-type")
		}
	}

	return nil
}

func CheckEntityName(name string) error {
	if name == "=" {
		return fmt.Errorf("name empty")
	}

	if len(name) > MaxEntityNameLength {
		return fmt.Errorf("name is too long (up to %v)", MaxEntityNameLength)
	}

	var validStringPattern = `^[a-zA-Z0-9][a-zA-Z0-9_]*$`
	re := regexp.MustCompile(validStringPattern)
	if !re.MatchString(name) {
		return fmt.Errorf("name can only contain letters, numbers or underscores")
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

func ParsePosNegList(posNegStr string) ([]string, []string, error) {
	posList := []string{}
	negList := []string{}

	if posNegStr == "" {
		return posList, negList, nil
	}

	posSet := make(map[string]bool)
	negSet := make(map[string]bool)

	hostList := strings.Split(posNegStr, ",")
	for _, host := range hostList {
		host = strings.TrimSpace(host)
		if host == "" {
			return nil, nil, fmt.Errorf("empty field")
		}
		if host[0] == '-' {
			negSet[host[1:]] = true
		} else if host[0] == '+' {
			posSet[host[1:]] = true
		} else {
			posSet[host] = true
		}
	}

	if len(posSet) < len(negSet) {
		for str := range posSet {
			if _, ok := negSet[str]; ok {
				return nil, nil, fmt.Errorf("item %s is in both positive and negative list", str)
			}
		}
	} else {
		for str := range negSet {
			if _, ok := posSet[str]; ok {
				return nil, nil, fmt.Errorf("item %s is in both positive and negative list", str)
			}
		}
	}

	for str := range posSet {
		posList = append(posList, str)
	}
	for str := range negSet {
		negList = append(negList, str)
	}
	return posList, negList, nil
}

func InvalidDuration() *durationpb.Duration {
	return &durationpb.Duration{
		Seconds: MaxJobTimeLimit,
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
			log.Errorf("Error parsing gres: %s\n", g)
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
	case "oom", "out-of-memory", "outofmemory", "o":
		return protos.TaskStatus_OutOfMemory, nil
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

func ParseStringParamList(parameters string, splitStr string) ([]string, error) {
	parameterList := strings.Split(parameters, splitStr)
	for i := 0; i < len(parameterList); i++ {
		if parameterList[i] == "" {
			return nil, fmt.Errorf("empty value")
		}
	}

	return parameterList, nil
}

func ParseJobIdList(jobIds string, splitStr string) ([]uint32, error) {
	jobIdStrList := strings.Split(jobIds, splitStr)
	var jobIdList []uint32
	for i := 0; i < len(jobIdStrList); i++ {
		jobId, err := strconv.ParseUint(jobIdStrList[i], 10, 32)
		if err != nil || jobId == 0 {
			return nil, fmt.Errorf("invalid job id \"%s\"", jobIdStrList[i])
		}
		jobIdList = append(jobIdList, uint32(jobId))
	}

	return jobIdList, nil
}

// StateToString converts a state value to a readable string
func StateToString(state int64) string {
	stateMap := map[int64]string{
		0: "Resume",
		1: "Drain",
	}
	if str, exists := stateMap[state]; exists {
		return str
	}
	return "Unknown"
}

func GetValidNodeList(CranedNodeList []ConfigNodesList) ([]string, error) {
	if len(CranedNodeList) == 0 {
		return nil, fmt.Errorf("nodes in config yaml file err")
	}

	nodeNameSet := make(map[string]struct{})
	var nodeNameList []string
	for _, cranedNode := range CranedNodeList {
		nodeNames, ok := ParseHostList(cranedNode.Name)
		if !ok || len(nodeNames) == 0 {
			continue
		}

		for _, nodeName := range nodeNames {
			if _, exists := nodeNameSet[nodeName]; !exists {
				nodeNameList = append(nodeNameList, nodeName)
				nodeNameSet[nodeName] = struct{}{}
			}
		}
	}

	if len(nodeNameList) == 0 {
		return nil, fmt.Errorf("no valid nodes found after parsing")
	}

	return nodeNameList, nil
}

// Merge two JSON strings.
// If there are overlapping keys, values from the second JSON take precedence.
func AmendJobExtraAttrs(origin, new string) string {
	if origin == "" {
		return new
	}

	result := gjson.Parse(new)
	result.ForEach(func(key, value gjson.Result) bool {
		var err error
		// Use sjson to set/override the value in the first JSON
		origin, err = sjson.Set(origin, key.String(), value.Value())
		return err == nil
	})

	return origin
}

type JobExtraAttrs struct {
	ExtraAttr string
	MailType  string
	MailUser  string
	Comment   string
}

func (j *JobExtraAttrs) Marshal(r *string) error {
	var err error

	extra := j.ExtraAttr
	if extra != "" && !gjson.Valid(extra) {
		return fmt.Errorf("invalid --extra-attr: invalid JSON string")
	}

	if j.MailType != "" {
		if !CheckMailType(j.MailType) {
			return fmt.Errorf("invalid --mail-type")
		}
		extra, err = sjson.Set(extra, "mail.type", j.MailType)
		if err != nil {
			return fmt.Errorf("invalid --mail-type: %v", err)
		}
	}

	if j.MailUser != "" {
		extra, err = sjson.Set(extra, "mail.user", j.MailUser)
		if err != nil {
			return fmt.Errorf("invalid --mail-user: %v", err)
		}
	}

	if j.Comment != "" {
		extra, err = sjson.Set(extra, "comment", j.Comment)
		if err != nil {
			return fmt.Errorf("invalid --comment: %v", err)
		}
	}

	*r = extra
	return nil
}

func ConvertSliceToString[T any](slice []T, sep string) string {
	str := make([]string, len(slice))
	for i, v := range slice {
		str[i] = fmt.Sprint(v)
	}
	return strings.Join(str, sep)
}

func ExtractExecNameFromArgs(args []string) string {
	for _, arg := range args {
		name := path.Base(arg)
		if name != "" {
			return name
		}
	}
	return "Interactive"
}

func FormatMemToMB(data uint64) string {
	var B2MBRatio uint64 = 1024 * 1024
	if data == 0 {
		return "0"
	} else {
		return fmt.Sprintf("%vM", data/B2MBRatio)
	}
}

var TxnActionMap = map[string]protos.TxnAction{
	"addaccount":    protos.TxnAction_AddAccount,
	"modifyaccount": protos.TxnAction_ModifyAccount,
	"deleteaccount": protos.TxnAction_DeleteAccount,
	"adduser":       protos.TxnAction_AddUser,
	"modifyuser":    protos.TxnAction_ModifyUser,
	"deleteuser":    protos.TxnAction_DeleteUser,
	"addqos":        protos.TxnAction_AddQos,
	"modifyqos":     protos.TxnAction_ModifyQos,
	"deleteqos":     protos.TxnAction_DeleteQos,
}

func StringToTxnAction(str string) (protos.TxnAction, bool) {
	if action, exists := TxnActionMap[str]; exists {
		return action, true
	}

	return 0, false
}
