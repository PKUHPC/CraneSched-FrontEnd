package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// 'a' group
func ProcessAccount(task *protos.TaskInfo) string {
	return task.Account
}

// 'c' group
func ProcessAllocCpus(task *protos.TaskInfo) string {
	return strconv.FormatFloat(task.AllocatedResView.AllocatableRes.CpuCoreLimit, 'f', 2, 64)
}

// 'C' group
func ProcessReqCpuPerNode(task *protos.TaskInfo) string {
	return strconv.FormatFloat(task.ReqResView.AllocatableRes.CpuCoreLimit, 'f', 2, 64)
}

// 'e' group
func ProcessElapsedTime(task *protos.TaskInfo) string {
	if task.Status == protos.TaskStatus_Running {
		return util.SecondTimeFormat(task.ElapsedTime.Seconds)
	}
	return "-"
}

// 'h' group
func ProcessHeld(task *protos.TaskInfo) string {
	return strconv.FormatBool(task.Held)
}

// 'j' group
func ProcessJobId(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.TaskId), 10)
}

// 'k' group
func ProcessComment(task *protos.TaskInfo) string {
	if !gjson.Valid(task.ExtraAttr) {
		return ""
	}
	return gjson.Get(task.ExtraAttr, "comment").String()
}

// 'l' group
func ProcessTimeLimit(task *protos.TaskInfo) string {
	if task.TimeLimit.Seconds >= util.InvalidDuration().Seconds {
		return "unlimited"
	}
	return util.SecondTimeFormat(task.TimeLimit.Seconds)
}

func ProcessNodeList(task *protos.TaskInfo) string {
	return task.GetCranedList()
}

// 'm' group
func ProcessAllocMemPerNode(task *protos.TaskInfo) string {
	if task.NodeNum == 0 {
		return "0"
	}
	return util.FormatMemToMB(task.AllocatedResView.AllocatableRes.MemoryLimitBytes /
		uint64(task.NodeNum))
}

// 'M' group
func ProcessReqMemPerNode(task *protos.TaskInfo) string {
	return util.FormatMemToMB(task.ReqResView.AllocatableRes.MemoryLimitBytes)
}

// 'n' group
func ProcessName(task *protos.TaskInfo) string {
	return task.Name
}

func ProcessNodeNum(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.NodeNum), 10)
}

// 'o' group
func ProcessCommand(task *protos.TaskInfo) string {
	return task.CmdLine
}

// 'p' group
func ProcessPriority(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.Priority), 10)
}

func ProcessPartition(task *protos.TaskInfo) string {
	return task.Partition
}

// 'q' group
func ProcessQoS(task *protos.TaskInfo) string {
	return task.Qos
}

// 'Q' group
func ProcessReqCPUs(task *protos.TaskInfo) string {
	return strconv.FormatFloat(task.ReqResView.AllocatableRes.CpuCoreLimit*float64(task.NodeNum), 'f', 2, 64)
}

// 'r' group
func ProcessReqNodes(task *protos.TaskInfo) string {
	return strings.Join(task.ReqNodes, ",")
}

func ProcessReason(task *protos.TaskInfo) string {
	if task.Status == protos.TaskStatus_Pending {
		return task.GetPendingReason()
	}
	return " "
}

// 's' group
func ProcessSubmitTime(task *protos.TaskInfo) string {
	submitTime := task.SubmitTime.AsTime()
	if !submitTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) {
		return submitTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return "unknown"
}

func ProcessStartTime(task *protos.TaskInfo) string {
	startTime := task.StartTime.AsTime()
	if !startTime.Before(time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)) && startTime.Before(time.Now()) {
		return startTime.In(time.Local).Format("2006-01-02 15:04:05")
	}
	return "unknown"
}

// 't' group
func ProcessState(task *protos.TaskInfo) string {
	return task.Status.String()
}

func ProcessJobType(task *protos.TaskInfo) string {
	return task.Type.String()
}

// 'u' group
func ProcessUser(task *protos.TaskInfo) string {
	return task.Username
}

func ProcessUid(task *protos.TaskInfo) string {
	return strconv.FormatUint(uint64(task.Uid), 10)
}

// 'x' group
func ProcessExcludeNodes(task *protos.TaskInfo) string {
	return strings.Join(task.ExcludeNodes, ",")
}

// 'X' group
func ProcessExclusive(task *protos.TaskInfo) string {
	return strconv.FormatBool(task.Exclusive)
}

type FieldProcessor struct {
	header  string
	process func(task *protos.TaskInfo) string
}

var fieldMap = map[string]FieldProcessor{
	// 'a' group
	"a":       {"Account", ProcessAccount},
	"account": {"Account", ProcessAccount},

	// 'c' group
	"c":         {"AllocCpus", ProcessAllocCpus},
	"alloccpus": {"AllocCpus", ProcessAllocCpus},
	"C":         {"ReqCpus", ProcessReqCPUs},
	"reqcpus":   {"ReqCpus", ProcessReqCPUs},

	// 'e' group
	"e":           {"ElapsedTime", ProcessElapsedTime},
	"elapsedtime": {"ElapsedTime", ProcessElapsedTime},

	// 'h' group
	"h":    {"Held", ProcessHeld},
	"held": {"Held", ProcessHeld},

	// 'j' group
	"j":     {"JobId", ProcessJobId},
	"jobid": {"JobId", ProcessJobId},

	// 'k' group
	"k":       {"Comment", ProcessComment},
	"comment": {"Comment", ProcessComment},

	// 'l' group
	"l":         {"TimeLimit", ProcessTimeLimit},
	"timelimit": {"TimeLimit", ProcessTimeLimit},
	"L":         {"NodeList(Reason)", ProcessNodeList},
	"nodelist":  {"NodeList(Reason)", ProcessNodeList},

	// 'm' group
	"m":               {"AllocMemPerNode", ProcessAllocMemPerNode},
	"allocmempernode": {"AllocMemPerNode", ProcessAllocMemPerNode},
	"M":               {"ReqMemPerNode", ProcessReqMemPerNode},
	"reqmempernode":   {"ReqMemPerNode", ProcessReqMemPerNode},

	// 'n' group
	"n":       {"Name", ProcessName},
	"name":    {"Name", ProcessName},
	"N":       {"NodeNum", ProcessNodeNum},
	"nodenum": {"NodeNum", ProcessNodeNum},

	"o":       {"Command", ProcessCommand},
	"command": {"Command", ProcessCommand},
	// 'p' group
	"p":         {"Priority", ProcessPriority},
	"priority":  {"Priority", ProcessPriority},
	"P":         {"Partition", ProcessPartition},
	"partition": {"Partition", ProcessPartition},

	// 'q' group
	"q":             {"QoS", ProcessQoS},
	"qos":           {"QoS", ProcessQoS},
	"Q":             {"ReqCpuPerNode", ProcessReqCpuPerNode},
	"reqcpupernode": {"ReqCpuPerNode", ProcessReqCpuPerNode},

	// 'r' group
	"r":        {"ReqNodes", ProcessReqNodes},
	"reqnodes": {"ReqNodes", ProcessReqNodes},
	"R":        {"Reason", ProcessReason},
	"reason":   {"Reason", ProcessReason},

	// 's' group
	"s":          {"SubmitTime", ProcessSubmitTime},
	"submittime": {"SubmitTime", ProcessSubmitTime},
	"S":          {"StartTime", ProcessStartTime},
	"starttime":  {"StartTime", ProcessStartTime},

	// 't' group
	"t":       {"State", ProcessState},
	"state":   {"State", ProcessState},
	"T":       {"JobType", ProcessJobType},
	"jobtype": {"JobType", ProcessJobType},

	// 'u' group
	"u":    {"User", ProcessUser},
	"user": {"User", ProcessUser},
	"U":    {"Uid", ProcessUid},
	"uid":  {"Uid", ProcessUid},

	// 'x' group
	"x":            {"ExcludeNodes", ProcessExcludeNodes},
	"excludenodes": {"ExcludeNodes", ProcessExcludeNodes},
	"X":            {"Exclusive", ProcessExclusive},
	"exclusive":    {"Exclusive", ProcessExclusive},
}

type segmentType int

const (
	textSegment      segmentType = iota // 0
	specifierSegment                    // 1
)

type formatSegment struct {
	segmentType segmentType
	content     string
	width       int
}
type tableBuilder struct {
	widths  []int
	headers []string
	cells   [][]string
}

func NewTableBuilder(rowCount int) *tableBuilder {
	return &tableBuilder{
		cells: make([][]string, rowCount),
	}
}
func BuildColumns(builder *tableBuilder, reply *protos.QueryTasksInfoReply, segments []formatSegment) error {
	for _, seg := range segments {
		switch seg.segmentType {
		case textSegment:
			AddTextColumn(builder, seg.content)
		case specifierSegment:
			if err := AddSpecifierColumn(builder, reply, seg); err != nil {
				return err
			}
		}
	}
	return nil
}
func AddTextColumn(builder *tableBuilder, text string) {
	builder.widths = append(builder.widths, -1)
	builder.headers = append(builder.headers, text)
	for j := range builder.cells {
		builder.cells[j] = append(builder.cells[j], text)
	}
}
func AddSpecifierColumn(builder *tableBuilder, reply *protos.QueryTasksInfoReply, seg formatSegment) error {
	key := seg.content  
	if len(key) > 1 {  
		key = strings.ToLower(key)  
	}  
	processor, found := fieldMap[key]
	if !found {
		return fmt.Errorf("invalid format specifier: %s", key)
	}

	builder.widths = append(builder.widths, seg.width)
	builder.headers = append(builder.headers, strings.ToUpper(processor.header))

	for j, task := range reply.TaskInfoList {
		builder.cells[j] = append(builder.cells[j], processor.process(task))
	}
	return nil
}
func ParseFormatSegments(format string, re *regexp.Regexp) ([]formatSegment, error) {
	specifiers := re.FindAllStringSubmatchIndex(format, -1)
	if specifiers == nil {
		return nil, fmt.Errorf("invalid format specifier")
	}

	var segments []formatSegment
	if start := specifiers[0][0]; start != 0 {
		segments = append(segments, formatSegment{
			segmentType: textSegment,
			content:     format[0:start],
		})
	}

	for i, spec := range specifiers {
		if i > 0 {
			prevEnd := specifiers[i-1][1]
			if gap := spec[0] - prevEnd; gap > 0 {
				segments = append(segments, formatSegment{
					segmentType: textSegment,
					content:     format[prevEnd:spec[0]],
				})
			}
		}
		width := -1
		if spec[2] != -1 {
			widthVal, err := strconv.ParseUint(format[spec[2]+1:spec[3]], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid width specifier")
			}
			width = int(widthVal)
		}
		field := format[spec[4]:spec[5]]
		segments = append(segments, formatSegment{
			segmentType: specifierSegment,
			content:     field,
			width:       width,
		})
	}

	if lastSpec := specifiers[len(specifiers)-1]; len(format) > lastSpec[1] {
		segments = append(segments, formatSegment{
			segmentType: textSegment,
			content:     format[lastSpec[1]:],
		})
	}

	return segments, nil
}

// FormatData formats the output data according to the format string
// format : "%j" or "%.5j" or  "ID:%j"  or "ID:%5j"    
// cqueue -o="ID:%.4j | Status:%t | user:%u
// ID:JOBID | Status:STATE   | user:USER
// ID:44    | Status:Running | user:internthree
// ID:45    | Status:Running | user:internthree
func FormatData(reply *protos.QueryTasksInfoReply) (header []string, tableData [][]string) {
	var formatSpecRegex = regexp.MustCompile(`%(\.\d+)?([a-zA-Z]+)`)

	segments, err := ParseFormatSegments(FlagFormat, formatSpecRegex)
	if err != nil {
		log.Errorln(err)
		os.Exit(util.ErrorInvalidFormat)
	}

	builder := NewTableBuilder(len(reply.TaskInfoList))
	if err := BuildColumns(builder, reply, segments); err != nil {
		log.Errorln(err)
		os.Exit(util.ErrorInvalidFormat)
	}

	return util.FormatTable(builder.widths, builder.headers, builder.cells)
}
