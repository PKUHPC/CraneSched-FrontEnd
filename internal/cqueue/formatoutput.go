package cqueue

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
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
	"c":             {"AllocCpus", ProcessAllocCpus},
	"alloccpus":     {"AllocCpus", ProcessAllocCpus},
	"C":             {"ReqCpus", ProcessReqCPUs},
	"reqcpus":       {"ReqCpus", ProcessReqCPUs},


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
	"X":          {"Exclusive", ProcessExclusive},
	"exclusive":  {"Exclusive", ProcessExclusive},
}

// FormatData formats the output data according to the format string.
// The format string can accept specifiers in the form of %.<width><format character>.
// Besides, it can contain prefix, padding and suffix strings, e.g.,
// "prefix%j_xx%t_x%.5L(Suffix)"
func FormatData(reply *protos.QueryTasksInfoReply) (header []string, tableData [][]string) {
	re := regexp.MustCompile(`%(\.\d+)?([a-zA-Z]+)`)
	specifiers := re.FindAllStringSubmatchIndex(FlagFormat, -1)
	if specifiers == nil {
		log.Errorln("Invalid format specifier.")
		os.Exit(util.ErrorInvalidFormat)
	}

	tableOutputWidth := make([]int, 0, len(specifiers))
	tableOutputHeader := make([]string, 0, len(specifiers))
	tableOutputCell := make([][]string, len(reply.TaskInfoList))

	// Get the prefix of the format string
	if specifiers[0][0] != 0 {
		prefix := FlagFormat[0:specifiers[0][0]]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, prefix)
		for j := 0; j < len(reply.TaskInfoList); j++ {
			tableOutputCell[j] = append(tableOutputCell[j], prefix)
		}
	}

	for i, spec := range specifiers {
		// Get the padding string between specifiers
		if i > 0 && spec[0]-specifiers[i-1][1] > 0 {
			padding := FlagFormat[specifiers[i-1][1]:spec[0]]
			tableOutputWidth = append(tableOutputWidth, -1)
			tableOutputHeader = append(tableOutputHeader, padding)
			for j := 0; j < len(reply.TaskInfoList); j++ {
				tableOutputCell[j] = append(tableOutputCell[j], padding)
			}
		}

		// Parse width specifier
		if spec[2] == -1 {
			// w/o width specifier
			tableOutputWidth = append(tableOutputWidth, -1)
		} else {
			// with width specifier
			width, err := strconv.ParseUint(FlagFormat[spec[2]+1:spec[3]], 10, 32)
			if err != nil {
				log.Errorln("Invalid width specifier.")
				os.Exit(util.ErrorInvalidFormat)
			}
			tableOutputWidth = append(tableOutputWidth, int(width))
		}

		// Parse format specifier
		field := FlagFormat[spec[4]:spec[5]]
		if len(field) > 1 {
			field = strings.ToLower(field)
		}

		//a-Account, c-AllocCPUs, C-ReqCpus, e-ElapsedTime, h-Held, j-JobID, l-TimeLimit, L-NodeList, k-Comment,
		//m-AllocMemPerNode, M-ReqMemPerNode, n-Name, N-NodeNum, p-Priority, P-Partition, q-Qos, Q-ReqCpuPerNode, r-ReqNodes,
		//R-Reason, s-SubmitTime, S-StartTime, t-State, T-JobType, u-User, U-Uid, x-ExcludeNodes, X-Exclusive.
		fieldProcessor, found := fieldMap[field]
		if !found {
			log.Errorf("Invalid format specifier or string : %s, string unfold case insensitive, reference:\n"+
				"a/Account, c/AllocCPUs, C/ReqCpus, e/ElapsedTime, h/Held, j/JobID, l/TimeLimit, L/NodeList, k/Comment,\n"+
				"m/AllocMemPerNode, M/ReqMemPerNode, n/Name, N/NodeNum, o/Command, p/Priority, P/Partition, q/Qos, Q/ReqCpuPerNode, r/ReqNodes,\n"+
				"R/Reason, s/SubmitTime, S/StartTime, t/State, T/JobType, u/User, U/Uid, x/ExcludeNodes, X/Exclusive.", field)
			os.Exit(util.ErrorInvalidFormat)
		}

		tableOutputHeader = append(tableOutputHeader, strings.ToUpper(fieldProcessor.header))
		for j, task := range reply.TaskInfoList {
			tableOutputCell[j] = append(tableOutputCell[j], fieldProcessor.process(task))
		}
	}

	// Get the suffix of the format string
	if len(FlagFormat)-specifiers[len(specifiers)-1][1] > 0 {
		suffix := FlagFormat[specifiers[len(specifiers)-1][1]:]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, suffix)
		for j := 0; j < len(reply.TaskInfoList); j++ {
			tableOutputCell[j] = append(tableOutputCell[j], suffix)
		}
	}

	return util.FormatTable(tableOutputWidth, tableOutputHeader, tableOutputCell)
}