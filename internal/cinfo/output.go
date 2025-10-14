package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
)

// Define the flattened data structure
type FlattenedData struct {
	PartitionName   string
	Avail           string
	CranedListRegex string
	ResourceState   string
	ControlState    string
	PowerState      string
	CranedListCount uint64
}

func GetFlattenedData(partitionCraned *protos.TrimmedPartitionInfo ,
					  commonCranedStateList *protos.TrimmedPartitionInfo_TrimmedCranedInfo) FlattenedData {
	return FlattenedData{
		PartitionName:   partitionCraned.Name,
		Avail:           strings.ToLower(partitionCraned.State.String()[10:]),
		CranedListRegex: commonCranedStateList.CranedListRegex,
		ResourceState:   strings.ToLower(commonCranedStateList.ResourceState.String()[6:]),
		ControlState:    strings.ToLower(commonCranedStateList.ControlState.String()[6:]),
		PowerState:      strings.ToLower(commonCranedStateList.PowerState.String()[6:]),
		CranedListCount: uint64(commonCranedStateList.Count),
	}
}
func GetValidFlattendData(partitionCraned *protos.TrimmedPartitionInfo) FlattenedData {
	return FlattenedData{
		PartitionName:   partitionCraned.Name,
		Avail:           strings.ToLower(partitionCraned.State.String()[10:]),
		CranedListRegex: "",
		ResourceState:   "n/a",
		ControlState:    "",
		PowerState:      "",
		CranedListCount: 0,
	}
}

// Flatten the nested structure into a one-dimensional array
func FlattenReplyData(reply *protos.QueryClusterInfoReply) []FlattenedData {
	var flattened []FlattenedData
	var partitionInValid []FlattenedData
	var partitionFilterValid bool

	for _, partitionCraned := range reply.Partitions {
		partitionFilterValid = false
		for _, commonCranedStateList := range partitionCraned.CranedLists {
			if commonCranedStateList.Count > 0 {
				partitionFilterValid = true
				flattened = append(flattened,GetFlattenedData(partitionCraned, commonCranedStateList))
			}
		}
		if !partitionFilterValid {
			partitionInValid = append(partitionInValid,GetValidFlattendData(partitionCraned))
		}
	}

	flattened = append(flattened, partitionInValid...)
	return flattened
}

type FieldProcessor struct {
	header  string
	process func(flattened []FlattenedData, tableOutputCell [][]string)
}

var fieldMap = map[string]FieldProcessor{
	"p":         {"Partition", ProcessPartition},
	"partition": {"Partition", ProcessPartition},
	"a":         {"Avail", ProcessAvail},
	"avail":     {"Avail", ProcessAvail},
	"n":         {"Nodes", ProcessNodes},
	"nodes":     {"Nodes", ProcessNodes},
	"s":         {"State", ProcessState},
	"state":     {"State", ProcessState},
	"l":         {"NodeList", ProcessNodeList},
	"nodelist":  {"NodeList", ProcessNodeList},
}

// / Partition
func ProcessPartition(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], data.PartitionName)
	}
}

// Avail
func ProcessAvail(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], data.Avail)
	}
}

// Nodes
func ProcessNodes(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], strconv.FormatUint(data.CranedListCount, 10))
	}
}

// State
func ProcessState(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		stateStr := data.ResourceState
		if data.ControlState != "" && data.ControlState != "none" {
			stateStr += "(" + data.ControlState + ")"
		}

		if data.ResourceState == "down" && (data.PowerState == "power_idle" || data.PowerState == "power_active") {
			stateStr += "[failed]"
		} else if data.PowerState != "" {
			stateStr += "[" + data.PowerState + "]"
		}
		tableOutputCell[idx] = append(tableOutputCell[idx], stateStr)
	}
}

// NodeList
func ProcessNodeList(flattened []FlattenedData, tableOutputCell [][]string) {
	for idx, data := range flattened {
		tableOutputCell[idx] = append(tableOutputCell[idx], data.CranedListRegex)
	}
}

func ParseBySpec(specifiers [][]int, reply *protos.QueryClusterInfoReply) ([]int, []string, [][]string,error) {
	tableOutputWidth := make([]int, 0, len(specifiers))
	tableOutputHeader := make([]string, 0, len(specifiers))
	flattened := FlattenReplyData(reply)
	tableLen := len(flattened)
	tableOutputCell := make([][]string, tableLen)

	// Get the prefix of the format string
	if specifiers[0][0] != 0 {
		prefix := FlagFormat[0:specifiers[0][0]]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, prefix)
		for j := 0; j < tableLen; j++ {
			tableOutputCell[j] = append(tableOutputCell[j], prefix)
		}
	}

	for i, spec := range specifiers {
		// Get the padding string between specifiers
		if i > 0 && spec[0]-specifiers[i-1][1] > 0 {
			padding := FlagFormat[specifiers[i-1][1]:spec[0]]
			tableOutputWidth = append(tableOutputWidth, -1)
			tableOutputHeader = append(tableOutputHeader, padding)
			for j := 0; j < tableLen; j++ {
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
				return nil, nil, nil, &util.CraneError{
					Code:    util.ErrorInvalidFormat,
					Message: "Invalid width specifier.",
				}
			}
			tableOutputWidth = append(tableOutputWidth, int(width))
		}

		// Parse format specifier
		field := FlagFormat[spec[4]:spec[5]]
		if len(field) > 1 {
			field = strings.ToLower(field)
		}

		if processor, exists := fieldMap[field]; exists {
			tableOutputHeader = append(tableOutputHeader, strings.ToUpper(processor.header))
			processor.process(flattened, tableOutputCell)
		} else {
			return nil, nil, nil, &util.CraneError{
				Code: util.ErrorInvalidFormat,
				Message: fmt.Sprintf("Invalid format specifier or string: %s, string unfold case insensitive, reference:\n"+
					"p/Partition, a/Avail, n/Nodes, s/State, l/NodeList.", field),
			}
		}
	}
	// Get the suffix of the format string
	if len(FlagFormat)-specifiers[len(specifiers)-1][1] > 0 {
		suffix := FlagFormat[specifiers[len(specifiers)-1][1]:]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, suffix)
		for j := 0; j < tableLen; j++ {
			tableOutputCell[j] = append(tableOutputCell[j], suffix)
		}
	}

	return tableOutputWidth, tableOutputHeader, tableOutputCell, nil
}

func FormatData(reply *protos.QueryClusterInfoReply) (header []string, tableData [][]string, err error) {
	re := regexp.MustCompile(`%(\.\d+)?([a-zA-Z]+)`)
	specifiers := re.FindAllStringSubmatchIndex(FlagFormat, -1)
	if specifiers == nil {
		return nil, nil, util.NewCraneErr(util.ErrorInvalidFormat,"Invalid format specifier.") 
	}

	tableOutputWidth, tableOutputHeader, tableOutputCell, err:= ParseBySpec(specifiers, reply)
	if  err != nil {
		return nil,nil,err
	}

	formattedHeader, formattedData := util.FormatTable(tableOutputWidth, tableOutputHeader, tableOutputCell)
	return formattedHeader, formattedData, nil
}

func GetInvalidMeg(partitionCraned *protos.TrimmedPartitionInfo) ([]string) {
	return []string {
		partitionCraned.Name,
		strings.ToLower(partitionCraned.State.String()[10:]),
		"0",
		"n/a",
		"",
	}
}

func BuildStateString(cranedList *protos.TrimmedPartitionInfo_TrimmedCranedInfo) string {
    stateStr := strings.ToLower(cranedList.ResourceState.String()[6:])

    if cranedList.ControlState != protos.CranedControlState_CRANE_NONE {
        controlState := strings.ToLower(cranedList.ControlState.String()[6:])
        stateStr += "(" + controlState + ")"
    }

    powerStateSuffix := "[" + strings.ToLower(cranedList.PowerState.String()[6:]) + "]"
    if cranedList.ResourceState == protos.CranedResourceState_CRANE_DOWN &&
         (cranedList.PowerState == protos.CranedPowerState_CRANE_POWER_IDLE ||
          cranedList.PowerState == protos.CranedPowerState_CRANE_POWER_ACTIVE) {
        powerStateSuffix = "[failed]"
    }
    stateStr += powerStateSuffix

    return stateStr
}

func CreateValidPartitionRow(partition *protos.TrimmedPartitionInfo, cranedList *protos.TrimmedPartitionInfo_TrimmedCranedInfo) []string {
    stateStr := BuildStateString(cranedList)
    return []string{
        partition.Name,
        strings.ToLower(partition.State.String()[10:]),
        strconv.FormatUint(uint64(cranedList.Count), 10),
        stateStr,
        cranedList.CranedListRegex,
    }
}

func PartitionDeal(partitionCraned *protos.TrimmedPartitionInfo,tableData *[][]string) bool {
	hasValidCraned := false
    for _, cranedList := range partitionCraned.CranedLists {
        if cranedList.Count > 0 {
            hasValidCraned = true
            *tableData = append(*tableData, CreateValidPartitionRow(partitionCraned, cranedList))
        }
    }
    return hasValidCraned
}

func FindtableDataByReply(reply *protos.QueryClusterInfoReply) ([][]string) {
	var partitionInValid [][]string
	var tableData [][]string

	for _,partitionCraned := range reply.Partitions {
		if hasValid := PartitionDeal(partitionCraned,&tableData); !hasValid {
			partitionInValid = append(partitionInValid,GetInvalidMeg(partitionCraned))
		}
	}

	tableData = append(tableData, partitionInValid...)
	return tableData
}

func FillTable(reply *protos.QueryClusterInfoReply ,table *tablewriter.Table) error {
	header := []string{"PARTITION", "AVAIL", "NODES", "STATE", "NODELIST"}
	var err error
	tableData := FindtableDataByReply(reply)
	if FlagFormat != "" {
		header, tableData, err = FormatData(reply)
		if err != nil {
			return err
		}
		table.SetTablePadding("")
		table.SetAutoFormatHeaders(false)
	}

	table.AppendBulk(tableData)
	if !FlagNoHeader {
		table.SetHeader(header)
	}
	if len(tableData) == 0 {
		log.Info("No matching partitions were found for the given filter.")
	} else {
		table.Render()
	}

	return nil
}

func GetFoundedNodes(reply *protos.QueryClusterInfoReply) map[string]bool {
	replyNodes := ""
	for _, partitionCraned := range reply.Partitions {
		for _, commonCranedStateList := range partitionCraned.CranedLists {
			if commonCranedStateList.Count > 0 {
				if replyNodes != "" {
					replyNodes += ","
				}
				replyNodes += commonCranedStateList.CranedListRegex
			}
		}
	}
	replyNodes_, _ := util.ParseHostList(replyNodes)

	foundedNodes := make(map[string]bool)
	for _, node := range replyNodes_ {
		foundedNodes[node] = true
	}
	return foundedNodes
}

func GetNodeList() []string {
	var nodeList []string
	for _, node := range FlagFilterNodes {
		if node == "" {
			log.Warn("Empty node name is ignored.")
			continue
		}
		nodeList = append(nodeList, node)
	}
	return nodeList
}

func ExtraDealNodeList(reply *protos.QueryClusterInfoReply) {
	var redList []string
	requestedNodes_, _ := util.ParseHostList(strings.Join(GetNodeList(), ","))
	for _, node := range requestedNodes_ {
		if _, exist := GetFoundedNodes(reply)[node]; !exist {
			redList = append(redList, node)
		}
	}
	if len(redList) > 0 {
		log.Infof("Requested nodes do not exist or do not meet the given filter condition: %s.",
			util.HostNameListToStr(redList))
	}
}

func QueryTableOutput(reply *protos.QueryClusterInfoReply) error {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)

	if err := FillTable(reply, table); err != nil {  
        return err  
    } 

	if len(FlagFilterNodes) != 0 {
		ExtraDealNodeList(reply)
	}
	return nil
}

func JsonOutput(reply * protos.QueryClusterInfoReply) error {
	fmt.Println(util.FmtJson.FormatReply(reply))
	if reply.GetOk() {
		return nil
	} else {
		return &util.CraneError{Code: util.ErrorBackend}
	}
}
