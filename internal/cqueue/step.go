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

package cqueue

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

// StepData holds both stepInfo and parent task for processing
type StepData struct {
	stepInfo *protos.StepInfo
	task     *protos.TaskInfo
}

// QueryStepsTableOutput displays step-level information in table format
// Columns: STEPID, NAME, PARTITION, USER, TIME, NODELIST
func QueryStepsTableOutput(reply *protos.QueryTasksInfoReply) error {
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderlessTable(table)

	// Collect all steps from all tasks
	var stepDataList []StepData
	for _, task := range reply.TaskInfoList {
		for _, stepInfo := range task.StepInfoList {
			stepDataList = append(stepDataList, StepData{
				stepInfo: stepInfo,
				task:     task,
			})
		}
	}

	var header []string
	var tableData [][]string

	// Support custom format
	if FlagFormat != "" {
		header, tableData = FormatStepData(stepDataList)
		table.SetTablePadding("")
		table.SetAutoFormatHeaders(false)
	} else {
		// Default format
		header = []string{"StepId", "Name", "Partition", "User", "Time", "NODELIST"}

		for _, stepData := range stepDataList {
			stepInfo := stepData.stepInfo
			task := stepData.task

			stepIdStr := fmt.Sprintf("%d.%d", stepInfo.JobId, stepInfo.StepId)

			name := stepInfo.Name

			partition := task.Partition

			user := task.Username

			var timeStr string
			if stepInfo.Status == protos.TaskStatus_Running {
				if stepInfo.ElapsedTime != nil {
					timeStr = util.SecondTimeFormat(stepInfo.ElapsedTime.Seconds)
				} else {
					timeStr = "0:00"
				}
			} else {
				timeStr = "-"
			}

			nodeList := stepInfo.GetCranedList()

			tableData = append(tableData, []string{
				stepIdStr,
				name,
				partition,
				user,
				timeStr,
				nodeList,
			})
		}
	}

	if !FlagNoHeader {
		table.SetHeader(header)
	}

	if !FlagFull && FlagFormat == "" {
		util.TrimTable(&tableData)
	}

	table.AppendBulk(tableData)
	table.Render()
	return nil
}

// Step field processors
func ProcessStepId(stepData StepData) string {
	return fmt.Sprintf("%d.%d", stepData.stepInfo.JobId, stepData.stepInfo.StepId)
}

func ProcessStepName(stepData StepData) string {
	return stepData.stepInfo.Name
}

func ProcessStepPartition(stepData StepData) string {
	return stepData.task.Partition
}

func ProcessStepUser(stepData StepData) string {
	return stepData.task.Username
}

func ProcessStepUid(stepData StepData) string {
	return strconv.FormatUint(uint64(stepData.stepInfo.Uid), 10)
}

func ProcessStepElapsedTime(stepData StepData) string {
	if stepData.stepInfo.Status == protos.TaskStatus_Running {
		if stepData.stepInfo.ElapsedTime != nil {
			return util.SecondTimeFormat(stepData.stepInfo.ElapsedTime.Seconds)
		}
		return "0:00"
	}
	return "-"
}

func ProcessStepNodeList(stepData StepData) string {
	return stepData.stepInfo.GetCranedList()
}

func ProcessStepState(stepData StepData) string {
	return stepData.stepInfo.Status.String()
}

func ProcessStepTimeLimit(stepData StepData) string {
	if stepData.stepInfo.TimeLimit.Seconds >= util.InvalidDuration().Seconds {
		return "unlimited"
	}
	return util.SecondTimeFormat(stepData.stepInfo.TimeLimit.Seconds)
}

func ProcessStepNodeNum(stepData StepData) string {
	return strconv.FormatUint(uint64(stepData.stepInfo.NodeNum), 10)
}

func ProcessStepAccount(stepData StepData) string {
	return stepData.task.Account
}

func ProcessStepQoS(stepData StepData) string {
	return stepData.task.Qos
}

func ProcessStepCommand(stepData StepData) string {
	return stepData.stepInfo.CmdLine
}

func ProcessStepJobId(stepData StepData) string {
	return strconv.FormatUint(uint64(stepData.stepInfo.JobId), 10)
}

type StepFieldProcessor struct {
	header  string
	process func(stepData StepData) string
}

var stepFieldMap = map[string]StepFieldProcessor{
	// Step-specific fields
	"i":      {"StepId", ProcessStepId},
	"stepid": {"StepId", ProcessStepId},

	"j":     {"JobId", ProcessStepJobId},
	"jobid": {"JobId", ProcessStepJobId},

	"n":    {"Name", ProcessStepName},
	"name": {"Name", ProcessStepName},

	"P":         {"Partition", ProcessStepPartition},
	"partition": {"Partition", ProcessStepPartition},

	"u":    {"User", ProcessStepUser},
	"user": {"User", ProcessStepUser},

	"U":   {"Uid", ProcessStepUid},
	"uid": {"Uid", ProcessStepUid},

	"e":           {"ElapsedTime", ProcessStepElapsedTime},
	"elapsedtime": {"ElapsedTime", ProcessStepElapsedTime},

	"L":        {"NodeList", ProcessStepNodeList},
	"nodelist": {"NodeList", ProcessStepNodeList},

	"t":     {"State", ProcessStepState},
	"state": {"State", ProcessStepState},

	"l":         {"TimeLimit", ProcessStepTimeLimit},
	"timelimit": {"TimeLimit", ProcessStepTimeLimit},

	"N":       {"NodeNum", ProcessStepNodeNum},
	"nodenum": {"NodeNum", ProcessStepNodeNum},

	"a":       {"Account", ProcessStepAccount},
	"account": {"Account", ProcessStepAccount},

	"q":   {"QoS", ProcessStepQoS},
	"qos": {"QoS", ProcessStepQoS},

	"o":       {"Command", ProcessStepCommand},
	"command": {"Command", ProcessStepCommand},
}

// FormatStepData formats the step output data according to the format string
func FormatStepData(stepDataList []StepData) (header []string, tableData [][]string) {
	re := regexp.MustCompile(`%(\.\d+)?([a-zA-Z]+)`)
	specifiers := re.FindAllStringSubmatchIndex(FlagFormat, -1)
	if specifiers == nil {
		log.Errorln("Invalid format specifier.")
		os.Exit(util.ErrorInvalidFormat)
	}

	tableOutputWidth := make([]int, 0, len(specifiers))
	tableOutputHeader := make([]string, 0, len(specifiers))
	tableOutputCell := make([][]string, len(stepDataList))

	// Get the prefix of the format string
	if specifiers[0][0] != 0 {
		prefix := FlagFormat[0:specifiers[0][0]]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, prefix)
		for j := 0; j < len(stepDataList); j++ {
			tableOutputCell[j] = append(tableOutputCell[j], prefix)
		}
	}

	for i, spec := range specifiers {
		// Get the padding string between specifiers
		if i > 0 && spec[0]-specifiers[i-1][1] > 0 {
			padding := FlagFormat[specifiers[i-1][1]:spec[0]]
			tableOutputWidth = append(tableOutputWidth, -1)
			tableOutputHeader = append(tableOutputHeader, padding)
			for j := 0; j < len(stepDataList); j++ {
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

		fieldProcessor, found := stepFieldMap[field]
		if !found {
			log.Errorf("Invalid format specifier or string for step: %s, string unfold case insensitive, reference:\n"+
				"i/StepId, j/JobId, n/Name, P/Partition, u/User, U/Uid, e/ElapsedTime, L/NodeList, t/State, l/TimeLimit, N/NodeNum, a/Account, q/QoS, o/Command", field)
			os.Exit(util.ErrorInvalidFormat)
		}

		tableOutputHeader = append(tableOutputHeader, strings.ToUpper(fieldProcessor.header))
		for j, stepData := range stepDataList {
			tableOutputCell[j] = append(tableOutputCell[j], fieldProcessor.process(stepData))
		}
	}

	// Get the suffix of the format string
	if len(FlagFormat)-specifiers[len(specifiers)-1][1] > 0 {
		suffix := FlagFormat[specifiers[len(specifiers)-1][1]:]
		tableOutputWidth = append(tableOutputWidth, -1)
		tableOutputHeader = append(tableOutputHeader, suffix)
		for j := 0; j < len(stepDataList); j++ {
			tableOutputCell[j] = append(tableOutputCell[j], suffix)
		}
	}

	return util.FormatTable(tableOutputWidth, tableOutputHeader, tableOutputCell)
}
