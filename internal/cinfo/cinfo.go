package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

func cinfoFunc() {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)

	req := &protos.QueryClusterInfoRequest{
		FilterOnlyDownNodes:        FlagFilterDownOnly,
		FilterOnlyRespondingNodes:  FlagFilterRespondingOnly,
		OptionNodesOnCentricFormat: FlagNodesOnCentricFormat,
	}

	var stateList []protos.CranedState
	if FlagFilterCranedStates != "" {
		filterCranedStateList := strings.Split(strings.ToLower(FlagFilterCranedStates), ",")
		for i := 0; i < len(filterCranedStateList); i++ {
			switch filterCranedStateList[i] {
			case "idle":
				stateList = append(stateList, protos.CranedState_CRANE_IDLE)
			case "mix":
				stateList = append(stateList, protos.CranedState_CRANE_MIX)
			case "alloc":
				stateList = append(stateList, protos.CranedState_CRANE_ALLOC)
			case "down":
				stateList = append(stateList, protos.CranedState_CRANE_DOWN)
			default:
				fmt.Fprintf(os.Stderr, "Invalid state given: %s\n", filterCranedStateList[i])
				os.Exit(1)
			}
		}
		req.FilterCranedStates = stateList
	}

	if FlagFilterPartitions != "" {
		filterPartitionList := strings.Split(FlagFilterPartitions, ",")
		req.FilterPartitions = filterPartitionList
	}

	if FlagFilterNodes != "" {
		filterNodeList := strings.Split(FlagFilterNodes, ",")
		req.FilterNodes = filterNodeList
	}

	reply, err := stub.QueryClusterInfo(context.Background(), req)
	if err != nil {
		panic("QueryClusterInfo failed: " + err.Error())
	}

	table := tablewriter.NewWriter(os.Stdout)
	util.SetTableStyle(table)
	var tableData [][]string
	if FlagNodesOnCentricFormat {
		table.SetHeader([]string{"NODELIST", "NODES", "PARTITION", "STATE"})
		for _, partitionCraned := range reply.Partitions {
			for _, commonCranedStateList := range partitionCraned.CranedLists {
				if commonCranedStateList.Count > 0 {
					if strings.Contains(commonCranedStateList.CranedListRegex, ",") {
						for _, cranedlistregex := range strings.Split(commonCranedStateList.CranedListRegex, ",") {
							tableData = append(tableData, []string{
								cranedlistregex,
								"1",
								partitionCraned.Name,
								strings.ToLower(commonCranedStateList.State.String()[6:]),
							})
						}
					} else {
						tableData = append(tableData, []string{
							commonCranedStateList.CranedListRegex,
							strconv.FormatUint(uint64(commonCranedStateList.Count), 10),
							partitionCraned.Name,
							strings.ToLower(commonCranedStateList.State.String()[6:]),
						})
					}
				}
			}
		}
		sort.Slice(tableData, func(i, j int) bool {
			return tableData[i][0] < tableData[j][0]
		})

	} else {
		if FlagSummarize {
			table.SetHeader([]string{"PARTITION", "AVAIL", "TIMELIMIT", "NODES(A/I/O/T)", "NODELIST"})
			for _, partitionCraned := range reply.Partitions {
				tableData = append(tableData, []string{
					partitionCraned.Name,
					strings.ToLower(partitionCraned.State.String()[10:]),
					"infinite",
					partitionCraned.AbstractInfo,
					partitionCraned.CranedAbstractNodesRegex,
				})
			}
		} else {
			if FlagFormat != "" {
				var tableHeader []string
				var tableRow []string
				var tableOutputWidth []int
				var alphabets []string
				table.SetHeader(tableHeader)

				pattern := `^%(?:\.(\d+))?([a-zA-Z])(,.*)?$`
				re := regexp.MustCompile(pattern)
				items := strings.Split(FlagFormat, " ")
				for _, item := range items {
					if !re.MatchString(item) {
						fmt.Printf("Invalid format")
						os.Exit(1)
					}
					match := re.FindStringSubmatch(item)
					numberStr := match[1] // 可能为空字符串
					if numberStr != "" {
						number, err := strconv.Atoi(numberStr)
						if err == nil {
							tableOutputWidth = append(tableOutputWidth, number)
						}
					} else {
						tableOutputWidth = append(tableOutputWidth, -1)
					}
					letter := match[2]
					alphabets = append(alphabets, string(letter))
				}
				for _, partitionCraned := range reply.Partitions {
					tableRow = []string{}
					tableHeader = []string{}
					for i := 0; i < len(alphabets); i++ {
						switch alphabets[i] {
						case "P":
							tableRow = append(tableRow, partitionCraned.Name)
							tableHeader = append(tableHeader, "PARTITION")
						case "F":
							tableRow = append(tableRow, partitionCraned.AbstractInfo)
							tableHeader = append(tableHeader, "NODES(A/I/O/T)")
						}
					}
					for _, commonCranedStateList := range partitionCraned.CranedLists {
						if commonCranedStateList.Count > 0 {
							for i := 0; i < len(alphabets); i++ {
								switch alphabets[i] {
								case "a":
									tableRow = append(tableRow, strings.ToLower(partitionCraned.State.String()[10:]))
									tableHeader = append(tableHeader, "AVAIL")
								case "D":
									tableRow = append(tableRow, strconv.FormatUint(uint64(commonCranedStateList.Count), 10))
									tableHeader = append(tableHeader, "NODES")
								case "N":
									tableRow = append(tableRow, commonCranedStateList.CranedListRegex)
									tableHeader = append(tableHeader, "NODELIST")
								case "l":
									tableRow = append(tableRow, "infinite")
									tableHeader = append(tableHeader, "TIMELIMIT")
								}
							}
						}
					}
					tableData = append(tableData, tableRow)
				}
				util.FormatTable(tableOutputWidth, tableHeader, tableData)
				table.SetHeader(tableHeader)

			} else {
				table.SetHeader([]string{"PARTITION", "AVAIL", "TIMELIMIT", "NODES", "STATE", "NODELIST"})
				for _, partitionCraned := range reply.Partitions {
					for _, commonCranedStateList := range partitionCraned.CranedLists {
						if commonCranedStateList.Count > 0 {
							tableData = append(tableData, []string{
								partitionCraned.Name,
								strings.ToLower(partitionCraned.State.String()[10:]),
								"infinite",
								strconv.FormatUint(uint64(commonCranedStateList.Count), 10),
								strings.ToLower(commonCranedStateList.State.String()[6:]),
								commonCranedStateList.CranedListRegex,
							})
						}
					}
				}
			}
		}
	}
	table.AppendBulk(tableData)
	if len(tableData) == 0 {
		fmt.Printf("No partition is available.\n")
	} else {
		table.Render()
	}
}

func loopedQuery(iterate uint64) {
	interval, _ := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	for {
		fmt.Println(time.Now().String()[0:19])
		cinfoFunc()
		time.Sleep(time.Duration(interval.Nanoseconds()))
		fmt.Println()
	}
}
