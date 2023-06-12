package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"os"
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
				log.Fatalf("Invalid state given: %s\n", filterCranedStateList[i])
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
	util.SetBorderlessTable(table)
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
				alphabets, tableOutputWidth, err := util.ParseFormatFlag(FlagFormat)
				if err != nil {
					fmt.Printf("Invalid format: %v\n", err)
					os.Exit(1)
				}
				alphaIndex := util.InitAlphabetIndex(alphabets)
				tableHeader := make([]string, len(alphabets))
				for _, partitionCraned := range reply.Partitions {
					tableRow := make([]string, len(alphabets))
					for _, alpha := range alphabets {
						for _, idx := range alphaIndex[string(alpha)] {
							if idx >= len(tableHeader) {
								break
							}
							switch alpha {
							case "P":
								tableHeader[idx] = "PARTITION"
								tableRow[idx] = partitionCraned.Name
							case "F":
								tableHeader[idx] = "NODES(A/I/O/T)"
								tableRow[idx] = partitionCraned.AbstractInfo
							case "a":
								tableHeader[idx] = "AVAIL"
								tableRow[idx] = strings.ToLower(partitionCraned.State.String()[10:])
							case "D":
								tableHeader[idx] = "NODES"
								NodelistInfoSplit := strings.Split(partitionCraned.AbstractInfo, "/")
								if len(NodelistInfoSplit) > 0 {
									lastInfo := NodelistInfoSplit[len(NodelistInfoSplit)-1]
									tableRow[idx] = lastInfo
								}
							case "l":
								tableHeader[idx] = "TIMELIMIT"
								tableRow[idx] = "infinite"
							default:
								fmt.Printf("Invalid alphabet: %s\n", alpha)
								os.Exit(1)
							}
						}
					}
					tableData = append(tableData, tableRow)
				}
				util.FormatTable(tableOutputWidth, tableHeader, tableData)
				table.SetHeader(tableHeader)

			} else {
				if FlagFilterDownOnly {
					table.SetHeader([]string{"PARTITION", "AVAIL", "TIMELIMIT", "NODES", "STATE", "NODELIST"})
					for _, partitionCraned := range reply.Partitions {
						abstractInfo := strings.Split(partitionCraned.AbstractInfo, "/")
						if abstractInfo[2] != abstractInfo[3] {
							tableData = append(tableData, []string{
								partitionCraned.Name,
								strings.ToLower(partitionCraned.State.String()[10:]),
								"infinite",
								"n/a",
								"0",
								"",
							})
						}
						for _, commonCranedStateList := range partitionCraned.CranedLists {
							if strings.ToLower(commonCranedStateList.State.String()[6:]) == "down" && commonCranedStateList.Count > 0 {
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
