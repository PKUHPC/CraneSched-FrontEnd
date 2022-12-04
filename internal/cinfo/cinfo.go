package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"strconv"
	"strings"
	"time"
)

func cinfoFun() {

	config := util.ParseConfig()

	serverAddr := fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("Cannot connect to CraneCtld: " + err.Error())
	}

	partitionList := strings.Split(partitions, ",")
	nodeList := strings.Split(nodes, ",")
	stateList := strings.Split(strings.ToLower(states), ",")

	stub := protos.NewCraneCtldClient(conn)
	req := &protos.QueryClusterInfoRequest{
		QueryDownNodes:       dead,
		QueryRespondingNodes: responding,
	}

	if partitions != "" {
		req.SetQueryPartitions = partitionList
	} else if nodes != "" {
		req.SetQueryNodes = nodeList
	} else if states != "" {
		req.SetQueryStates = stateList
	}

	reply, err := stub.QueryClusterInfo(context.Background(), req)
	if err != nil {
		panic("QueryClusterInfo failed: " + err.Error())
	}

	if len(reply.PartitionCraned) == 0 {
		fmt.Printf("No partition is available.\n")
	} else {
		fmt.Printf("PARTITION   AVAIL  TIMELIMIT  NODES  STATE  NODELIST\n")
		for _, partitionCraned := range reply.PartitionCraned {
			for _, commonCranedStateList := range partitionCraned.CommonCranedStateList {
				if commonCranedStateList.CranedNum > 0 {
					fmt.Printf("%9s%8s%11s%7d%7s  %v\n", partitionCraned.Name, partitionCraned.State.String(), "infinite", commonCranedStateList.CranedNum, commonCranedStateList.State.String(), commonCranedStateList.CranedListRegex)
				}
			}
		}
	}
}

func IterateQuery(iterate uint64) {
	iter, _ := time.ParseDuration(strconv.FormatUint(iterate, 10) + "s")
	for {
		cinfoFun()
		time.Sleep(time.Duration(iter.Nanoseconds()))
	}
}
