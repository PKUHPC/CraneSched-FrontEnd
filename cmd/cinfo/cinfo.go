package main

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"google.golang.org/grpc"
)

func main() {

	path := "/etc/crane/config.yaml"
	config := util.ParseConfig(path)

	serverAddr := fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		panic("Cannot connect to CraneCtld: " + err.Error())
	}

	stub := protos.NewCraneCtldClient(conn)
	req := &protos.QueryClusterInfoRequest{}

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
					fmt.Printf("%9s%8s%11s%7d%7s  %v\n", partitionCraned.Name, partitionCraned.State.String(), "infinite", commonCranedStateList.CranedNum, commonCranedStateList.State, commonCranedStateList.CranedList)
				}
			}
		}
	}

}
