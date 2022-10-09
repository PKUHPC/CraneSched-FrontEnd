package main

import (
	"CraneFrontEnd/internal/cqueue"
	"CraneFrontEnd/internal/util"
	"fmt"
	"os"
)

func main() {
	findAll := true
	partitionName := ""
	if len(os.Args) > 1 {
		findAll = false
		partitionName = os.Args[1]
	}

	config := util.ParseConfig()

	serverAddr := fmt.Sprintf("%s:%s", config.ControlMachine, config.CraneCtldListenPort)

	cqueue.Query(serverAddr, partitionName, findAll)
}
