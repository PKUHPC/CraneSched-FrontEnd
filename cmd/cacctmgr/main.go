package main

import (
	"CraneFrontEnd/cmd/cacctmgr/cacctmgr"
	"CraneFrontEnd/cmd/cacctmgr/cmd"
)

func main() {
	cacctmgr.Init()
	cmd.Execute()
}
