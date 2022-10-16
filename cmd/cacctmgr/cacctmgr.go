package main

import (
	cacctmgr "CraneFrontEnd/internal/cacctmgr"
)

func main() {
	cacctmgr.Init()
	cacctmgr.ParseCmdArgs()
}
