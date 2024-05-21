package util

type CraneCmdError = int

const (
	ErrorSuccess       CraneCmdError = 0
	ErrorCmdArg        CraneCmdError = 1
	ErrorAllocation    CraneCmdError = 2
	ErrorGrpc          CraneCmdError = 3
	ErrorScriptParsing CraneCmdError = 4
)
