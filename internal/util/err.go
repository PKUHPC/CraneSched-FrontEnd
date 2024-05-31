package util

type CraneCmdError = int

// general
const (
	ErrorSuccess       CraneCmdError = 0
	ErrorExecuteFailed CraneCmdError = 1
	ErrorCmdArg        CraneCmdError = 2
	ErrorGrpc          CraneCmdError = 3
)

// cbatch
const (
	ErrorCbatchScriptParsing CraneCmdError = 100
	ErrorCbatchAllocation    CraneCmdError = 101
)

// ccancel
const (
	ErrorCcancelFailed CraneCmdError = 100
)
