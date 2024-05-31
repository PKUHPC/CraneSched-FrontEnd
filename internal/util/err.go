package util

type CraneCmdError = int

// general
const (
	ErrorSuccess       CraneCmdError = 0
	ErrorExecuteFailed CraneCmdError = 1
	ErrorCmdArg        CraneCmdError = 2
	ErrorGrpc          CraneCmdError = 3
	ErrorBackEnd       CraneCmdError = 4
)

// cbatch
const (
	ErrorCbatchScriptParsing CraneCmdError = 100
)

// ccancel
const ()

// ccontrol
const ()
