package util

type CraneCmdError = int

// general
const (
	ErrorSuccess            CraneCmdError = 0
	ErrorExecuteFailed      CraneCmdError = 1
	ErrorCmdArg             CraneCmdError = 2
	ErrorGrpc               CraneCmdError = 3
	ErrorBackEnd            CraneCmdError = 4
	ErrorInvalidTableFormat CraneCmdError = 5
)

// cacct
const ()

// cacctmgr
const (
	ErrorCacctmgrUserNotFound CraneCmdError = 200
)

// cbatch
const (
	ErrorCbatchScriptParsing CraneCmdError = 300
)

// ccancel
const ()

// ccontrol
const ()

// cinfo
const ()

// cqueue
const ()
