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

// cacct
const (
	ErrorCacctInvalidFormat CraneCmdError = 100
)

// cbatch
const (
	ErrorCbatchScriptParsing CraneCmdError = 200
)

// ccancel
const ()

// ccontrol
const ()

// cinfo
const ()

// cqueue
const (
	ErrorCqueueInvalidFormat CraneCmdError = 300
)
