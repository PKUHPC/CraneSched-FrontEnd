package util

type CraneCmdError = int

// Do not use error code bigger than 127

// general
const (
	ErrorSuccess       CraneCmdError = 0
	ErrorGeneric       CraneCmdError = 1
	ErrorCmdArg        CraneCmdError = 2
	ErrorNetwork       CraneCmdError = 3
	ErrorBackend       CraneCmdError = 4
	ErrorInvalidFormat CraneCmdError = 5
)

// cacct
const ()

// cacctmgr
const ()

// cbatch
const ()

// ccancel
const ()

// ccontrol
const ()

// cinfo
const ()

// cqueue
const ()
