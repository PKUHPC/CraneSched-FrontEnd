package util

type CraneCmdError = int

const (
	ErrorSuccess       CraneCmdError = 0
	ErrorCmdArgError   CraneCmdError = 1
	ErrorAllocateError CraneCmdError = 2
	ErrorGrpcError     CraneCmdError = 3
)
