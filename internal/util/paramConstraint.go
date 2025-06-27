package util

const (
	OpenModeAppend   = "append"
	OpenModeTruncate = "truncate"
)

const (
	QOSFlagDenyOnLimit uint32 = 1 << iota
)

var QoSFlagNameMap = map[string]uint32{
	"None":        0,
	"DenyOnLimit": QOSFlagDenyOnLimit,
}
