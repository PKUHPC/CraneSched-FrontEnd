package cqueue

import (
	"CraneFrontEnd/internal/util"
	"fmt"
	"sync"
)

var flagRegistry = new(sync.Map)

func RegisterFlag(name string, handler FlagHandler) {
	flagRegistry.Store(name, handler)
}

type FlagHandler interface {
	Parse(value string) (interface{}, error)
	Error(msg string, err error) error
}

// FlagFilterJobNames, FlagFilterUsers, FlagFilterQos  FlagFilterAccounts, FlagFilterPartitions
type StringHandler struct{}
func (h *StringHandler) Parse(value string) (interface{}, error) {
	return util.ParseStringParamList(value, ",")
}
func (h *StringHandler) Error(msg string, err error) error {
	return util.GetCraneError(util.ErrorCmdArg, fmt.Sprintf("msg: %s", err))
}

// FlagFilterJobIDs
type JobIDHandler struct{}
func (h *JobIDHandler) Parse(value string) (interface{}, error) {
	return util.ParseJobIdList(value, ",")
}
func (h *JobIDHandler) Error(msg string, err error) error {
	return util.GetCraneError(util.ErrorCmdArg, fmt.Sprintf("%s %s",msg,err))
}

// FlagFilterStates
type StatesHandler struct{}
func (h *StatesHandler) Parse(value string) (interface{}, error) {
	return util.ParseInRamTaskStatusList(value)
}
func (h *StatesHandler) Error(msg string, err error) error {
	return util.GetCraneError(util.ErrorCmdArg, err.Error())
}