package cinfo

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

var (
	resourceStateMap = map[string]protos.CranedResourceState{
		"idle":  protos.CranedResourceState_CRANE_IDLE,
		"mix":   protos.CranedResourceState_CRANE_MIX,
		"alloc": protos.CranedResourceState_CRANE_ALLOC,
		"down":  protos.CranedResourceState_CRANE_DOWN,
	}

	controlStateMap = map[string]protos.CranedControlState{
		"none":  protos.CranedControlState_CRANE_NONE,
		"drain": protos.CranedControlState_CRANE_DRAIN,
	}

	powerStateMap = map[string]protos.CranedPowerState{
		"active":     protos.CranedPowerState_CRANE_POWER_ACTIVE,
		"power-idle": protos.CranedPowerState_CRANE_POWER_IDLE,
		"sleeping":   protos.CranedPowerState_CRANE_POWER_SLEEPING,
		"poweredoff": protos.CranedPowerState_CRANE_POWER_POWEREDOFF,
		"off":        protos.CranedPowerState_CRANE_POWER_POWEREDOFF,
		"to-sleep":   protos.CranedPowerState_CRANE_POWER_TO_SLEEPING,
		"waking":     protos.CranedPowerState_CRANE_POWER_WAKING_UP,
		"oning":      protos.CranedPowerState_CRANE_POWER_POWERING_ON,
		"offing":     protos.CranedPowerState_CRANE_POWER_POWERING_OFF,
	}
)

func ConvertStates[T comparable](stateMap map[string]T, stateType string) ([]T, error) {
	var states []T
	for _, stateStr := range FlagFilterCranedStates {
		if state, exists := stateMap[strings.ToLower(stateStr)]; exists {
			states = append(states, state)
		} else {
			return nil, util.GetCraneError(util.ErrorCmdArg, fmt.Sprintf("Invalid %s state given: %s", stateType, stateStr))
		}
	}

	return states, nil
}

func ConvertToResourceStates() ([]protos.CranedResourceState, error) {
	return ConvertStates(resourceStateMap, "resource")
}
func ConvertToControlStates() ([]protos.CranedControlState, error) {
	return ConvertStates(controlStateMap, "control")
}
func ConvertToPowerStates() ([]protos.CranedPowerState, error) {
	return ConvertStates(powerStateMap, "power")
}

func ApplyResourceStateDefaults(states []protos.CranedResourceState) []protos.CranedResourceState {
	if len(states) > 0 {
		return states
	}
	if FlagFilterRespondingOnly {
		return []protos.CranedResourceState{
			protos.CranedResourceState_CRANE_IDLE,
			protos.CranedResourceState_CRANE_MIX,
			protos.CranedResourceState_CRANE_ALLOC,
		}
	}
	if FlagFilterDownOnly {
		return []protos.CranedResourceState{protos.CranedResourceState_CRANE_DOWN}
	}
	return []protos.CranedResourceState{
		protos.CranedResourceState_CRANE_IDLE,
		protos.CranedResourceState_CRANE_MIX,
		protos.CranedResourceState_CRANE_ALLOC,
		protos.CranedResourceState_CRANE_DOWN,
	}
}
func ApplyControlStateDefaults(states []protos.CranedControlState) []protos.CranedControlState {
	if len(states) > 0 {
		return states
	}
	return []protos.CranedControlState{
		protos.CranedControlState_CRANE_NONE,
		protos.CranedControlState_CRANE_DRAIN,
	}
}
func ApplyPowerStateDefaults(states []protos.CranedPowerState) []protos.CranedPowerState {
	if len(states) > 0 {
		return states
	}
	return []protos.CranedPowerState{
		protos.CranedPowerState_CRANE_POWER_ACTIVE,
		protos.CranedPowerState_CRANE_POWER_IDLE,
		protos.CranedPowerState_CRANE_POWER_SLEEPING,
		protos.CranedPowerState_CRANE_POWER_POWEREDOFF,
		protos.CranedPowerState_CRANE_POWER_TO_SLEEPING,
		protos.CranedPowerState_CRANE_POWER_WAKING_UP,
		protos.CranedPowerState_CRANE_POWER_POWERING_ON,
		protos.CranedPowerState_CRANE_POWER_POWERING_OFF,
	}
}

func BuildFilteredStringList(items []string, warnMsg string) []string {
	var list []string
	for _, item := range items {
		if item != "" {
			list = append(list, item)
		} else {
			log.Warn(warnMsg)
		}
	}
	return list
}

func FillReqByFilterFlag() (*protos.QueryClusterInfoRequest, error) {
	resourceStateList, err := ConvertToResourceStates()
	if err != nil {
		return nil, err
	}
	controlStateList, err := ConvertToControlStates()
	if err != nil {
		return nil, err
	}
	powerStateList, err := ConvertToPowerStates()
	if err != nil {
		return nil, err
	}

	resourceStateList = ApplyResourceStateDefaults(resourceStateList)
	controlStateList = ApplyControlStateDefaults(controlStateList)
	powerStateList = ApplyPowerStateDefaults(powerStateList)

	nodeList := BuildFilteredStringList(FlagFilterNodes, "Empty node name is ignored.")
	partList := BuildFilteredStringList(FlagFilterPartitions, "Empty partition name is ignored.")

	req := &protos.QueryClusterInfoRequest{
		FilterPartitions:           partList,
		FilterNodes:                nodeList,
		FilterCranedResourceStates: resourceStateList,
		FilterCranedControlStates:  controlStateList,
		FilterCranedPowerStates:    powerStateList,
	}

	return req, nil
}

func QueryClusterInfo() (*protos.QueryClusterInfoReply, error) {
	config := util.ParseConfig(FlagConfigFilePath)
	stub := util.GetStubToCtldByConfig(config)

	req, err := FillReqByFilterFlag()
	if err != nil {
		return &protos.QueryClusterInfoReply{}, err
	}
	reply, err := stub.QueryClusterInfo(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query cluster information")
		return nil, &util.CraneError{Code: util.ErrorNetwork}
	}

	return reply, nil
}
