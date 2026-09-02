package main

import (
	"errors"
	"sync"
	"time"
)

var (
	errStaleNodeGeneration = errors.New("stale node generation")
	// The node's definition is current but it has no runtime info, e.g. the
	// power tool registration failed for lack of a BMC mapping.
	errNodeNotTracked = errors.New("node not tracked by power manager")
)

type NodeState string

const (
	Active      NodeState = "active"
	Idle        NodeState = "idle"
	Sleep       NodeState = "sleep"
	PoweredOff  NodeState = "powered_off"
	ToSleeping  NodeState = "to_sleeping"
	Wakingup    NodeState = "waking_up"
	PoweringOn  NodeState = "powering_on"
	PoweringOff NodeState = "powering_off"
)

type NodeInfo struct {
	Exclude             bool
	State               NodeState
	LastStateChangeTime time.Time
	Jobs                map[string]struct{}
	Generation          uint64
	Revision            uint64
}

type nodeVersion struct {
	generation uint64
	revision   uint64
	updatedAt  time.Time
}

type nodeOperationLock struct {
	mutex sync.Mutex
	users int
}

type PredictionResponse struct {
	Prediction int    `json:"prediction"`
	Error      string `json:"error,omitempty"`
}

type NetworkInterface struct {
	MAC string
	IP  string
}

type PowerTool interface {
	RegisterNode(nodeID string, interfaces []NetworkInterface) error
	UnregisterNode(nodeID string)
	GetPowerState(nodeID string) (bool, error)
	CheckNodeAlive(nodeID string) bool
	HasNetworkInfo(nodeID string) bool

	WakeUp(nodeID string) error
	Sleep(nodeID string) error
	PowerOn(nodeID string) error
	PowerOff(nodeID string) error
}
