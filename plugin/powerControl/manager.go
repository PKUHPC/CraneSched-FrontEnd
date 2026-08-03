package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
)

const (
	maxRetries    = 3
	retryInterval = 5 * time.Second
)

type PowerManager struct {
	config    *Config
	powerTool PowerTool

	nodesInfo      sync.Map
	nodesInfoMutex sync.Mutex

	ctldClient      protos.CraneCtldClient
	ctldClientMutex sync.Mutex

	stopChan chan struct{}
}

type PredictionRequest struct {
	TotalNodes int `json:"total_nodes"`
}

type nodeSnapshot struct {
	nodeID              string
	lastStateChangeTime time.Time
}

func sortByLastStateChange(nodes []nodeSnapshot) {
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].lastStateChangeTime.Equal(nodes[j].lastStateChangeTime) {
			return nodes[i].nodeID < nodes[j].nodeID
		}
		return nodes[i].lastStateChangeTime.Before(nodes[j].lastStateChangeTime)
	})
}

func NewPowerManager(config *Config) *PowerManager {
	manager := &PowerManager{
		config:    config,
		powerTool: NewIPMITool(config),
		stopChan:  make(chan struct{}),
	}

	manager.initCtldClient()

	manager.startPowerStateMonitor()

	// Log NodeStateChangeFile configuration status
	if config.PowerControl.NodeStateChangeFile == "" {
		log.Info("NodeStateChangeFile is not configured - node state change recording is disabled")
	} else {
		log.Infof("NodeStateChangeFile is configured - node state changes will be recorded to: %s", config.PowerControl.NodeStateChangeFile)
	}

	return manager
}

func cloneJobs(jobs map[string]struct{}) map[string]struct{} {
	if jobs == nil {
		return nil
	}
	result := make(map[string]struct{}, len(jobs))
	for jobID := range jobs {
		result[jobID] = struct{}{}
	}
	return result
}

func (c *PowerManager) RegisterNode(nodeID string, initialState NodeState, jobs map[string]struct{}) bool {
	now := time.Now()
	hasJobSnapshot := jobs != nil
	jobs = cloneJobs(jobs)

	c.nodesInfoMutex.Lock()
	if value, exists := c.nodesInfo.Load(nodeID); exists {
		info := value.(*NodeInfo)
		if !hasJobSnapshot {
			jobs = info.Jobs
		}
		if initialState == Idle && len(jobs) != 0 {
			initialState = Active
		}
		stateChanged := info.State != initialState
		lastStateChangeTime := info.LastStateChangeTime
		if stateChanged {
			lastStateChangeTime = now
		}
		c.nodesInfo.Store(nodeID, &NodeInfo{
			Exclude:             info.Exclude,
			State:               initialState,
			LastStateChangeTime: lastStateChangeTime,
			Jobs:                jobs,
		})
		c.nodesInfoMutex.Unlock()
		if stateChanged {
			logNodeStateChange(nodeID, info.State, initialState)
			c.recordStateChange(now, nodeID, info.State, initialState)
		}
		return false
	}
	if jobs == nil {
		jobs = make(map[string]struct{})
	}
	if initialState == Idle && len(jobs) != 0 {
		initialState = Active
	}

	isExcluded := false
	for _, excludeNode := range c.config.IPMI.ExcludeNodes {
		if excludeNode == nodeID {
			isExcluded = true
			break
		}
	}

	c.nodesInfo.Store(nodeID, &NodeInfo{
		Exclude:             isExcluded,
		State:               initialState,
		LastStateChangeTime: now,
		Jobs:                jobs,
	})
	c.nodesInfoMutex.Unlock()
	c.recordStateChange(now, nodeID, "", initialState)

	if isExcluded {
		log.Infof("Initialized node %s in %s state (excluded from power management)", nodeID, initialState)
	} else {
		log.Infof("Initialized node %s in %s state", nodeID, initialState)
	}
	return true
}

func (c *PowerManager) RemoveNode(nodeID string) {
	c.nodesInfoMutex.Lock()
	if _, exists := c.nodesInfo.LoadAndDelete(nodeID); !exists {
		c.nodesInfoMutex.Unlock()
		return
	}
	c.nodesInfoMutex.Unlock()
	c.powerTool.UnregisterNode(nodeID)
	log.Infof("Removed node %s from power management", nodeID)
}

func (c *PowerManager) SetNodeExclude(nodeID string, exclude bool) error {
	c.nodesInfoMutex.Lock()
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		c.nodesInfoMutex.Unlock()
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	c.nodesInfo.Store(nodeID, &NodeInfo{
		Exclude:             exclude,
		State:               info.State,
		LastStateChangeTime: info.LastStateChangeTime,
		Jobs:                info.Jobs,
	})
	c.nodesInfoMutex.Unlock()

	if exclude {
		log.Infof("Node %s is now excluded from power management", nodeID)
	} else {
		log.Infof("Node %s is now included in power management", nodeID)
	}

	return nil
}

func (c *PowerManager) StartAutoPowerManager() {
	go func() {
		ticker := time.NewTicker(time.Duration(c.config.PowerControl.CheckIntervalSeconds) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				wake, on, sleep, off := c.start()
				log.Infof("Auto power control executed: wake=%d, on=%d, sleep=%d, off=%d",
					wake, on, sleep, off)
			case <-c.stopChan:
				return
			}
		}
	}()
}

func (c *PowerManager) StopAutoPowerManager() {
	close(c.stopChan)
}

func (c *PowerManager) AddJobToNode(nodeID string, jobID string) bool {
	return c.updateNodeJob(nodeID, jobID, true)
}

func (c *PowerManager) RemoveJobFromNode(nodeID string, jobID string) bool {
	return c.updateNodeJob(nodeID, jobID, false)
}

func (c *PowerManager) initCtldClient() {
	configPath := util.DefaultConfigPath
	config := util.ParseConfig(configPath)
	c.ctldClient = util.GetStubToCtldByConfig(config)
}

func (c *PowerManager) start() (int, int, int, int) {
	nodesToWake, nodesToPowerOn, nodesToSleep, nodesToPowerOff := c.makeDecision()

	if !c.config.PowerControl.EnableSleep {
		if len(nodesToWake) > 0 {
			log.Warnf("EnableSleep is disabled, Try to power on nodes: %v", nodesToWake)
			nodesToPowerOn = append(nodesToPowerOn, nodesToWake...)
			nodesToWake = nil
		}

		if len(nodesToSleep) > 0 {
			log.Warnf("EnableSleep is disabled, Try to power off nodes: %v", nodesToSleep)
			nodesToPowerOff = append(nodesToPowerOff, nodesToSleep...)
			nodesToSleep = nil
		}
	}

	if len(nodesToWake) > 0 {
		if err := c.wakeupNodes(nodesToWake); err != nil {
			log.Errorf("Failed to wake up nodes: %v", err)
		}
	}

	if len(nodesToPowerOn) > 0 {
		if err := c.powerOnNodes(nodesToPowerOn); err != nil {
			log.Errorf("Failed to power on nodes: %v", err)
		}
	}

	if len(nodesToSleep) > 0 {
		if err := c.sleepNodes(nodesToSleep); err != nil {
			log.Errorf("Failed to sleep nodes: %v", err)
		}
	}

	if len(nodesToPowerOff) > 0 {
		if err := c.powerOffNodes(nodesToPowerOff); err != nil {
			log.Errorf("Failed to power off nodes: %v", err)
		}
	}

	return len(nodesToWake), len(nodesToPowerOn),
		len(nodesToSleep), len(nodesToPowerOff)
}

func (c *PowerManager) updateNodeJob(nodeID string, jobID string, isAdd bool) bool {
	c.nodesInfoMutex.Lock()
	value, exists := c.nodesInfo.Load(nodeID)

	if !exists {
		c.nodesInfoMutex.Unlock()
		log.Errorf("Node %s not found in nodesInfo, please check the node registration", nodeID)
		return false
	}

	info := value.(*NodeInfo)
	if info.State != Active && info.State != Idle {
		c.nodesInfoMutex.Unlock()
		log.Errorf("Node %s is not in Active or Idle state, please check the node state", nodeID)
		return false
	}

	jobs := cloneJobs(info.Jobs)
	if jobs == nil {
		jobs = make(map[string]struct{})
	}

	if isAdd {
		log.Debugf("Add job %s to node %s", jobID, nodeID)
		jobs[jobID] = struct{}{}
	} else {
		log.Debugf("Remove job %s from node %s", jobID, nodeID)
		delete(jobs, jobID)
	}

	newState := Idle
	if len(jobs) != 0 {
		newState = Active
	}
	stateChanged := info.State != newState
	now := time.Now()
	lastStateChangeTime := info.LastStateChangeTime
	if stateChanged {
		lastStateChangeTime = now
	}
	c.nodesInfo.Store(nodeID, &NodeInfo{
		Exclude:             info.Exclude,
		State:               newState,
		LastStateChangeTime: lastStateChangeTime,
		Jobs:                jobs,
	})
	c.nodesInfoMutex.Unlock()

	if stateChanged {
		logNodeStateChange(nodeID, info.State, newState)
		c.recordStateChange(now, nodeID, info.State, newState)
		c.notifyCtldPowerStateChange(nodeID, newState)
	}
	return true
}

func logNodeStateChange(nodeID string, oldState NodeState, newState NodeState) {
	// Idle and Active transitions occur for every job allocation and completion.
	if (oldState == Idle && newState == Active) || (oldState == Active && newState == Idle) {
		log.Debugf("Node %s state changed from %s to %s", nodeID, oldState, newState)
	} else {
		log.Infof("Node %s state changed from %s to %s", nodeID, oldState, newState)
	}
}

func (c *PowerManager) updateNodeStateIfCurrent(nodeID string, expected *NodeInfo, newState NodeState) *NodeInfo {
	c.nodesInfoMutex.Lock()
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		c.nodesInfoMutex.Unlock()
		log.Warnf("Node %s not found in nodesInfo, please check the node registration", nodeID)
		return nil
	}

	info := value.(*NodeInfo)
	if info != expected || info.State == newState {
		c.nodesInfoMutex.Unlock()
		return nil
	}

	now := time.Now()
	updated := &NodeInfo{
		Exclude:             info.Exclude,
		State:               newState,
		LastStateChangeTime: now,
		Jobs:                info.Jobs,
	}
	c.nodesInfo.Store(nodeID, updated)
	c.nodesInfoMutex.Unlock()

	logNodeStateChange(nodeID, info.State, newState)
	c.recordStateChange(now, nodeID, info.State, newState)
	c.notifyCtldPowerStateChange(nodeID, newState)
	return updated
}

func (c *PowerManager) notifyCtldPowerStateChange(nodeID string, state NodeState) {
	c.sendCtldPowerStateChange(nodeID, state, true)
}

func (c *PowerManager) reportCtldPowerStateChange(nodeID string, state NodeState) {
	c.sendCtldPowerStateChange(nodeID, state, false)
}

func (c *PowerManager) sendCtldPowerStateChange(nodeID string, state NodeState, onlyIfCurrent bool) {
	c.ctldClientMutex.Lock()
	defer c.ctldClientMutex.Unlock()

	if onlyIfCurrent {
		value, exists := c.nodesInfo.Load(nodeID)
		if !exists || value.(*NodeInfo).State != state {
			return
		}
	}

	if c.ctldClient == nil {
		c.initCtldClient()
	}

	var powerType protos.CranedPowerState
	switch state {
	case Active:
		powerType = protos.CranedPowerState_CRANE_POWER_ACTIVE
	case Idle:
		powerType = protos.CranedPowerState_CRANE_POWER_IDLE
	case Sleep:
		powerType = protos.CranedPowerState_CRANE_POWER_SLEEPING
	case PoweredOff:
		powerType = protos.CranedPowerState_CRANE_POWER_POWEREDOFF
	case ToSleeping:
		powerType = protos.CranedPowerState_CRANE_POWER_TO_SLEEPING
	case Wakingup:
		powerType = protos.CranedPowerState_CRANE_POWER_WAKING_UP
	case PoweringOn:
		powerType = protos.CranedPowerState_CRANE_POWER_POWERING_ON
	case PoweringOff:
		powerType = protos.CranedPowerState_CRANE_POWER_POWERING_OFF
	default:
		log.Errorf("Unknown node state: %s", state)
		return
	}

	req := &protos.PowerStateChangeRequest{
		CranedId: nodeID,
		State:    powerType,
		Reason:   fmt.Sprintf("Node %s power state changed to %s by power manager(plugin/powerControl)", nodeID, state),
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			log.Warnf("Retrying PowerStateChange for node %s, attempt %d/%d", nodeID, attempt, maxRetries)
			time.Sleep(retryInterval)
			c.initCtldClient()
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		reply, err := c.ctldClient.PowerStateChange(ctx, req)
		cancel()

		if err != nil {
			log.Warnf("Attempt %d/%d: Failed to call PowerStateChange RPC for node %s: %v",
				attempt, maxRetries, nodeID, err)

			if attempt == maxRetries {
				log.Errorf("All retry attempts failed for PowerStateChange RPC for node %s", nodeID)
			}
			continue
		}

		if !reply.Ok {
			log.Errorf("Failed to update node %s power state in CraneCtld", nodeID)
		} else {
			// Log at Debug level for routine Idle <-> Active transitions, Info for power state changes
			if state == Idle || state == Active {
				log.Debugf("Successfully updated node %s power state to %s in CraneCtld", nodeID, powerType)
			} else {
				log.Infof("Successfully updated node %s power state to %s in CraneCtld", nodeID, powerType)
			}
		}

		return
	}
}

func (c *PowerManager) getPredictedActiveNodeCount(totalNodes int) int {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	reqBody := PredictionRequest{
		TotalNodes: totalNodes,
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		log.Errorf("Failed to marshal prediction request: %v", err)
		return -1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/predict", c.config.Predictor.URL),
		bytes.NewBuffer(jsonData))
	if err != nil {
		log.Errorf("Failed to create request: %v", err)
		return -1
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Failed to get prediction: %v", err)
		return -1
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("Failed to read response body: %v", err)
		return -1
	}

	if resp.StatusCode != http.StatusOK {
		log.Errorf("Prediction request failed with status %d: %s", resp.StatusCode, string(body))
		return -1
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		log.Errorf("Unexpected content type %s: %s", contentType, string(body))
		return -1
	}

	var predResp PredictionResponse
	if err := json.Unmarshal(body, &predResp); err != nil {
		log.Errorf("Failed to decode prediction response: %v\nResponse body: %s", err, string(body))
		return -1
	}

	return predResp.Prediction
}

func (c *PowerManager) makeDecision() ([]string, []string, []string, []string) {
	currentTime := time.Now()

	predictionNodeCount := 0
	c.nodesInfo.Range(func(key, value interface{}) bool {
		if !value.(*NodeInfo).Exclude {
			predictionNodeCount++
		}
		return true
	})
	predictedActiveNodeCount := c.getPredictedActiveNodeCount(predictionNodeCount)

	var activeNodes, idleNodes, sleepNodes, poweredOffNodes []nodeSnapshot
	totalNodes := 0
	c.nodesInfo.Range(func(key, value interface{}) bool {
		info := value.(*NodeInfo)
		if info.Exclude {
			return true
		}
		totalNodes++
		node := nodeSnapshot{
			nodeID:              key.(string),
			lastStateChangeTime: info.LastStateChangeTime,
		}
		switch info.State {
		case Active:
			activeNodes = append(activeNodes, node)
		case Idle:
			idleNodes = append(idleNodes, node)
		case Sleep:
			sleepNodes = append(sleepNodes, node)
		case PoweredOff:
			poweredOffNodes = append(poweredOffNodes, node)
		}
		return true
	})

	log.Debugf("Current total node count: %d", totalNodes)
	log.Debugf("Predicted active node count: %d", predictedActiveNodeCount)
	log.Debugf("Current active node count: %d", len(activeNodes))
	log.Debugf("Current idle node count: %d", len(idleNodes))
	log.Debugf("Current sleep node count: %d", len(sleepNodes))
	log.Debugf("Current powered off node count: %d", len(poweredOffNodes))

	if predictedActiveNodeCount == -1 {
		log.Warnf("Failed to get predicted active node count, skip power control")
		return nil, nil, nil, nil
	}

	currentIdleNodeCount := len(idleNodes)
	currentActiveNodeCount := len(activeNodes)

	var nodesToWake, nodesToPowerOn, nodesToSleep, nodesToPowerOff []string
	nodesToWake, nodesToPowerOn = c.getNodesForWakeUpOrPowerOn(
		currentTime,
		totalNodes,
		predictedActiveNodeCount,
		currentActiveNodeCount,
		currentIdleNodeCount,
		sleepNodes,
		poweredOffNodes,
	)

	if len(nodesToWake) == 0 && len(nodesToPowerOn) == 0 {
		nodesToSleep, nodesToPowerOff = c.getNodesForSleepOrPowerOff(
			currentTime,
			totalNodes,
			predictedActiveNodeCount,
			currentActiveNodeCount,
			idleNodes,
			sleepNodes,
		)
	}

	c.recordClusterState(
		currentTime,
		predictedActiveNodeCount,
		currentActiveNodeCount,
		currentIdleNodeCount,
		len(sleepNodes),
		len(poweredOffNodes),
		len(nodesToWake),
		len(nodesToPowerOn),
		len(nodesToSleep),
		len(nodesToPowerOff),
	)

	// Log decision summary
	log.Infof("Power decision: predicted=%d nodes, current: active=%d idle=%d sleep=%d off=%d | actions: wake=%d on=%d sleep=%d off=%d",
		predictedActiveNodeCount,
		len(activeNodes), len(idleNodes), len(sleepNodes), len(poweredOffNodes),
		len(nodesToWake), len(nodesToPowerOn), len(nodesToSleep), len(nodesToPowerOff))

	return nodesToWake, nodesToPowerOn, nodesToSleep, nodesToPowerOff
}

func (c *PowerManager) getNodesForWakeUpOrPowerOn(
	currentTime time.Time,
	totalNodes int,
	predictedActiveNodeCount int,
	currentActiveNodeCount int,
	currentIdleNodeCount int,
	sleepingNodes []nodeSnapshot,
	poweredOffNodes []nodeSnapshot,
) ([]string, []string) {
	requiredIdleNodeCount := int(math.Ceil(float64(totalNodes) * c.config.PowerControl.IdleReserveRatio))
	totalAvailableNodeCount := currentActiveNodeCount + currentIdleNodeCount
	requiredTotalNodeCount := predictedActiveNodeCount + requiredIdleNodeCount

	neededNodeCount := requiredTotalNodeCount - totalAvailableNodeCount
	if neededNodeCount <= 0 {
		return nil, nil
	}

	var nodesToWake []string
	var nodesToPowerOn []string

	if len(sleepingNodes) > 0 {
		sortedNodes := make([]nodeSnapshot, len(sleepingNodes))
		copy(sortedNodes, sleepingNodes)
		sortByLastStateChange(sortedNodes)

		selectedCount := min(len(sortedNodes), neededNodeCount)
		for _, node := range sortedNodes[:selectedCount] {
			nodesToWake = append(nodesToWake, node.nodeID)
		}
		neededNodeCount -= selectedCount
	}

	if neededNodeCount > 0 && len(poweredOffNodes) > 0 {
		sortedNodes := make([]nodeSnapshot, len(poweredOffNodes))
		copy(sortedNodes, poweredOffNodes)
		sortByLastStateChange(sortedNodes)

		selectedCount := min(len(sortedNodes), neededNodeCount)
		for _, node := range sortedNodes[:selectedCount] {
			nodesToPowerOn = append(nodesToPowerOn, node.nodeID)
		}
		neededNodeCount -= selectedCount
	}

	log.Debugf("[Time %v] Wake-up decision:\n"+
		"  Current active: %d\n"+
		"  Predicted active: %d\n"+
		"  Current idle: %d\n"+
		"  Required idle: %d\n"+
		"  Total available: %d\n"+
		"  Required total: %d\n"+
		"  Nodes needed: %d\n"+
		"  Nodes to wake up: %d\n"+
		"  Nodes to power on: %d\n",
		currentTime,
		currentActiveNodeCount,
		predictedActiveNodeCount,
		currentIdleNodeCount,
		requiredIdleNodeCount,
		totalAvailableNodeCount,
		requiredTotalNodeCount,
		neededNodeCount,
		len(nodesToWake),
		len(nodesToPowerOn))

	return nodesToWake, nodesToPowerOn
}

func (c *PowerManager) getNodesForSleepOrPowerOff(
	currentTime time.Time,
	totalNodes int,
	predictedActiveNodeCount int,
	currentActiveNodeCount int,
	idleNodes []nodeSnapshot,
	sleepingNodes []nodeSnapshot,
) ([]string, []string) {
	var nodesToSleep []string
	var nodesToPowerOff []string

	if len(idleNodes) > 0 {
		requiredIdleCount := int(math.Ceil(float64(totalNodes) * c.config.PowerControl.IdleReserveRatio))
		currentIdleNodeCount := len(idleNodes)
		log.Debugf("Idle reserve ratio: %f", c.config.PowerControl.IdleReserveRatio)
		log.Debugf("Total nodes: %d, Current idle node count: %d, required idle count: %d", totalNodes, currentIdleNodeCount, requiredIdleCount)

		nodesCanSleepCount := currentIdleNodeCount - requiredIdleCount
		if nodesCanSleepCount > 0 {
			sortedIdleNodes := make([]nodeSnapshot, len(idleNodes))
			copy(sortedIdleNodes, idleNodes)
			sortByLastStateChange(sortedIdleNodes)

			if c.config.PowerControl.EnableSleep {
				for _, node := range sortedIdleNodes[:nodesCanSleepCount] {
					nodesToSleep = append(nodesToSleep, node.nodeID)
				}
			} else {
				for _, node := range sortedIdleNodes[:nodesCanSleepCount] {
					nodesToPowerOff = append(nodesToPowerOff, node.nodeID)
				}
			}
		}
	}

	for _, node := range sleepingNodes {
		log.Debugf("node %s last state change time: %s", node.nodeID, node.lastStateChangeTime)
		sleepTime := currentTime.Sub(node.lastStateChangeTime)
		log.Debugf("node %s sleep time: %s", node.nodeID, sleepTime)

		if sleepTime >= time.Duration(c.config.PowerControl.SleepTimeThresholdSeconds)*time.Second {
			nodesToPowerOff = append(nodesToPowerOff, node.nodeID)
		}
	}

	log.Debugf("[Time %v] Sleep/PowerOff decision:\n"+
		"  Current active: %d\n"+
		"  Predicted active: %d\n"+
		"  Current idle: %d\n"+
		"  Nodes to sleep: %d\n"+
		"  Nodes to shutdown: %d\n"+
		"  Remaining idle after sleep and power off: %d\n",
		currentTime,
		currentActiveNodeCount,
		predictedActiveNodeCount,
		len(idleNodes),
		len(nodesToSleep),
		len(nodesToPowerOff),
		len(idleNodes)-len(nodesToSleep)-len(nodesToPowerOff))

	return nodesToSleep, nodesToPowerOff
}

func (c *PowerManager) startPowerStateMonitor() {
	go func() {
		ticker := time.NewTicker(time.Duration(c.config.PowerControl.NodeStateCheckIntervalSeconds) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.checkPowerState()
			case <-c.stopChan:
				return
			}
		}
	}()
}

func (c *PowerManager) checkPowerState() {
	actionTimeout := time.Duration(c.config.PowerControl.ActionTimeoutSeconds) * time.Second
	c.nodesInfo.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		info := value.(*NodeInfo)

		switch info.State {
		case PoweringOn, Wakingup:
			timedOut := time.Since(info.LastStateChangeTime) >= actionTimeout
			poweredOn, err := c.powerTool.GetPowerState(nodeID)
			if err != nil {
				log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
				if timedOut {
					if info.State == Wakingup {
						c.updateNodeStateIfCurrent(nodeID, info, Sleep)
					} else {
						c.updateNodeStateIfCurrent(nodeID, info, PoweredOff)
					}
				}
				return true
			}

			if !poweredOn {
				if timedOut {
					c.updateNodeStateIfCurrent(nodeID, info, PoweredOff)
				}
				return true
			}

			alive := c.powerTool.CheckNodeAlive(nodeID)
			if alive {
				if c.updateNodeStateIfCurrent(nodeID, info, Idle) != nil {
					log.Infof("Node %s is now available", nodeID)
				}
			} else if timedOut {
				if c.updateNodeStateIfCurrent(nodeID, info, Sleep) != nil {
					log.Warnf("Node %s power action timed out while the host remained unreachable", nodeID)
				}
			}

		case PoweringOff:
			timedOut := time.Since(info.LastStateChangeTime) >= actionTimeout
			poweredOn, err := c.powerTool.GetPowerState(nodeID)
			if err != nil {
				log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
				if timedOut {
					c.updateNodeStateIfCurrent(nodeID, info, Sleep)
				}
				return true
			}

			if !poweredOn {
				if c.updateNodeStateIfCurrent(nodeID, info, PoweredOff) != nil {
					log.Infof("Node %s is now powered off", nodeID)
				}
			} else if timedOut {
				var updated *NodeInfo
				if c.powerTool.CheckNodeAlive(nodeID) {
					updated = c.updateNodeStateIfCurrent(nodeID, info, Idle)
				} else {
					updated = c.updateNodeStateIfCurrent(nodeID, info, Sleep)
				}
				if updated != nil {
					log.Warnf("Node %s power-off action timed out", nodeID)
				}
			}

		case ToSleeping:
			alive := c.powerTool.CheckNodeAlive(nodeID)
			if !alive {
				if c.updateNodeStateIfCurrent(nodeID, info, Sleep) != nil {
					log.Infof("Node %s is now sleeping", nodeID)
				}
			} else if time.Since(info.LastStateChangeTime) >= actionTimeout {
				if c.updateNodeStateIfCurrent(nodeID, info, Idle) != nil {
					log.Warnf("Node %s sleep action timed out", nodeID)
				}
			}

		case Idle, Active:
			alive := c.powerTool.CheckNodeAlive(nodeID)
			if !alive {
				powered, err := c.powerTool.GetPowerState(nodeID)
				if err != nil {
					log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
					return true
				}
				if !powered {
					if c.updateNodeStateIfCurrent(nodeID, info, PoweredOff) != nil {
						log.Warnf("Node %s was idle but found powered off", nodeID)
					}
				} else {
					if c.updateNodeStateIfCurrent(nodeID, info, Sleep) != nil {
						log.Warnf("Node %s was idle but found sleeping", nodeID)
					}
				}
			}

		case Sleep:
			powered, err := c.powerTool.GetPowerState(nodeID)
			if err != nil {
				log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
				return true
			}
			if !powered {
				if c.updateNodeStateIfCurrent(nodeID, info, PoweredOff) != nil {
					log.Warnf("Node %s was sleeping but found powered off", nodeID)
				}
			} else {
				alive := c.powerTool.CheckNodeAlive(nodeID)
				if alive {
					if c.updateNodeStateIfCurrent(nodeID, info, Idle) != nil {
						log.Warnf("Node %s was sleeping but found active", nodeID)
					}
				}
			}

		case PoweredOff:
			powered, err := c.powerTool.GetPowerState(nodeID)
			if err != nil {
				log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
				return true
			}
			if powered {
				alive := c.powerTool.CheckNodeAlive(nodeID)
				if alive {
					if c.updateNodeStateIfCurrent(nodeID, info, Idle) != nil {
						log.Warnf("Node %s was powered off but found active", nodeID)
					}
				} else {
					if c.updateNodeStateIfCurrent(nodeID, info, Sleep) != nil {
						log.Warnf("Node %s was powered off but found sleeping", nodeID)
					}
				}
			}
		}
		return true
	})
}

func (c *PowerManager) recordClusterState(
	currentTime time.Time,
	prediction int,
	activeCount int,
	idleCount int,
	sleepCount int,
	powerOffCount int,
	toWakeCount int,
	toPowerOnCount int,
	toSleepCount int,
	toPowerOffCount int,
) {
	dir := filepath.Dir(c.config.PowerControl.ClusterStateFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Errorf("Failed to create directory %s: %v", dir, err)
		return
	}

	if _, err := os.Stat(c.config.PowerControl.ClusterStateFile); os.IsNotExist(err) {
		file, err := os.Create(c.config.PowerControl.ClusterStateFile)
		if err != nil {
			log.Errorf("Failed to create cluster state file: %v", err)
			return
		}
		writer := csv.NewWriter(file)
		headers := []string{
			"Timestamp",
			"PredictedActive",
			"CurrentActive",
			"CurrentIdle",
			"CurrentSleep",
			"CurrentPowerOff",
			"PoweringOn",
			"WakingUp",
			"ToSleeping",
			"PoweringOff",
			"ToWake",
			"ToPowerOn",
			"ToSleep",
			"ToPowerOff",
		}
		if err := writer.Write(headers); err != nil {
			log.Warnf("Failed to write headers: %v", err)
			file.Close()
			return
		}
		writer.Flush()
		file.Close()
	}

	file, err := os.OpenFile(c.config.PowerControl.ClusterStateFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Warnf("Failed to open cluster state file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	poweringOnCount := 0
	wakingUpCount := 0
	toSleepingCount := 0
	poweringOffCount := 0

	c.nodesInfo.Range(func(key, value interface{}) bool {
		info := value.(*NodeInfo)
		if info.Exclude {
			return true
		}
		switch info.State {
		case PoweringOn:
			poweringOnCount++
		case Wakingup:
			wakingUpCount++
		case ToSleeping:
			toSleepingCount++
		case PoweringOff:
			poweringOffCount++
		}
		return true
	})

	dateTime := currentTime.Format("2006-01-02 15:04:05")
	record := []string{
		dateTime,
		fmt.Sprintf("%d", prediction),
		fmt.Sprintf("%d", activeCount),
		fmt.Sprintf("%d", idleCount),
		fmt.Sprintf("%d", sleepCount),
		fmt.Sprintf("%d", powerOffCount),
		fmt.Sprintf("%d", poweringOnCount),
		fmt.Sprintf("%d", wakingUpCount),
		fmt.Sprintf("%d", toSleepingCount),
		fmt.Sprintf("%d", poweringOffCount),
		fmt.Sprintf("%d", toWakeCount),
		fmt.Sprintf("%d", toPowerOnCount),
		fmt.Sprintf("%d", toSleepCount),
		fmt.Sprintf("%d", toPowerOffCount),
	}

	if err := writer.Write(record); err != nil {
		log.Warnf("Failed to write cluster state record: %v", err)
	}
}

func (c *PowerManager) recordStateChange(time time.Time, nodeID string, oldState, newState NodeState) {
	// If NodeStateChangeFile is not configured, skip recording
	if c.config.PowerControl.NodeStateChangeFile == "" {
		return
	}

	dir := filepath.Dir(c.config.PowerControl.NodeStateChangeFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Errorf("Failed to create directory %s: %v", dir, err)
		return
	}

	if _, err := os.Stat(c.config.PowerControl.NodeStateChangeFile); os.IsNotExist(err) {
		file, err := os.Create(c.config.PowerControl.NodeStateChangeFile)
		if err != nil {
			log.Errorf("Failed to create state change log: %v", err)
			return
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		if err := writer.Write([]string{"time", "node_id", "old_state", "new_state"}); err != nil {
			log.Errorf("Failed to write header: %v", err)
			return
		}
		writer.Flush()
	}

	file, err := os.OpenFile(c.config.PowerControl.NodeStateChangeFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Failed to open state change log: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	oldStateStr := string(oldState)
	if oldState == "" {
		oldStateStr = "INIT"
	}
	err = writer.Write([]string{
		time.Format("2006-01-02 15:04:05"),
		nodeID,
		oldStateStr,
		string(newState),
	})
	if err != nil {
		log.Errorf("Failed to write state change: %v", err)
	}
	writer.Flush()
}
