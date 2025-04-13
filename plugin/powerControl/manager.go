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
)

type PowerManager struct {
	config          *Config
	powerTool       PowerTool
	excludeNodesMap map[string]struct{}

	nodesInfo map[string]*NodeInfo
	nodeMutex sync.RWMutex

	stopChan chan struct{}
}

type PredictionRequest struct {
	TotalNodes int `json:"total_nodes"`
}

func NewPowerManager(config *Config) *PowerManager {
	files := []string{
		config.PowerControl.NodeStateChangeFile,
		config.PowerControl.ClusterStateFile,
	}
	for _, file := range files {
		if _, err := os.Stat(file); err == nil {
			if err := os.Remove(file); err != nil {
				log.Warnf("Failed to remove file %s: %v", file, err)
			} else {
				log.Infof("Removed existing file: %s", file)
			}
		}
	}

	excludeNodesMap := make(map[string]struct{})
	for _, node := range config.IPMI.ExcludeNodes {
		excludeNodesMap[node] = struct{}{}
	}

	controller := &PowerManager{
		config:          config,
		powerTool:       NewIPMITool(config),
		nodesInfo:       make(map[string]*NodeInfo),
		stopChan:        make(chan struct{}),
		excludeNodesMap: excludeNodesMap,
	}

	controller.StartPowerStateMonitor()

	return controller
}

func (c *PowerManager) RegisterNode(nodeID string) {
	c.nodeMutex.Lock()
	defer c.nodeMutex.Unlock()

	if _, exists := c.nodesInfo[nodeID]; !exists {
		c.nodesInfo[nodeID] = &NodeInfo{
			State:               Idle,
			LastStateChangeTime: time.Time{},
			Jobs:                make(map[string]struct{}),
		}
		log.Infof("Initialized node %s in idle state", nodeID)
	}
}

func (c *PowerManager) StartAutoPowerManager() {
	go func() {
		ticker := time.NewTicker(time.Duration(c.config.PowerControl.CheckIntervalSeconds) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				wake, on, sleep, off := c.Start()
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

func (c *PowerManager) GetNodesByState(states ...NodeState) []string {
	c.nodeMutex.RLock()
	defer c.nodeMutex.RUnlock()

	var nodes []string
	for nodeID, info := range c.nodesInfo {
		for _, state := range states {
			if info.State == state {
				nodes = append(nodes, nodeID)
				break
			}
		}
	}
	return nodes
}

func (c *PowerManager) Start() (int, int, int, int) {
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
	c.nodeMutex.Lock()
	defer c.nodeMutex.Unlock()

	if info, exists := c.nodesInfo[nodeID]; exists {
		if info.State != Active && info.State != Idle {
			return false
		}

		if info.Jobs == nil {
			info.Jobs = make(map[string]struct{})
		}

		if isAdd {
			info.Jobs[jobID] = struct{}{}
			if len(info.Jobs) == 1 {
				c.updateNodeState(nodeID, Active)
			}
		} else {
			delete(info.Jobs, jobID)
			if len(info.Jobs) == 0 {
				c.updateNodeState(nodeID, Idle)
			}
		}
		return true
	}
	return false
}

func (c *PowerManager) updateNodeState(nodeID string, newState NodeState) {
	currentTime := time.Now()

	if _, exists := c.nodesInfo[nodeID]; !exists {
		c.nodesInfo[nodeID] = &NodeInfo{
			State:               newState,
			LastStateChangeTime: time.Time{},
			Jobs:                make(map[string]struct{}),
		}
		log.Errorf("Node %s not found in nodesInfo, please check the node registration", nodeID)
		c.recordStateChange(currentTime, nodeID, "", newState)
		return
	}

	if c.nodesInfo[nodeID].State != newState {
		oldState := c.nodesInfo[nodeID].State
		c.nodesInfo[nodeID].State = newState
		c.nodesInfo[nodeID].LastStateChangeTime = currentTime
		c.recordStateChange(currentTime, nodeID, oldState, newState)
	}
}

func (c *PowerManager) getPredictedActiveNodeCount() int {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	reqBody := PredictionRequest{
		TotalNodes: len(c.nodesInfo),
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

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Errorf("Prediction request failed with status %d: %s", resp.StatusCode, string(body))
		return -1
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		body, _ := io.ReadAll(resp.Body)
		log.Errorf("Unexpected content type %s: %s", contentType, string(body))
		return -1
	}

	var predResp PredictionResponse
	if err := json.NewDecoder(resp.Body).Decode(&predResp); err != nil {
		body, _ := io.ReadAll(resp.Body)
		log.Errorf("Failed to decode prediction response: %v\nResponse body: %s", err, string(body))
		return -1
	}

	return predResp.Prediction
}

func (c *PowerManager) makeDecision() ([]string, []string, []string, []string) {
	currentTime := time.Now()

	predictedActiveNodeCount := c.getPredictedActiveNodeCount()

	log.Infof("Predicted active node count: %d", predictedActiveNodeCount)
	log.Infof("Current active node count: %d", len(c.GetNodesByState(Active)))
	log.Infof("Current idle node count: %d", len(c.GetNodesByState(Idle)))
	log.Infof("Current sleep node count: %d", len(c.GetNodesByState(Sleep)))
	log.Infof("Current powered off node count: %d", len(c.GetNodesByState(PoweredOff)))

	if predictedActiveNodeCount == -1 {
		return nil, nil, nil, nil
	}

	idleNodes := c.GetNodesByState(Idle)
	sleepNodes := c.GetNodesByState(Sleep)
	poweredOffNodes := c.GetNodesByState(PoweredOff)
	currentIdleNodeCount := len(idleNodes)
	currentActiveNodeCount := len(c.GetNodesByState(Active))

	var nodesToWake, nodesToPowerOn, nodesToSleep, nodesToPowerOff []string
	nodesToWake, nodesToPowerOn = c.getNodesForWakeUpOrPowerOn(
		currentTime,
		predictedActiveNodeCount,
		currentActiveNodeCount,
		currentIdleNodeCount,
		sleepNodes,
		poweredOffNodes,
	)

	if nodesToWake == nil && nodesToPowerOn == nil {
		nodesToSleep, nodesToPowerOff = c.getNodesForSleepOrPowerOff(
			currentTime,
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

	return nodesToWake, nodesToPowerOn, nodesToSleep, nodesToPowerOff
}

func (c *PowerManager) getNodesForWakeUpOrPowerOn(
	currentTime time.Time,
	predictedActiveNodeCount int,
	currentActiveNodeCount int,
	currentIdleNodeCount int,
	sleepingNodes []string,
	poweredOffNodes []string,
) ([]string, []string) {
	requiredIdleNodeCount := int(math.Ceil(float64(len(c.nodesInfo)) * c.config.PowerControl.IdleReserveRatio))
	totalAvailableNodeCount := currentActiveNodeCount + currentIdleNodeCount
	requiredTotalNodeCount := predictedActiveNodeCount + requiredIdleNodeCount

	neededNodeCount := requiredTotalNodeCount - totalAvailableNodeCount
	if neededNodeCount <= 0 {
		return nil, nil
	}

	var nodesToWake []string
	var nodesToPowerOn []string

	if len(sleepingNodes) > 0 {
		sortedNodes := make([]string, len(sleepingNodes))
		copy(sortedNodes, sleepingNodes)
		sort.Slice(sortedNodes, func(i, j int) bool {
			return c.nodesInfo[sortedNodes[i]].LastStateChangeTime.Before(
				c.nodesInfo[sortedNodes[j]].LastStateChangeTime)
		})

		if len(sortedNodes) >= neededNodeCount {
			nodesToWake = sortedNodes[:neededNodeCount]
			neededNodeCount = 0
		} else {
			nodesToWake = sortedNodes
			neededNodeCount -= len(sortedNodes)
		}
	}

	if neededNodeCount > 0 && len(poweredOffNodes) > 0 {
		sortedNodes := make([]string, len(poweredOffNodes))
		copy(sortedNodes, poweredOffNodes)
		sort.Slice(sortedNodes, func(i, j int) bool {
			return c.nodesInfo[sortedNodes[i]].LastStateChangeTime.Before(
				c.nodesInfo[sortedNodes[j]].LastStateChangeTime)
		})

		if len(sortedNodes) >= neededNodeCount {
			nodesToPowerOn = sortedNodes[:neededNodeCount]
			neededNodeCount = 0
		} else {
			nodesToPowerOn = sortedNodes
			neededNodeCount -= len(sortedNodes)
		}
	}

	log.Infof("[Time %v] Wake-up decision:\n"+
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
	predictedActiveNodeCount int,
	currentActiveNodeCount int,
	idleNodes []string,
	sleepingNodes []string,
) ([]string, []string) {
	var nodesToSleep []string
	var nodesToPowerOff []string

	if len(idleNodes) > 0 {
		requiredIdleCount := int(math.Ceil(float64(len(c.nodesInfo)) * c.config.PowerControl.IdleReserveRatio))
		currentIdleNodeCount := len(idleNodes)

		nodesCanSleepCount := currentIdleNodeCount - requiredIdleCount
		if nodesCanSleepCount > 0 {
			sortedIdleNodes := make([]string, len(idleNodes))
			copy(sortedIdleNodes, idleNodes)
			sort.Slice(sortedIdleNodes, func(i, j int) bool {
				return c.nodesInfo[sortedIdleNodes[i]].LastStateChangeTime.Before(
					c.nodesInfo[sortedIdleNodes[j]].LastStateChangeTime)
			})

			if c.config.PowerControl.EnableSleep {
				nodesToSleep = sortedIdleNodes[:nodesCanSleepCount]
			} else {
				nodesToPowerOff = sortedIdleNodes[:nodesCanSleepCount]
			}
		}
	}

	for _, node := range sleepingNodes {
		log.Infof("node %s last state change time: %s", node, c.nodesInfo[node].LastStateChangeTime)
		sleepTime := currentTime.Sub(c.nodesInfo[node].LastStateChangeTime)
		log.Infof("node %s sleep time: %s", node, sleepTime)
		if sleepTime >= time.Duration(c.config.PowerControl.SleepTimeThresholdSeconds)*time.Second {
			nodesToPowerOff = append(nodesToPowerOff, node)
		}
	}

	log.Infof("[Time %v] Sleep/PowerOff decision:\n"+
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

func (c *PowerManager) StartPowerStateMonitor() {
	go func() {
		ticker := time.NewTicker(time.Duration(c.config.IPMI.NodeStateCheckIntervalSeconds) * time.Second)
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
	c.nodeMutex.Lock()
	defer c.nodeMutex.Unlock()

	for nodeID, info := range c.nodesInfo {
		switch info.State {
		case PoweringOn, Wakingup:
			poweredOn, err := c.powerTool.GetPowerState(nodeID)
			if err != nil {
				log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
				continue
			}

			if !poweredOn {
				continue
			}

			alive := c.powerTool.CheckNodeAlive(nodeID)
			if alive {
				c.updateNodeState(nodeID, Idle)
				log.Infof("Node %s is now available", nodeID)
			}

		case PoweringOff:
			powered, err := c.powerTool.GetPowerState(nodeID)
			if err != nil {
				log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
				continue
			}

			if !powered {
				c.updateNodeState(nodeID, PoweredOff)
				log.Infof("Node %s is now powered off", nodeID)
			}

		case ToSleeping:
			alive := c.powerTool.CheckNodeAlive(nodeID)
			if !alive {
				c.updateNodeState(nodeID, Sleep)
				log.Infof("Node %s is now sleeping", nodeID)
			}

		case Idle, Active:
			alive := c.powerTool.CheckNodeAlive(nodeID)
			if !alive {
				powered, err := c.powerTool.GetPowerState(nodeID)
				if err != nil {
					log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
					continue
				}
				if !powered {
					c.updateNodeState(nodeID, PoweredOff)
					log.Warnf("Node %s was idle but found powered off", nodeID)
				} else {
					c.updateNodeState(nodeID, Sleep)
					log.Warnf("Node %s was idle but found sleeping", nodeID)
				}
			}

		case Sleep:
			powered, err := c.powerTool.GetPowerState(nodeID)
			if err != nil {
				log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
				continue
			}
			if !powered {
				c.updateNodeState(nodeID, PoweredOff)
				log.Warnf("Node %s was sleeping but found powered off", nodeID)
			} else {
				alive := c.powerTool.CheckNodeAlive(nodeID)
				if alive {
					c.updateNodeState(nodeID, Idle)
					log.Warnf("Node %s was sleeping but found active", nodeID)
				}
			}

		case PoweredOff:
			powered, err := c.powerTool.GetPowerState(nodeID)
			if err != nil {
				log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
				continue
			}
			if powered {
				alive := c.powerTool.CheckNodeAlive(nodeID)
				if alive {
					c.updateNodeState(nodeID, Idle)
					log.Warnf("Node %s was powered off but found active", nodeID)
				} else {
					c.updateNodeState(nodeID, Sleep)
					log.Warnf("Node %s was powered off but found sleeping", nodeID)
				}
			}
		}
	}
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
		log.Warnf("Failed to create directory %s: %v", dir, err)
		return
	}

	if _, err := os.Stat(c.config.PowerControl.ClusterStateFile); os.IsNotExist(err) {
		file, err := os.Create(c.config.PowerControl.ClusterStateFile)
		if err != nil {
			log.Warnf("Failed to create cluster state file: %v", err)
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
	for _, info := range c.nodesInfo {
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
	}

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
