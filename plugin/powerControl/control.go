package main

import (
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

type PowerController struct {
	config    *Config
	powerTool PowerTool

	nodesInfo map[string]*NodeInfo
	mu        sync.RWMutex

	stopChan chan struct{}
}

func NewPowerController(config *Config) *PowerController {
	files := []string{
		config.Predictor.NodeStateChangeFile,
		config.Predictor.ClusterStateFile,
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

	controller := &PowerController{
		config:    config,
		powerTool: NewIPMITool(config),
		nodesInfo: make(map[string]*NodeInfo),
		stopChan:  make(chan struct{}),
	}

	controller.StartPowerStateMonitor()

	return controller
}

func (c *PowerController) RegisterNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.nodesInfo[nodeID]; !exists {
		c.nodesInfo[nodeID] = &NodeInfo{
			State:               Idle,
			LastStateChangeTime: time.Time{},
			Jobs:                make(map[string]struct{}),
		}
		log.Infof("Initialized node %s in idle state", nodeID)
	}
}

func (c *PowerController) StartAutoPowerControl() {
	go func() {
		ticker := time.NewTicker(time.Duration(c.config.Predictor.CheckIntervalSeconds) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				wake, on, sleep, off := c.ExecutePowerControl()
				log.Infof("Auto power control executed: wake=%d, on=%d, sleep=%d, off=%d",
					wake, on, sleep, off)
			case <-c.stopChan:
				return
			}
		}
	}()
}

func (c *PowerController) StopAutoPowerControl() {
	close(c.stopChan)
}

func (c *PowerController) AddJobToNode(nodeID string, jobID string) bool {
	return c.updateNodeJob(nodeID, jobID, true)
}

func (c *PowerController) RemoveJobFromNode(nodeID string, jobID string) bool {
	return c.updateNodeJob(nodeID, jobID, false)
}

func (c *PowerController) GetNodesByState(states ...NodeState) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

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

func (c *PowerController) ExecutePowerControl() (int, int, int, int) {
	nodesToWake, nodesToPowerOn, nodesToSleep, nodesToShutdown := c.makeDecision()

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

	if len(nodesToShutdown) > 0 {
		if err := c.powerOffNodes(nodesToShutdown); err != nil {
			log.Errorf("Failed to shutdown nodes: %v", err)
		}
	}

	return len(nodesToWake), len(nodesToPowerOn),
		len(nodesToSleep), len(nodesToShutdown)
}

func (c *PowerController) updateNodeJob(nodeID string, jobID string, isAdd bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

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

func (c *PowerController) updateNodeState(nodeID string, newState NodeState) {
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

func (c *PowerController) getPredictedActiveNodeCount() int {
	resp, err := http.Post(fmt.Sprintf("%s/predict", c.config.Predictor.URL),
		"application/json",
		nil)

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

func (c *PowerController) makeDecision() ([]string, []string, []string, []string) {
	currentTime := time.Now()

	predictedActiveNodeCount := c.getPredictedActiveNodeCount()
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
		nodesToWake,
		nodesToPowerOn,
		nodesToSleep,
		nodesToPowerOff,
	)

	return nodesToWake, nodesToPowerOn, nodesToSleep, nodesToPowerOff
}

func (c *PowerController) getNodesForWakeUpOrPowerOn(
	currentTime time.Time,
	predictedActiveNodeCount int,
	currentActiveNodeCount int,
	currentIdleNodeCount int,
	sleepingNodes []string,
	poweredOffNodes []string,
) ([]string, []string) {
	requiredIdleNodeCount := int(math.Ceil(float64(len(c.nodesInfo)) * c.config.Predictor.IdleReserveRatio))
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

func (c *PowerController) getNodesForSleepOrPowerOff(
	currentTime time.Time,
	predictedActiveNodeCount int,
	currentActiveNodeCount int,
	idleNodes []string,
	sleepingNodes []string,
) ([]string, []string) {
	var nodesToSleep []string
	var nodesToPowerOff []string

	if len(idleNodes) > 0 {
		requiredIdleCount := int(math.Ceil(float64(len(c.nodesInfo)) * c.config.Predictor.IdleReserveRatio))
		currentIdleNodeCount := len(idleNodes)

		nodesCanSleepCount := currentIdleNodeCount - requiredIdleCount
		if nodesCanSleepCount > 0 {
			sortedIdleNodes := make([]string, len(idleNodes))
			copy(sortedIdleNodes, idleNodes)
			sort.Slice(sortedIdleNodes, func(i, j int) bool {
				return c.nodesInfo[sortedIdleNodes[i]].LastStateChangeTime.Before(
					c.nodesInfo[sortedIdleNodes[j]].LastStateChangeTime)
			})
			nodesToSleep = sortedIdleNodes[:nodesCanSleepCount]
		}
	}

	for _, node := range sleepingNodes {
		log.Infof("node %s last state change time: %s", node, c.nodesInfo[node].LastStateChangeTime)
		sleepTime := currentTime.Sub(c.nodesInfo[node].LastStateChangeTime)
		log.Infof("node %s sleep time: %s", node, sleepTime)
		if sleepTime >= time.Duration(c.config.Predictor.SleepTimeThresholdSeconds)*time.Second {
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

func (c *PowerController) StartPowerStateMonitor() {
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

func (c *PowerController) checkPowerState() {
	c.mu.Lock()
	defer c.mu.Unlock()

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

func (c *PowerController) recordClusterState(
	currentTime time.Time,
	prediction int,
	activeCount int,
	idleCount int,
	sleepCount int,
	powerOffCount int,
	nodesToWake []string,
	nodesToPowerOn []string,
	nodesToSleep []string,
	nodesToPowerOff []string,
) {
	dir := filepath.Dir(c.config.Predictor.ClusterStateFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Warnf("Failed to create directory %s: %v", dir, err)
		return
	}

	if _, err := os.Stat(c.config.Predictor.ClusterStateFile); os.IsNotExist(err) {
		file, err := os.Create(c.config.Predictor.ClusterStateFile)
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
			"ToWakeNodes",
			"ToPowerOnNodes",
			"ToSleepNodes",
			"ToPowerOffNodes",
		}
		if err := writer.Write(headers); err != nil {
			log.Warnf("Failed to write headers: %v", err)
			file.Close()
			return
		}
		writer.Flush()
		file.Close()
	}

	file, err := os.OpenFile(c.config.Predictor.ClusterStateFile, os.O_APPEND|os.O_WRONLY, 0644)
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
		strings.Join(nodesToWake, ";"),
		strings.Join(nodesToPowerOn, ";"),
		strings.Join(nodesToSleep, ";"),
		strings.Join(nodesToPowerOff, ";"),
	}

	if err := writer.Write(record); err != nil {
		log.Warnf("Failed to write cluster state record: %v", err)
	}
}

func (c *PowerController) recordStateChange(time time.Time, nodeID string, oldState, newState NodeState) {
	if _, err := os.Stat(c.config.Predictor.NodeStateChangeFile); os.IsNotExist(err) {
		file, err := os.Create(c.config.Predictor.NodeStateChangeFile)
		if err != nil {
			panic(fmt.Sprintf("Failed to create state change log: %v", err))
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		err = writer.Write([]string{"time", "node_id", "old_state", "new_state"})
		if err != nil {
			panic(fmt.Sprintf("Failed to write header: %v", err))
		}
		writer.Flush()
	}

	file, err := os.OpenFile(c.config.Predictor.NodeStateChangeFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("Failed to open state change log: %v", err))
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
