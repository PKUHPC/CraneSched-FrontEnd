package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var isSimulation = true

type PowerController struct {
	mu        sync.RWMutex
	time      Time
	powerTool PowerTool

	nodeCount  int
	nodeStates map[string]*NodeInfo

	predictorURL      string
	minUptime         int
	minDowntime       int
	sleepThreshold    int
	bufferRatio       float64
	idleReserveRatio  float64
	redundancyNodeNum int
	batchSize         int
	checkInterval     int
}

func NewPowerController(
	predictorURL string,
	minUptime int,
	minDowntime int,
	sleepThreshold int,
	bufferRatio float64,
	idleReserveRatio float64,
	redundancyNodeNum int,
	batchSize int,
	checkInterval int,
) *PowerController {
	controller := &PowerController{
		nodeCount:         0,
		nodeStates:        make(map[string]*NodeInfo),
		predictorURL:      predictorURL,
		minUptime:         minUptime,
		minDowntime:       minDowntime,
		sleepThreshold:    sleepThreshold,
		bufferRatio:       bufferRatio,
		idleReserveRatio:  idleReserveRatio,
		redundancyNodeNum: redundancyNodeNum,
		batchSize:         batchSize,
		checkInterval:     checkInterval,
	}

	if isSimulation {
		controller.time = NewSimulationTime()

		for i := 0; i < 500; i++ {
			controller.InitializeNode(fmt.Sprintf("node%d", i))
		}

		controller.powerTool = NewSimPowerTool()
		controller.time = NewSimulationTime()
	} else {
		controller.powerTool = NewIPMITool()
		controller.time = NewRealTime()
	}

	return controller
}

func (c *PowerController) InitializeNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.nodeStates[nodeID]; !exists {
		c.nodeStates[nodeID] = &NodeInfo{
			State:           NodeStateIdle,
			LastStateChange: c.time.Now(),
			JobCount:        0,
		}
		c.nodeCount++
		log.Infof("Initialized node %s in idle state", nodeID)
	}
}

// 对外提供的接口
func (c *PowerController) AddJobToNode(nodeID string) bool {
	return c.updateNodeJobCount(nodeID, 1)
}

// 对外提供的接口
func (c *PowerController) RemoveJobFromNode(nodeID string) bool {
	return c.updateNodeJobCount(nodeID, -1)
}

// 对外提供的接口
func (c *PowerController) GetNodeJobCount(nodeID string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if info, exists := c.nodeStates[nodeID]; exists {
		return info.JobCount
	}
	return 0
}

// 对外提供的接口
func (c *PowerController) GetActiveNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var activeNodes []string
	for nodeID, info := range c.nodeStates {
		if info.State == NodeStateActive {
			activeNodes = append(activeNodes, nodeID)
		}
	}
	return activeNodes
}

// 对外提供的接口
func (c *PowerController) GetIdleNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var idleNodes []string
	for nodeID, info := range c.nodeStates {
		if info.State == NodeStateIdle {
			idleNodes = append(idleNodes, nodeID)
		}
	}
	return idleNodes
}

// 对外提供的接口
func (c *PowerController) GetSleepingNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var sleepingNodes []string
	for nodeID, info := range c.nodeStates {
		if info.State == NodeStateSleeping {
			sleepingNodes = append(sleepingNodes, nodeID)
		}
	}
	return sleepingNodes
}

// 对外提供的接口
func (c *PowerController) GetPoweredOffNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var poweredOffNodes []string
	for nodeID, info := range c.nodeStates {
		if info.State == NodeStatePoweredOff {
			poweredOffNodes = append(poweredOffNodes, nodeID)
		}
	}
	return poweredOffNodes
}

// 对外提供的接口
func (c *PowerController) GetAvailableNodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var availableNodes []string
	for nodeID, info := range c.nodeStates {
		if info.State == NodeStateActive || info.State == NodeStateIdle {
			availableNodes = append(availableNodes, nodeID)
		}
	}
	return availableNodes
}

// 对外提供的接口
func (c *PowerController) GetNodeState(nodeID string) NodeState {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if info, exists := c.nodeStates[nodeID]; exists {
		return info.State
	}
	return ""
}

// 对外提供的接口
// GetNodeStateDuration 获取节点在当前状态持续的时间（秒）
func (c *PowerController) GetNodeStateDuration(nodeID string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if info, exists := c.nodeStates[nodeID]; exists {
		return c.time.Since(info.LastStateChange)
	}
	return 0
}

// 对外提供的接口
// HandleStateTransitionComplete 处理节点状态转换完成
func (c *PowerController) HandleStateTransitionComplete(nodeID interface{}, newPowerState interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeIDStr := fmt.Sprintf("%v", nodeID)
	if _, exists := c.nodeStates[nodeIDStr]; !exists {
		panic(fmt.Sprintf("Node %v not found in node_states", nodeID))
	}

	newPowerStateNum, err := strconv.Atoi(fmt.Sprintf("%v", newPowerState))
	if err != nil {
		panic(fmt.Sprintf("Invalid power state: %v", newPowerState))
	}

	oldNodeState := c.nodeStates[nodeIDStr].State
	switch PState(newPowerStateNum) {
	case PStateOn:
		if oldNodeState == NodeStateWakingup {
			c.updateNodeState(nodeIDStr, NodeStateIdle)
		}
	case PStateSleep:
		if oldNodeState == NodeStateSwitchingToSleep {
			c.updateNodeState(nodeIDStr, NodeStateSleeping)
		}
	default:
		panic(fmt.Sprintf("Invalid state transition from %s to %d for node %v", oldNodeState, newPowerStateNum, nodeID))
	}
}

// updateNodeJobCount 更新节点的作业数量并相应更新节点状态
func (c *PowerController) updateNodeJobCount(nodeID string, jobChangeCount int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if info, exists := c.nodeStates[nodeID]; exists {
		if info.State != NodeStateActive && info.State != NodeStateIdle {
			return false
		}

		info.JobCount += jobChangeCount
		if info.JobCount < 0 {
			info.JobCount = 0
		}

		if info.JobCount > 0 {
			c.updateNodeState(nodeID, NodeStateActive)
		} else {
			c.updateNodeState(nodeID, NodeStateIdle)
		}
		return true
	}
	return false
}

func (c *PowerController) updateNodeState(nodeID string, state NodeState) {
	currentTime := c.time.Now()

	if _, exists := c.nodeStates[nodeID]; !exists {
		c.nodeStates[nodeID] = &NodeInfo{
			State:           state,
			LastStateChange: currentTime,
			JobCount:        0,
		}
		c.recordStateChange(currentTime, nodeID, "", state)
		return
	}

	if c.nodeStates[nodeID].State != state {
		oldState := c.nodeStates[nodeID].State
		c.nodeStates[nodeID].State = state
		c.nodeStates[nodeID].LastStateChange = currentTime
		c.recordStateChange(currentTime, nodeID, oldState, state)
	}
}

func (c *PowerController) recordStateChange(time float64, nodeID string, oldState, newState NodeState) {
	if _, err := os.Stat("node_state_changes.csv"); os.IsNotExist(err) {
		file, err := os.Create("node_state_changes.csv")
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

	file, err := os.OpenFile("node_state_changes.csv", os.O_APPEND|os.O_WRONLY, 0644)
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
		fmt.Sprintf("%f", time),
		nodeID,
		oldStateStr,
		string(newState),
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to write state change: %v", err))
	}
	writer.Flush()
}

// 对外提供的接口
func (c *PowerController) ExecutePowerActions() (int, int, int, int) {
	currentActiveNodeCount := len(c.GetActiveNodes())
	idleNodes := c.GetIdleNodes()
	sleepingNodes := c.GetSleepingNodes()
	poweredOffNodes := c.GetPoweredOffNodes()

	predictedActiveNodeCount := c.getPredictedActiveNodes()

	if predictedActiveNodeCount == -1 {
		return 0, 0, 0, 0
	}

	nodesToWake, nodesToPowerOn, nodesToSleep, nodesToShutdown := c.makePowerDecision(
		predictedActiveNodeCount,
		currentActiveNodeCount,
		idleNodes,
		sleepingNodes,
		poweredOffNodes,
	)

	for _, node := range nodesToWake {
		c.wakeUpNode(node)
	}

	for _, node := range nodesToPowerOn {
		c.powerOnNode(node)
	}

	for _, node := range nodesToSleep {
		c.sleepNode(node)
	}

	for _, node := range nodesToShutdown {
		c.powerOffNode(node)
	}

	return len(nodesToWake), len(nodesToPowerOn),
		len(nodesToSleep), len(nodesToShutdown)
}

// getPredictedActiveNodes 从预测服务获取预测的未来活跃节点数量
func (c *PowerController) getPredictedActiveNodes() int {
	// 检查系统运行时间是否达到4小时
	if c.time.Now() < 15000 { // 4小时 = 14400秒
		fmt.Printf("系统运行时间（%.2f秒）不足4小时，暂不执行预测\n", c.time.Now())
		return -1
	}

	reqBody := map[string]string{
		"csv_path": "/root/PredictModel/pybatsim-3.2.1/schedulers/green-energy/power_control/record.csv",
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal request body: %v", err))
	}

	resp, err := http.Post(
		fmt.Sprintf("%s/predict", c.predictorURL),
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to get prediction: %v", err))
	}
	defer resp.Body.Close()

	var predResp PredictionResponse
	if err := json.NewDecoder(resp.Body).Decode(&predResp); err != nil {
		panic(fmt.Sprintf("Failed to decode prediction response: %v", err))
	}

	if resp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("Prediction request failed: %s", predResp.Error))
	}

	return predResp.Prediction
}

// makePowerDecision 根据预测值和当前状态做出节点状态转换决策
func (c *PowerController) makePowerDecision(
	predictedActive int,
	currentActive int,
	idleNodes []string,
	sleepingNodes []string,
	poweredOffNodes []string,
) ([]string, []string, []string, []string) {
	// 获取需要唤醒的节点
	nodesToWake, nodesToPowerOn := c.getNodesForWakeUp(
		predictedActive,
		currentActive,
		sleepingNodes,
		poweredOffNodes,
	)

	// 如果需要唤醒节点，直接返回
	if len(nodesToWake) > 0 || len(nodesToPowerOn) > 0 {
		return nodesToWake, nodesToPowerOn, nil, nil
	}

	// 否则考虑让节点睡眠
	nodesToSleep, nodesToShutdown := c.getNodesForSleep(
		predictedActive,
		currentActive,
		idleNodes,
	)

	return nil, nil, nodesToSleep, nodesToShutdown
}

// getNodesForWakeUp 决定需要唤醒的节点
func (c *PowerController) getNodesForWakeUp(
	predictedActive int,
	currentActive int,
	sleepingNodes []string,
	poweredOffNodes []string,
) ([]string, []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 计算当前可用节点总数（活跃+空闲）
	currentIdle := len(c.GetIdleNodes())
	totalAvailable := currentActive + currentIdle

	// 计算负载变化率（每分钟变化的节点数）
	loadChangeRate := float64(predictedActive-currentActive) / float64(c.checkInterval)

	// 根据变化率动态调整冗余节点数量
	dynamicRedundancy := max(
		c.redundancyNodeNum, // 基础冗余数量
		int(abs(loadChangeRate)*float64(c.checkInterval)*1.5), // 根据变化速率调整
	)

	// 计算最小需要的空闲节点数
	minIdleNodes := max(2, int(float64(c.nodeCount)*c.idleReserveRatio))

	// 判断是否需要唤醒节点
	needWakeup := (predictedActive >= totalAvailable) || (currentIdle < minIdleNodes)

	if !needWakeup {
		return nil, nil
	}

	// 计算需要唤醒的节点数量
	needed := max(
		predictedActive-totalAvailable+dynamicRedundancy, // 预测需求 + 动态冗余
		minIdleNodes-currentIdle,                         // 补充最小空闲数量
	)

	if needed <= 0 {
		return nil, nil
	}

	// 优先从睡眠节点中唤醒
	var nodesToWake []string
	var nodesToPowerOn []string

	if len(sleepingNodes) > 0 {
		if len(sleepingNodes) > needed {
			nodesToWake = sleepingNodes[:needed]
		} else {
			nodesToWake = sleepingNodes
			needed -= len(nodesToWake)
		}
	}

	if needed > 0 && len(poweredOffNodes) > 0 {
		if len(poweredOffNodes) > needed {
			nodesToPowerOn = poweredOffNodes[:needed]
		} else {
			nodesToPowerOn = poweredOffNodes
		}
	}

	fmt.Printf("[Time %.2f] Wake-up decision:\n"+
		"  Current active: %d\n"+
		"  Current idle: %d\n"+
		"  Total available: %d\n"+
		"  Predicted active: %d\n"+
		"  Load change rate: %.2f nodes/min\n"+
		"  Dynamic redundancy: %d\n"+
		"  Min idle required: %d\n"+
		"  Nodes needed: %d\n"+
		"  Waking up: %d sleeping, %d powered-off\n",
		c.time.Now(), currentActive, currentIdle, totalAvailable,
		predictedActive, loadChangeRate, dynamicRedundancy,
		minIdleNodes, needed, len(nodesToWake), len(nodesToPowerOn))

	return nodesToWake, nodesToPowerOn
}

// getNodesForSleep 决定需要睡眠或关机的节点
func (c *PowerController) getNodesForSleep(
	predictedActive int,
	currentActive int,
	idleNodes []string,
) ([]string, []string) {
	if len(idleNodes) == 0 {
		return nil, nil
	}

	// 计算负载变化率
	loadChangeRate := float64(predictedActive-currentActive) / float64(c.checkInterval)

	// 根据负载变化趋势调整buffer ratio
	adjustedBuffer := c.bufferRatio
	if loadChangeRate > 0 {
		// 负载上升趋势，更保守地减少节点
		adjustedBuffer *= 1.5
	} else {
		// 负载下降趋势，更激进地减少节点
		adjustedBuffer *= 0.8
	}

	// 调整预测值，添加buffer
	adjustedPredicted := int(float64(predictedActive) * (1 + adjustedBuffer))

	// 计算需要保留的空闲节点数量
	minIdleNodes := max(2, int(float64(c.nodeCount)*c.idleReserveRatio))
	currentIdle := len(idleNodes)

	// 计算最小需要保留的可用节点总数
	minTotalAvailable := max(
		adjustedPredicted+minIdleNodes, // 预测值 + 最小空闲数量
		currentActive+minIdleNodes,     // 当前活跃 + 最小空闲数量
	)

	// 计算可以休眠的节点数量
	totalAvailable := currentActive + currentIdle
	nodesCanSleep := totalAvailable - minTotalAvailable

	// 确保休眠后仍保持最小空闲节点数
	maxCanSleep := currentIdle - minIdleNodes
	nodesCanSleep = min(nodesCanSleep, maxCanSleep)

	if nodesCanSleep <= 0 {
		return nil, nil
	}

	fmt.Printf("[Time %.2f] Sleep decision:\n"+
		"  Current active: %d\n"+
		"  Current idle: %d\n"+
		"  Predicted active: %d\n"+
		"  Adjusted predicted: %d\n"+
		"  Load change rate: %.2f nodes/min\n"+
		"  Adjusted buffer ratio: %.2f\n"+
		"  Min idle required: %d\n"+
		"  Min total available: %d\n"+
		"  Can sleep: %d\n"+
		"  Idle nodes after sleep: %d\n",
		c.time.Now(), currentActive, currentIdle,
		predictedActive, adjustedPredicted, loadChangeRate,
		adjustedBuffer, minIdleNodes, minTotalAvailable,
		nodesCanSleep, currentIdle-nodesCanSleep)

	var nodesToSleep []string
	var nodesToShutdown []string

	for _, node := range idleNodes[:nodesCanSleep] {
		idleTime := c.time.Since(c.nodeStates[node].LastStateChange)
		if idleTime >= float64(c.sleepThreshold) {
			nodesToShutdown = append(nodesToShutdown, node)
		} else {
			nodesToSleep = append(nodesToSleep, node)
		}
	}

	return nodesToSleep, nodesToShutdown
}

// 对外提供的接口
func (c *PowerController) RecordSystemState(currentTime float64, runningJobs []interface{}, waitingJobs []interface{}, currentPower float64) {
	nbComputing := len(c.GetActiveNodes())
	nbIdle := len(c.GetIdleNodes())
	nbSleeping := len(c.GetSleepingNodes())
	nbPoweredOff := len(c.GetPoweredOffNodes())

	nbSwitchingToSleep := 0
	nbWakingFromSleep := 0
	nbPoweringOn := 0
	nbPoweringOff := 0

	c.mu.RLock()
	for _, info := range c.nodeStates {
		switch info.State {
		case NodeStateSwitchingToSleep:
			nbSwitchingToSleep++
		case NodeStateWakingup:
			nbWakingFromSleep++
		case NodeStatePoweringOn:
			nbPoweringOn++
		case NodeStatePoweringOff:
			nbPoweringOff++
		}
	}
	c.mu.RUnlock()

	totalNodes := c.nodeCount
	utilizationRate := float64(nbComputing) / float64(totalNodes)

	datetimeObj := StartTime.Add(time.Duration(currentTime * float64(time.Second)))

	if _, err := os.Stat("record.csv"); os.IsNotExist(err) {
		file, err := os.Create("record.csv")
		if err != nil {
			panic(fmt.Sprintf("Failed to create record file: %v", err))
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		err = writer.Write([]string{
			"timestamp", "datetime", "running_jobs", "waiting_jobs",
			"nb_computing", "utilization_rate", "epower", "nb_idle",
			"nb_sleeping", "nb_powered_off", "nb_switching_to_sleep",
			"nb_waking_from_sleep", "nb_powering_on", "nb_powering_off",
		})
		if err != nil {
			panic(fmt.Sprintf("Failed to write header: %v", err))
		}
		writer.Flush()
	}

	file, err := os.OpenFile("record.csv", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("Failed to open record file: %v", err))
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	err = writer.Write([]string{
		fmt.Sprintf("%.2f", currentTime),
		datetimeObj.Format("2006-01-02 15:04:05"),
		fmt.Sprintf("%d", len(runningJobs)),
		fmt.Sprintf("%d", len(waitingJobs)),
		fmt.Sprintf("%d", nbComputing),
		fmt.Sprintf("%.4f", utilizationRate),
		fmt.Sprintf("%.2f", currentPower),
		fmt.Sprintf("%d", nbIdle),
		fmt.Sprintf("%d", nbSleeping),
		fmt.Sprintf("%d", nbPoweredOff),
		fmt.Sprintf("%d", nbSwitchingToSleep),
		fmt.Sprintf("%d", nbWakingFromSleep),
		fmt.Sprintf("%d", nbPoweringOn),
		fmt.Sprintf("%d", nbPoweringOff),
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to write record: %v", err))
	}
	writer.Flush()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
