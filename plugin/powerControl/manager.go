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

	// How long a version entry of a node absent from nodesInfo is kept so
	// that late stale hooks are still rejected.
	nodeVersionRetention = 24 * time.Hour
)

type PowerManager struct {
	config    *Config
	powerTool PowerTool

	nodesInfo sync.Map
	// Guards compound read-modify-write access to nodesInfo and the
	// nodeVersions map.
	nodesInfoMutex sync.Mutex
	nodeVersions   map[string]nodeVersion

	nodeOperations      map[string]*nodeOperationLock
	nodeOperationsMutex sync.Mutex

	powerReporters      map[string]*nodeReporter
	powerReportersMutex sync.Mutex

	ctldClient      protos.CraneCtldClient
	ctldClientMutex sync.Mutex // only guards the ctldClient reference

	stopChan chan struct{}
}

// nodeReporter serializes the PowerStateChange reports of one node and
// stamps them with a monotonically increasing sequence, so concurrent
// reporters cannot deliver reports to CraneCtld out of order.
type nodeReporter struct {
	mu       sync.Mutex
	queue    []powerReport
	draining bool
	sequence uint64
}

type powerReport struct {
	state      NodeState
	generation uint64
}

type PredictionRequest struct {
	TotalNodes int `json:"total_nodes"`
}

type nodeSnapshot struct {
	nodeTarget
	lastStateChangeTime time.Time
}

type nodeTarget struct {
	nodeID     string
	generation uint64
}

func (n nodeTarget) String() string {
	return n.nodeID
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
		config:         config,
		powerTool:      NewIPMITool(config),
		nodeVersions:   make(map[string]nodeVersion),
		nodeOperations: make(map[string]*nodeOperationLock),
		powerReporters: make(map[string]*nodeReporter),
		stopChan:       make(chan struct{}),
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

func compareNodeVersion(generation, revision uint64, current nodeVersion) int {
	if generation < current.generation ||
		(generation == current.generation && revision < current.revision) {
		return -1
	}
	if generation == current.generation && revision == current.revision {
		return 0
	}
	return 1
}

func (c *PowerManager) lockNodeOperation(nodeID string) func() {
	c.nodeOperationsMutex.Lock()
	operation, exists := c.nodeOperations[nodeID]
	if !exists {
		operation = &nodeOperationLock{}
		c.nodeOperations[nodeID] = operation
	}
	operation.users++
	c.nodeOperationsMutex.Unlock()

	operation.mutex.Lock()
	return func() {
		operation.mutex.Unlock()

		c.nodeOperationsMutex.Lock()
		operation.users--
		if operation.users == 0 {
			delete(c.nodeOperations, nodeID)
		}
		c.nodeOperationsMutex.Unlock()
	}
}

// tryLockNodeOperation is the non-blocking variant of lockNodeOperation.
// It returns (nil, false) when an operation on the node is in flight.
func (c *PowerManager) tryLockNodeOperation(nodeID string) (func(), bool) {
	c.nodeOperationsMutex.Lock()
	operation, exists := c.nodeOperations[nodeID]
	if !exists {
		operation = &nodeOperationLock{}
		c.nodeOperations[nodeID] = operation
	}
	operation.users++
	c.nodeOperationsMutex.Unlock()

	release := func() {
		c.nodeOperationsMutex.Lock()
		operation.users--
		if operation.users == 0 {
			delete(c.nodeOperations, nodeID)
		}
		c.nodeOperationsMutex.Unlock()
	}

	if !operation.mutex.TryLock() {
		release()
		return nil, false
	}
	return func() {
		operation.mutex.Unlock()
		release()
	}, true
}

// claimNodeVersionLocked rejects a stale generation/revision and records
// the accepted one. Returns the comparison against the previously recorded
// version (1 when none was recorded).
func (c *PowerManager) claimNodeVersionLocked(nodeID string, generation, revision uint64) int {
	comparison := 1
	if current, exists := c.nodeVersions[nodeID]; exists {
		comparison = compareNodeVersion(generation, revision, current)
		if comparison < 0 {
			return comparison
		}
	}
	c.nodeVersions[nodeID] = nodeVersion{generation: generation, revision: revision, updatedAt: time.Now()}
	return comparison
}

// ApplyNodeDefinitionVersion additionally reports whether a redelivered
// (equal-version) hook still has work to do, making hook processing
// idempotent: an UPSERT whose RegisterNode failed can be retried.
func (c *PowerManager) ApplyNodeDefinitionVersion(nodeID string, generation, revision uint64, present bool) bool {
	c.nodesInfoMutex.Lock()
	defer c.nodesInfoMutex.Unlock()

	comparison := c.claimNodeVersionLocked(nodeID, generation, revision)
	if comparison < 0 {
		return false
	}
	if comparison == 0 {
		value, nodePresent := c.nodesInfo.Load(nodeID)
		if !present {
			return nodePresent
		}
		if !nodePresent {
			return true
		}
		info := value.(*NodeInfo)
		return info.Generation != generation || info.Revision != revision
	}
	return true
}

// AcceptNodeVersion is the check-and-claim primitive for node versions:
// it rejects stale generations/revisions and records the accepted one.
// Callers must claim the version before calling RegisterNode.
func (c *PowerManager) AcceptNodeVersion(nodeID string, generation, revision uint64) bool {
	c.nodesInfoMutex.Lock()
	defer c.nodesInfoMutex.Unlock()

	return c.claimNodeVersionLocked(nodeID, generation, revision) >= 0
}

type nodeTrackingStatus int

const (
	nodeTrackingCurrent nodeTrackingStatus = iota
	// A newer definition of the node took over; the caller's generation is
	// obsolete and its request can be ignored silently.
	nodeTrackingStale
	// The definition was accepted but the node has no runtime info, e.g.
	// the power tool registration failed for lack of a BMC mapping. Such a
	// node can never be powered; the caller must surface this loudly.
	nodeTrackingUntracked
)

func (c *PowerManager) NodeTrackingStatus(nodeID string, generation uint64) nodeTrackingStatus {
	c.nodesInfoMutex.Lock()
	defer c.nodesInfoMutex.Unlock()

	if current, exists := c.nodeVersions[nodeID]; exists &&
		current.generation != generation {
		return nodeTrackingStale
	}
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return nodeTrackingUntracked
	}
	if value.(*NodeInfo).Generation != generation {
		return nodeTrackingStale
	}
	return nodeTrackingCurrent
}

func (c *PowerManager) ResetNodeForNewGeneration(nodeID string, generation uint64) {
	if generation == 0 {
		return
	}

	c.nodesInfoMutex.Lock()
	value, exists := c.nodesInfo.Load(nodeID)
	stale := exists && value.(*NodeInfo).Generation != generation
	if stale {
		c.nodesInfo.Delete(nodeID)
	}
	c.nodesInfoMutex.Unlock()
	if stale {
		c.powerTool.UnregisterNode(nodeID)
	}
}

// RegisterNode stores the node runtime info. For dynamic nodes the version
// must already be claimed via AcceptNodeVersion/ApplyNodeDefinitionVersion
// under the same node operation lock.
func (c *PowerManager) RegisterNode(nodeID string, initialState NodeState, jobs map[string]struct{}, generation, revision uint64) {
	now := time.Now()
	hasJobSnapshot := jobs != nil
	jobs = cloneJobs(jobs)

	c.nodesInfoMutex.Lock()
	if value, exists := c.nodesInfo.Load(nodeID); exists &&
		value.(*NodeInfo).Generation == generation {
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
			Generation:          generation,
			Revision:            revision,
		})
		c.nodesInfoMutex.Unlock()
		if stateChanged {
			logNodeStateChange(nodeID, info.State, initialState)
			c.recordStateChange(now, nodeID, info.State, initialState)
		}
		return
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
		Generation:          generation,
		Revision:            revision,
	})
	c.nodesInfoMutex.Unlock()
	c.recordStateChange(now, nodeID, "", initialState)

	if isExcluded {
		log.Infof("Initialized node %s in %s state (excluded from power management)", nodeID, initialState)
	} else {
		log.Infof("Initialized node %s in %s state", nodeID, initialState)
	}
}

func (c *PowerManager) RemoveNode(nodeID string, generation uint64) {
	c.nodesInfoMutex.Lock()
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists || (generation != 0 &&
		(value.(*NodeInfo).Generation == 0 || value.(*NodeInfo).Generation > generation)) {
		c.nodesInfoMutex.Unlock()
		return
	}
	c.nodesInfo.Delete(nodeID)
	c.nodesInfoMutex.Unlock()
	c.powerTool.UnregisterNode(nodeID)

	// A running drainer keeps its own reference and exits when its queue is
	// empty; a later re-created node starts with a fresh reporter.
	c.powerReportersMutex.Lock()
	delete(c.powerReporters, nodeID)
	c.powerReportersMutex.Unlock()

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
		Generation:          info.Generation,
		Revision:            info.Revision,
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
	client := util.GetStubToCtldByConfig(config)

	c.ctldClientMutex.Lock()
	c.ctldClient = client
	c.ctldClientMutex.Unlock()
}

func (c *PowerManager) getCtldClient() protos.CraneCtldClient {
	c.ctldClientMutex.Lock()
	defer c.ctldClientMutex.Unlock()
	return c.ctldClient
}

func (c *PowerManager) rebuildCtldClient(stale protos.CraneCtldClient) protos.CraneCtldClient {
	configPath := util.DefaultConfigPath
	config := util.ParseConfig(configPath)
	client := util.GetStubToCtldByConfig(config)

	c.ctldClientMutex.Lock()
	defer c.ctldClientMutex.Unlock()
	if c.ctldClient != stale {
		// Another goroutine already replaced the stale client; reuse it.
		return c.ctldClient
	}
	c.ctldClient = client
	return client
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
		Generation:          info.Generation,
		Revision:            info.Revision,
	})
	c.nodesInfoMutex.Unlock()

	if stateChanged {
		logNodeStateChange(nodeID, info.State, newState)
		c.recordStateChange(now, nodeID, info.State, newState)
		c.notifyCtldPowerStateChange(nodeID, newState, info.Generation)
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
		Generation:          info.Generation,
		Revision:            info.Revision,
	}
	c.nodesInfo.Store(nodeID, updated)
	c.nodesInfoMutex.Unlock()

	logNodeStateChange(nodeID, info.State, newState)
	c.recordStateChange(now, nodeID, info.State, newState)
	c.notifyCtldPowerStateChange(nodeID, newState, info.Generation)
	return updated
}

// notifyCtldPowerStateChange enqueues a state report for the node. Reports
// of one node are delivered by a single drainer goroutine in enqueue order
// and carry a monotonic sequence, so a reordered or replayed report can
// never regress the state CraneCtld holds.
func (c *PowerManager) notifyCtldPowerStateChange(nodeID string, state NodeState, generation uint64) {
	c.powerReportersMutex.Lock()
	reporter, exists := c.powerReporters[nodeID]
	if !exists {
		// Seed the sequence with wall-clock nanoseconds so a restarted
		// plugin keeps producing sequences above the previous run's.
		reporter = &nodeReporter{sequence: uint64(time.Now().UnixNano())}
		c.powerReporters[nodeID] = reporter
	}
	c.powerReportersMutex.Unlock()

	reporter.mu.Lock()
	reporter.queue = append(reporter.queue, powerReport{state: state, generation: generation})
	if reporter.draining {
		reporter.mu.Unlock()
		return
	}
	reporter.draining = true
	reporter.mu.Unlock()
	go c.drainPowerReports(nodeID, reporter)
}

func (c *PowerManager) drainPowerReports(nodeID string, reporter *nodeReporter) {
	for {
		reporter.mu.Lock()
		if len(reporter.queue) == 0 {
			reporter.draining = false
			reporter.mu.Unlock()
			return
		}
		report := reporter.queue[0]
		reporter.queue = reporter.queue[1:]
		reporter.sequence++
		sequence := reporter.sequence
		reporter.mu.Unlock()

		c.sendPowerStateChange(nodeID, report, sequence)
	}
}

func (c *PowerManager) sendPowerStateChange(nodeID string, report powerReport, sequence uint64) {
	state, generation := report.state, report.generation
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
		CranedId:       nodeID,
		State:          powerType,
		Reason:         fmt.Sprintf("Node %s power state changed to %s by power manager(plugin/powerControl)", nodeID, state),
		Generation:     generation,
		Uid:            uint32(os.Getuid()),
		ReportSequence: sequence,
	}

	client := c.getCtldClient()
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			log.Warnf("Retrying PowerStateChange for node %s, attempt %d/%d", nodeID, attempt, maxRetries)
			time.Sleep(retryInterval)
		}
		// Fast-forward an obsolete backlog entry: skipping is safe because a
		// report of the newer state is already queued behind this one.
		value, exists := c.nodesInfo.Load(nodeID)
		if !exists || value.(*NodeInfo).State != state ||
			value.(*NodeInfo).Generation != generation {
			return
		}
		if client == nil || attempt > 1 {
			client = c.rebuildCtldClient(client)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		reply, err := client.PowerStateChange(ctx, req)
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
			log.Warnf("Attempt %d/%d: CraneCtld rejected the power state update for node %s",
				attempt, maxRetries, nodeID)
			if attempt == maxRetries {
				log.Errorf("All retry attempts failed to update node %s power state in CraneCtld", nodeID)
			}
			continue
		}

		// Log at Debug level for routine Idle <-> Active transitions, Info for power state changes
		if state == Idle || state == Active {
			log.Debugf("Successfully updated node %s power state to %s in CraneCtld", nodeID, powerType)
		} else {
			log.Infof("Successfully updated node %s power state to %s in CraneCtld", nodeID, powerType)
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

func (c *PowerManager) makeDecision() ([]nodeTarget, []nodeTarget, []nodeTarget, []nodeTarget) {
	currentTime := time.Now()

	var activeNodes, idleNodes, sleepNodes, poweredOffNodes []nodeSnapshot
	totalNodes := 0
	c.nodesInfo.Range(func(key, value interface{}) bool {
		info := value.(*NodeInfo)
		if info.Exclude {
			return true
		}
		totalNodes++
		node := nodeSnapshot{
			nodeTarget: nodeTarget{
				nodeID:     key.(string),
				generation: info.Generation,
			},
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

	predictedActiveNodeCount := c.getPredictedActiveNodeCount(totalNodes)

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

	var nodesToWake, nodesToPowerOn, nodesToSleep, nodesToPowerOff []nodeTarget
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
) ([]nodeTarget, []nodeTarget) {
	requiredIdleNodeCount := int(math.Ceil(float64(totalNodes) * c.config.PowerControl.IdleReserveRatio))
	totalAvailableNodeCount := currentActiveNodeCount + currentIdleNodeCount
	requiredTotalNodeCount := predictedActiveNodeCount + requiredIdleNodeCount

	neededNodeCount := requiredTotalNodeCount - totalAvailableNodeCount
	if neededNodeCount <= 0 {
		return nil, nil
	}

	var nodesToWake []nodeTarget
	var nodesToPowerOn []nodeTarget

	if len(sleepingNodes) > 0 {
		sortedNodes := make([]nodeSnapshot, len(sleepingNodes))
		copy(sortedNodes, sleepingNodes)
		sortByLastStateChange(sortedNodes)

		selectedCount := min(len(sortedNodes), neededNodeCount)
		for _, node := range sortedNodes[:selectedCount] {
			nodesToWake = append(nodesToWake, node.nodeTarget)
		}
		neededNodeCount -= selectedCount
	}

	if neededNodeCount > 0 && len(poweredOffNodes) > 0 {
		sortedNodes := make([]nodeSnapshot, len(poweredOffNodes))
		copy(sortedNodes, poweredOffNodes)
		sortByLastStateChange(sortedNodes)

		selectedCount := min(len(sortedNodes), neededNodeCount)
		for _, node := range sortedNodes[:selectedCount] {
			nodesToPowerOn = append(nodesToPowerOn, node.nodeTarget)
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
) ([]nodeTarget, []nodeTarget) {
	var nodesToSleep []nodeTarget
	var nodesToPowerOff []nodeTarget

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
					nodesToSleep = append(nodesToSleep, node.nodeTarget)
				}
			} else {
				for _, node := range sortedIdleNodes[:nodesCanSleepCount] {
					nodesToPowerOff = append(nodesToPowerOff, node.nodeTarget)
				}
			}
		}
	}

	for _, node := range sleepingNodes {
		log.Debugf("node %s last state change time: %s", node.nodeID, node.lastStateChangeTime)
		sleepTime := currentTime.Sub(node.lastStateChangeTime)
		log.Debugf("node %s sleep time: %s", node.nodeID, sleepTime)

		if sleepTime >= time.Duration(c.config.PowerControl.SleepTimeThresholdSeconds)*time.Second {
			nodesToPowerOff = append(nodesToPowerOff, node.nodeTarget)
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

		// While a power action holds the node operation lock its outcome is
		// undecided; checking now would race the action and misreport a
		// transient state. Skip the node until the next round.
		switch info.State {
		case PoweringOn, Wakingup, PoweringOff, ToSleeping:
			unlock, ok := c.tryLockNodeOperation(nodeID)
			if !ok {
				return true
			}
			defer unlock()
			latest, exists := c.nodesInfo.Load(nodeID)
			if !exists {
				return true
			}
			info = latest.(*NodeInfo)
		}

		// Nodes registered without network interfaces (e.g. via
		// NodeDefinitionHook before the craned first registers) cannot be
		// pinged, so liveness-based inference would misreport them as Sleep.
		// For such nodes only act on what the BMC power status can determine.
		canPing := c.powerTool.HasNetworkInfo(nodeID)

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
			if !canPing {
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
			} else if timedOut && canPing {
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
			if !canPing {
				return true
			}
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
			alive := canPing && c.powerTool.CheckNodeAlive(nodeID)
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
				} else if canPing {
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
			if powered && canPing {
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

	c.pruneStaleNodeVersions()
}

// pruneStaleNodeVersions drops version entries of nodes that left nodesInfo
// long ago so the map does not grow without bound.
func (c *PowerManager) pruneStaleNodeVersions() {
	cutoff := time.Now().Add(-nodeVersionRetention)

	c.nodesInfoMutex.Lock()
	defer c.nodesInfoMutex.Unlock()
	for nodeID, version := range c.nodeVersions {
		if !version.updatedAt.Before(cutoff) {
			continue
		}
		if _, exists := c.nodesInfo.Load(nodeID); !exists {
			delete(c.nodeVersions, nodeID)
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
