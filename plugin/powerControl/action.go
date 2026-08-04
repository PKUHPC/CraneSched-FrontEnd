package main

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

func (c *PowerManager) filterExcludedNodes(nodes []nodeTarget) []nodeTarget {
	var allowedNodes []nodeTarget
	for _, node := range nodes {
		value, exists := c.nodesInfo.Load(node.nodeID)
		if !exists {
			log.Warnf("Node %s not found in nodesInfo, skipping", node.nodeID)
			continue
		}

		info := value.(*NodeInfo)
		if info.Generation != node.generation {
			log.Debugf("Skipping stale power decision for node %s generation %d", node.nodeID, node.generation)
			continue
		}
		if !info.Exclude {
			allowedNodes = append(allowedNodes, node)
		} else {
			log.Infof("Node %s is excluded from power management", node.nodeID)
		}
	}
	return allowedNodes
}

func (c *PowerManager) wakeupNodes(nodes []nodeTarget) error {
	allowedNodes := c.filterExcludedNodes(nodes)
	var failedNodes []string
	for _, node := range allowedNodes {
		if err := c.wakeUpNode(node.nodeID, node.generation); err != nil {
			log.Errorf("Failed to wake up node %s: %v", node.nodeID, err)
			failedNodes = append(failedNodes, node.nodeID)
		}
	}
	if len(failedNodes) > 0 {
		return fmt.Errorf("failed to wake up nodes: %v", failedNodes)
	}
	return nil
}

func (c *PowerManager) sleepNodes(nodes []nodeTarget) error {
	allowedNodes := c.filterExcludedNodes(nodes)
	var failedNodes []string
	for _, node := range allowedNodes {
		if err := c.sleepNode(node.nodeID, node.generation); err != nil {
			log.Errorf("Failed to sleep node %s: %v", node.nodeID, err)
			failedNodes = append(failedNodes, node.nodeID)
		}
	}
	if len(failedNodes) > 0 {
		return fmt.Errorf("failed to sleep nodes: %v", failedNodes)
	}
	return nil
}

func (c *PowerManager) processBatchNodes(nodes []nodeTarget, operation string, nodeFunc func(string, uint64) error,
	maxNodesPerBatch int, batchIntervalSeconds int) error {

	if len(nodes) == 0 {
		return nil
	}

	batchInterval := time.Duration(batchIntervalSeconds) * time.Second

	go func() {
		for i := 0; i < len(nodes); i += maxNodesPerBatch {
			end := i + maxNodesPerBatch
			if end > len(nodes) {
				end = len(nodes)
			}

			currentBatch := nodes[i:end]
			log.Infof("Processing %s batch %d-%d of %d nodes", operation, i, end-1, len(nodes))

			for _, node := range currentBatch {
				if err := nodeFunc(node.nodeID, node.generation); err != nil {
					log.Errorf("Failed to %s node %s: %v", operation, node.nodeID, err)
				} else {
					log.Infof("Successfully %s node %s", operation, node.nodeID)
				}
			}

			if end < len(nodes) {
				log.Infof("Waiting %d seconds before processing next %s batch...",
					batchIntervalSeconds, operation)
				time.Sleep(batchInterval)
			}
		}
	}()

	return nil
}

func (c *PowerManager) powerOnNodes(nodes []nodeTarget) error {
	allowedNodes := c.filterExcludedNodes(nodes)

	maxNodesPerBatch := c.config.IPMI.MaxNodesPerBatch
	batchIntervalSeconds := c.config.IPMI.BatchIntervalSeconds
	if batchIntervalSeconds > 10 {
		batchIntervalSeconds = 10
	}

	return c.processBatchNodes(allowedNodes, "power on", c.powerOnNode,
		maxNodesPerBatch, batchIntervalSeconds)
}

func (c *PowerManager) powerOffNodes(nodes []nodeTarget) error {
	allowedNodes := c.filterExcludedNodes(nodes)

	maxNodesPerBatch := c.config.IPMI.MaxNodesPerBatch
	batchIntervalSeconds := c.config.IPMI.BatchIntervalSeconds

	return c.processBatchNodes(allowedNodes, "power off", c.powerOffNode,
		maxNodesPerBatch, batchIntervalSeconds)
}

func (c *PowerManager) wakeUpNode(nodeID string, generation uint64) error {
	unlock := c.lockNodeOperation(nodeID)
	defer unlock()
	if generation != 0 && !c.IsNodeGenerationCurrent(nodeID, generation) {
		return errStaleNodeGeneration
	}

	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	if info.State != Sleep && info.State != Wakingup {
		return fmt.Errorf("node %s is not in sleeping state", nodeID)
	}

	if info.State == Sleep {
		info = c.updateNodeStateIfCurrent(nodeID, info, Wakingup)
		if info == nil {
			return fmt.Errorf("node %s state changed before wake-up", nodeID)
		}
	}

	err := c.powerTool.WakeUp(nodeID)
	if err != nil {
		log.Errorf("Failed to wake up node %s: %v", nodeID, err)
		c.updateNodeStateIfCurrent(nodeID, info, Sleep)
		return err
	}

	return nil
}

func (c *PowerManager) powerOnNode(nodeID string, generation uint64) error {
	unlock := c.lockNodeOperation(nodeID)
	defer unlock()
	if generation != 0 && !c.IsNodeGenerationCurrent(nodeID, generation) {
		return errStaleNodeGeneration
	}

	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		if err := c.powerTool.RegisterNode(nodeID, nil); err != nil {
			return err
		}
		c.RegisterNode(nodeID, PoweredOff, nil, 0, 0)
		value, exists = c.nodesInfo.Load(nodeID)
		if !exists {
			return fmt.Errorf("node %s not found", nodeID)
		}
	}

	info := value.(*NodeInfo)
	oldState := info.State
	if oldState != PoweredOff && oldState != Sleep && oldState != PoweringOn {
		return fmt.Errorf("node %s is not powered off or sleeping", nodeID)
	}

	if oldState != PoweringOn {
		info = c.updateNodeStateIfCurrent(nodeID, info, PoweringOn)
		if info == nil {
			return fmt.Errorf("node %s state changed before power-on", nodeID)
		}
	}

	err := c.powerTool.PowerOn(nodeID)
	if err != nil {
		log.Errorf("Failed to power on node %s: %v", nodeID, err)
		if oldState == PoweringOn {
			c.updateNodeStateIfCurrent(nodeID, info, PoweredOff)
		} else {
			c.updateNodeStateIfCurrent(nodeID, info, oldState)
		}
		return err
	}

	return nil
}

func (c *PowerManager) sleepNode(nodeID string, generation uint64) error {
	unlock := c.lockNodeOperation(nodeID)
	defer unlock()
	if generation != 0 && !c.IsNodeGenerationCurrent(nodeID, generation) {
		return errStaleNodeGeneration
	}

	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	if info.State != Idle && info.State != ToSleeping {
		return fmt.Errorf("node %s is not in idle state", nodeID)
	}

	if info.State == Idle {
		info = c.updateNodeStateIfCurrent(nodeID, info, ToSleeping)
		if info == nil {
			return fmt.Errorf("node %s state changed before sleep", nodeID)
		}
	}

	err := c.powerTool.Sleep(nodeID)
	if err != nil {
		log.Errorf("Failed to put node %s to sleep: %v", nodeID, err)
		c.updateNodeStateIfCurrent(nodeID, info, Idle)
		return err
	}

	return nil
}

func (c *PowerManager) powerOffNode(nodeID string, generation uint64) error {
	unlock := c.lockNodeOperation(nodeID)
	defer unlock()
	if generation != 0 && !c.IsNodeGenerationCurrent(nodeID, generation) {
		return errStaleNodeGeneration
	}

	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	oldState := info.State
	if oldState != Sleep && oldState != Idle && oldState != PoweringOff {
		return fmt.Errorf("node %s is not in sleep or idle state", nodeID)
	}

	if oldState != PoweringOff {
		info = c.updateNodeStateIfCurrent(nodeID, info, PoweringOff)
		if info == nil {
			return fmt.Errorf("node %s state changed before power-off", nodeID)
		}
	}

	err := c.powerTool.PowerOff(nodeID)
	if err != nil {
		log.Errorf("Failed to power off node %s: %v", nodeID, err)
		if oldState == PoweringOff {
			c.updateNodeStateIfCurrent(nodeID, info, Sleep)
		} else {
			c.updateNodeStateIfCurrent(nodeID, info, oldState)
		}
		return err
	}

	return nil
}
