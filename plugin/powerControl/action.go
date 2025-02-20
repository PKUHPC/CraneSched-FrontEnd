package main

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

func (c *PowerManager) filterExcludedNodes(nodeIDs []string) []string {
	var allowedNodes []string
	for _, nodeID := range nodeIDs {
		if _, excluded := c.excludeNodesMap[nodeID]; !excluded {
			allowedNodes = append(allowedNodes, nodeID)
		} else {
			log.Infof("Node %s is excluded from power management", nodeID)
		}
	}
	return allowedNodes
}

func (c *PowerManager) wakeupNodes(nodeIDs []string) error {
	for _, nodeID := range nodeIDs {
		err := c.wakeUpNode(nodeID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *PowerManager) sleepNodes(nodeIDs []string) error {
	allowedNodes := c.filterExcludedNodes(nodeIDs)
	for _, nodeID := range allowedNodes {
		if err := c.sleepNode(nodeID); err != nil {
			return err
		}
	}
	return nil
}

func (c *PowerManager) powerOnNodes(nodeIDs []string) error {
	for _, nodeID := range nodeIDs {
		err := c.powerOnNode(nodeID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *PowerManager) powerOffNodes(nodeIDs []string) error {
	allowedNodes := c.filterExcludedNodes(nodeIDs)
	if allowedNodes == nil || len(allowedNodes) == 0 {
		return nil
	}

	maxNodesPerPowerOff := c.config.IPMI.PowerOffMaxNodesPerBatch
	powerOffBatchInterval := time.Duration(c.config.IPMI.PowerOffBatchIntervalSeconds) * time.Second

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < len(allowedNodes); i += maxNodesPerPowerOff {
			end := i + maxNodesPerPowerOff
			if end > len(allowedNodes) {
				end = len(allowedNodes)
			}

			currentBatch := allowedNodes[i:end]
			log.Infof("Processing power off batch %d-%d of %d nodes", i, end-1, len(allowedNodes))

			for _, nodeID := range currentBatch {
				if err := c.powerOffNode(nodeID); err != nil {
					log.Errorf("Failed to power off node %s: %v", nodeID, err)
				} else {
					log.Infof("Successfully powered off node %s", nodeID)
				}
			}

			if end < len(allowedNodes) {
				log.Infof("Waiting %d seconds before processing next power off batch...",
					c.config.IPMI.PowerOffBatchIntervalSeconds)
				time.Sleep(powerOffBatchInterval)
			}
		}
	}()

	return nil
}

func (c *PowerManager) wakeUpNode(nodeID string) error {
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	if info.State != Sleep {
		return fmt.Errorf("node %s is not in sleeping state", nodeID)
	}

	c.updateNodeState(nodeID, Wakingup)

	err := c.powerTool.WakeUp(nodeID)
	if err != nil {
		log.Errorf("Failed to wake up node %s: %v", nodeID, err)
		c.updateNodeState(nodeID, Sleep)
		return err
	}

	return nil
}

func (c *PowerManager) powerOnNode(nodeID string) error {
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	if info.State != PoweredOff {
		return fmt.Errorf("node %s is not in powered off state", nodeID)
	}

	c.updateNodeState(nodeID, PoweringOn)

	err := c.powerTool.PowerOn(nodeID)
	if err != nil {
		log.Errorf("Failed to power on node %s: %v", nodeID, err)
		c.updateNodeState(nodeID, PoweredOff)
		return err
	}

	return nil
}

func (c *PowerManager) sleepNode(nodeID string) error {
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	if info.State != Idle {
		return fmt.Errorf("node %s is not in idle state", nodeID)
	}

	c.updateNodeState(nodeID, ToSleeping)

	err := c.powerTool.Sleep(nodeID)
	if err != nil {
		log.Errorf("Failed to put node %s to sleep: %v", nodeID, err)
		c.updateNodeState(nodeID, Idle)
		return err
	}

	return nil
}

func (c *PowerManager) powerOffNode(nodeID string) error {
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	oldState := info.State
	if oldState != Sleep && oldState != Idle {
		return fmt.Errorf("node %s is not in sleep or idle state", nodeID)
	}

	c.updateNodeState(nodeID, PoweringOff)

	err := c.powerTool.PowerOff(nodeID)
	if err != nil {
		c.updateNodeState(nodeID, oldState)
		return err
	}

	return nil
}
