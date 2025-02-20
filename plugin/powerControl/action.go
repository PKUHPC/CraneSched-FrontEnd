package main

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

func (c *PowerController) wakeUpNode(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	info, exists := c.nodeStates[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	if info.State != NodeStateSleeping {
		return fmt.Errorf("node %s is not in sleeping state", nodeID)
	}

	c.updateNodeState(nodeID, NodeStateWakingup)

	err := c.powerTool.WakeUp(nodeID)
	if err != nil {
		log.Errorf("Failed to wake up node %s: %v", nodeID, err)
		c.updateNodeState(nodeID, NodeStateSleeping)
		return err
	}

	c.updateNodeState(nodeID, NodeStateIdle)
	return nil
}

func (c *PowerController) powerOnNode(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	info, exists := c.nodeStates[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	if info.State != NodeStatePoweredOff {
		return fmt.Errorf("node %s is not in powered off state", nodeID)
	}

	c.updateNodeState(nodeID, NodeStatePoweringOn)

	err := c.powerTool.PowerOn(nodeID)
	if err != nil {
		log.Errorf("Failed to power on node %s: %v", nodeID, err)
		c.updateNodeState(nodeID, NodeStatePoweredOff)
		return err
	}

	c.updateNodeState(nodeID, NodeStateIdle)
	return nil
}

func (c *PowerController) sleepNode(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	info, exists := c.nodeStates[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	if info.State != NodeStateIdle {
		return fmt.Errorf("node %s is not in idle state", nodeID)
	}

	c.updateNodeState(nodeID, NodeStateSwitchingToSleep)

	err := c.powerTool.Sleep(nodeID)
	if err != nil {
		log.Errorf("Failed to put node %s to sleep: %v", nodeID, err)
		c.updateNodeState(nodeID, NodeStateIdle)
		return err
	}

	c.updateNodeState(nodeID, NodeStateSleeping)
	return nil
}

func (c *PowerController) powerOffNode(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Infof("Starting powerOffNode for %s", nodeID)

	info, exists := c.nodeStates[nodeID]
	if !exists {
		log.Errorf("Node %s not found in nodeStates", nodeID)
		return fmt.Errorf("node %s not found", nodeID)
	}

	log.Infof("Node %s current state: %v", nodeID, info.State)

	if info.State != NodeStateIdle {
		log.Errorf("Node %s is not in idle state (current state: %v)", nodeID, info.State)
		return fmt.Errorf("node %s is not in idle state", nodeID)
	}

	log.Infof("Updating node %s state to PoweringOff", nodeID)
	c.updateNodeState(nodeID, NodeStatePoweringOff)

	log.Infof("Executing IPMI power off command for node %s", nodeID)
	err := c.powerTool.PowerOff(nodeID)
	if err != nil {
		log.Errorf("Failed to power off node %s: %v", nodeID, err)
		c.updateNodeState(nodeID, NodeStateIdle)
		return err
	}

	log.Infof("Updating node %s state to PoweredOff", nodeID)
	c.updateNodeState(nodeID, NodeStatePoweredOff)
	return nil
}
