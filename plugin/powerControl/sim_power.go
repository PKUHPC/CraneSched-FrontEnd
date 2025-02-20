package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// SimPowerTool 实现仿真环境下的电源操作
type SimPowerTool struct {
	client  *http.Client
	baseURL string
}

func NewSimPowerTool() *SimPowerTool {
	return &SimPowerTool{
		client:  &http.Client{Timeout: 5 * time.Second},
		baseURL: "http://localhost:8082",
	}
}

// 实现与IPMITool相同的接口
func (s *SimPowerTool) WakeUp(nodeID string) error {
	return s.executePowerAction(nodeID, "wake_up")
}

func (s *SimPowerTool) Sleep(nodeID string) error {
	return s.executePowerAction(nodeID, "sleep")
}

func (s *SimPowerTool) PowerOn(nodeID string) error {
	return s.executePowerAction(nodeID, "power_on")
}

func (s *SimPowerTool) PowerOff(nodeID string) error {
	return s.executePowerAction(nodeID, "power_off")
}

func (s *SimPowerTool) executePowerAction(nodeID, action string) error {
	payload := map[string]string{
		"node_id": nodeID,
		"action":  action,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	resp, err := s.client.Post(
		s.baseURL+"/scheduler/power/action",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed with status: %d", resp.StatusCode)
	}

	return nil
}

// 添加 RegisterNode 方法的空实现
func (s *SimPowerTool) RegisterNode(nodeID, mac, ip string) error {
	// 仿真环境下不需要实际注册节点
	return nil
}
