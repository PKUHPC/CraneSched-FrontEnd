package main

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

type HostInfo struct {
	BMCHost    string
	Interfaces []NetworkInterface
}

type IPMITool struct {
	hostInfos map[string]HostInfo
	BMCUser   string
	BMCPass   string
	sshUser   string
	sshPass   string

	mu sync.RWMutex
}

func NewIPMITool(config *Config) *IPMITool {
	hostInfos := make(map[string]HostInfo)
	for nodeID, bmcHost := range config.IPMI.NodeBMCMapping {
		hostInfos[nodeID] = HostInfo{
			BMCHost:    bmcHost,
			Interfaces: make([]NetworkInterface, 0),
		}
	}

	return &IPMITool{
		hostInfos: hostInfos,
		BMCUser:   config.IPMI.User,
		BMCPass:   config.IPMI.Password,
		sshUser:   config.SSH.User,
		sshPass:   config.SSH.Password,
	}
}

func (i *IPMITool) RegisterNode(nodeID string, interfaces []NetworkInterface) error {
	if nodeID == "" {
		return fmt.Errorf("nodeID is required")
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	info, exists := i.hostInfos[nodeID]
	if !exists {
		return fmt.Errorf("no BMC mapping found for node %s", nodeID)
	}
	if info.BMCHost == "" {
		return fmt.Errorf("empty BMC mapping for node %s", nodeID)
	}

	if len(interfaces) != 0 {
		info.Interfaces = interfaces
		i.hostInfos[nodeID] = info
	}

	log.Infof("Node registered: %s (BMC: %s)", nodeID, info.BMCHost)
	return nil
}

func (i *IPMITool) UnregisterNode(nodeID string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	info, exists := i.hostInfos[nodeID]
	if !exists {
		return
	}
	info.Interfaces = nil
	i.hostInfos[nodeID] = info
}

func (i *IPMITool) PowerOn(nodeID string) error {
	i.mu.RLock()
	info, ok := i.hostInfos[nodeID]
	i.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no configuration found for node %s", nodeID)
	}

	cmd := exec.Command("ipmitool",
		"-I", "lanplus",
		"-H", info.BMCHost,
		"-U", i.BMCUser,
		"-P", i.BMCPass,
		"power", "on")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("IPMI power on failed for node %s: %v, output: %s",
			nodeID, err, string(output))
	}

	if !strings.Contains(string(output), "Up/On") {
		return fmt.Errorf("unexpected IPMI output for power on: %s", string(output))
	}

	return nil
}

func (i *IPMITool) PowerOff(nodeID string) error {
	i.mu.RLock()
	info, ok := i.hostInfos[nodeID]
	i.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no configuration found for node %s", nodeID)
	}

	cmd := exec.Command("ipmitool",
		"-I", "lanplus",
		"-H", info.BMCHost,
		"-U", i.BMCUser,
		"-P", i.BMCPass,
		"power", "off")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("IPMI power off failed for node %s: %v, output: %s",
			nodeID, err, string(output))
	}

	if !strings.Contains(string(output), "Down/Off") {
		return fmt.Errorf("unexpected IPMI output for power off: %s", string(output))
	}

	return nil
}

func (i *IPMITool) WakeUp(nodeID string) error {
	i.mu.RLock()
	info, ok := i.hostInfos[nodeID]
	i.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no configuration found for node %s", nodeID)
	}
	if len(info.Interfaces) == 0 {
		return fmt.Errorf("no network interface found for node %s", nodeID)
	}

	sent := false
	var lastErr error
	for _, iface := range info.Interfaces {
		mac, err := net.ParseMAC(iface.MAC)
		if err != nil || len(mac) != 6 {
			lastErr = fmt.Errorf("invalid MAC address %q", iface.MAC)
			log.Error(lastErr)
			continue
		}

		packet := make([]byte, 102)
		for i := 0; i < 6; i++ {
			packet[i] = 0xFF
		}
		for i := 1; i <= 16; i++ {
			copy(packet[i*6:], mac)
		}

		conn, err := net.Dial("udp", "255.255.255.255:9")
		if err != nil {
			lastErr = fmt.Errorf("failed to create UDP connection: %w", err)
			log.Error(lastErr)
			continue
		}

		_, err = conn.Write(packet)
		closeErr := conn.Close()
		if err != nil {
			lastErr = fmt.Errorf("failed to send WoL packet: %w", err)
			log.Error(lastErr)
			continue
		}
		sent = true
		if closeErr != nil {
			log.Warnf("failed to close WoL connection: %v", closeErr)
		}

		log.Infof("Wake-on-LAN packet sent to %s (%s)", nodeID, iface.MAC)
	}

	if sent {
		return nil
	}
	return fmt.Errorf("failed to send Wake-on-LAN packet for node %s: %w", nodeID, lastErr)
}

func (i *IPMITool) Sleep(nodeID string) error {
	i.mu.RLock()
	info, ok := i.hostInfos[nodeID]
	i.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no configuration found for node %s", nodeID)
	}
	if len(info.Interfaces) == 0 {
		return fmt.Errorf("no network interface found for node %s", nodeID)
	}

	sshCmd := exec.Command("sshpass", "-p", i.sshPass,
		"ssh",
		"-o", "StrictHostKeyChecking=no",
		"-o", "ConnectTimeout=10",
		fmt.Sprintf("%s@%s", i.sshUser, info.Interfaces[0].IP),
		"systemctl suspend")

	output, err := sshCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to execute sleep command: %v, output: %s", err, string(output))
	}

	log.Infof("Sleep command sent to %s (%s)", nodeID, info.Interfaces[0].IP)
	return nil
}

func (i *IPMITool) GetPowerState(nodeID string) (bool, error) {
	i.mu.RLock()
	info, ok := i.hostInfos[nodeID]
	i.mu.RUnlock()

	if !ok {
		return false, fmt.Errorf("no configuration found for node %s", nodeID)
	}

	cmd := exec.Command("ipmitool",
		"-I", "lanplus",
		"-H", info.BMCHost,
		"-U", i.BMCUser,
		"-P", i.BMCPass,
		"power", "status")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("IPMI power status check failed for node %s: %v, output: %s",
			nodeID, err, string(output))
	}

	return strings.Contains(string(output), "on"), nil
}

func (i *IPMITool) CheckNodeAlive(nodeID string) bool {
	i.mu.RLock()
	info, ok := i.hostInfos[nodeID]
	i.mu.RUnlock()

	if !ok || len(info.Interfaces) == 0 {
		return false
	}

	cmd := exec.Command("ping", "-c", "1", "-W", "3", info.Interfaces[0].IP)
	err := cmd.Run()

	return err == nil
}
