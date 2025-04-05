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
	MAC      string
	IP       string
	IPMIHost string
}

type IPMITool struct {
	hostInfos  map[string]HostInfo
	bmcMapping map[string]string
	ipmiUser   string
	ipmiPass   string
	sshUser    string
	sshPass    string

	mu sync.RWMutex
}

func NewIPMITool(config *Config) *IPMITool {
	return &IPMITool{
		hostInfos:  make(map[string]HostInfo),
		bmcMapping: config.IPMI.NodeBMCMapping,
		ipmiUser:   config.IPMI.User,
		ipmiPass:   config.IPMI.Password,
		sshUser:    config.SSH.User,
		sshPass:    config.SSH.Password,
	}
}

func (i *IPMITool) RegisterNode(nodeID, mac, ip string) error {
	if nodeID == "" || mac == "" || ip == "" {
		return fmt.Errorf("all parameters (nodeID, mac, ip) are required")
	}

	IPMIHost, exists := i.bmcMapping[nodeID]
	if !exists {
		return fmt.Errorf("no BMC mapping found for node %s", nodeID)
	}

	i.mu.Lock()
	i.hostInfos[nodeID] = HostInfo{
		MAC:      mac,
		IP:       ip,
		IPMIHost: IPMIHost,
	}
	i.mu.Unlock()

	log.Infof("Node registered: %s (MAC: %s, IP: %s, BMC: %s)",
		nodeID, mac, ip, IPMIHost)
	return nil
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
		"-H", info.IPMIHost,
		"-U", i.ipmiUser,
		"-P", i.ipmiPass,
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
		"-H", info.IPMIHost,
		"-U", i.ipmiUser,
		"-P", i.ipmiPass,
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

	mac := make([]byte, 6)
	macParts := strings.Split(info.MAC, ":")
	if len(macParts) != 6 {
		return fmt.Errorf("invalid MAC address format: %s", info.MAC)
	}

	for i := 0; i < 6; i++ {
		var value byte
		if _, err := fmt.Sscanf(macParts[i], "%02x", &value); err != nil {
			return fmt.Errorf("invalid MAC address part: %s", macParts[i])
		}
		mac[i] = value
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
		return fmt.Errorf("failed to create UDP connection: %v", err)
	}
	defer conn.Close()

	_, err = conn.Write(packet)
	if err != nil {
		return fmt.Errorf("failed to send WoL packet: %v", err)
	}

	log.Infof("Wake-on-LAN packet sent to %s (%s)", nodeID, info.MAC)
	return nil
}

func (i *IPMITool) Sleep(nodeID string) error {
	i.mu.RLock()
	info, ok := i.hostInfos[nodeID]
	i.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no configuration found for node %s", nodeID)
	}

	sshCmd := exec.Command("sshpass", "-p", i.sshPass,
		"ssh",
		"-o", "StrictHostKeyChecking=no",
		"-o", "ConnectTimeout=10",
		fmt.Sprintf("%s@%s", i.sshUser, info.IP),
		"systemctl suspend")

	output, err := sshCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to execute sleep command: %v, output: %s", err, string(output))
	}

	log.Infof("Sleep command sent to %s (%s)", nodeID, info.IP)
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
		"-H", info.IPMIHost,
		"-U", i.ipmiUser,
		"-P", i.ipmiPass,
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

	if !ok {
		return false
	}

	cmd := exec.Command("ping", "-c", "1", "-W", "3", info.IP)
	err := cmd.Run()

	return err == nil
}
