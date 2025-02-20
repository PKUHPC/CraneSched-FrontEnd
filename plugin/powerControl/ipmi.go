package main

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	ipmiHost = "10.129.227.172"
	ipmiUser = "ADMIN"
	ipmiPass = "Admin@123"
	sshUser  = "root"
	sshPass  = "hyq"
)

type NodeConfig struct {
	MAC      string
	IP       string
	IPMIHost string
}

type IPMITool struct {
	mu      sync.RWMutex
	configs map[string]NodeConfig
}

func NewIPMITool() *IPMITool {
	return &IPMITool{
		configs: make(map[string]NodeConfig),
	}
}

func (i *IPMITool) RegisterNode(nodeID, mac, ip string) error {
	if nodeID == "" || mac == "" || ip == "" {
		return fmt.Errorf("all parameters (nodeID, mac, ip, ipmiHost) are required")
	}

	i.mu.Lock()
	i.configs[nodeID] = NodeConfig{
		MAC:      mac,
		IP:       ip,
		IPMIHost: ipmiHost,
	}
	i.mu.Unlock()

	log.Infof("Node registered: %s (MAC: %s, IP: %s, IPMI: %s)", nodeID, mac, ip, ipmiHost)
	return nil
}

func (i *IPMITool) PowerOn(nodeID string) error {
	i.mu.RLock()
	config, ok := i.configs[nodeID]
	i.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no configuration found for node %s", nodeID)
	}

	cmd := exec.Command("ipmitool",
		"-I", "lanplus",
		"-H", config.IPMIHost,
		"-U", ipmiUser,
		"-P", ipmiPass,
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
	config, ok := i.configs[nodeID]
	i.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no configuration found for node %s", nodeID)
	}

	cmd := exec.Command("ipmitool",
		"-I", "lanplus",
		"-H", config.IPMIHost,
		"-U", ipmiUser,
		"-P", ipmiPass,
		"power", "soft")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("IPMI power off failed for node %s: %v, output: %s",
			nodeID, err, string(output))
	}

	fmt.Println("PowerOff", nodeID, "output", string(output))

	log.Infof("Soft power off command sent to node %s, output: %s", nodeID, string(output))

	time.Sleep(5 * time.Second)
	statusCmd := exec.Command("ipmitool",
		"-I", "lanplus",
		"-H", ipmiHost,
		"-U", ipmiUser,
		"-P", ipmiPass,
		"power", "status")

	statusOutput, err := statusCmd.CombinedOutput()
	if err != nil {
		log.Warnf("Failed to check power status after soft shutdown: %v", err)
	} else {
		log.Infof("Power status after soft shutdown command: %s", string(statusOutput))
	}

	return nil
}

func (i *IPMITool) WakeUp(nodeID string) error {
	i.mu.RLock()
	config, ok := i.configs[nodeID]
	i.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no configuration found for node %s", nodeID)
	}

	mac := make([]byte, 6)
	macParts := strings.Split(config.MAC, ":")
	if len(macParts) != 6 {
		return fmt.Errorf("invalid MAC address format: %s", config.MAC)
	}

	for i := 0; i < 6; i++ {
		var value byte
		if _, err := fmt.Sscanf(macParts[i], "%02x", &value); err != nil {
			return fmt.Errorf("invalid MAC address part: %s", macParts[i])
		}
		mac[i] = value
	}

	// 构造魔术包
	packet := make([]byte, 102)
	// 前6个字节填充0xFF
	for i := 0; i < 6; i++ {
		packet[i] = 0xFF
	}
	// 重复MAC地址16次
	for i := 1; i <= 16; i++ {
		copy(packet[i*6:], mac)
	}

	// 创建UDP连接
	conn, err := net.Dial("udp", "255.255.255.255:9")
	if err != nil {
		return fmt.Errorf("failed to create UDP connection: %v", err)
	}
	defer conn.Close()

	// 发送魔术包
	_, err = conn.Write(packet)
	if err != nil {
		return fmt.Errorf("failed to send WoL packet: %v", err)
	}

	log.Infof("Wake-on-LAN packet sent to %s (%s)", nodeID, config.MAC)
	return nil
}

// Sleep 通过SSH使节点进入睡眠状态
func (i *IPMITool) Sleep(nodeID string) error {
	i.mu.RLock()
	config, ok := i.configs[nodeID]
	i.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no configuration found for node %s", nodeID)
	}

	sshCmd := exec.Command("sshpass", "-p", sshPass,
		"ssh",
		"-o", "StrictHostKeyChecking=no",
		"-o", "ConnectTimeout=10",
		fmt.Sprintf("%s@%s", sshUser, config.IP),
		"systemctl suspend")

	output, err := sshCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to execute sleep command: %v, output: %s", err, string(output))
	}

	log.Infof("Sleep command sent to %s (%s)", nodeID, config.IP)
	return nil
}

func (i *IPMITool) GetPowerStatus(nodeID string) (bool, error) {
	i.mu.RLock()
	config, ok := i.configs[nodeID]
	i.mu.RUnlock()

	if !ok {
		return false, fmt.Errorf("no configuration found for node %s", nodeID)
	}

	cmd := exec.Command("ipmitool",
		"-I", "lanplus",
		"-H", config.IPMIHost,
		"-U", ipmiUser,
		"-P", ipmiPass,
		"power", "status")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("IPMI power status check failed for node %s: %v, output: %s",
			nodeID, err, string(output))
	}

	return strings.Contains(string(output), "on"), nil
}
