package main

import (
	"encoding/json"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

// StartTime 设置起始时间
var StartTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// NodeState 表示节点的状态
type NodeState string

const (
	NodeStateActive           NodeState = "active"             // 活跃状态
	NodeStateIdle             NodeState = "idle"               // 空闲状态
	NodeStateSleeping         NodeState = "sleeping"           // 睡眠状态
	NodeStatePoweredOff       NodeState = "powered_off"        // 关机状态
	NodeStateSwitchingToSleep NodeState = "switching_to_sleep" // 正在进入睡眠
	NodeStateWakingup         NodeState = "waking_up"          // 正在从睡眠唤醒
	NodeStatePoweringOn       NodeState = "powering_on"        // 正在开机
	NodeStatePoweringOff      NodeState = "powering_off"       // 正在关机
)

// PState 表示电源状态
type PState int

const (
	PStateOn              PState = 0 // 开机状态
	PStateSleep           PState = 1 // 睡眠状态
	PStateSwitchingSleep  PState = 2 // 正在睡眠
	PStateSwitchingWakeup PState = 3 // 正在唤醒
)

// NodeInfo 存储节点的详细信息
type NodeInfo struct {
	State           NodeState
	LastStateChange float64
	JobCount        int
}

// PredictionResponse 预测服务的响应结构
type PredictionResponse struct {
	Prediction int    `json:"prediction"`
	Error      string `json:"error,omitempty"`
}

// Time 定义了时间管理接口
type Time interface {
	// Now 返回当前时间（浮点数表示的秒数）
	Now() float64
	// Since 返回从某个时间点到现在经过的时间（秒）
	Since(float64) float64
	// Until 返回从现在到某个时间点的时间（秒）
	Until(float64) float64
}

// RealTime 实现真实系统的时间管理
type RealTime struct {
	startTime time.Time
}

func NewRealTime() *RealTime {
	return &RealTime{
		startTime: time.Now(),
	}
}

func (rt *RealTime) Now() float64 {
	return float64(time.Since(rt.startTime).Seconds())
}

func (rt *RealTime) Since(t float64) float64 {
	return rt.Now() - t
}

func (rt *RealTime) Until(t float64) float64 {
	return t - rt.Now()
}

// SimulationTime 实现仿真系统的时间管理
type SimulationTime struct {
	client  *http.Client
	baseURL string
}

func NewSimulationTime() *SimulationTime {
	return &SimulationTime{
		client:  &http.Client{Timeout: 5 * time.Second},
		baseURL: "http://localhost:8082",
	}
}

func (st *SimulationTime) Now() float64 {
	resp, err := st.client.Get(st.baseURL + "/scheduler/time")
	if err != nil {
		log.Errorf("Failed to get simulation time: %v", err)
		return 0
	}
	defer resp.Body.Close()

	var result struct {
		CurrentTime float64 `json:"current_time"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Errorf("Failed to decode time response: %v", err)
		return 0
	}

	return result.CurrentTime
}

func (st *SimulationTime) Since(t float64) float64 {
	return st.Now() - t
}

func (st *SimulationTime) Until(t float64) float64 {
	return t - st.Now()
}

// PowerTool 定义了节点电源操作的接口
type PowerTool interface {
	RegisterNode(nodeID, mac, ip string) error
	WakeUp(nodeID string) error
	Sleep(nodeID string) error
	PowerOn(nodeID string) error
	PowerOff(nodeID string) error
}
