package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Predictor struct {
		URL                       string  `yaml:"URL"`
		SleepTimeThresholdSeconds int     `yaml:"SleepTimeThresholdSeconds"`
		IdleReserveRatio          float64 `yaml:"IdleReserveRatio"`
		CheckIntervalSeconds      int     `yaml:"CheckIntervalSeconds"`
		CPUsPerNode               int     `yaml:"CPUsPerNode"`
		ForecastMinutes           int     `yaml:"ForecastMinutes"`
		LookbackMinutes           int     `yaml:"LookbackMinutes"`
		CheckpointPath            string  `yaml:"CheckpointPath"`
		ScalersPath               string  `yaml:"ScalersPath"`
		NodeStateChangeFile       string  `yaml:"NodeStateChangeFile"`
		ClusterStateFile          string  `yaml:"ClusterStateFile"`
		PredictorScript           string  `yaml:"PredictorScript"`
	} `yaml:"Predictor"`

	InfluxDB struct {
		URL    string `yaml:"URL"`
		Token  string `yaml:"Token"`
		Org    string `yaml:"Org"`
		Bucket string `yaml:"Bucket"`
	} `yaml:"InfluxDB"`

	IPMI struct {
		User                          string            `yaml:"User"`
		Password                      string            `yaml:"Password"`
		NodeStateCheckIntervalSeconds int               `yaml:"NodeStateCheckIntervalSeconds"`
		PowerOffMaxNodesPerBatch      int               `yaml:"PowerOffMaxNodesPerBatch"`
		PowerOffBatchIntervalSeconds  int               `yaml:"PowerOffBatchIntervalSeconds"`
		ExcludeNodes                  []string          `yaml:"ExcludeNodes"`
		NodeBMCMapping                map[string]string `yaml:"NodeBMCMapping"`
	} `yaml:"IPMI"`

	SSH struct {
		User     string `yaml:"User"`
		Password string `yaml:"Password"`
	} `yaml:"SSH"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := &Config{}
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	PrintConfig(config)
	return config, nil
}

func PrintConfig(cfg *Config) {
	log.Info("Power Control Plugin Configuration:")
	log.Info("----------------------------------------")
	log.Info("Predictor:")
	log.Infof("  URL: %s", cfg.Predictor.URL)
	log.Infof("  SleepTimeThreshold: %d", cfg.Predictor.SleepTimeThresholdSeconds)
	log.Infof("  IdleReserveRatio: %.2f", cfg.Predictor.IdleReserveRatio)
	log.Infof("  CheckInterval: %d", cfg.Predictor.CheckIntervalSeconds)
	log.Infof("  CPUsPerNode: %d", cfg.Predictor.CPUsPerNode)
	log.Infof("  ForecastMinutes: %d", cfg.Predictor.ForecastMinutes)
	log.Infof("  LookbackMinutes: %d", cfg.Predictor.LookbackMinutes)
	log.Infof("  CheckpointPath: %s", cfg.Predictor.CheckpointPath)
	log.Infof("  ScalersPath: %s", cfg.Predictor.ScalersPath)
	log.Infof("  NodeStateFile: %s", cfg.Predictor.NodeStateChangeFile)
	log.Infof("  PredictionFile: %s", cfg.Predictor.ClusterStateFile)
	log.Infof("  PredictorScript: %s", cfg.Predictor.PredictorScript)

	log.Info("IPMI:")
	log.Infof("  User: %s", cfg.IPMI.User)
	log.Info("  Password: ********")
	log.Infof("  NodeStateCheckInterval: %d", cfg.IPMI.NodeStateCheckIntervalSeconds)
	log.Infof("  ExcludeNodes: %v", cfg.IPMI.ExcludeNodes)
	log.Infof("  NodeBMCMapping: %v", cfg.IPMI.NodeBMCMapping)
	log.Info("SSH:")
	log.Infof("  User: %s", cfg.SSH.User)
	log.Info("  Password: ********")
}
