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

	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("config validation failed: %v", err)
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

func validateConfig(config *Config) error {
	if config.Predictor.URL == "" {
		return fmt.Errorf("Predictor.URL cannot be empty")
	}
	if config.Predictor.PredictorScript == "" {
		return fmt.Errorf("Predictor.PredictorScript cannot be empty")
	}
	if config.Predictor.SleepTimeThresholdSeconds <= 0 {
		return fmt.Errorf("Predictor.SleepTimeThresholdSeconds must be positive")
	}
	if config.Predictor.IdleReserveRatio < 0 || config.Predictor.IdleReserveRatio > 1 {
		return fmt.Errorf("Predictor.IdleReserveRatio must be between 0 and 1")
	}
	if config.Predictor.CheckIntervalSeconds <= 0 {
		return fmt.Errorf("Predictor.CheckIntervalSeconds must be positive")
	}

	if config.InfluxDB.URL == "" {
		return fmt.Errorf("InfluxDB.URL cannot be empty")
	}
	if config.InfluxDB.Token == "" {
		return fmt.Errorf("InfluxDB.Token cannot be empty")
	}
	if config.InfluxDB.Org == "" {
		return fmt.Errorf("InfluxDB.Org cannot be empty")
	}
	if config.InfluxDB.Bucket == "" {
		return fmt.Errorf("InfluxDB.Bucket cannot be empty")
	}

	if config.IPMI.User == "" {
		return fmt.Errorf("IPMI.User cannot be empty")
	}
	if config.IPMI.Password == "" {
		return fmt.Errorf("IPMI.Password cannot be empty")
	}
	if config.IPMI.NodeStateCheckIntervalSeconds <= 0 {
		return fmt.Errorf("IPMI.NodeStateCheckIntervalSeconds must be positive")
	}
	if config.IPMI.PowerOffMaxNodesPerBatch <= 0 {
		return fmt.Errorf("IPMI.PowerOffMaxNodesPerBatch must be positive")
	}
	if config.IPMI.PowerOffBatchIntervalSeconds <= 0 {
		return fmt.Errorf("IPMI.PowerOffBatchIntervalSeconds must be positive")
	}
	if len(config.IPMI.NodeBMCMapping) == 0 {
		return fmt.Errorf("IPMI.NodeBMCMapping cannot be empty")
	}

	if config.SSH.User == "" {
		return fmt.Errorf("SSH.User cannot be empty")
	}
	if config.SSH.Password == "" {
		return fmt.Errorf("SSH.Password cannot be empty")
	}

	return nil
}
