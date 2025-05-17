package config

import (
	"fmt"

	logrus "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var log = logrus.WithField("component", "Config")

type Config struct {
	Monitor MonitorConfig `mapstructure:"Monitor"`
	DB      DBConfig      `mapstructure:"Database"`
}

type MonitorConfig struct {
	SamplePeriod string  `mapstructure:"SamplePeriod"`
	Enabled      Enabled `mapstructure:"Enabled"`
	LogPath      string  `mapstructure:"LogPath"`
	GPUType      string  `mapstructure:"GPUType"`
}

type Enabled struct {
	Job    bool `mapstructure:"Job"`
	IPMI   bool `mapstructure:"Ipmi"`
	GPU    bool `mapstructure:"Gpu"`
	RAPL   bool `mapstructure:"Rapl"`
	System bool `mapstructure:"System"`
}

type DBConfig struct {
	Type     string          `mapstructure:"Type"`
	InfluxDB *InfluxDBConfig `mapstructure:"Influxdb"`
}

type InfluxDBConfig struct {
	URL        string `mapstructure:"Url"`
	Token      string `mapstructure:"Token"`
	Org        string `mapstructure:"Org"`
	NodeBucket string `mapstructure:"NodeBucket"`
	JobBucket  string `mapstructure:"JobBucket"`
}

func LoadConfig(path string) (*Config, error) {
	v := viper.New()

	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	if err := validateConfig(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

func validateConfig(cfg *Config) error {
	switch cfg.DB.Type {
	case "influxdb":
		if cfg.DB.InfluxDB == nil {
			return fmt.Errorf("influxdb configuration is required when type is influxdb")
		}
		if cfg.DB.InfluxDB.URL == "" || cfg.DB.InfluxDB.Token == "" ||
			cfg.DB.InfluxDB.Org == "" || cfg.DB.InfluxDB.NodeBucket == "" ||
			cfg.DB.InfluxDB.JobBucket == "" {
			return fmt.Errorf("incomplete influxdb configuration")
		}
	default:
		return fmt.Errorf("unsupported database type: %s", cfg.DB.Type)
	}

	return nil
}

func PrintConfig(cfg *Config) {
	log.Infof("=== Current Configuration Start ===")

	// Monitor
	log.Infof("Monitor Configuration:")
	log.Infof("  Sample Period: %v", cfg.Monitor.SamplePeriod)
	log.Infof("  Log Path: %v", cfg.Monitor.LogPath)
	log.Infof("  GPU Type: %v", cfg.Monitor.GPUType)

	// Enabled
	log.Infof("  Enabled:")
	log.Infof("    Job: %v", cfg.Monitor.Enabled.Job)
	log.Infof("    IPMI: %v", cfg.Monitor.Enabled.IPMI)
	log.Infof("    GPU: %v", cfg.Monitor.Enabled.GPU)
	log.Infof("    RAPL: %v", cfg.Monitor.Enabled.RAPL)
	log.Infof("    System: %v", cfg.Monitor.Enabled.System)

	// Database
	log.Infof("Database Configuration:")
	log.Infof("  Type: %s", cfg.DB.Type)

	switch cfg.DB.Type {
	case "influxdb":
		if cfg.DB.InfluxDB != nil {
			log.Infof("  InfluxDB Settings:")
			log.Infof("    URL: %s", cfg.DB.InfluxDB.URL)
			log.Infof("    Organization: %s", cfg.DB.InfluxDB.Org)
			log.Infof("    Node Bucket: %s", cfg.DB.InfluxDB.NodeBucket)
			log.Infof("    Job Bucket: %s", cfg.DB.InfluxDB.JobBucket)
			if cfg.DB.InfluxDB.Token != "" {
				tokenPreview := cfg.DB.InfluxDB.Token[:10] + "..."
				log.Infof("    Token: %s", tokenPreview)
			}
		}
	}

	log.Infof("=== Current Configuration End ===")
}
