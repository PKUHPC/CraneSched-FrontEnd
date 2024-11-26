package config

import (
	"fmt"

	logrus "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var log = logrus.WithField("component", "Config")

type Config struct {
	Monitor MonitorConfig `mapstructure:"monitor"`
	DB      DBConfig      `mapstructure:"db"`
}

type MonitorConfig struct {
	SamplePeriod string   `mapstructure:"samplePeriod"`
	Switches     Switches `mapstructure:"switches"`
	LogPath      string   `mapstructure:"logPath"`
}

type Switches struct {
	Task   bool `mapstructure:"task"`
	IPMI   bool `mapstructure:"ipmi"`
	GPU    bool `mapstructure:"gpu"`
	RAPL   bool `mapstructure:"rapl"`
	System bool `mapstructure:"system"`
}

type DBConfig struct {
	Type      string          `mapstructure:"type"`
	BatchSize int             `mapstructure:"batchSize"`
	FlushTime string          `mapstructure:"flushInterval"`
	InfluxDB  *InfluxDBConfig `mapstructure:"influxdb"`
}

type InfluxDBConfig struct {
	URL        string `mapstructure:"url"`
	Token      string `mapstructure:"token"`
	Org        string `mapstructure:"org"`
	NodeBucket string `mapstructure:"nodeBucket"`
	TaskBucket string `mapstructure:"taskBucket"`
}

type MongoConfig struct {
	URI      string `mapstructure:"uri"`
	Database string `mapstructure:"database"`
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

	if cfg.DB.BatchSize <= 0 {
		return fmt.Errorf("batch size must be greater than 0")
	}

	if cfg.DB.FlushTime == "" {
		return fmt.Errorf("flush interval must be specified")
	}

	switch cfg.DB.Type {
	case "influxdb":
		if cfg.DB.InfluxDB == nil {
			return fmt.Errorf("influxdb configuration is required when type is influxdb")
		}
		if cfg.DB.InfluxDB.URL == "" || cfg.DB.InfluxDB.Token == "" ||
			cfg.DB.InfluxDB.Org == "" || cfg.DB.InfluxDB.NodeBucket == "" ||
			cfg.DB.InfluxDB.TaskBucket == "" {
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

	// Switches
	log.Infof("  Switches:")
	log.Infof("    Task: %v", cfg.Monitor.Switches.Task)
	log.Infof("    IPMI: %v", cfg.Monitor.Switches.IPMI)
	log.Infof("    GPU: %v", cfg.Monitor.Switches.GPU)
	log.Infof("    RAPL: %v", cfg.Monitor.Switches.RAPL)
	log.Infof("    System: %v", cfg.Monitor.Switches.System)

	// Database
	log.Infof("Database Configuration:")
	log.Infof("  Type: %s", cfg.DB.Type)
	log.Infof("  Batch Size: %d", cfg.DB.BatchSize)
	log.Infof("  Flush Time: %s", cfg.DB.FlushTime)

	switch cfg.DB.Type {
	case "influxdb":
		if cfg.DB.InfluxDB != nil {
			log.Infof("  InfluxDB Settings:")
			log.Infof("    URL: %s", cfg.DB.InfluxDB.URL)
			log.Infof("    Organization: %s", cfg.DB.InfluxDB.Org)
			log.Infof("    Node Bucket: %s", cfg.DB.InfluxDB.NodeBucket)
			log.Infof("    Task Bucket: %s", cfg.DB.InfluxDB.TaskBucket)
			if cfg.DB.InfluxDB.Token != "" {
				tokenPreview := cfg.DB.InfluxDB.Token[:10] + "..."
				log.Infof("    Token: %s", tokenPreview)
			}
		}
	}

	log.Infof("=== Current Configuration End ===")
}
