package config

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Config struct {
	Monitor MonitorConfig `mapstructure:"monitor"`
	DB      DBConfig      `mapstructure:"db"`
}

type MonitorConfig struct {
	SamplePeriod string   `mapstructure:"sample_period"`
	Switches     Switches `mapstructure:"switches"`
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
	BatchSize int             `mapstructure:"batch_size"`
	FlushTime string          `mapstructure:"flush_interval"`
	InfluxDB  *InfluxDBConfig `mapstructure:"influxdb"`
	MongoDB   *MongoConfig    `mapstructure:"mongodb"`
}

type InfluxDBConfig struct {
	URL        string `mapstructure:"url"`
	Token      string `mapstructure:"token"`
	Org        string `mapstructure:"org"`
	NodeBucket string `mapstructure:"node_bucket"`
	TaskBucket string `mapstructure:"task_bucket"`
}

type MongoConfig struct {
	URI      string `mapstructure:"uri"`
	Database string `mapstructure:"database"`
}

func LoadConfig(path string) (*Config, error) {
	v := viper.New()

	setDefaultConfig(v)

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

func setDefaultConfig(v *viper.Viper) {
	// Monitor defaults
	v.SetDefault("monitor.sample_period", "1s")

	v.SetDefault("monitor.switches.task", true)
	v.SetDefault("monitor.switches.ipmi", true)
	v.SetDefault("monitor.switches.gpu", true)
	v.SetDefault("monitor.switches.rapl", true)
	v.SetDefault("monitor.switches.system", true)

	// DB defaults
	v.SetDefault("db.type", "influxdb")
	v.SetDefault("db.batch_size", 1)
	v.SetDefault("db.flush_interval", "30s")
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
	log.Printf("\033[32m[EnergyPlugin] === Current Configuration Start ===\033[0m")

	// Monitor
	log.Printf("Monitor Configuration:")
	log.Printf("  Sample Period: %v", cfg.Monitor.SamplePeriod)

	// Switches
	log.Printf("  Switches:")
	log.Printf("    Task: %v", cfg.Monitor.Switches.Task)
	log.Printf("    IPMI: %v", cfg.Monitor.Switches.IPMI)
	log.Printf("    GPU: %v", cfg.Monitor.Switches.GPU)
	log.Printf("    RAPL: %v", cfg.Monitor.Switches.RAPL)
	log.Printf("    System: %v", cfg.Monitor.Switches.System)

	// Database
	log.Printf("Database Configuration:")
	log.Printf("  Type: %s", cfg.DB.Type)
	log.Printf("  Batch Size: %d", cfg.DB.BatchSize)
	log.Printf("  Flush Time: %s", cfg.DB.FlushTime)

	switch cfg.DB.Type {
	case "influxdb":
		if cfg.DB.InfluxDB != nil {
			log.Printf("  InfluxDB Settings:")
			log.Printf("    URL: %s", cfg.DB.InfluxDB.URL)
			log.Printf("    Organization: %s", cfg.DB.InfluxDB.Org)
			log.Printf("    Node Bucket: %s", cfg.DB.InfluxDB.NodeBucket)
			log.Printf("    Task Bucket: %s", cfg.DB.InfluxDB.TaskBucket)
			if cfg.DB.InfluxDB.Token != "" {
				tokenPreview := cfg.DB.InfluxDB.Token[:10] + "..."
				log.Printf("    Token: %s", tokenPreview)
			}
		}
	}

	log.Printf("\033[32m[EnergyPlugin] === Current Configuration End ===\033[0m")
}
