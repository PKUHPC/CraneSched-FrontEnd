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
	SamplePeriod   string   `mapstructure:"sample_period"`
	CgroupBasePath string   `mapstructure:"cgroup_base_path"`
	Switches       Switches `mapstructure:"switches"`
}

type Switches struct {
	CPU         bool `mapstructure:"cpu"`
	Memory      bool `mapstructure:"memory"`
	IO          bool `mapstructure:"io"`
	Network     bool `mapstructure:"network"`
	Energy      bool `mapstructure:"energy"`
	Package     bool `mapstructure:"package"`
	Core        bool `mapstructure:"core"`
	Uncore      bool `mapstructure:"uncore"`
	DRAM        bool `mapstructure:"dram"`
	GPU         bool `mapstructure:"gpu"`
	Temperature bool `mapstructure:"temperature"`
	Frequency   bool `mapstructure:"frequency"`
}

type DBConfig struct {
	Type      string          `mapstructure:"type"`
	BatchSize int             `mapstructure:"batch_size"`
	FlushTime string          `mapstructure:"flush_interval"`
	SQLite    *SQLiteConfig   `mapstructure:"sqlite"`
	InfluxDB  *InfluxDBConfig `mapstructure:"influxdb"`
	MongoDB   *MongoConfig    `mapstructure:"mongodb"`
}

type SQLiteConfig struct {
	Path string `mapstructure:"path"`
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
	v.SetDefault("monitor.cgroup_base_path", "/sys/fs/cgroup")

	// Switch defaults
	v.SetDefault("monitor.switches.cpu", true)
	v.SetDefault("monitor.switches.memory", true)
	v.SetDefault("monitor.switches.io", true)
	v.SetDefault("monitor.switches.network", false)
	v.SetDefault("monitor.switches.energy", true)
	v.SetDefault("monitor.switches.package", true)
	v.SetDefault("monitor.switches.core", true)
	v.SetDefault("monitor.switches.uncore", true)
	v.SetDefault("monitor.switches.dram", true)
	v.SetDefault("monitor.switches.gpu", false)
	v.SetDefault("monitor.switches.temperature", true)
	v.SetDefault("monitor.switches.frequency", true)

	// DB defaults
	v.SetDefault("db.type", "sqlite")
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
	case "sqlite":
		if cfg.DB.SQLite == nil || cfg.DB.SQLite.Path == "" {
			return fmt.Errorf("sqlite configuration is required when type is sqlite")
		}
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
	log.Printf("  Cgroup Base Path: %v", cfg.Monitor.CgroupBasePath)

	// Switches
	log.Printf("  Switches:")
	log.Printf("    CPU: %v", cfg.Monitor.Switches.CPU)
	log.Printf("    Memory: %v", cfg.Monitor.Switches.Memory)
	log.Printf("    IO: %v", cfg.Monitor.Switches.IO)
	log.Printf("    Network: %v", cfg.Monitor.Switches.Network)
	log.Printf("    Energy: %v", cfg.Monitor.Switches.Energy)
	log.Printf("    Package: %v", cfg.Monitor.Switches.Package)
	log.Printf("    Core: %v", cfg.Monitor.Switches.Core)
	log.Printf("    Uncore: %v", cfg.Monitor.Switches.Uncore)
	log.Printf("    DRAM: %v", cfg.Monitor.Switches.DRAM)
	log.Printf("    GPU: %v", cfg.Monitor.Switches.GPU)
	log.Printf("    Temperature: %v", cfg.Monitor.Switches.Temperature)
	log.Printf("    Frequency: %v", cfg.Monitor.Switches.Frequency)

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
	case "sqlite":
		if cfg.DB.SQLite != nil {
			log.Printf("  SQLite Settings:")
			log.Printf("    Path: %s", cfg.DB.SQLite.Path)
		}
	}

	log.Printf("\033[32m[EnergyPlugin] === Current Configuration End ===\033[0m")
}
