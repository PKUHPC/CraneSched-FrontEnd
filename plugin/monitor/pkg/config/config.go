package config

import (
	"fmt"

	logrus "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var log = logrus.WithField("component", "Config")

type Config struct {
	Monitor MonitorConfig `mapstructure:"Monitor"`
	Cgroup  CgroupConfig  `mapstructure:"Cgroup"`
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
	Event  bool `mapstructure:"Event"`
}

type CgroupConfig struct {
	CPU      string `mapstructure:"CPU"`
	Memory   string `mapstructure:"Memory"`
	ProcList string `mapstructure:"ProcList"`
}

type DBConfig struct {
	Type        string            `mapstructure:"Type"`
	InfluxDB    *InfluxDBConfig   `mapstructure:"Influxdb"`
	TraceWriter TraceWriterConfig `mapstructure:"TraceWriter"`
	Interval    uint32            `mapstructure:"Interval"`
	BufferSize  uint32            `mapstructure:"BufferSize"`
}

type InfluxDBConfig struct {
	URL                 string   `mapstructure:"Url"`
	Token               string   `mapstructure:"Token"`
	Org                 string   `mapstructure:"Org"`
	NodeBucket          string   `mapstructure:"NodeBucket"`
	JobBucket           string   `mapstructure:"JobBucket"`
	ClusterBucket       string   `mapstructure:"ClusterBucket"`
	TraceBucket         string   `mapstructure:"TraceBucket"`
	TraceCoreBucket     string   `mapstructure:"TraceCoreBucket"`
	TraceDetailBucket   string   `mapstructure:"TraceDetailBucket"`
	TraceErrorBucket    string   `mapstructure:"TraceErrorBucket"`
	TraceShardBuckets   []string `mapstructure:"TraceShardBuckets"`
	EventMeasurement    string   `mapstructure:"EventMeasurement"`
	ResourceMeasurement string   `mapstructure:"ResourceMeasurement"`
}

type TraceWriterConfig struct {
	Shards            int `mapstructure:"Shards"`
	BatchSpans        int `mapstructure:"BatchSpans"`
	QueueBatches      int `mapstructure:"QueueBatches"`
	FlushIntervalMs   int `mapstructure:"FlushIntervalMs"`
	RetryBackoffMs    int `mapstructure:"RetryBackoffMs"`
	MaxRetryBackoffMs int `mapstructure:"MaxRetryBackoffMs"`
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
			cfg.DB.InfluxDB.JobBucket == "" || cfg.DB.InfluxDB.ClusterBucket == "" {
			return fmt.Errorf("incomplete influxdb configuration")
		}
		if cfg.DB.InfluxDB.TraceBucket == "" {
			cfg.DB.InfluxDB.TraceBucket = "crane_trace"
		}
		if cfg.DB.InfluxDB.TraceCoreBucket == "" {
			cfg.DB.InfluxDB.TraceCoreBucket = cfg.DB.InfluxDB.TraceBucket
		}
		if cfg.DB.InfluxDB.TraceDetailBucket == "" {
			cfg.DB.InfluxDB.TraceDetailBucket = cfg.DB.InfluxDB.TraceBucket
		}
		if cfg.DB.InfluxDB.TraceErrorBucket == "" {
			cfg.DB.InfluxDB.TraceErrorBucket = cfg.DB.InfluxDB.TraceBucket
		}
		if cfg.DB.InfluxDB.EventMeasurement == "" {
			cfg.DB.InfluxDB.EventMeasurement = "NodeEvents"
		}
		if cfg.DB.InfluxDB.ResourceMeasurement == "" {
			cfg.DB.InfluxDB.ResourceMeasurement = "ResourceUsage"
		}
	default:
		return fmt.Errorf("unsupported database type: %s", cfg.DB.Type)
	}

	if cfg.DB.Interval <= 0 {
		cfg.DB.Interval = 1000
	}
	if cfg.DB.BufferSize <= 0 {
		cfg.DB.BufferSize = 32
	}
	if cfg.DB.TraceWriter.Shards <= 0 {
		cfg.DB.TraceWriter.Shards = 1
	}
	if cfg.DB.TraceWriter.BatchSpans <= 0 {
		cfg.DB.TraceWriter.BatchSpans = 1024
	}
	if cfg.DB.TraceWriter.QueueBatches <= 0 {
		cfg.DB.TraceWriter.QueueBatches = 4096
	}
	if cfg.DB.TraceWriter.FlushIntervalMs <= 0 {
		cfg.DB.TraceWriter.FlushIntervalMs = 50
	}
	if cfg.DB.TraceWriter.RetryBackoffMs <= 0 {
		cfg.DB.TraceWriter.RetryBackoffMs = 200
	}
	if cfg.DB.TraceWriter.MaxRetryBackoffMs <= 0 {
		cfg.DB.TraceWriter.MaxRetryBackoffMs = 5000
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
	log.Infof("    Event: %v", cfg.Monitor.Enabled.Event)

	// Cgroup
	log.Infof("Cgroup Configuration:")
	log.Infof("  CPU: %s", cfg.Cgroup.CPU)
	log.Infof("  Memory: %s", cfg.Cgroup.Memory)
	log.Infof("  ProcList: %s", cfg.Cgroup.ProcList)

	// Database
	log.Infof("Database Configuration:")
	log.Infof("  Type: %s", cfg.DB.Type)
	log.Infof("  Interval: %d ms", cfg.DB.Interval)
	log.Infof("  BufferSize: %d", cfg.DB.BufferSize)

	switch cfg.DB.Type {
	case "influxdb":
		if cfg.DB.InfluxDB != nil {
			log.Infof("  InfluxDB Settings:")
			log.Infof("    URL: %s", cfg.DB.InfluxDB.URL)
			log.Infof("    Organization: %s", cfg.DB.InfluxDB.Org)
			log.Infof("    Node Bucket: %s", cfg.DB.InfluxDB.NodeBucket)
			log.Infof("    Job Bucket: %s", cfg.DB.InfluxDB.JobBucket)
			log.Infof("    Cluster Bucket: %s", cfg.DB.InfluxDB.ClusterBucket)
			log.Infof("    Trace Core Bucket: %s", cfg.DB.InfluxDB.TraceCoreBucket)
			log.Infof("    Trace Detail Bucket: %s", cfg.DB.InfluxDB.TraceDetailBucket)
			log.Infof("    Trace Error Bucket: %s", cfg.DB.InfluxDB.TraceErrorBucket)
			log.Infof("    Event Measurement: %s", cfg.DB.InfluxDB.EventMeasurement)
			log.Infof("    Resource Measurement: %s", cfg.DB.InfluxDB.ResourceMeasurement)
			if cfg.DB.InfluxDB.Token != "" {
				tokenPreview := cfg.DB.InfluxDB.Token[:10] + "..."
				log.Infof("    Token: %s", tokenPreview)
			}
		}
	}
	log.Infof("  TraceWriter: shards=%d batch_spans=%d queue_batches=%d flush_interval_ms=%d retry_backoff_ms=%d max_retry_backoff_ms=%d",
		cfg.DB.TraceWriter.Shards, cfg.DB.TraceWriter.BatchSpans,
		cfg.DB.TraceWriter.QueueBatches, cfg.DB.TraceWriter.FlushIntervalMs,
		cfg.DB.TraceWriter.RetryBackoffMs, cfg.DB.TraceWriter.MaxRetryBackoffMs)

	log.Infof("=== Current Configuration End ===")
}
