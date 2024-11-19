package db

import (
	"fmt"

	"CraneFrontEnd/plugin/energy/pkg/config"
)

func NewDatabase(config config.DBConfig) (DBInterface, error) {
	switch config.Type {
	case "sqlite":
		if config.SQLite == nil {
			return nil, fmt.Errorf("sqlite config is nil")
		}
		return NewSQLiteDB(&config)

	case "influxdb":
		if config.InfluxDB == nil {
			return nil, fmt.Errorf("influxdb config is nil")
		}
		return NewInfluxDB(&config)

	case "mongodb":
		if config.MongoDB == nil {
			return nil, fmt.Errorf("mongodb config is nil")
		}
		return NewMongoDB(&config)

	default:
		return nil, fmt.Errorf("unsupported database type: %s", config.Type)
	}
}
