package db

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type MongoDB struct {
	client     *mongo.Client
	database   string
	nodeEnergy *mongo.Collection
	taskEnergy *mongo.Collection

	buffer    []*types.NodeData
	bufferMu  sync.Mutex
	batchSize int
}

func NewMongoDB(config *config.DBConfig) (*MongoDB, error) {
	log.Printf("\033[34m[MongoDB]\033[0m Initializing MongoDB with config: %+v", config)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.MongoDB.URI))
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	db := client.Database(config.MongoDB.Database)

	mongodb := &MongoDB{
		client:     client,
		database:   config.MongoDB.Database,
		nodeEnergy: db.Collection("node_energy"),
		taskEnergy: db.Collection("task_energy"),
		batchSize:  config.BatchSize,
		buffer:     make([]*types.NodeData, 0, config.BatchSize),
	}

	flushTime, err := time.ParseDuration(config.FlushTime)
	if err != nil {
		log.Printf("\033[31m[MongoDB]\033[0m Invalid flush time format: %v, using default 30s", err)
		flushTime = 30 * time.Second
	}

	go mongodb.periodicFlush(flushTime)
	return mongodb, nil
}

func (db *MongoDB) SaveNodeEnergy(data *types.NodeData) error {
	var bufferToWrite []*types.NodeData

	db.bufferMu.Lock()
	db.buffer = append(db.buffer, data)
	if len(db.buffer) >= db.batchSize {
		bufferToWrite = db.buffer
		db.buffer = make([]*types.NodeData, 0, db.batchSize)
	}
	db.bufferMu.Unlock()

	if bufferToWrite != nil {
		return db.writeBatch(bufferToWrite)
	}
	return nil
}

func (db *MongoDB) writeBatch(dataList []*types.NodeData) error {
	if len(dataList) == 0 {
		return nil
	}

	log.Printf("\033[34m[MongoDB]\033[0m Batch saving node energy data, count: %d", len(dataList))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	docs := make([]interface{}, 0, len(dataList))
	for _, data := range dataList {
		doc := map[string]interface{}{
			"node_id":     NodeID,
			"cluster_id":  ClusterID,
			"timestamp":   data.Timestamp,
			"package":     data.Package,
			"core":        data.Core,
			"uncore":      data.Uncore,
			"dram":        data.DRAM,
			"gt":          data.GT,
			"temperature": data.Temperature,
			"frequencies": data.Frequencies,
			"cpu_util":    data.CPUUtil,
			"mem_util":    data.MemoryUtil,
			"disk_io":     data.DiskIO,
			"network_io":  data.NetworkIO,
		}
		docs = append(docs, doc)
	}

	_, err := db.nodeEnergy.InsertMany(ctx, docs)
	return err
}

func (db *MongoDB) periodicFlush(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		var bufferToWrite []*types.NodeData

		db.bufferMu.Lock()
		if len(db.buffer) > 0 {
			bufferToWrite = db.buffer
			db.buffer = make([]*types.NodeData, 0, db.batchSize)
		}
		db.bufferMu.Unlock()

		if bufferToWrite != nil {
			if err := db.writeBatch(bufferToWrite); err != nil {
				log.Printf("\033[31m[MongoDB]\033[0m Failed to flush buffer: %v", err)
			}
		}
	}
}

func (db *MongoDB) SaveTaskEnergy(monitor *types.TaskData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	history, _ := json.Marshal(monitor.TaskResourceUsage)

	doc := map[string]interface{}{
		"task_name":        monitor.TaskName,
		"start_time":       monitor.StartTime,
		"end_time":         monitor.EndTime,
		"duration":         monitor.Duration,
		"resource_history": string(history),
		"total_energy":     monitor.TotalEnergy,
		"cpu_energy":       monitor.CPUEnergy,
		"gpu_energy":       monitor.GPUEnergy,
		"dram_energy":      monitor.DRAMEnergy,
		"average_power":    monitor.AveragePower,
		"cpu_time":         monitor.CPUTime,
		"cpu_utilization":  monitor.CPUUtilization,
		"gpu_utilization":  monitor.GPUUtilization,
		"memory_usage":     monitor.MemoryUsage,
		"memory_util":      monitor.MemoryUtil,
		"disk_read_bytes":  monitor.DiskReadBytes,
		"disk_write_bytes": monitor.DiskWriteBytes,
		"disk_read_speed":  monitor.DiskReadSpeed,
		"disk_write_speed": monitor.DiskWriteSpeed,
	}

	_, err := db.taskEnergy.InsertOne(ctx, doc)
	return err
}

func (db *MongoDB) Close() error {
	// 在关闭前刷新剩余数据
	db.bufferMu.Lock()
	if len(db.buffer) > 0 {
		if err := db.writeBatch(db.buffer); err != nil {
			log.Printf("\033[31m[MongoDB]\033[0m Failed to write remaining data: %v", err)
		}
	}
	db.bufferMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return db.client.Disconnect(ctx)
}
