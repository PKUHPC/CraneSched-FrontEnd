package db

import (
	"context"
	"log"
	"strings"
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

	switches *config.Switches
}

func NewMongoDB(config *config.Config) (*MongoDB, error) {
	log.Printf("\033[34m[MongoDB]\033[0m Initializing MongoDB with config: %+v", config)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.DB.MongoDB.URI))
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	db := client.Database(config.DB.MongoDB.Database)

	mongodb := &MongoDB{
		client:     client,
		database:   config.DB.MongoDB.Database,
		nodeEnergy: db.Collection("node_energy"),
		taskEnergy: db.Collection("task_energy"),
		batchSize:  config.DB.BatchSize,
		buffer:     make([]*types.NodeData, 0, config.DB.BatchSize),
		switches:   &config.Monitor.Switches,
	}

	flushTime, err := time.ParseDuration(config.DB.FlushTime)
	if err != nil {
		log.Printf("\033[31m[MongoDB]\033[0m Invalid flush time format: %v, using default 30s", err)
		flushTime = 30 * time.Second
	}

	go mongodb.periodicFlush(flushTime)
	return mongodb, nil
}

func (db *MongoDB) SaveNodeEnergy(data *types.NodeData) error {
	db.bufferMu.Lock()
	db.buffer = append(db.buffer, data)

	if len(db.buffer) >= db.batchSize {
		buffer := db.buffer
		db.buffer = make([]*types.NodeData, 0, db.batchSize)
		db.bufferMu.Unlock()
		return db.writeBatch(buffer)
	}

	db.bufferMu.Unlock()
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
		// 基础文档结构
		doc := map[string]interface{}{
			"node_id":    NodeID,
			"cluster_id": ClusterID,
			"timestamp":  data.Timestamp,

			// 初始化所有可能的字段为 nil
			// 系统负载
			"cpu_util":        nil,
			"mem_util":        nil,
			"disk_io":         nil,
			"network_io":      nil,
			"sys_temperature": nil,
			"cpu_frequencies": nil,

			// RAPL
			"rapl_package": nil,
			"rapl_core":    nil,
			"rapl_uncore":  nil,
			"rapl_dram":    nil,
			"rapl_gt":      nil,

			// IPMI
			"ipmi_power":       nil,
			"ipmi_energy":      nil,
			"ipmi_temperature": nil,

			// GPU
			"gpu_energy":      nil,
			"gpu_power":       nil,
			"gpu_util":        nil,
			"gpu_mem_util":    nil,
			"gpu_temperature": nil,

			// 总能耗
			"total_energy": nil,
		}

		// 记录启用的模块
		enabledSources := make([]string, 0)

		// 根据开关填充实际值
		if db.switches.System {
			doc["cpu_util"] = data.SystemLoad.CPUUtil
			doc["mem_util"] = data.SystemLoad.MemoryUtil
			doc["disk_io"] = data.SystemLoad.DiskIO
			doc["network_io"] = data.SystemLoad.NetworkIO
			doc["sys_temperature"] = data.SystemLoad.Temperature
			doc["cpu_frequencies"] = data.SystemLoad.Frequencies
			enabledSources = append(enabledSources, "system")
		}

		if db.switches.RAPL {
			doc["rapl_package"] = data.RAPL.Package
			doc["rapl_core"] = data.RAPL.Core
			doc["rapl_uncore"] = data.RAPL.Uncore
			doc["rapl_dram"] = data.RAPL.DRAM
			doc["rapl_gt"] = data.RAPL.GT
			doc["rapl_total_energy"] = data.RAPL.Package + data.RAPL.DRAM
			enabledSources = append(enabledSources, "rapl")
		}

		if db.switches.IPMI {
			doc["ipmi_power"] = data.IPMI.Power
			doc["ipmi_energy"] = data.IPMI.Energy
			enabledSources = append(enabledSources, "ipmi")
		}

		if db.switches.GPU {
			doc["gpu_energy"] = data.GPU.Energy
			doc["gpu_power"] = data.GPU.Power
			doc["gpu_util"] = data.GPU.Util
			doc["gpu_mem_util"] = data.GPU.MemUtil
			doc["gpu_temperature"] = data.GPU.Temp
			enabledSources = append(enabledSources, "gpu")
		}

		// 设置总能耗（优先级：IPMI > RAPL > GPU）
		if db.switches.IPMI {
			doc["total_energy"] = data.IPMI.Energy
			doc["energy_source"] = "ipmi"
		} else if db.switches.RAPL {
			doc["total_energy"] = doc["rapl_total_energy"]
			doc["energy_source"] = "rapl"
		} else if db.switches.GPU {
			doc["total_energy"] = data.GPU.Energy
			doc["energy_source"] = "gpu"
		}

		// 添加已启用模块的信息
		doc["enabled_modules"] = strings.Join(enabledSources, ",")

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

	doc := map[string]interface{}{
		"task_name":        monitor.TaskName,
		"start_time":       monitor.StartTime,
		"end_time":         monitor.EndTime,
		"duration":         monitor.Duration,
		"total_energy":     monitor.TotalEnergy,
		"cpu_energy":       monitor.CPUEnergy,
		"gpu_energy":       monitor.GPUEnergy,
		"dram_energy":      monitor.DRAMEnergy,
		"average_power":    monitor.AveragePower,
		"cpu_time":         monitor.TaskStats.CPUStats.UsageSeconds,
		"cpu_utilization":  monitor.TaskStats.CPUStats.Utilization,
		"gpu_utilization":  monitor.TaskStats.GPUStats.Utilization,
		"memory_usage":     monitor.TaskStats.MemoryStats.UsageMB,
		"memory_util":      monitor.TaskStats.MemoryStats.Utilization,
		"disk_read_mb":     monitor.TaskStats.IOStats.ReadMB,
		"disk_write_mb":    monitor.TaskStats.IOStats.WriteMB,
		"disk_read_speed":  monitor.TaskStats.IOStats.ReadMBPS,
		"disk_write_speed": monitor.TaskStats.IOStats.WriteMBPS,
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
