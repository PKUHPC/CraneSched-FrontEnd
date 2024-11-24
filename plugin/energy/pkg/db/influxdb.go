package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/types"
)

type InfluxDB struct {
	client     influxdb2.Client
	org        string
	nodeBucket string
	taskBucket string

	buffer    []*types.NodeData
	bufferMu  sync.Mutex
	batchSize int

	switches *config.Switches
}

var NodeID string
var ClusterID string

func getNodeID() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

func getClusterID() string {
	return "cluster-1"
}

func NewInfluxDB(config *config.Config) (*InfluxDB, error) {
	log.Infof("\033[34m[InfluxDB]\033[0m Initializing InfluxDB with config: %+v", config)

	client := influxdb2.NewClient(config.DB.InfluxDB.URL, config.DB.InfluxDB.Token)
	if _, err := client.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("\033[31m[InfluxDB] failed to ping InfluxDB: %v\033[0m", err)
	}

	db := &InfluxDB{
		client:     client,
		org:        config.DB.InfluxDB.Org,
		nodeBucket: config.DB.InfluxDB.NodeBucket,
		taskBucket: config.DB.InfluxDB.TaskBucket,
		batchSize:  config.DB.BatchSize,
		buffer:     make([]*types.NodeData, 0, config.DB.BatchSize),
		switches:   &config.Monitor.Switches,
	}

	if err := db.createBucketIfNotExists(db.nodeBucket); err != nil {
		client.Close()
		return nil, fmt.Errorf("\033[31m[InfluxDB] failed to create node bucket: %v\033[0m", err)
	}

	if err := db.createBucketIfNotExists(db.taskBucket); err != nil {
		client.Close()
		return nil, fmt.Errorf("\033[31m[InfluxDB] failed to create task bucket: %v\033[0m", err)
	}

	NodeID = getNodeID()
	ClusterID = getClusterID()

	flushTime, err := time.ParseDuration(config.DB.FlushTime)
	if err != nil {
		log.Errorf("\033[31m[InfluxDB]\033[0m Invalid flush time format: %v, using default 30s", err)
		flushTime = 30 * time.Second
	}
	go db.periodicFlush(flushTime)

	return db, nil
}

func (db *InfluxDB) SaveNodeEnergy(data *types.NodeData) error {
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

func (db *InfluxDB) writeBatch(dataList []*types.NodeData) error {
	// log.Infof("\033[34m[InfluxDB]\033[0m Batch saving node energy data, count: %d", len(dataList))

	if len(dataList) == 0 {
		return fmt.Errorf("\033[31m[InfluxDB] no data to write\033[0m")
	}

	writeAPI := db.client.WriteAPIBlocking(db.org, db.nodeBucket)
	points := make([]*write.Point, 0, len(dataList))

	for _, data := range dataList {
		tags := map[string]string{
			"node_id":    data.NodeID,
			"cluster_id": data.ClusterID,
		}

		// 始终包含所有字段，未启用的模块对应值为 null
		fields := map[string]interface{}{
			// 系统负载相关字段
			"cpu_util":        nil,
			"mem_util":        nil,
			"disk_io":         nil,
			"network_io":      nil,
			"sys_temperature": nil,
			"cpu_frequencies": nil,

			// RAPL 相关字段
			"rapl_package":      nil,
			"rapl_core":         nil,
			"rapl_uncore":       nil,
			"rapl_dram":         nil,
			"rapl_gt":           nil,
			"rapl_total_energy": nil,

			// IPMI 相关字段
			"ipmi_power":       nil,
			"ipmi_energy":      nil,
			"ipmi_temperature": nil,

			// GPU 相关字段
			"gpu_energy":      nil,
			"gpu_power":       nil,
			"gpu_util":        nil,
			"gpu_mem_util":    nil,
			"gpu_temperature": nil,

			// 总能耗字段
			"total_energy": nil,
		}

		// 记录启用的模块
		enabledSources := make([]string, 0)

		// 根据开关填充实际值
		if db.switches.System {
			fields["cpu_util"] = data.SystemLoad.CPUUtil
			fields["mem_util"] = data.SystemLoad.MemoryUtil
			fields["disk_io"] = data.SystemLoad.DiskIO
			fields["network_io"] = data.SystemLoad.NetworkIO
			fields["sys_temperature"] = data.SystemLoad.Temperature
			fields["cpu_frequencies"] = data.SystemLoad.Frequencies
			enabledSources = append(enabledSources, "system")
		}

		if db.switches.RAPL {
			fields["rapl_package"] = data.RAPL.Package
			fields["rapl_core"] = data.RAPL.Core
			fields["rapl_uncore"] = data.RAPL.Uncore
			fields["rapl_dram"] = data.RAPL.DRAM
			fields["rapl_gt"] = data.RAPL.GT
			fields["rapl_total_energy"] = data.RAPL.Package + data.RAPL.DRAM
			enabledSources = append(enabledSources, "rapl")
		}

		if db.switches.IPMI {
			fields["ipmi_power"] = data.IPMI.Power
			fields["ipmi_energy"] = data.IPMI.Energy
			enabledSources = append(enabledSources, "ipmi")
		}

		if db.switches.GPU {
			fields["gpu_energy"] = data.GPU.Energy
			fields["gpu_power"] = data.GPU.Power
			fields["gpu_util"] = data.GPU.Util
			fields["gpu_mem_util"] = data.GPU.MemUtil
			fields["gpu_temperature"] = data.GPU.Temp
			enabledSources = append(enabledSources, "gpu")
		}

		// 设置总能耗（保持优先级：IPMI > RAPL > GPU）
		if db.switches.IPMI {
			fields["total_energy"] = data.IPMI.Energy
			tags["energy_source"] = "ipmi"
		} else if db.switches.RAPL {
			fields["total_energy"] = fields["rapl_total_energy"]
			tags["energy_source"] = "rapl"
		} else if db.switches.GPU {
			fields["total_energy"] = data.GPU.Energy
			tags["energy_source"] = "gpu"
		}

		// 添加已启用模块的标签
		tags["enabled_modules"] = strings.Join(enabledSources, ",")

		p := influxdb2.NewPoint(
			db.nodeBucket,
			tags,
			fields,
			data.Timestamp,
		)
		points = append(points, p)
	}

	if err := writeAPI.WritePoint(context.Background(), points...); err != nil {
		return fmt.Errorf("\033[31m[InfluxDB] failed to write batch data: %v\033[0m", err)
	}

	return nil
}

func (db *InfluxDB) periodicFlush(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		db.bufferMu.Lock()
		if len(db.buffer) > 0 {
			buffer := db.buffer
			db.buffer = make([]*types.NodeData, 0, db.batchSize)
			db.bufferMu.Unlock()

			if err := db.writeBatch(buffer); err != nil {
				log.Errorf("\033[31m[InfluxDB]\033[0m Failed to flush buffer: %v", err)
			}
		} else {
			db.bufferMu.Unlock()
		}
	}
}

func (db *InfluxDB) SaveTaskEnergy(taskData *types.TaskData) error {
	log.Infof("\033[34m[InfluxDB]\033[0m Saving task energy data for task: %s, node: %s, cluster: %s", taskData.TaskName, NodeID, ClusterID)
	writeAPI := db.client.WriteAPIBlocking(db.org, db.taskBucket)

	// history, _ := json.Marshal(taskData.TaskResourceUsage)

	p := influxdb2.NewPoint(
		db.taskBucket,
		map[string]string{
			"task_name":  taskData.TaskName,
			"node_id":    NodeID,
			"cluster_id": ClusterID,
		},
		map[string]interface{}{
			"start_time": taskData.StartTime,
			"end_time":   taskData.EndTime,
			"duration":   taskData.Duration.Seconds(),

			"total_energy":  taskData.TotalEnergy,
			"cpu_energy":    taskData.CPUEnergy,
			"gpu_energy":    taskData.GPUEnergy,
			"dram_energy":   taskData.DRAMEnergy,
			"average_power": taskData.AveragePower,

			"cpu_time":         taskData.TaskStats.CPUStats.UsageSeconds,
			"cpu_utilization":  taskData.TaskStats.CPUStats.Utilization,
			"gpu_utilization":  taskData.TaskStats.GPUStats.Utilization,
			"memory_usage":     taskData.TaskStats.MemoryStats.UsageMB,
			"memory_util":      taskData.TaskStats.MemoryStats.Utilization,
			"disk_read_mb":     taskData.TaskStats.IOStats.ReadMB,
			"disk_write_mb":    taskData.TaskStats.IOStats.WriteMB,
			"disk_read_speed":  taskData.TaskStats.IOStats.ReadMBPS,
			"disk_write_speed": taskData.TaskStats.IOStats.WriteMBPS,

			"resource_history": "TODO",
		},
		taskData.EndTime,
	)

	return writeAPI.WritePoint(context.Background(), p)
}

func (db *InfluxDB) Close() error {
	db.bufferMu.Lock()
	if len(db.buffer) > 0 {
		if err := db.writeBatch(db.buffer); err != nil {
			log.Errorf("\033[31m[InfluxDB]\033[0m Failed to flush buffer: %v", err)
		}
	}
	db.bufferMu.Unlock()

	db.client.Close()
	return nil
}

func (db *InfluxDB) createBucketIfNotExists(bucketName string) error {
	ctx := context.Background()

	if err := db.createOrgIfNotExists(); err != nil {
		return fmt.Errorf("\033[31m[InfluxDB] failed to ensure organization exists: %v\033[0m", err)
	}

	bucketsAPI := db.client.BucketsAPI()
	bucket, _ := bucketsAPI.FindBucketByName(ctx, bucketName)

	if bucket != nil {
		log.Infof("\033[34m[InfluxDB]\033[0m Bucket already exists: %s", bucketName)
		return nil
	}

	log.Infof("\033[34m[InfluxDB]\033[0m Creating bucket: %s", bucketName)
	orgAPI := db.client.OrganizationsAPI()
	org, err := orgAPI.FindOrganizationByName(ctx, db.org)
	if err != nil {
		return fmt.Errorf("\033[31m[InfluxDB] failed to find organization: %v\033[0m", err)
	}

	_, err = bucketsAPI.CreateBucketWithName(ctx, org, bucketName)
	if err != nil {
		return fmt.Errorf("\033[31m[InfluxDB] failed to create bucket: %v\033[0m", err)
	}

	log.Infof("\033[34m[InfluxDB]\033[0m Successfully created bucket: %s", bucketName)
	return nil
}

func (db *InfluxDB) createOrgIfNotExists() error {
	ctx := context.Background()
	orgAPI := db.client.OrganizationsAPI()

	org, _ := orgAPI.FindOrganizationByName(ctx, db.org)

	if org != nil {
		log.Infof("\033[34m[InfluxDB]\033[0m Organization already exists: %s", db.org)
		return nil
	}

	log.Infof("\033[34m[InfluxDB]\033[0m Creating organization: %s", db.org)
	_, err := orgAPI.CreateOrganizationWithName(ctx, db.org)
	if err != nil {
		return fmt.Errorf("\033[31m[InfluxDB] failed to create organization: %v\033[0m", err)
	}

	log.Infof("\033[34m[InfluxDB]\033[0m Successfully created organization: %s", db.org)
	return nil
}
