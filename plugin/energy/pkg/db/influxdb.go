package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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

func NewInfluxDB(config *config.DBConfig) (*InfluxDB, error) {
	log.Infof("\033[34m[InfluxDB]\033[0m Initializing InfluxDB with config: %+v", config)

	client := influxdb2.NewClient(config.InfluxDB.URL, config.InfluxDB.Token)
	if _, err := client.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("\033[31m[InfluxDB] failed to ping InfluxDB: %v\033[0m", err)
	}

	db := &InfluxDB{
		client:     client,
		org:        config.InfluxDB.Org,
		nodeBucket: config.InfluxDB.NodeBucket,
		taskBucket: config.InfluxDB.TaskBucket,
		batchSize:  config.BatchSize,
		buffer:     make([]*types.NodeData, 0, config.BatchSize),
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

	flushTime, err := time.ParseDuration(config.FlushTime)
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
	log.Infof("\033[34m[InfluxDB]\033[0m Batch saving node energy data, count: %d", len(dataList))

	if len(dataList) == 0 {
		return fmt.Errorf("\033[31m[InfluxDB] no data to write\033[0m")
	}

	writeAPI := db.client.WriteAPIBlocking(db.org, db.nodeBucket)
	points := make([]*write.Point, 0, len(dataList))

	for _, data := range dataList {
		p := influxdb2.NewPoint(
			db.nodeBucket,
			map[string]string{
				"node_id":    NodeID,
				"cluster_id": ClusterID,
			},
			map[string]interface{}{
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
			},
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

	history, _ := json.Marshal(taskData.TaskResourceUsage)

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

			"cpu_time":         taskData.CPUTime,
			"cpu_utilization":  taskData.CPUUtilization,
			"gpu_utilization":  taskData.GPUUtilization,
			"memory_usage":     taskData.MemoryUsage,
			"memory_util":      taskData.MemoryUtil,
			"disk_read_bytes":  taskData.DiskReadBytes,
			"disk_write_bytes": taskData.DiskWriteBytes,
			"disk_read_speed":  taskData.DiskReadSpeed,
			"disk_write_speed": taskData.DiskWriteSpeed,

			"resource_history": string(history),
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
