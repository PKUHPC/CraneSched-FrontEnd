package db

import (
	"context"
	"fmt"
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
	log.Infof("\033[34m[InfluxDB]\033[0m Batch saving node energy data, count: %d", len(dataList))

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
			"cpu_utilization":    nil,
			"cpu_load_1":         nil,
			"cpu_load_5":         nil,
			"cpu_load_15":        nil,
			"cpu_frequencies":    nil,
			"cpu_temperature":    nil,
			"memory_utilization": nil,
			"memory_used_gb":     nil,
			"memory_total_gb":    nil,
			"disk_utilization":   nil,
			"disk_io_mb_ps":      nil,
			"network_io_mb_ps":   nil,
			"network_rx_mb_ps":   nil,
			"network_tx_mb_ps":   nil,

			// RAPL 相关字段
			"rapl_package_energy_j": nil,
			"rapl_core_energy_j":    nil,
			"rapl_uncore_energy_j":  nil,
			"rapl_dram_energy_j":    nil,
			"rapl_gt_energy_j":      nil,

			// IPMI 相关字段
			"ipmi_power_w":      nil,
			"ipmi_energy_j":     nil,
			"ipmi_cpu_power_w":  nil,
			"ipmi_cpu_energy_j": nil,
			"ipmi_fan_power_w":  nil,
			"ipmi_disk_power_w": nil,

			// GPU 相关字段
			"gpu_energy_j":           nil,
			"gpu_power_w":            nil,
			"gpu_utilization":        nil,
			"gpu_memory_utilization": nil,
			"gpu_temperature":        nil,

			// 总能耗字段
			"total_energy_j": nil,
		}

		// 记录启用的模块
		enabledSources := make([]string, 0)

		// 根据开关填充实际值
		if db.switches.System {
			fields["cpu_utilization"] = data.SystemLoad.CPUUtil
			fields["cpu_load_1"] = data.SystemLoad.CPULoad1
			fields["cpu_load_5"] = data.SystemLoad.CPULoad5
			fields["cpu_load_15"] = data.SystemLoad.CPULoad15
			fields["cpu_frequencies"] = data.SystemLoad.Frequencies
			fields["cpu_temperature"] = data.SystemLoad.Temperature

			fields["memory_utilization"] = data.SystemLoad.MemoryUtil
			fields["memory_used_gb"] = data.SystemLoad.MemoryUsed
			fields["memory_total_gb"] = data.SystemLoad.MemoryTotal

			fields["disk_utilization"] = data.SystemLoad.DiskUtil
			fields["disk_io_mb_ps"] = data.SystemLoad.DiskIO

			fields["network_io_mb_ps"] = data.SystemLoad.NetworkIO
			fields["network_rx_mb_ps"] = data.SystemLoad.NetworkRx
			fields["network_tx_mb_ps"] = data.SystemLoad.NetworkTx

			enabledSources = append(enabledSources, "system")
		}

		if db.switches.RAPL {
			fields["rapl_package_energy_j"] = data.RAPL.Package
			fields["rapl_core_energy_j"] = data.RAPL.Core
			fields["rapl_uncore_energy_j"] = data.RAPL.Uncore
			fields["rapl_dram_energy_j"] = data.RAPL.DRAM
			fields["rapl_gt_energy_j"] = data.RAPL.GT
			enabledSources = append(enabledSources, "rapl")
		}

		if db.switches.IPMI {
			fields["ipmi_power_w"] = data.IPMI.Power
			fields["ipmi_energy_j"] = data.IPMI.Energy
			fields["ipmi_cpu_power_w"] = data.IPMI.CPUPower
			fields["ipmi_cpu_energy_j"] = data.IPMI.CPUEnergy
			fields["ipmi_fan_power_w"] = data.IPMI.FanPower
			fields["ipmi_disk_power_w"] = data.IPMI.HDDPower
			enabledSources = append(enabledSources, "ipmi")
		}

		if db.switches.GPU {
			fields["gpu_energy_j"] = data.GPU.Energy
			fields["gpu_power_w"] = data.GPU.Power
			fields["gpu_utilization"] = data.GPU.Util
			fields["gpu_memory_utilization"] = data.GPU.MemUtil
			fields["gpu_temperature"] = data.GPU.Temp
			enabledSources = append(enabledSources, "gpu")
		}

		// 设置总能耗（保持优先级：IPMI > RAPL > GPU）
		if db.switches.IPMI {
			fields["total_energy_j"] = data.IPMI.Energy
			tags["energy_source"] = "ipmi"
		} else if db.switches.RAPL {
			fields["total_energy_j"] = data.RAPL.Package
			tags["energy_source"] = "rapl"
		} else if db.switches.GPU {
			fields["total_energy_j"] = data.GPU.Energy
			tags["energy_source"] = "gpu"
		}

		tags["node_id"] = data.NodeID
		tags["cluster_id"] = data.ClusterID
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

// 定期刷新缓冲区
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
	log.Infof("\033[34m[InfluxDB]\033[0m Saving task energy data for task: %s, node: %s, cluster: %s", taskData.TaskName, taskData.NodeID, taskData.ClusterID)
	writeAPI := db.client.WriteAPIBlocking(db.org, db.taskBucket)

	p := influxdb2.NewPoint(
		db.taskBucket,
		map[string]string{
			"task_name":  taskData.TaskName,
			"node_id":    taskData.NodeID,
			"cluster_id": taskData.ClusterID,
		},
		map[string]interface{}{
			"start_time": taskData.StartTime,
			"end_time":   taskData.EndTime,
			"duration":   taskData.Duration.Seconds(),

			"total_energy_j":  taskData.TotalEnergy,
			"cpu_energy_j":    taskData.CPUEnergy,
			"gpu_energy_j":    taskData.GPUEnergy,
			"average_power_w": taskData.AveragePower,

			"cpu_time_usage_seconds": taskData.TaskStats.CPUStats.UsageSeconds,
			"cpu_utilization":        taskData.TaskStats.CPUStats.Utilization,
			"gpu_utilization":        taskData.TaskStats.GPUStats.Utilization,
			"gpu_memory_utilization": taskData.TaskStats.GPUStats.MemoryUtil,
			"memory_usage_mb":        taskData.TaskStats.MemoryStats.UsageMB,
			"memory_utilization":     taskData.TaskStats.MemoryStats.Utilization,
			"disk_read_mb":           taskData.TaskStats.IOStats.ReadMB,
			"disk_write_mb":          taskData.TaskStats.IOStats.WriteMB,
			"disk_read_speed_mb_ps":  taskData.TaskStats.IOStats.ReadMBPS,
			"disk_write_speed_mb_ps": taskData.TaskStats.IOStats.WriteMBPS,
			"disk_read_iops":         taskData.TaskStats.IOStats.ReadOperations,
			"disk_write_iops":        taskData.TaskStats.IOStats.WriteOperations,
			"disk_read_iops_ps":      taskData.TaskStats.IOStats.ReadOpsPerSec,
			"disk_write_iops_ps":     taskData.TaskStats.IOStats.WriteOpsPerSec,
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
