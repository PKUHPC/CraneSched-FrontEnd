package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	logrus "github.com/sirupsen/logrus"

	"CraneFrontEnd/plugin/energy/pkg/config"
	"CraneFrontEnd/plugin/energy/pkg/types"
)

var log = logrus.WithField("component", "InfluxDB")

type InfluxDB struct {
	client     influxdb2.Client
	org        string
	nodeBucket string
	jobBucket  string
	enabled    *config.Enabled
}

func NewInfluxDB(config *config.Config) (*InfluxDB, error) {
	client := influxdb2.NewClient(config.DB.InfluxDB.URL, config.DB.InfluxDB.Token)
	if _, err := client.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping InfluxDB: %v", err)
	}

	db := &InfluxDB{
		client:     client,
		org:        config.DB.InfluxDB.Org,
		nodeBucket: config.DB.InfluxDB.NodeBucket,
		jobBucket:  config.DB.InfluxDB.JobBucket,
		enabled:    &config.Monitor.Enabled,
	}

	if err := db.createBucketIfNotExists(db.nodeBucket); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create node bucket: %v", err)
	}

	if err := db.createBucketIfNotExists(db.jobBucket); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create job bucket: %v", err)
	}

	return db, nil
}

func (db *InfluxDB) SaveNodeEnergy(data *types.NodeData) error {
	log.Infof("Saving node energy data")

	writeAPI := db.client.WriteAPIBlocking(db.org, db.nodeBucket)

	tags := map[string]string{}
	fields := map[string]interface{}{}
	enabledModules := make([]string, 0)

	currentTime := time.Now()
	roundedTime := currentTime.Truncate(time.Minute)
	fields["minute_time"] = roundedTime.Unix()

	fields["cpu_utilization"] = data.SystemLoad.CPUUtil
	fields["cpu_load_1"] = data.SystemLoad.CPULoad1
	fields["cpu_load_5"] = data.SystemLoad.CPULoad5
	fields["cpu_load_15"] = data.SystemLoad.CPULoad15
	fields["cpu_frequencies"] = data.SystemLoad.Frequencies
	fields["cpu_temperature"] = data.SystemLoad.CPUTemperature

	fields["memory_utilization"] = data.SystemLoad.MemoryUtil
	fields["memory_used_gb"] = data.SystemLoad.MemoryUsed
	fields["memory_total_gb"] = data.SystemLoad.MemoryTotal

	fields["disk_utilization"] = data.SystemLoad.DiskUtil
	fields["disk_io_mb_ps"] = data.SystemLoad.DiskIO

	fields["network_io_mb_ps"] = data.SystemLoad.NetworkIO
	fields["network_rx_mb_ps"] = data.SystemLoad.NetworkRx
	fields["network_tx_mb_ps"] = data.SystemLoad.NetworkTx
	enabledModules = append(enabledModules, "system")

	fields["rapl_package_energy_j"] = data.RAPL.Package
	fields["rapl_core_energy_j"] = data.RAPL.Core
	fields["rapl_uncore_energy_j"] = data.RAPL.Uncore
	fields["rapl_dram_energy_j"] = data.RAPL.DRAM
	fields["rapl_gt_energy_j"] = data.RAPL.GT
	enabledModules = append(enabledModules, "rapl")

	fields["ipmi_power_w"] = data.IPMI.Power
	fields["ipmi_energy_j"] = data.IPMI.Energy
	fields["ipmi_cpu_power_w"] = data.IPMI.CPUPower
	fields["ipmi_cpu_energy_j"] = data.IPMI.CPUEnergy
	fields["ipmi_fan_power_w"] = data.IPMI.FanPower
	fields["ipmi_disk_power_w"] = data.IPMI.HDDPower
	enabledModules = append(enabledModules, "ipmi")

	fields["gpu_energy_j"] = data.GPU.Energy
	fields["gpu_power_w"] = data.GPU.Power
	fields["gpu_utilization"] = data.GPU.Util
	fields["gpu_memory_utilization"] = data.GPU.MemUtil
	fields["gpu_temperature"] = data.GPU.Temp
	enabledModules = append(enabledModules, "gpu")

	fields["job_count"] = data.JobMetrics.JobCount
	fields["req_cpu_rate"] = data.JobMetrics.ReqCPURate
	fields["req_memory_rate"] = data.JobMetrics.ReqMemoryRate
	fields["avg_req_cpu_per_job"] = data.JobMetrics.AvgReqCPUPerJob
	fields["avg_req_memory_gb_per_job"] = data.JobMetrics.AvgReqMemoryGBPerJob
	fields["avg_job_runtime"] = data.JobMetrics.AvgJobRuntime

	// set total energy (priority: IPMI > RAPL > GPU)
	if db.enabled.IPMI {
		fields["total_energy_j"] = data.IPMI.Energy
		tags["energy_source"] = "ipmi"
	} else if db.enabled.RAPL {
		fields["total_energy_j"] = data.RAPL.Package
		tags["energy_source"] = "rapl"
	} else if db.enabled.GPU {
		fields["total_energy_j"] = data.GPU.Energy
		tags["energy_source"] = "gpu"
	} else {
		return fmt.Errorf("no energy source enabled")
	}

	tags["node_id"] = data.NodeID
	tags["enabled_modules"] = strings.Join(enabledModules, ",")

	p := influxdb2.NewPoint(
		db.nodeBucket,
		tags,
		fields,
		currentTime,
	)

	if err := writeAPI.WritePoint(context.Background(), p); err != nil {
		return fmt.Errorf("failed to write data: %v", err)
	}

	return nil
}

func (db *InfluxDB) SaveJobEnergy(jobData *types.JobData) error {
	log.Infof("Saving job energy data for job: %d, node: %s", jobData.JobID, jobData.NodeID)

	writeAPI := db.client.WriteAPIBlocking(db.org, db.jobBucket)

	p := influxdb2.NewPoint(
		db.jobBucket,
		map[string]string{
			"node_id": jobData.NodeID,
			"job_id":  fmt.Sprintf("%d", jobData.JobID),
		},
		map[string]interface{}{
			"start_time": jobData.StartTime,
			"end_time":   jobData.EndTime,
			"duration":   jobData.Duration.Seconds(),

			"total_energy_j":  jobData.TotalEnergy,
			"cpu_energy_j":    jobData.CPUEnergy,
			"gpu_energy_j":    jobData.GPUEnergy,
			"average_power_w": jobData.AveragePower,

			"cpu_time_usage_seconds": jobData.CgroupStats.CPUStats.UsageSeconds,
			"cpu_utilization":        jobData.CgroupStats.CPUStats.Utilization,
			"gpu_utilization":        jobData.CgroupStats.GPUStats.Utilization,
			"gpu_memory_utilization": jobData.CgroupStats.GPUStats.MemoryUtil,
			"memory_usage_mb":        jobData.CgroupStats.MemoryStats.UsageMB,
			"memory_utilization":     jobData.CgroupStats.MemoryStats.Utilization,
			"disk_read_mb":           jobData.CgroupStats.IOStats.ReadMB,
			"disk_write_mb":          jobData.CgroupStats.IOStats.WriteMB,
			"disk_read_speed_mb_ps":  jobData.CgroupStats.IOStats.ReadMBPS,
			"disk_write_speed_mb_ps": jobData.CgroupStats.IOStats.WriteMBPS,
			"disk_read_iops":         jobData.CgroupStats.IOStats.ReadOperations,
			"disk_write_iops":        jobData.CgroupStats.IOStats.WriteOperations,
			"disk_read_iops_ps":      jobData.CgroupStats.IOStats.ReadOpsPerSec,
			"disk_write_iops_ps":     jobData.CgroupStats.IOStats.WriteOpsPerSec,
		},
		time.Now(),
	)

	return writeAPI.WritePoint(context.Background(), p)
}

func (db *InfluxDB) Close() error {
	db.client.Close()
	return nil
}

func (db *InfluxDB) createBucketIfNotExists(bucketName string) error {
	ctx := context.Background()

	if err := db.createOrgIfNotExists(); err != nil {
		return fmt.Errorf("failed to ensure organization exists: %v", err)
	}

	bucketsAPI := db.client.BucketsAPI()
	bucket, _ := bucketsAPI.FindBucketByName(ctx, bucketName)

	if bucket != nil {
		log.Infof("Bucket already exists: %s", bucketName)
		return nil
	}

	log.Infof("Creating bucket: %s", bucketName)
	orgAPI := db.client.OrganizationsAPI()
	org, err := orgAPI.FindOrganizationByName(ctx, db.org)
	if err != nil {
		return fmt.Errorf("failed to find organization: %v", err)
	}

	_, err = bucketsAPI.CreateBucketWithName(ctx, org, bucketName)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %v", err)
	}

	log.Infof("Successfully created bucket: %s", bucketName)
	return nil
}

func (db *InfluxDB) createOrgIfNotExists() error {
	ctx := context.Background()
	orgAPI := db.client.OrganizationsAPI()

	org, _ := orgAPI.FindOrganizationByName(ctx, db.org)

	if org != nil {
		log.Infof("Organization already exists: %s", db.org)
		return nil
	}

	log.Infof("Creating organization: %s", db.org)
	_, err := orgAPI.CreateOrganizationWithName(ctx, db.org)
	if err != nil {
		return fmt.Errorf("failed to create organization: %v", err)
	}

	log.Infof("Successfully created organization: %s", db.org)
	return nil
}
