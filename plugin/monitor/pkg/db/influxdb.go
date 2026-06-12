package db

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	logrus "github.com/sirupsen/logrus"

	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/plugin/monitor/pkg/config"
	"CraneFrontEnd/plugin/monitor/pkg/types"
)

var log = logrus.WithField("component", "InfluxDB")

const (
	maxRetries    = 3
	retryInterval = 5 * time.Second
)

type InfluxDB struct {
	client              influxdb2.Client
	org                 string
	nodeBucket          string
	jobBucket           string
	clusterBucket       string
	traceBucket         string
	traceCoreBucket     string
	traceDetailBucket   string
	traceErrorBucket    string
	traceShardBuckets   []string
	eventMeasurement    string
	resourceMeasurement string
	enabled             *config.Enabled
}

var coreTraceSpanNames = map[string]struct{}{
	"job/pending":   {},
	"job/lifecycle": {},
	"step/execute":  {},
	"job/end":       {},
}

func NewInfluxDB(config *config.Config) (*InfluxDB, error) {
	var client influxdb2.Client
	var err error

	for i := 0; i < maxRetries; i++ {
		client = influxdb2.NewClient(config.DB.InfluxDB.URL, config.DB.InfluxDB.Token)
		_, err = client.Ping(context.Background())

		if err == nil {
			break
		}

		log.Warnf("Failed to connect to InfluxDB (attempt %d/%d): %v", i+1, maxRetries, err)
		client.Close()

		if i < maxRetries-1 {
			time.Sleep(retryInterval)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to ping InfluxDB after %d attempts: %v", maxRetries, err)
	}

	db := &InfluxDB{
		client:              client,
		org:                 config.DB.InfluxDB.Org,
		nodeBucket:          config.DB.InfluxDB.NodeBucket,
		jobBucket:           config.DB.InfluxDB.JobBucket,
		clusterBucket:       config.DB.InfluxDB.ClusterBucket,
		traceBucket:         config.DB.InfluxDB.TraceBucket,
		traceCoreBucket:     config.DB.InfluxDB.TraceCoreBucket,
		traceDetailBucket:   config.DB.InfluxDB.TraceDetailBucket,
		traceErrorBucket:    config.DB.InfluxDB.TraceErrorBucket,
		traceShardBuckets:   append([]string(nil), config.DB.InfluxDB.TraceShardBuckets...),
		eventMeasurement:    config.DB.InfluxDB.EventMeasurement,
		resourceMeasurement: config.DB.InfluxDB.ResourceMeasurement,
		enabled:             &config.Monitor.Enabled,
	}

	if err := db.createBucketIfNotExists(db.nodeBucket); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create node bucket: %v", err)
	}

	if err := db.createBucketIfNotExists(db.jobBucket); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create job bucket: %v", err)
	}

	if err := db.createBucketIfNotExists(db.clusterBucket); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create cluster bucket: %v", err)
	}

	for _, bucket := range db.traceBuckets() {
		if err := db.createBucketIfNotExists(bucket); err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to create trace bucket %s: %v", bucket, err)
		}
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

func (db *InfluxDB) SaveNodeEvents(events []*protos.CranedEventInfo) error {
	if len(events) == 0 {
		return nil
	}

	log.Infof("Saving %d node events", len(events))

	writeAPI := db.client.WriteAPIBlocking(db.org, db.nodeBucket)

	measurement := db.eventMeasurement
	if measurement == "" {
		measurement = "NodeEvents"
	}

	ctx := context.Background()
	for _, event := range events {
		tags := map[string]string{
			"cluster_name": event.ClusterName,
			"node_name":    event.NodeName,
		}

		reason := event.Reason
		if reason == "" {
			reason = " "
		}

		var stateValue int32
		if controlState, ok := event.StateType.(*protos.CranedEventInfo_ControlState); ok {
			stateValue = int32(controlState.ControlState)
		} else if powerState, ok := event.StateType.(*protos.CranedEventInfo_PowerState); ok {
			stateValue = int32(powerState.PowerState)
		} else {
			stateValue = -1 // unknown state type
		}

		fields := map[string]any{
			"uid":        event.Uid,
			"start_time": event.StartTime.AsTime().UnixNano(),
			"state":      stateValue,
			"reason":     reason,
		}

		point := influxdb2.NewPoint(measurement, tags, fields, time.Now())

		if err := writeAPI.WritePoint(ctx, point); err != nil {
			log.Errorf("Failed to write event to InfluxDB: %v", err)
			return fmt.Errorf("failed to write event: %v", err)
		}

		log.Tracef("Recorded cluster_name: %v, uid: %v, node_name: %s, state: %d, start_time: %s, Reason: %s",
			event.ClusterName, event.Uid, event.NodeName, stateValue, event.StartTime.AsTime().Format(time.RFC3339), event.Reason)
	}

	return nil
}

func (db *InfluxDB) SaveLicenseUsage(licenses []*protos.LicenseInfo) error {
	if len(licenses) == 0 {
		return nil
	}

	log.Infof("Saving %d licenses", len(licenses))

	writeAPI := db.client.WriteAPIBlocking(db.org, db.clusterBucket)

	measurement := db.resourceMeasurement
	if measurement == "" {
		measurement = "LicenseUsage"
	}
	ctx := context.Background()
	for _, license := range licenses {
		tags := map[string]string{
			"license_name": license.GetName(),
		}

		fields := map[string]interface{}{
			"total":         license.Total,
			"used":          license.Used,
			"free":          license.Free,
			"reserved":      license.Reserved,
			"last_deficit":  license.LastDeficit,
			"last_consumed": license.LastConsumed,
		}
		point := influxdb2.NewPoint(measurement, tags, fields, time.Now())

		if err := writeAPI.WritePoint(ctx, point); err != nil {
			log.Errorf("Failed to write license to InfluxDB: %v", err)
			return fmt.Errorf("failed to write license: %v", err)
		}

		log.Tracef("License info: %v, total: %v, used: %v, free: %v, ts=%v", license.Name, license.Total, license.Used, license.Free, point.Time())
	}

	return nil
}

func (db *InfluxDB) SaveSpans(spans []*protos.SpanInfo) error {
	if len(spans) == 0 {
		return nil
	}

	byBucket := make(map[string][]*protos.SpanInfo)
	for _, span := range spans {
		for _, bucket := range db.TraceBucketsForSpan(span) {
			byBucket[bucket] = append(byBucket[bucket], span)
		}
	}

	buckets := make([]string, 0, len(byBucket))
	for bucket := range byBucket {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)

	for _, bucket := range buckets {
		if err := db.SaveSpansToBucket(bucket, byBucket[bucket]); err != nil {
			return err
		}
	}
	return nil
}

func (db *InfluxDB) SaveSpansToBucket(bucket string, spans []*protos.SpanInfo) error {
	if len(spans) == 0 {
		return nil
	}
	if bucket == "" {
		bucket = db.traceBucket
	}

	start := time.Now()
	writeAPI := db.client.WriteAPIBlocking(db.org, bucket)
	ctx := context.Background()
	points := make([]*write.Point, 0, len(spans))

	for _, span := range spans {
		tags := map[string]string{
			"name": span.Name,
		}
		if span.ServiceName != "" {
			tags["service"] = span.ServiceName
		}

		// Calculate duration in microseconds
		startTime := span.StartTime.AsTime()
		endTime := span.EndTime.AsTime()
		duration := endTime.Sub(startTime).Microseconds()

		fields := map[string]interface{}{
			"trace_id":       span.TraceId,
			"span_id":        span.SpanId,
			"parent_span_id": span.ParentSpanId,
			"duration_us":    duration,
		}

		// Add custom attributes
		for k, v := range span.Attributes {
			fields[k] = v
		}

		point := influxdb2.NewPoint("spans", tags, fields, endTime)
		points = append(points, point)
	}

	if err := writeAPI.WritePoint(ctx, points...); err != nil {
		log.Errorf("Failed to write %d spans to InfluxDB bucket=%s: %v", len(points), bucket, err)
		return fmt.Errorf("failed to write spans to bucket %s: %v", bucket, err)
	}

	elapsed := time.Since(start)
	log.Debugf("Saved %d trace spans to InfluxDB bucket=%s in %s", len(points), bucket, elapsed)
	if elapsed > time.Second {
		log.Warnf("Slow trace span write: saved %d spans to InfluxDB bucket=%s in %s",
			len(points), bucket, elapsed)
	}
	return nil
}

func (db *InfluxDB) TraceBucketForSpan(span *protos.SpanInfo) string {
	buckets := db.TraceBucketsForSpan(span)
	if len(buckets) == 0 {
		return db.traceBucket
	}
	return buckets[0]
}

func (db *InfluxDB) TraceBucketsForSpan(span *protos.SpanInfo) []string {
	if span == nil {
		return []string{db.traceBucket}
	}

	primary := db.traceBucket
	if _, ok := coreTraceSpanNames[span.Name]; ok {
		if len(db.traceShardBuckets) > 0 {
			primary = db.traceShardBuckets[stableTraceShardKey(span)%uint32(len(db.traceShardBuckets))]
		} else if db.traceCoreBucket != "" {
			primary = db.traceCoreBucket
		}

		buckets := []string{primary}
		if spanShouldWriteErrorBucket(span) {
			errorBucket := db.traceErrorBucket
			if errorBucket == "" {
				errorBucket = db.traceBucket
			}
			if errorBucket != "" && errorBucket != primary {
				buckets = append(buckets, errorBucket)
			}
		}
		return buckets
	}

	if spanShouldWriteErrorBucket(span) {
		if db.traceErrorBucket != "" {
			primary = db.traceErrorBucket
		}
	} else if db.traceDetailBucket != "" {
		primary = db.traceDetailBucket
	}
	return []string{primary}
}

func spanShouldWriteErrorBucket(span *protos.SpanInfo) bool {
	if span == nil {
		return false
	}
	if span.Status == protos.SpanStatus_SPAN_STATUS_ERROR {
		return true
	}
	if v, ok := span.Attributes["final_status"]; ok && v != "" &&
		v != "2" && v != "Completed" && v != "completed" {
		return true
	}
	return false
}

func stableTraceShardKey(span *protos.SpanInfo) uint32 {
	key := ""
	if span != nil {
		if jobID := span.Attributes["job_id"]; jobID != "" {
			key = jobID
		} else if span.TraceId != "" {
			key = span.TraceId
		} else {
			key = span.SpanId
		}
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func (db *InfluxDB) traceBuckets() []string {
	seen := make(map[string]struct{})
	add := func(bucket string) {
		if bucket == "" {
			return
		}
		seen[bucket] = struct{}{}
	}
	add(db.traceBucket)
	add(db.traceCoreBucket)
	add(db.traceDetailBucket)
	add(db.traceErrorBucket)
	for _, bucket := range db.traceShardBuckets {
		add(bucket)
	}
	buckets := make([]string, 0, len(seen))
	for bucket := range seen {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)
	return buckets
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
