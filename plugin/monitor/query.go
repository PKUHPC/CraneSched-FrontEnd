package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"CraneFrontEnd/plugin/monitor/pkg/config"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type QueryService struct {
	protos.UnimplementedCeffQueryServiceServer

	config         *config.Config
	hostConfigPath string
	craneConfig    *util.Config
}

func NewQueryService(cfg *config.Config) *QueryService {
	return &QueryService{
		config:         cfg,
		hostConfigPath: util.DefaultConfigPath,
	}
}

func (s *QueryService) QueryTaskEfficiency(ctx context.Context, req *protos.QueryTaskEfficiencyRequest) (*protos.QueryTaskEfficiencyReply, error) {
	log.Infof("QueryService received QueryTaskEfficiency request from UID %d for tasks: %v", req.Uid, req.TaskIds)

	if len(req.TaskIds) == 0 {
		return &protos.QueryTaskEfficiencyReply{
			Ok:           false,
			ErrorMessage: "No task IDs provided",
		}, nil
	}

	efficiencyData, err := s.queryTaskEfficiencyData(ctx, req.TaskIds, req.Uid)
	if err != nil {
		log.Errorf("QueryService failed to query efficiency data: %v", err)
		return &protos.QueryTaskEfficiencyReply{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("Failed to query efficiency data: %v", err),
		}, nil
	}

	log.Infof("QueryService successfully returned %d efficiency records for user %d", len(efficiencyData), req.Uid)
	return &protos.QueryTaskEfficiencyReply{
		Ok:             true,
		EfficiencyData: efficiencyData,
	}, nil
}

func (s *QueryService) queryTaskEfficiencyData(ctx context.Context, taskIds []uint32, userID uint32) ([]*protos.TaskEfficiencyInfo, error) {
	if s.config.DB.InfluxDB == nil {
		return nil, fmt.Errorf("InfluxDB configuration not initialized")
	}

	if s.craneConfig == nil {
		configPath := s.hostConfigPath
		if configPath == "" {
			configPath = util.DefaultConfigPath
		}
		s.craneConfig = util.ParseConfig(configPath)
		if s.craneConfig == nil {
			return nil, fmt.Errorf("failed to parse crane config from %s", configPath)
		}
	}

	taskInfos, err := s.getTaskInformation(ctx, taskIds)
	if err != nil {
		return nil, fmt.Errorf("failed to get task information: %w", err)
	}

	if err := s.authorizeEfficiencyQuery(taskInfos, taskIds, userID); err != nil {
		return nil, fmt.Errorf("authorization failed: %w", err)
	}

	if len(taskInfos) == 0 {
		return nil, fmt.Errorf("no tasks found for the specified task IDs")
	}

	nodeNames := s.extractNodeNames(taskInfos)
	if len(nodeNames) == 0 {
		return nil, fmt.Errorf("no nodes found for the specified tasks")
	}

	efficiencyData, err := s.queryInfluxDbDataByTags(taskIds, nodeNames)
	if err != nil {
		return nil, fmt.Errorf("failed to query InfluxDB: %w", err)
	}

	if len(efficiencyData) == 0 {
		return nil, fmt.Errorf("no matching efficiency data available")
	}

	return efficiencyData, nil
}

func (s *QueryService) authorizeEfficiencyQuery(taskInfos []*protos.TaskInfo, taskIds []uint32, userID uint32) error {
	for _, taskInfo := range taskInfos {
		if taskInfo == nil {
			continue
		}

		if taskInfo.Uid != userID {
			return fmt.Errorf("user %d not authorized to access task %d (owned by user %d)", userID, taskInfo.TaskId, taskInfo.Uid)
		}
	}

	log.Infof("User %d authorized to access tasks: %v", userID, taskIds)
	return nil
}

func (s *QueryService) getTaskInformation(ctx context.Context, taskIds []uint32) ([]*protos.TaskInfo, error) {
	if s.craneConfig == nil {
		return nil, fmt.Errorf("crane config not initialized")
	}

	ctldClient := util.GetStubToCtldByConfig(s.craneConfig)

	req := &protos.QueryTasksInfoRequest{
		FilterTaskIds:               taskIds,
		OptionIncludeCompletedTasks: true,
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	reply, err := ctldClient.QueryTasksInfo(timeoutCtx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to query tasks info from CraneCtld: %w", err)
	}

	return reply.TaskInfoList, nil
}

func (s *QueryService) extractNodeNames(taskInfos []*protos.TaskInfo) []string {
	var nodeNames []string
	for _, taskInfo := range taskInfos {
		if taskInfo == nil {
			continue
		}

		nodes, ok := util.ParseHostList(taskInfo.GetCranedList())
		if !ok {
			log.Warnf("Failed to parse host list: %s", taskInfo.GetCranedList())
			continue
		}
		nodeNames = append(nodeNames, nodes...)
	}
	return nodeNames
}

func (s *QueryService) queryInfluxDbDataByTags(taskIds []uint32, hostNames []string) ([]*protos.TaskEfficiencyInfo, error) {
	influxCfg := s.config.DB.InfluxDB
	client := influxdb2.NewClient(influxCfg.URL, influxCfg.Token)
	defer client.Close()

	ctx := context.Background()
	if pong, err := client.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping InfluxDB: %v", err)
	} else if !pong {
		return nil, fmt.Errorf("failed to ping InfluxDB: not pong")
	}

	measurement := influxCfg.ResourceMeasurement
	if measurement == "" {
		measurement = "ResourceUsage"
	}

	jobIDFilters := make([]string, len(taskIds))
	for i, id := range taskIds {
		jobIDFilters[i] = fmt.Sprintf(`r["job_id"] == "%d"`, id)
	}
	jobIDCondition := strings.Join(jobIDFilters, " or ")

	hostnameFilters := make([]string, len(hostNames))
	for i, hostname := range hostNames {
		hostnameFilters[i] = fmt.Sprintf(`r["hostname"] == "%s"`, hostname)
	}
	hostnameCondition := strings.Join(hostnameFilters, " or ")

	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
|> range(start: 0)
|> filter(fn: (r) =>
    r["_measurement"] == "%s" and
        (r["_field"] == "cpu_usage" or r["_field"] == "memory_usage" or r["_field"] == "proc_count") and
        (%s) and (%s))
|> group(columns: ["job_id", "hostname", "_field"])
|> max(column: "_value")`,
		influxCfg.JobBucket, measurement,
		jobIDCondition, hostnameCondition)

	queryAPI := client.QueryAPI(influxCfg.Org)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := queryAPI.Query(timeoutCtx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("execute query failed: %w", err)
	}

	dataMap := make(map[string]*protos.TaskEfficiencyInfo)
	for result.Next() {
		record := result.Record()

		jobIDStr, ok := record.ValueByKey("job_id").(string)
		if !ok {
			log.Printf("Invalid type for job_id")
			continue
		}
		jobID, err := strconv.ParseInt(jobIDStr, 10, 32)
		if err != nil {
			log.Printf("Failed to parse job_id: %v", err)
			continue
		}
		hostname, ok := record.ValueByKey("hostname").(string)
		if !ok {
			log.Printf("Invalid type for hostname")
			continue
		}

		key := fmt.Sprintf("%d:%s", jobID, hostname)
		if _, exists := dataMap[key]; !exists {
			dataMap[key] = &protos.TaskEfficiencyInfo{
				TaskId:    uint32(jobID),
				Hostname:  hostname,
				Timestamp: timestamppb.New(record.Time()),
			}
		}

		field, ok := record.ValueByKey("_field").(string)
		if !ok {
			log.Printf("Invalid type for _field")
			continue
		}

		value, ok := record.Value().(uint64)
		if !ok {
			log.Printf("Invalid type for _value")
			continue
		}

		switch field {
		case "cpu_usage":
			dataMap[key].CpuUsage = value
		case "memory_usage":
			dataMap[key].MemoryUsage = value
		case "proc_count":
			dataMap[key].ProcCount = value
		}
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query parsing error: %w", result.Err())
	}

	var efficiencyData []*protos.TaskEfficiencyInfo
	for _, data := range dataMap {
		efficiencyData = append(efficiencyData, data)
	}

	return efficiencyData, nil
}

func (s *QueryService) QueryNodeEvents(ctx context.Context, req *protos.QueryNodeEventsRequest) (*protos.QueryNodeEventsReply, error) {
	log.Infof("QueryService received QueryNodeEvents request from UID %d for nodes: %v", req.Uid, req.NodeNames)

	if s.config.DB.InfluxDB == nil {
		return &protos.QueryNodeEventsReply{
			Ok:           false,
			ErrorMessage: "InfluxDB configuration not initialized",
		}, nil
	}

	if s.craneConfig == nil {
		configPath := s.hostConfigPath
		if configPath == "" {
			configPath = util.DefaultConfigPath
		}
		s.craneConfig = util.ParseConfig(configPath)
		if s.craneConfig == nil {
			return &protos.QueryNodeEventsReply{
				Ok:           false,
				ErrorMessage: fmt.Sprintf("Failed to parse crane config from %s", configPath),
			}, nil
		}
	}

	// Get valid node list from crane config
	nodeNames := req.NodeNames
	if len(nodeNames) == 0 {
		// Query all nodes
		var err error
		nodeNames, err = util.GetValidNodeList(s.craneConfig.CranedNodeList)
		if err != nil {
			return &protos.QueryNodeEventsReply{
				Ok:           false,
				ErrorMessage: fmt.Sprintf("Failed to get valid node list: %v", err),
			}, nil
		}
	} else {
		// Validate requested nodes
		validNodes, err := util.GetValidNodeList(s.craneConfig.CranedNodeList)
		if err != nil {
			return &protos.QueryNodeEventsReply{
				Ok:           false,
				ErrorMessage: fmt.Sprintf("Failed to get valid node list: %v", err),
			}, nil
		}
		validNodeSet := make(map[string]struct{})
		for _, node := range validNodes {
			validNodeSet[node] = struct{}{}
		}

		for _, node := range nodeNames {
			if _, exists := validNodeSet[node]; !exists {
				return &protos.QueryNodeEventsReply{
					Ok:           false,
					ErrorMessage: fmt.Sprintf("Invalid node name: %s", node),
				}, nil
			}
		}
	}

	clusterName := s.craneConfig.ClusterName
	if clusterName == "" {
		return &protos.QueryNodeEventsReply{
			Ok:           false,
			ErrorMessage: "ClusterName is empty in configuration",
		}, nil
	}

	eventInfoList, err := s.queryNodeEventsFromInfluxDB(ctx, clusterName, nodeNames)
	if err != nil {
		log.Errorf("Failed to query node events: %v", err)
		return &protos.QueryNodeEventsReply{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("Failed to query node events: %v", err),
		}, nil
	}

	log.Infof("Successfully returned %d event records", len(eventInfoList))
	return &protos.QueryNodeEventsReply{
		Ok:            true,
		EventInfoList: eventInfoList,
	}, nil
}

func (s *QueryService) queryNodeEventsFromInfluxDB(ctx context.Context, clusterName string, nodeNames []string) ([]*protos.NodeEventInfo, error) {
	influxCfg := s.config.DB.InfluxDB
	client := influxdb2.NewClient(influxCfg.URL, influxCfg.Token)
	defer client.Close()

	pingCtx := context.Background()
	if pong, err := client.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("failed to ping InfluxDB: %v", err)
	} else if !pong {
		return nil, fmt.Errorf("failed to ping InfluxDB: not pong")
	}

	measurement := influxCfg.EventMeasurement
	if measurement == "" {
		measurement = "NodeEvents"
	}

	nodeNameFilters := make([]string, len(nodeNames))
	for i, nodeName := range nodeNames {
		nodeNameFilters[i] = fmt.Sprintf(`r["node_name"] == "%s"`, nodeName)
	}
	nodeNameCondition := strings.Join(nodeNameFilters, " or ")

	clusterNameCondition := fmt.Sprintf(`r["cluster_name"] == "%s"`, clusterName)

	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
|> range(start: 0)
|> filter(fn: (r) => 
    r["_measurement"] == "%s" and 
	(r["_field"] == "state" or r["_field"] == "uid" or 
	r["_field"] == "reason" or r["_field"] == "start_time") and
	(%s) and (%s))`,
		influxCfg.NodeBucket, measurement,
		nodeNameCondition, clusterNameCondition)

	queryAPI := client.QueryAPI(influxCfg.Org)
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, err := queryAPI.Query(timeoutCtx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("execute query failed: %w", err)
	}

	type eventKey struct {
		nodeName  string
		timestamp int64
	}

	eventMap := make(map[eventKey]*protos.NodeEventInfo)

	for result.Next() {
		record := result.Record()

		nodeName, ok := record.ValueByKey("node_name").(string)
		if !ok {
			continue
		}

		clusterNameVal, ok := record.ValueByKey("cluster_name").(string)
		if !ok {
			continue
		}

		timestamp := record.Time().UnixNano()
		key := eventKey{nodeName: nodeName, timestamp: timestamp}

		if _, exists := eventMap[key]; !exists {
			eventMap[key] = &protos.NodeEventInfo{
				ClusterName: clusterNameVal,
				NodeName:    nodeName,
			}
		}

		field, ok := record.ValueByKey("_field").(string)
		if !ok {
			continue
		}

		switch field {
		case "state":
			if state, ok := record.Value().(string); ok {
				eventMap[key].State = state
			}
		case "reason":
			if reason, ok := record.Value().(string); ok {
				eventMap[key].Reason = reason
			}
		case "uid":
			if uid, ok := record.Value().(uint64); ok {
				eventMap[key].Uid = uid
			} else if uidFloat, ok := record.Value().(float64); ok {
				eventMap[key].Uid = uint64(uidFloat)
			}
		case "start_time":
			if startTime, ok := record.Value().(int64); ok {
				eventMap[key].StartTime = startTime
			} else if startTimeFloat, ok := record.Value().(float64); ok {
				eventMap[key].StartTime = int64(startTimeFloat)
			}
		}

		// Try to get end_time if available
		if endTime, ok := record.ValueByKey("end_time").(int64); ok {
			eventMap[key].EndTime = endTime
		} else if endTimeFloat, ok := record.ValueByKey("end_time").(float64); ok {
			eventMap[key].EndTime = int64(endTimeFloat)
		}
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query parsing error: %w", result.Err())
	}

	var eventInfoList []*protos.NodeEventInfo
	for _, event := range eventMap {
		eventInfoList = append(eventInfoList, event)
	}

	return eventInfoList, nil
}
