/**
 * Copyright (c) 2024 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CeffQueryService struct {
	protos.UnimplementedCeffQueryServiceServer

	influxConfig *util.InfluxDbConfig
	craneConfig  *util.Config
}

func NewCeffQueryService(plugin *MonitorPlugin, hostConfigPath string) (*CeffQueryService, error) {
	if plugin == nil {
		return nil, fmt.Errorf("monitor plugin reference is nil")
	}

	influxCfg := &util.InfluxDbConfig{
		Username:    plugin.config.Database.Username,
		Bucket:      plugin.config.Database.Bucket,
		Org:         plugin.config.Database.Org,
		Token:       plugin.config.Database.Token,
		Measurement: plugin.config.Database.Measurement,
		Url:         plugin.config.Database.Url,
	}

	configPath := hostConfigPath
	if configPath == "" {
		configPath = util.DefaultConfigPath
	}

	craneCfg := util.ParseConfig(configPath)
	if craneCfg == nil {
		return nil, fmt.Errorf("failed to parse crane config from %s", configPath)
	}

	return &CeffQueryService{
		influxConfig: influxCfg,
		craneConfig:  craneCfg,
	}, nil
}

func (s *CeffQueryService) QueryTaskEfficiency(ctx context.Context, req *protos.QueryTaskEfficiencyRequest) (*protos.QueryTaskEfficiencyReply, error) {
	log.Infof("CeffQueryService received QueryTaskEfficiency request from UID %d for tasks: %v", req.Uid, req.TaskIds)

	if len(req.TaskIds) == 0 {
		return &protos.QueryTaskEfficiencyReply{
			Ok:           false,
			ErrorMessage: "No task IDs provided",
		}, nil
	}

	efficiencyData, err := s.queryTaskEfficiencyData(ctx, req.TaskIds, req.Uid)
	if err != nil {
		log.Errorf("CeffQueryService failed to query efficiency data: %v", err)
		return &protos.QueryTaskEfficiencyReply{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("Failed to query efficiency data: %v", err),
		}, nil
	}

	log.Infof("CeffQueryService successfully returned %d efficiency records for user %d", len(efficiencyData), req.Uid)
	return &protos.QueryTaskEfficiencyReply{
		Ok:             true,
		EfficiencyData: efficiencyData,
	}, nil
}

func (s *CeffQueryService) queryTaskEfficiencyData(ctx context.Context, taskIds []uint32, userID uint32) ([]*protos.TaskEfficiencyInfo, error) {
	if s.influxConfig == nil {
		return nil, fmt.Errorf("InfluxDB configuration not initialized")
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

func (s *CeffQueryService) authorizeEfficiencyQuery(taskInfos []*protos.TaskInfo, taskIds []uint32, userID uint32) error {
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

func (s *CeffQueryService) getTaskInformation(ctx context.Context, taskIds []uint32) ([]*protos.TaskInfo, error) {
	if s.craneConfig == nil {
		return nil, fmt.Errorf("crane config not initialized")
	}

	ctldClient := util.GetStubToCtldByConfig(s.craneConfig)

	stepIds := make(map[uint32]*protos.JobStepIds)
	for _, id := range taskIds {
		stepIds[id] = &protos.JobStepIds{}
	}

	req := &protos.QueryTasksInfoRequest{
		FilterIds:                   stepIds,
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

func (s *CeffQueryService) extractNodeNames(taskInfos []*protos.TaskInfo) []string {
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

func (s *CeffQueryService) queryInfluxDbDataByTags(taskIds []uint32, hostNames []string) ([]*protos.TaskEfficiencyInfo, error) {
	client := influxdb2.NewClient(s.influxConfig.Url, s.influxConfig.Token)
	defer client.Close()

	ctx := context.Background()
	if pong, err := client.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping InfluxDB: %v", err)
	} else if !pong {
		return nil, fmt.Errorf("failed to ping InfluxDB: not pong")
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
		s.influxConfig.Bucket, s.influxConfig.Measurement,
		jobIDCondition, hostnameCondition)

	queryAPI := client.QueryAPI(s.influxConfig.Org)
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
