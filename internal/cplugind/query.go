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

package cplugind

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

// TaskQueryService handles task efficiency data queries
type TaskQueryService struct {
	influxConfig *util.InfluxDbConfig
	craneConfig  *util.Config
}

var gTaskQueryService *TaskQueryService

// InitTaskQueryService initializes the task query service
func InitTaskQueryService() error {
	service := &TaskQueryService{}
	
	// Load InfluxDB configuration from monitor plugin config
	influxConfig, err := service.loadInfluxConfig()
	if err != nil {
		return fmt.Errorf("failed to load InfluxDB config: %w", err)
	}
	service.influxConfig = influxConfig
	
	// Load crane configuration
	craneConfig, err := service.loadCraneConfig()
	if err != nil {
		return fmt.Errorf("failed to load crane config: %w", err)
	}
	service.craneConfig = craneConfig
	
	gTaskQueryService = service
	
	log.Info("Task query service initialized successfully")
	return nil
}

// loadInfluxConfig loads InfluxDB configuration from monitor plugin config
func (s *TaskQueryService) loadInfluxConfig() (*util.InfluxDbConfig, error) {
	// Find monitor plugin config path
	var monitorConfigPath string
	for _, plugin := range gPluginConfig.Plugins {
		if plugin.Name == "monitor" {
			monitorConfigPath = plugin.Config
			break
		}
	}

	if monitorConfigPath == "" {
		return nil, fmt.Errorf("monitor plugin not found in configuration")
	}

	// Read monitor config file
	confFile, err := os.ReadFile(monitorConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read monitor config file %s: %w", monitorConfigPath, err)
	}

	// Parse YAML config
	dbConf := &struct {
		Database *util.InfluxDbConfig `yaml:"Database"`
	}{}
	
	if err := yaml.Unmarshal(confFile, dbConf); err != nil {
		return nil, fmt.Errorf("failed to parse monitor config YAML: %w", err)
	}
	
	if dbConf.Database == nil {
		return nil, fmt.Errorf("Database section not found in monitor config")
	}

	return dbConf.Database, nil
}

// loadCraneConfig loads Crane configuration
func (s *TaskQueryService) loadCraneConfig() (*util.Config, error) {
	// Use the same config path that cplugind is using
	configPath := util.DefaultConfigPath
	config := util.ParseConfig(configPath)
	if config == nil {
		return nil, fmt.Errorf("failed to parse crane config from %s", configPath)
	}
	
	return config, nil
}

// QueryTaskEfficiencyData queries task efficiency data with authorization
func (s *TaskQueryService) QueryTaskEfficiencyData(taskIds []uint32, userID uint32) ([]*protos.TaskEfficiencyInfo, error) {
	if s.influxConfig == nil {
		return nil, fmt.Errorf("InfluxDB configuration not initialized")
	}

	// Authorize the request
	if err := s.authorizeEfficiencyQuery(taskIds, userID); err != nil {
		return nil, fmt.Errorf("authorization failed: %w", err)
	}

	// Get task information to extract node names
	taskInfos, err := s.getTaskInformation(taskIds)
	if err != nil {
		return nil, fmt.Errorf("failed to get task information: %w", err)
	}

	if len(taskInfos) == 0 {
		return nil, fmt.Errorf("no tasks found for the specified task IDs")
	}

	nodeNames := s.extractNodeNames(taskInfos)
	if len(nodeNames) == 0 {
		return nil, fmt.Errorf("no nodes found for the specified tasks")
	}

	// Query InfluxDB for efficiency data
	efficiencyData, err := s.queryInfluxDbDataByTags(taskIds, nodeNames)
	if err != nil {
		return nil, fmt.Errorf("failed to query InfluxDB: %w", err)
	}

	return efficiencyData, nil
}

// authorizeEfficiencyQuery checks if user is authorized to access the requested tasks
func (s *TaskQueryService) authorizeEfficiencyQuery(taskIds []uint32, userID uint32) error {
	// Get task information to check ownership
	taskInfos, err := s.getTaskInformation(taskIds)
	if err != nil {
		return fmt.Errorf("failed to get task information for authorization: %w", err)
	}

	// Check if user owns all requested tasks or is admin
	for _, taskInfo := range taskInfos {
		if taskInfo.Uid != userID {
			// TODO: Add admin privilege check here
			// For now, only allow users to access their own tasks
			return fmt.Errorf("user %d not authorized to access task %d (owned by user %d)", 
				userID, taskInfo.TaskId, taskInfo.Uid)
		}
	}

	log.Infof("User %d authorized to access tasks: %v", userID, taskIds)
	return nil
}

// getTaskInformation retrieves task information from CraneCtld
func (s *TaskQueryService) getTaskInformation(taskIds []uint32) ([]*protos.TaskInfo, error) {
	if s.craneConfig == nil {
		return nil, fmt.Errorf("crane config not initialized")
	}

	// Get CraneCtld client
	ctldClient := util.GetStubToCtldByConfig(s.craneConfig)
	
	req := &protos.QueryTasksInfoRequest{
		FilterTaskIds:               taskIds,
		OptionIncludeCompletedTasks: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reply, err := ctldClient.QueryTasksInfo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to query tasks info from CraneCtld: %w", err)
	}

	return reply.TaskInfoList, nil
}

// extractNodeNames extracts node names from task information
func (s *TaskQueryService) extractNodeNames(taskInfos []*protos.TaskInfo) []string {
	var nodeNames []string
	for _, taskInfo := range taskInfos {
		nodes, ok := util.ParseHostList(taskInfo.GetCranedList())
		if !ok {
			log.Warnf("Failed to parse host list: %s", taskInfo.GetCranedList())
			continue
		}
		nodeNames = append(nodeNames, nodes...)
	}
	return nodeNames
}

// queryInfluxDbDataByTags queries InfluxDB for efficiency data
func (s *TaskQueryService) queryInfluxDbDataByTags(taskIds []uint32, hostNames []string) ([]*protos.TaskEfficiencyInfo, error) {
	client := influxdb2.NewClient(s.influxConfig.Url, s.influxConfig.Token)
	defer client.Close()

	ctx := context.Background()
	if pong, err := client.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping InfluxDB: %v", err)
	} else if !pong {
		return nil, fmt.Errorf("failed to ping InfluxDB: not pong")
	}

	// Build job ID filter conditions
	jobIDFilters := make([]string, len(taskIds))
	for i, id := range taskIds {
		jobIDFilters[i] = fmt.Sprintf(`r["job_id"] == "%d"`, id)
	}
	jobIDCondition := strings.Join(jobIDFilters, " or ")

	// Build hostname filter conditions
	hostnameFilters := make([]string, len(hostNames))
	for i, hostname := range hostNames {
		hostnameFilters[i] = fmt.Sprintf(`r["hostname"] == "%s"`, hostname)
	}
	hostnameCondition := strings.Join(hostnameFilters, " or ")

	// Construct the Flux query
	fluxQuery := fmt.Sprintf(`
	from(bucket: "%s")
	|> range(start: 0)
	|> filter(fn: (r) => 
	    r["_measurement"] == "%s" and 
		(r["_field"] == "cpu_usage" or r["_field"] == "memory_usage") and
		(%s) and (%s))
	|> group(columns: ["job_id", "hostname", "_field"])
	|> max(column: "_value")`, 
		s.influxConfig.Bucket, s.influxConfig.Measurement, 
		jobIDCondition, hostnameCondition)

	// Execute the query
	queryAPI := client.QueryAPI(s.influxConfig.Org)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("execute query failed: %w", err)
	}

	// Parse and aggregate the query results
	dataMap := make(map[string]*protos.TaskEfficiencyInfo)
	for result.Next() {
		record := result.Record()

		// Extract job_id and hostname
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

		// Construct a unique key for aggregation
		key := fmt.Sprintf("%d:%s", jobID, hostname)
		if _, exists := dataMap[key]; !exists {
			dataMap[key] = &protos.TaskEfficiencyInfo{
				TaskId:   uint32(jobID),
				Hostname: hostname,
				Timestamp: timestamppb.New(record.Time()),
			}
		}

		// Extract _field and _value
		field := record.ValueByKey("_field").(string)
		value := record.Value().(uint64)

		// Update the corresponding field in the record
		if field == "cpu_usage" {
			dataMap[key].CpuUsage = value
		} else if field == "memory_usage" {
			dataMap[key].MemoryUsage = value
		}
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query parsing error: %w", result.Err())
	}

	// Convert the aggregated data into a slice
	var efficiencyData []*protos.TaskEfficiencyInfo
	for _, data := range dataMap {
		efficiencyData = append(efficiencyData, data)
	}

	if len(efficiencyData) == 0 {
		return nil, fmt.Errorf("no matching efficiency data available")
	}

	log.Infof("Successfully queried %d efficiency records for tasks %v", len(efficiencyData), taskIds)
	return efficiencyData, nil
}