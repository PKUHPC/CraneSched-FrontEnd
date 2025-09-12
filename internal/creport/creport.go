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

package creport

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"io"

	// "math"
	"os"
	// "os/user"
	"bytes"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/domain"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"

	//"github.com/xlab/treeprint"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

var (
	stub        protos.CraneCtldClient
	dbConfig    *util.InfluxDbConfig
	influxURL   = "http://localhost:8086"
	token       = "r5uVkisz9GkbavAiCn38BImwNgYWSgFhdNxHIiFjAqd-Cc_HUN1wa9fEXdqjya6qh7UfEA1LF9vuNxOAgEM7Jw=="
	org         = "pkuhpc"
	orgID       = "52ef37c445482e6f"
	bucketRaw   = "cranesched"
	bucketHour  = "cpu_hourly_mv"
	bucketDay   = "cpu_daily_mv"
	bucketMonth = "cpu_monthly_mv"
	parallelism = 8
)

type AggResult struct {
	UserName    string
	Account     string
	CpuUsageSum float64
	JobCount    uint64
}

type FinalAggResult struct {
	TotalCpuUsage float64
	TotalCpuTime  uint64
	TotalCpuAlloc uint64
}

type AccountUser struct {
	Account  string
	UserName string
}

func rangeHours(start, end time.Time) []time.Time {
	loc := start.Location()
	hourStart := time.Date(start.Year(), start.Month(), start.Day(), start.Hour(), 0, 0, 0, loc)
	var hours []time.Time
	for t := hourStart; !t.Add(time.Hour).After(end); t = t.Add(time.Hour) {
		hours = append(hours, t)
	}
	return hours
}

func rangeDays(start, end time.Time) []time.Time {
	loc := start.Location()
	dayStart := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, loc)
	var days []time.Time
	for t := dayStart; !t.AddDate(0, 0, 1).After(end); t = t.AddDate(0, 0, 1) {
		days = append(days, t)
	}
	return days
}

func rangeMonths(start, end time.Time) []time.Time {
	loc := start.Location()
	monthStart := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, loc)
	var months []time.Time
	for t := monthStart; !t.AddDate(0, 1, 0).After(end); t = t.AddDate(0, 1, 0) {
		months = append(months, t)
	}
	return months
}

func deleteBucketByName(client influxdb2.Client, orgID, name string) {
	bucketsAPI := client.BucketsAPI()
	buckets, err := bucketsAPI.GetBuckets(context.Background())
	if err != nil {
		fmt.Println("GetBuckets error:", err)
		return
	}
	for _, bucket := range *buckets {
		if bucket.Name == name && bucket.OrgID != nil && *bucket.OrgID == orgID {
			err := bucketsAPI.DeleteBucket(context.Background(), &bucket)
			if err != nil {
				fmt.Println("DeleteBucket error:", err)
			} else {
				fmt.Println("Deleted bucket:", bucket.Name)
			}
		}
	}
}

func deleteTaskByName(client influxdb2.Client, orgID, name string) {
	tasksAPI := client.TasksAPI()
	tasks, err := tasksAPI.FindTasks(context.Background(), nil)
	if err != nil {
		fmt.Println("FindTasks error:", err)
		return
	}
	for _, task := range tasks {
		if task.OrgID == orgID && task.Name == name {
			err := tasksAPI.DeleteTask(context.Background(), &task)
			if err != nil {
				fmt.Println("DeleteTask error:", err)
			} else {
				fmt.Println("Deleted task:", task.Name)
			}
		}
	}
}

func getOrgByName(orgName, token, influxURL string) (*domain.Organization, error) {
	req, _ := http.NewRequest("GET", influxURL+"/api/v2/orgs?org="+orgName, nil)
	req.Header.Set("Authorization", "Token "+token)
	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var result struct {
		Orgs []domain.Organization `json:"orgs"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if len(result.Orgs) == 0 {
		return nil, fmt.Errorf("org not found")
	}
	return &result.Orgs[0], nil
}

func createBucket(client influxdb2.Client, orgObj *domain.Organization, name string) {
	bucketsAPI := client.BucketsAPI()
	retentionType := domain.RetentionRuleTypeExpire
	retention := domain.RetentionRule{
		EverySeconds: 0,
		Type:         &retentionType,
	}
	_, err := bucketsAPI.CreateBucketWithName(context.Background(), orgObj, name, retention)
	if err != nil {
		fmt.Println("CreateBucket error:", err)
	} else {
		fmt.Println("Created bucket:", name)
	}
}

// Aggregate data by hour. Only output logs if data exists.
func aggregateHour(queryAPI api.QueryAPI, writeAPI api.WriteAPIBlocking, hours []time.Time) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, parallelism)
	for _, hourLocal := range hours {
		wg.Add(1)
		sem <- struct{}{}
		go func(hourLocal time.Time) {
			defer wg.Done()
			defer func() { <-sem }()

			// Use UTC for query window
			hourUTC := hourLocal.In(time.UTC)
			nextHourUTC := hourLocal.Add(time.Hour).In(time.UTC)

			flux := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "ResourceUsage" and r._field == "cpu_usage")
  |> group(columns: ["job_id", "user_name", "account", "hostname"])
  |> sort(columns: ["_time"], desc: true)
  |> unique(column: "hostname")
`, bucketRaw, hourUTC.Format(time.RFC3339), nextHourUTC.Format(time.RFC3339))

			result, err := queryAPI.Query(context.Background(), flux)
			if err != nil {
				fmt.Println("Query error:", err)
				return
			}

			type JobGroupKey struct {
				JobID, UserName, Account string
			}
			latestJobTime := make(map[JobGroupKey]time.Time)
			jobGroup := make(map[JobGroupKey]uint64)

			for result.Next() {
				record := result.Record()
				jobID := fmt.Sprintf("%v", record.ValueByKey("job_id"))
				userName := fmt.Sprintf("%v", record.ValueByKey("user_name"))
				account := fmt.Sprintf("%v", record.ValueByKey("account"))
				t := record.Time()
				cpuUsage, ok := record.Value().(uint64)
				if !ok {
					panic(fmt.Sprintf("cpu_usage type error: got %T, want uint64", record.Value()))
				}
				jgk := JobGroupKey{jobID, userName, account}
				if t.After(latestJobTime[jgk]) {
					latestJobTime[jgk] = t
				}
				jobGroup[jgk] += cpuUsage
			}

			type UserGroupKey struct {
				UserName, Account string
			}
			userGroup := make(map[UserGroupKey]struct {
				Usage    uint64
				jobCount uint64
			})
			for jgk, usage := range jobGroup {
				latest := latestJobTime[jgk]
				// Only keep jobs whose latest data time is within this hour
				if latest.Before(hourUTC) || !latest.Before(nextHourUTC) {
					continue
				}
				uk := UserGroupKey{jgk.UserName, jgk.Account}
				u := userGroup[uk]
				u.Usage += usage
				u.jobCount++
				userGroup[uk] = u
			}

			wrote := false
			for uk, u := range userGroup {
				point := influxdb2.NewPoint(
					"cpu_hourly_mv",
					map[string]string{
						"user_name": uk.UserName,
						"account":   uk.Account,
					},
					map[string]interface{}{
						"cpu_usage_sum": u.Usage,
						"job_count":     u.jobCount,
					},
					hourLocal,
				)
				err := writeAPI.WritePoint(context.Background(), point)
				if err != nil {
					fmt.Println("Write error:", err)
				}
				fmt.Printf("Writing to cpu_hourly_mv: user_name=%v, account=%v, sum=%v, job_count=%v, hour(local)=%v, hour(UTC)=%v\n",
					uk.UserName, uk.Account, u.Usage, u.jobCount, hourLocal, hourUTC)
				wrote = true
			}
			if wrote {
				fmt.Println("Aggregated hour:", hourLocal)
			}
		}(hourLocal)
	}
	wg.Wait()
}

// Aggregate data by day. Only output logs if data exists.
func aggregateDay(queryAPI api.QueryAPI, writeAPI api.WriteAPIBlocking, days []time.Time) {
	for _, dayLocal := range days {
		dayUTC := dayLocal.In(time.UTC)
		nextDayUTC := dayLocal.AddDate(0, 0, 1).In(time.UTC)
		flux := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._field == "cpu_usage_sum" or r._field == "job_count")
  |> group(columns: ["user_name", "account", "_field"])
  |> sum(column: "_value")
`, bucketHour, dayUTC.Format(time.RFC3339), nextDayUTC.Format(time.RFC3339))

		result, err := queryAPI.Query(context.Background(), flux)
		if err != nil {
			fmt.Println("Query error:", err)
			continue
		}

		type Key struct {
			UserName, Account string
		}
		sumMap := make(map[Key]uint64)
		countMap := make(map[Key]uint64)

		for result.Next() {
			userName := fmt.Sprintf("%v", result.Record().ValueByKey("user_name"))
			account := fmt.Sprintf("%v", result.Record().ValueByKey("account"))
			field := result.Record().Field()
			val := result.Record().Value()
			key := Key{userName, account}
			switch field {
			case "cpu_usage_sum":
				v, ok := val.(uint64)
				if !ok {
					panic(fmt.Sprintf("cpu_usage_sum type error: got %T, want uint64", val))
				}
				sumMap[key] = v
			case "job_count":
				v, ok := val.(uint64)
				if !ok {
					panic(fmt.Sprintf("job_count type error: got %T, want uint64", val))
				}
				countMap[key] = v
			}
		}

		wrote := false
		for key, sum := range sumMap {
			job_count := countMap[key]
			point := influxdb2.NewPoint(
				"cpu_daily_mv",
				map[string]string{
					"user_name": key.UserName,
					"account":   key.Account,
				},
				map[string]interface{}{
					"cpu_usage_sum": sum,       // uint64
					"job_count":     job_count, // uint64
				},
				dayLocal,
			)
			err := writeAPI.WritePoint(context.Background(), point)
			if err != nil {
				fmt.Println("Write error:", err)
			}
			fmt.Printf("Writing to cpu_daily_mv: user_name=%v, account=%v, sum=%v, job_count=%v, day(local)=%v, day(UTC)=%v\n",
				key.UserName, key.Account, sum, job_count, dayLocal, dayUTC)
			wrote = true
		}
		if wrote {
			fmt.Println("Aggregated day:", dayLocal)
		}
	}
}

// Aggregate data by month. Only output logs if data exists.
func aggregateMonth(queryAPI api.QueryAPI, writeAPI api.WriteAPIBlocking, months []time.Time) {
	for _, monthLocal := range months {
		monthUTC := monthLocal.In(time.UTC)
		nextMonthUTC := monthLocal.AddDate(0, 1, 0).In(time.UTC)
		flux := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._field == "cpu_usage_sum" or r._field == "job_count")
  |> group(columns: ["user_name", "account", "_field"])
  |> sum(column: "_value")
`, bucketDay, monthUTC.Format(time.RFC3339), nextMonthUTC.Format(time.RFC3339))

		result, err := queryAPI.Query(context.Background(), flux)
		if err != nil {
			fmt.Println("Query error:", err)
			continue
		}

		type Key struct {
			UserName, Account string
		}
		sumMap := make(map[Key]uint64)
		countMap := make(map[Key]uint64)

		for result.Next() {
			userName := fmt.Sprintf("%v", result.Record().ValueByKey("user_name"))
			account := fmt.Sprintf("%v", result.Record().ValueByKey("account"))
			field := result.Record().Field()
			val := result.Record().Value()
			key := Key{userName, account}
			switch field {
			case "cpu_usage_sum":
				v, ok := val.(uint64)
				if !ok {
					panic(fmt.Sprintf("cpu_usage_sum type error: got %T, want uint64", val))
				}
				sumMap[key] = v
			case "job_count":
				v, ok := val.(uint64)
				if !ok {
					panic(fmt.Sprintf("job_count type error: got %T, want uint64", val))
				}
				countMap[key] = v
			}
		}

		wrote := false
		for key, sum := range sumMap {
			job_count := countMap[key]
			point := influxdb2.NewPoint(
				"cpu_monthly_mv",
				map[string]string{
					"user_name": key.UserName,
					"account":   key.Account,
				},
				map[string]interface{}{
					"cpu_usage_sum": sum,       // uint64
					"job_count":     job_count, // uint64
				},
				monthLocal,
			)
			err := writeAPI.WritePoint(context.Background(), point)
			if err != nil {
				fmt.Println("Write error:", err)
			}
			fmt.Printf("Writing to cpu_monthly_mv: user_name=%v, account=%v, sum=%v, job_count=%v, month(local)=%v, month(UTC)=%v\n",
				key.UserName, key.Account, sum, job_count, monthLocal, monthUTC)
			wrote = true
		}
		if wrote {
			fmt.Println("Aggregated month:", monthLocal)
		}
	}
}

func createTask(name, fluxScript string) {
	url := influxURL + "/api/v2/tasks"
	data := map[string]interface{}{
		"orgID": orgID,
		"name":  name,
		"flux":  fluxScript,
	}
	body, _ := json.Marshal(data)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header.Set("Authorization", "Token "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("CreateTask error:", err)
		return
	}
	defer resp.Body.Close()
	fmt.Println("Task created:", name, resp.Status)
}

func Init() {
	client := influxdb2.NewClient(influxURL, token)

	// Delete aggregation buckets and tasks
	deleteAggregationBucketsAndTasks(client, orgID)

	// Create buckets
	orgObj, err := getOrgByName(org, token, influxURL)
	if err != nil {
		panic(err)
	}
	createAggregationBuckets(client, orgObj)

	// Set time zone and time range
	loc, _ := time.LoadLocation("Asia/Shanghai")
	startHour := time.Date(2025, 9, 8, 8, 0, 0, 0, loc)
	now := time.Now().In(loc)
	//now := time.Date(2025, 9, 9, 12, 0, 0, 0, loc)
	endHour := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, loc)

	// Aggregate hourly
	hours := rangeHours(startHour, endHour)
	aggregateHour(client.QueryAPI(org), client.WriteAPIBlocking(org, bucketHour), hours)

	// Aggregate daily (after hourly is done)
	days := rangeDays(startHour, endHour)
	aggregateDay(client.QueryAPI(org), client.WriteAPIBlocking(org, bucketDay), days)

	// Aggregate monthly (after daily is done)
	months := rangeMonths(startHour, endHour)
	aggregateMonth(client.QueryAPI(org), client.WriteAPIBlocking(org, bucketMonth), months)

	// Create scheduled aggregation tasks
	createAggregationTasks(
		"cranesched",
		"cpu_hourly_mv",
		"cpu_daily_mv",
		"cpu_monthly_mv",
		"pkuhpc",
	)

	fmt.Println("All done.")
	client.Close()
}

// Delete aggregation buckets and tasks
func deleteAggregationBucketsAndTasks(client influxdb2.Client, orgID string) {
	deleteBucketByName(client, orgID, bucketHour)
	deleteBucketByName(client, orgID, bucketDay)
	deleteBucketByName(client, orgID, bucketMonth)
	deleteTaskByName(client, orgID, "cpu_hourly_mv")
	deleteTaskByName(client, orgID, "cpu_daily_mv")
	deleteTaskByName(client, orgID, "cpu_monthly_mv")
}

// Create aggregation buckets
func createAggregationBuckets(client influxdb2.Client, orgObj *domain.Organization) {
	createBucket(client, orgObj, bucketHour)
	createBucket(client, orgObj, bucketDay)
	createBucket(client, orgObj, bucketMonth)
}

// Create scheduled aggregation tasks
func createAggregationTasks(
	rawBucket string,
	hourBucket string,
	dayBucket string,
	monthBucket string,
	org string,
) {
	// Hourly task (every hour, no delay)
	// Create scheduled aggregation tasks
	hourTaskFlux := fmt.Sprintf(`
import "date"
import "experimental"

option task = {name: "%s", every: 1h, offset: 0m}

now_time = now()
stop = date.truncate(t: now_time, unit: 1h)
start = experimental.subDuration(d: 1h, from: stop)

base = from(bucket: "%s")
  |> range(start: start, stop: stop)
  |> filter(fn: (r) => r._measurement == "ResourceUsage" and r._field == "cpu_usage")
  |> group(columns: ["job_id", "user_name", "account", "hostname"])
  |> sort(columns: ["_time"], desc: true)
  |> unique(column: "hostname")
  |> group(columns: ["job_id", "user_name", "account"])
  |> sum(column: "_value")
  |> group(columns: ["user_name", "account"])

base
  |> sum(column: "_value")
  |> map(fn: (r) => ({
      _time: start,
      user_name: r.user_name,
      account: r.account,
      cpu_usage_sum: uint(v: r._value),
      _value: uint(v: r._value),
      _field: "cpu_usage_sum",
      _measurement: "%s"
  }))
  |> to(bucket: "%s", org: "%s")

base
  |> count(column: "_value")
  |> map(fn: (r) => ({
      _time: start,
      user_name: r.user_name,
      account: r.account,
      job_count: uint(v: r._value),
      _value: uint(v: r._value),
      _field: "job_count",
      _measurement: "%s"
  }))
  |> to(bucket: "%s", org: "%s")
`, hourBucket, rawBucket, hourBucket, hourBucket, org, hourBucket, hourBucket, org)

	createTask(hourBucket, hourTaskFlux)

	// Daily task (every day, delayed 1 hour, UTC 17:00 = Beijing 01:00)
	dayTaskFlux := fmt.Sprintf(`
import "date"
import "experimental"

option task = {name: "%s", cron: "0 17 * * *"}

now_time = now()
stop_utc = date.truncate(t: now_time, unit: 1d)
stop = experimental.addDuration(d: 16h, to: stop_utc)
start = experimental.subDuration(d: 1d, from: stop)
stat_time = start                    

// aggregate cpu_usage_sum for each user_name/account
from(bucket: "%s")
  |> range(start: start, stop: stop)
  |> filter(fn: (r) => r._field == "cpu_usage_sum")
  |> group(columns: ["user_name", "account"])
  |> sum(column: "_value")
  |> map(fn: (r) => ({
      _time: stat_time,
      user_name: r.user_name,
      account: r.account,
      cpu_usage_sum: uint(v: r._value),
      _value: uint(v: r._value),
      _field: "cpu_usage_sum",
      _measurement: "%s"
  }))
  |> to(bucket: "%s", org: "%s")

// aggregate job_count for each user_name/account
from(bucket: "%s")
  |> range(start: start, stop: stop)
  |> filter(fn: (r) => r._field == "job_count")
  |> group(columns: ["user_name", "account"])
  |> sum(column: "_value")
  |> map(fn: (r) => ({
      _time: stat_time,
      user_name: r.user_name,
      account: r.account,
      job_count: uint(v: r._value),
      _value: uint(v: r._value),
      _field: "job_count",
      _measurement: "%s"
  }))
  |> to(bucket: "%s", org: "%s")
`, dayBucket, hourBucket, dayBucket, dayBucket, org, hourBucket, dayBucket, dayBucket, org)
	createTask(dayBucket, dayTaskFlux)
	// Monthly task (every month, delayed, UTC 18:00 on the 1st = Beijing 02:00)
	monthTaskFlux := fmt.Sprintf(`
import "date"
import "experimental"

option task = {name: "%s", cron: "0 0 1 * *"}

now_time = 2025-09-30T16:00:00Z
stop_utc = date.truncate(t: now_time, unit: 1mo)
stop = experimental.addDuration(d: 16h, to: stop_utc)
start = experimental.subDuration(d: 1mo, from: stop)
stat_time = start

// aggregate cpu_usage_sum for each user_name/account
from(bucket: "%s")
  |> range(start: start, stop: stop)
  |> filter(fn: (r) => r._field == "cpu_usage_sum")
  |> group(columns: ["user_name", "account"])
  |> sum(column: "_value")
  |> map(fn: (r) => ({
      _time: stat_time,
      user_name: r.user_name,
      account: r.account,
      cpu_usage_sum: uint(v: r._value),
      _value: uint(v: r._value),
      _field: "cpu_usage_sum",
      _measurement: "%s"
  }))
  |> to(bucket: "%s", org: "%s")

// aggregate job_count for each user_name/account
from(bucket: "%s")
  |> range(start: start, stop: stop)
  |> filter(fn: (r) => r._field == "job_count")
  |> group(columns: ["user_name", "account"])
  |> sum(column: "_value")
  |> map(fn: (r) => ({
      _time: stat_time,
      user_name: r.user_name,
      account: r.account,
      job_count: uint(v: r._value),
      _value: uint(v: r._value),
      _field: "job_count",
      _measurement: "%s"
  }))
  |> to(bucket: "%s", org: "%s")
`, monthBucket, dayBucket, monthBucket, monthBucket, org, dayBucket, monthBucket, monthBucket, org)
	createTask(monthBucket, monthTaskFlux)
}

func QueryAccountUserSummaryItemInfluxdb() error {
	// loc, _ := time.LoadLocation("Local") // or use time.UTC
	// start := time.Date(2025, 3, 3, 9, 0, 0, 0, loc)
	// end := time.Date(2025, 7, 10, 10, 0, 0, 0, loc)
	var start_time, end_time time.Time
	var err error
	start_time, err = util.ParseTime(FlagFilterStartTime)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Failed to parse the StartTime filter: %s.", err),
		}
	}
	end_time, err = util.ParseTime(FlagFilterEndTime)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Failed to parse the EndTime filter: %s.", err),
		}
	}
	fmt.Println("start =", start_time)
	fmt.Println("end   =", end_time)
	aggResult, err := AggregateByTimeRange(dbConfig, start_time, end_time)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Failed to query influxdb data: %s.", err),
		}
	}
	for _, v := range aggResult {
		fmt.Printf("[AGG] account=%s user_name=%s cpu_usage_sum=%f job_count=%d\n",
			v.Account, v.UserName, v.CpuUsageSum, v.JobCount)
	}

	return nil

}

func QueryAccountUserSummaryItem() error {
	req := &protos.QueryAccountUserSummaryItemRequest{
		Account:  FlagFilterAccounts,
		Username: FlagFilterUsers,
	}
fmt.Printf("Enter")
	var start_time, end_time time.Time
	var err error
	start_time, err = util.ParseTime(FlagFilterStartTime)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Failed to parse the StartTime filter: %s.", err),
		}
	}
	end_time, err = util.ParseTime(FlagFilterEndTime)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Failed to parse the EndTime filter: %s.", err),
		}
	}
	if !util.CheckCreportOutType(FlagOutType) {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Invalid argument: invalid: --time/-t"),
		}
	} 
	req.StartTime = timestamppb.New(start_time)
	req.EndTime = timestamppb.New(end_time)

	reply, err := stub.QueryAccountUserSummaryItem(context.Background(), req)
	if err != nil {
		util.GrpcErrorPrintf(err, "Failed to query job info")
		return &util.CraneError{Code: util.ErrorNetwork}
	}

	for _, item := range reply.Items {
		fmt.Printf("Account: %s, Username: %s, CPU Time: %d, CPU Alloc: %d, job_count: %d\n",
			item.Account, item.Username, item.TotalCpuTime, item.TotalCpuAlloc, item.TotalCount)
	}

	aggResult, err := AggregateByTimeRange(dbConfig, start_time, end_time)
	if err != nil {
		return &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Failed to query influxdb data: %s.", err),
		}
	}

	finalAggResult := MergeAccountUserAggregations(reply, aggResult)

	PrintAccountUserList(finalAggResult, reply.GetCluster(), start_time, end_time, FlagOutType)

	return nil
}

func PrintAccountUserList(accountUserMap map[AccountUser]FinalAggResult, cluster string, startTime, endTime time.Time, outType string) {
	if len(accountUserMap) == 0 {
		return
	}

	totalSecs := int64(endTime.Sub(startTime).Seconds())
	keyList := make([]AccountUser, 0, len(accountUserMap))
	for accountUser := range accountUserMap {
		keyList = append(keyList, accountUser)
	}
	sort.Slice(keyList, func(i, j int) bool {
		if keyList[i].Account < keyList[j].Account {
			return true
		}
		if keyList[i].Account > keyList[j].Account {
			return false
		}
		return keyList[i].UserName < keyList[j].UserName
	})

	borderLen := 100
	fmt.Println(strings.Repeat("-", borderLen))
	if (totalSecs > 0) {
	fmt.Printf("Cluster/Account/User Utilization %s - %s (%d secs)\n",
		startTime.Format("2006-01-02T15:04:05"), endTime.Format("2006-01-02T15:04:05"), totalSecs)
	}
	util.ReportUsageType(outType)

	var divisor uint64 = 1
	if outType == "Seconds" {
		divisor = 1
	} else if outType == "Minutes" {
		divisor = 60
	} else if outType == "Hours" {
		divisor = 3600
	}

	header := []string{"CLUSTER", "ACCOUNT", "USER", "USED", "ENERGY"}
	table := tablewriter.NewWriter(os.Stdout)
	util.SetBorderTable(table)
	table.SetHeader(header)
	tableData := make([][]string, len(keyList))
	for _, accountUser := range keyList {
		tableData = append(tableData, []string{
			cluster,
			accountUser.Account,
			accountUser.UserName,
			strconv.FormatUint(accountUserMap[accountUser].TotalCpuTime / divisor, 10),
			"0",
		})
	}
	table.AppendBulk(tableData)
	table.Render()
}


// Extracts the InfluxDB configuration from the specified YAML configuration files
func GetInfluxDbConfig(config *util.Config) (*util.InfluxDbConfig, error) {
	if !config.Plugin.Enabled {
		return nil, &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "Plugin is not enabled",
		}
	}

	var monitorConfigPath string
	for _, plugin := range config.Plugin.Plugins {
		if plugin.Name == "monitor" {
			monitorConfigPath = plugin.Config
			break
		}
	}

	if monitorConfigPath == "" {
		return nil, &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "Monitor plugin not found",
		}
	}

	confFile, err := os.ReadFile(monitorConfigPath)
	if err != nil {
		return nil, &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Failed to read config file %s: %s.", monitorConfigPath, err),
		}
	}

	dbConf := &struct {
		Database *util.InfluxDbConfig `yaml:"Database"`
	}{}
	if err := yaml.Unmarshal(confFile, dbConf); err != nil {
		return nil, &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: fmt.Sprintf("Failed to parse YAML config file: %s", err),
		}
	}
	if dbConf.Database == nil {
		return nil, &util.CraneError{
			Code:    util.ErrorCmdArg,
			Message: "Database section not found in YAML",
		}
	}

	return dbConf.Database, nil
}

func QueryInfluxAggByTags(
	cfg *util.InfluxDbConfig,
	bucket, measurement string,
	start, end time.Time,
) ([]AggResult, error) {
	client := influxdb2.NewClient(cfg.Url, cfg.Token)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if pong, err := client.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping InfluxDB: %v", err)
	} else if !pong {
		return nil, fmt.Errorf("failed to ping InfluxDB: not pong")
	}

	startUTC := start.In(time.UTC)
	endUTC := end.In(time.UTC)

	fluxQuery := fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: %s, stop: %s)
  |> filter(fn: (r) => r._measurement == "%s")
  |> filter(fn: (r) => r._field == "cpu_usage_sum" or r._field == "job_count")
  |> group(columns: ["account", "user_name", "_field"])
  |> sum()
  |> pivot(rowKey: ["account", "user_name"], columnKey: ["_field"], valueColumn: "_value")
`, bucket, startUTC.Format(time.RFC3339), endUTC.Format(time.RFC3339), measurement)

	//log.Printf("[DEBUG] Flux query:\n%s", fluxQuery)

	queryAPI := client.QueryAPI(cfg.Org)
	result, err := queryAPI.Query(ctx, fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("execute query failed: %w", err)
	}

	var aggResults []AggResult
	for result.Next() {
		record := result.Record()
		account, ok := record.ValueByKey("account").(string)
		if !ok {
			log.Printf("account tag missing or not string")
			continue
		}
		userName, ok := record.ValueByKey("user_name").(string)
		if !ok {
			log.Printf("user_name tag missing or not string")
			continue
		}
		var cpuUsageSum uint64
		var jobCount uint64

		if v, ok := record.ValueByKey("cpu_usage_sum").(uint64); ok {
			cpuUsageSum = v
		}

		if v, ok := record.ValueByKey("job_count").(uint64); ok {
			jobCount = v
		}
		if cpuUsageSum == 0 && jobCount == 0 {
			continue
		}
		// log.Printf("[DEBUG] QueryInfluxAggByTags: account=%s user_name=%s cpu_usage_sum=%d job_count=%d",
		// 	account, userName, cpuUsageSum, jobCount)
		cpuUsageSumSec := float64(cpuUsageSum) / 10e7
		aggResults = append(aggResults, AggResult{
			Account:     account,
			UserName:    userName,
			CpuUsageSum: cpuUsageSumSec,
			JobCount:    jobCount,
		})
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("query parsing error: %w", result.Err())
	}

	return aggResults, nil
}

func AggregateByTimeRange(cfg *util.InfluxDbConfig, start, end time.Time) (map[AccountUser]AggResult, error) {
	timeRanges := util.EfficientSplitTimeRange(start, end)
	aggMap := make(map[AccountUser]AggResult)

	for _, tr := range timeRanges {
		var bucket string
		switch tr.Type {
		case "month":
			bucket = "cpu_monthly_mv"
		case "day":
			bucket = "cpu_daily_mv"
		case "hour":
			bucket = "cpu_hourly_mv"
		}
		results, err := QueryInfluxAggByTags(cfg, bucket, bucket, tr.Start, tr.End)
		if err != nil {
			log.Printf("[DEBUG] QueryInfluxAggByTags: err: %v",
				err)
			return nil, err
		}
		for _, r := range results {
			key := AccountUser{Account: r.Account, UserName: r.UserName}
			agg := aggMap[key]
			agg.UserName = r.UserName
			agg.Account = r.Account
			agg.CpuUsageSum += r.CpuUsageSum
			agg.JobCount += r.JobCount

			aggMap[key] = agg
		}
	}

	for accountUser, aggResult := range aggMap {
		fmt.Printf(
			"Influxdb Account: %s, UserName: %s, CpuUsageSum: %f, JobCount: %d\n",
			accountUser.Account, accountUser.UserName,
			aggResult.CpuUsageSum, aggResult.JobCount,
		)
	}

	return aggMap, nil

}

func MergeAccountUserAggregations(
	mongodbSummary *protos.QueryAccountUserSummaryItemReply,
	influxdbAggMap map[AccountUser]AggResult,
) map[AccountUser]FinalAggResult {
	mergedResult := make(map[AccountUser]FinalAggResult)

	for _, summaryItem := range mongodbSummary.Items {
		userKey := AccountUser{
			Account:  summaryItem.Account,
			UserName: summaryItem.Username,
		}

		influxAgg, found := influxdbAggMap[userKey]
		mongoJobCount := uint64(summaryItem.TotalCount)
		influxJobCount := influxAgg.JobCount

		if !found {
			log.Printf("[WARN] No InfluxDB data for Account=%s UserName=%s", userKey.Account, userKey.UserName)
		} else if mongoJobCount != influxJobCount {
			log.Printf("[WARN] JobCount mismatch for Account=%s UserName=%s: MongoDB=%d, InfluxDB=%d",
				userKey.Account, userKey.UserName, mongoJobCount, influxJobCount)
		}

		// Fill merged result from MongoDB
		mergedResult[userKey] = FinalAggResult{
			TotalCpuTime:  uint64(summaryItem.TotalCpuTime),
			TotalCpuAlloc: uint64(summaryItem.TotalCpuAlloc),
		}
	}

	// Merge InfluxDB aggregation data
	for userKey, aggResult := range influxdbAggMap {
		result := mergedResult[userKey]
		result.TotalCpuUsage = aggResult.CpuUsageSum
		mergedResult[userKey] = result
	}
	fmt.Printf("--------final-data-------\n")
	for accountUser, agg := range mergedResult {
		fmt.Printf(
			"Account: %s, UserName: %s, TotalCpuUsage: %f, TotalCpuTime: %d, TotalCpuAlloc: %d\n",
			accountUser.Account, accountUser.UserName,
			agg.TotalCpuUsage, agg.TotalCpuTime, agg.TotalCpuAlloc,
		)
	}

	return mergedResult
}
