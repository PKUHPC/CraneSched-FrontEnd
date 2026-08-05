package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"CraneFrontEnd/generated/protos"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxhttp "github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/influxdata/influxdb-client-go/v2/domain"
)

const (
	maxRetries    = 3
	retryInterval = 5 * time.Second
)

type InfluxTraceStore struct {
	client       influxdb2.Client
	org          string
	traceBucket  string
	router       TraceBucketRouter
	closeMu      sync.Mutex
	activeWrites int
	writesDone   chan struct{}
	closing      bool
	closed       bool
	closeDone    chan struct{}
	closeClient  func()
}

func NewTraceStore(cfg *Config) (TraceSink, error) {
	switch cfg.DB.Type {
	case "influxdb":
		return NewInfluxTraceStore(cfg)
	default:
		return nil, fmt.Errorf("unsupported trace database type: %s", cfg.DB.Type)
	}
}

func NewInfluxTraceStore(cfg *Config) (*InfluxTraceStore, error) {
	var client influxdb2.Client
	var err error

	for i := 0; i < maxRetries; i++ {
		client = influxdb2.NewClient(cfg.DB.InfluxDB.URL, cfg.DB.InfluxDB.Token)
		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Duration(cfg.DB.InfluxDB.StartupTimeoutMs)*time.Millisecond,
		)
		_, err = client.Ping(ctx)
		cancel()

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

	store := &InfluxTraceStore{
		client:      client,
		org:         cfg.DB.InfluxDB.Org,
		traceBucket: cfg.DB.InfluxDB.TraceBucket,
		router:      NewTraceBucketRouter(cfg),
	}

	for _, bucket := range store.router.TraceBuckets() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Duration(cfg.DB.InfluxDB.StartupTimeoutMs)*time.Millisecond,
		)
		err := store.createBucketIfNotExists(ctx, bucket)
		cancel()
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to create trace bucket %s: %v", bucket, err)
		}
	}

	return store, nil
}

func (s *InfluxTraceStore) WriteBatch(
	ctx context.Context,
	points []encodedTracePoint,
) (traceSinkBatchResult, error) {
	if len(points) == 0 {
		return traceSinkBatchResult{}, nil
	}
	if err := ctx.Err(); err != nil {
		return traceSinkBatchResult{failed: append([]encodedTracePoint(nil), points...)}, err
	}
	if err := s.beginWrite(); err != nil {
		return traceSinkBatchResult{failed: append([]encodedTracePoint(nil), points...)},
			err
	}
	defer s.endWrite()

	byBucket := make(map[string][]encodedTracePoint)
	for _, point := range points {
		for _, bucket := range s.router.TraceBucketsForDecision(point.routing) {
			byBucket[bucket] = append(byBucket[bucket], point)
		}
	}
	buckets := make([]string, 0, len(byBucket))
	for bucket := range byBucket {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)

	var writeErrors []error
	failed := make([]encodedTracePoint, 0)
	for _, bucket := range buckets {
		bucketPoints := byBucket[bucket]
		influxPoints := make([]*write.Point, 0, len(bucketPoints))
		for _, point := range bucketPoints {
			influxPoints = append(influxPoints, influxPoint(point))
		}

		start := time.Now()
		writeAPI := s.client.WriteAPIBlocking(s.org, bucket)
		if err := writeAPI.WritePoint(ctx, influxPoints...); err != nil {
			log.Errorf("Failed to write %d spans to InfluxDB bucket=%s: %v", len(bucketPoints), bucket, err)
			writeErrors = append(writeErrors, fmt.Errorf("write spans to bucket %s: %w", bucket, err))
			for _, point := range bucketPoints {
				failed = append(failed, s.retryPointForBucket(point, bucket))
			}
			continue
		}

		elapsed := time.Since(start)
		log.Debugf("Saved %d trace spans to InfluxDB bucket=%s in %s", len(bucketPoints), bucket, elapsed)
		if elapsed > time.Second {
			log.Warnf("Slow trace span write: saved %d spans to InfluxDB bucket=%s in %s",
				len(bucketPoints), bucket, elapsed)
		}
	}
	return traceSinkBatchResult{failed: failed}, errors.Join(writeErrors...)
}

func (s *InfluxTraceStore) retryPointForBucket(
	point encodedTracePoint,
	bucket string,
) encodedTracePoint {
	destinations := make([]traceDestination, 0, len(point.routing.destinations))
	for _, destination := range point.routing.destinations {
		decision := traceRoutingDecision{
			destinations: []traceDestination{destination},
			shard:        point.routing.shard,
		}
		for _, candidate := range s.router.TraceBucketsForDecision(decision) {
			if candidate == bucket {
				destinations = append(destinations, destination)
				break
			}
		}
	}
	point.routing.destinations = destinations
	return point
}

func influxPointForSpan(span *protos.SpanInfo) *write.Point {
	point, err := influxPointForSpanWithEnvironment(span, "")
	if err != nil {
		panic(err)
	}
	return point
}

func influxPointForSpanWithEnvironment(
	span *protos.SpanInfo,
	flowEnvironmentID string,
) (*write.Point, error) {
	validator, err := newExecutionFlowValidator(flowEnvironmentID, generatedExecutionFlowCatalog)
	if err != nil {
		return nil, err
	}
	point, err := (protobufTracePointDecoder{}).Decode(rawTracePoint{span: span})
	if err != nil {
		return nil, err
	}
	validated, err := validator.Validate(point)
	if err != nil {
		return nil, err
	}
	routed := NewTracePointRouter().Route(validated)
	encoded, err := (&influxTracePointEncoder{}).Encode(routed)
	if err != nil {
		return nil, err
	}
	return influxPoint(encoded), nil
}

func influxPoint(point encodedTracePoint) *write.Point {
	return influxdb2.NewPoint("spans", point.tags, point.fields, point.time)
}

func (s *InfluxTraceStore) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closing = true
	writesDone := s.writesDone
	s.closeMu.Unlock()

	if writesDone != nil {
		select {
		case <-writesDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	if s.closeDone == nil {
		s.closeDone = make(chan struct{})
		closeDone := s.closeDone
		closeClient := s.closeClient
		client := s.client
		go func() {
			if closeClient != nil {
				closeClient()
			} else {
				client.Close()
			}
			s.closeMu.Lock()
			s.closed = true
			s.closing = false
			close(closeDone)
			s.closeMu.Unlock()
		}()
	}
	closeDone := s.closeDone
	s.closeMu.Unlock()

	select {
	case <-closeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *InfluxTraceStore) beginWrite() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return errors.New("InfluxDB trace sink is closed")
	}
	if s.closing {
		return errors.New("InfluxDB trace sink is closing")
	}
	if s.activeWrites == 0 {
		s.writesDone = make(chan struct{})
	}
	s.activeWrites++
	return nil
}

func (s *InfluxTraceStore) endWrite() {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.activeWrites--
	if s.activeWrites == 0 {
		close(s.writesDone)
		s.writesDone = nil
	}
}

func (s *InfluxTraceStore) createBucketIfNotExists(
	ctx context.Context,
	bucketName string,
) error {
	if err := s.createOrgIfNotExists(ctx); err != nil {
		return fmt.Errorf("failed to ensure organization exists: %w", err)
	}

	bucketsAPI := s.client.BucketsAPI()
	bucket, err := findBucketByName(ctx, s.client, s.org, bucketName)
	if err != nil {
		return fmt.Errorf("failed to find bucket %s: %w", bucketName, err)
	}

	if bucket != nil {
		log.Infof("Bucket already exists: %s", bucketName)
		return nil
	}

	log.Infof("Creating bucket: %s", bucketName)
	org, err := findOrganizationByName(ctx, s.client, s.org)
	if err != nil {
		return fmt.Errorf("failed to find organization: %w", err)
	}
	if org == nil {
		return fmt.Errorf("organization %s is missing after initialization", s.org)
	}

	_, err = bucketsAPI.CreateBucketWithName(ctx, org, bucketName)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	log.Infof("Successfully created bucket: %s", bucketName)
	return nil
}

func (s *InfluxTraceStore) createOrgIfNotExists(ctx context.Context) error {
	orgAPI := s.client.OrganizationsAPI()

	org, err := findOrganizationByName(ctx, s.client, s.org)
	if err != nil {
		return fmt.Errorf("failed to find organization %s: %w", s.org, err)
	}

	if org != nil {
		log.Infof("Organization already exists: %s", s.org)
		return nil
	}

	log.Infof("Creating organization: %s", s.org)
	_, err = orgAPI.CreateOrganizationWithName(ctx, s.org)
	if err != nil {
		return fmt.Errorf("failed to create organization: %w", err)
	}

	log.Infof("Successfully created organization: %s", s.org)
	return nil
}

func findOrganizationByName(
	ctx context.Context,
	client influxdb2.Client,
	name string,
) (*domain.Organization, error) {
	var response domain.Organizations
	if err := queryInfluxResource(ctx, client.HTTPService(), "orgs", url.Values{
		"org": {name}, "limit": {"1"},
	}, &response); err != nil {
		if isInfluxResourceNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if response.Orgs == nil || len(*response.Orgs) == 0 {
		return nil, nil
	}
	return &(*response.Orgs)[0], nil
}

func findBucketByName(
	ctx context.Context,
	client influxdb2.Client,
	organization string,
	name string,
) (*domain.Bucket, error) {
	var response domain.Buckets
	if err := queryInfluxResource(ctx, client.HTTPService(), "buckets", url.Values{
		"org": {organization}, "name": {name}, "limit": {"1"},
	}, &response); err != nil {
		if isInfluxResourceNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if response.Buckets == nil || len(*response.Buckets) == 0 {
		return nil, nil
	}
	return &(*response.Buckets)[0], nil
}

func isInfluxResourceNotFound(err error) bool {
	var httpErr *influxhttp.Error
	return errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound
}

func queryInfluxResource(
	ctx context.Context,
	service influxhttp.Service,
	resource string,
	query url.Values,
	destination any,
) error {
	endpoint, err := url.Parse(service.ServerAPIURL())
	if err != nil {
		return fmt.Errorf("parse InfluxDB API URL: %w", err)
	}
	endpoint, err = endpoint.Parse(resource)
	if err != nil {
		return fmt.Errorf("resolve InfluxDB resource URL: %w", err)
	}
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("create InfluxDB lookup request: %w", err)
	}
	if requestErr := service.DoHTTPRequest(request, nil, func(response *http.Response) (err error) {
		defer func() {
			_, drainErr := io.Copy(io.Discard, response.Body)
			err = errors.Join(err, drainErr, response.Body.Close())
		}()
		return json.NewDecoder(response.Body).Decode(destination)
	}); requestErr != nil {
		return requestErr
	}
	return nil
}
