package main

import "sort"

type TraceBucketRouter interface {
	TraceBucketsForDecision(traceRoutingDecision) []string
	TraceBuckets() []string
}

type traceBucketRouter struct {
	traceBucket       string
	traceCoreBucket   string
	traceDetailBucket string
	traceErrorBucket  string
	traceShardBuckets []string
}

func NewTraceBucketRouter(cfg *Config) TraceBucketRouter {
	return &traceBucketRouter{
		traceBucket:       cfg.DB.InfluxDB.TraceBucket,
		traceCoreBucket:   cfg.DB.InfluxDB.TraceCoreBucket,
		traceDetailBucket: cfg.DB.InfluxDB.TraceDetailBucket,
		traceErrorBucket:  cfg.DB.InfluxDB.TraceErrorBucket,
		traceShardBuckets: append([]string(nil), cfg.DB.InfluxDB.TraceShardBuckets...),
	}
}

func (r *traceBucketRouter) TraceBucketsForDecision(decision traceRoutingDecision) []string {
	seen := make(map[string]struct{}, len(decision.destinations))
	buckets := make([]string, 0, len(decision.destinations))
	for _, destination := range decision.destinations {
		bucket := r.bucketForDestination(destination, decision.shard)
		if bucket == "" {
			continue
		}
		if _, duplicate := seen[bucket]; duplicate {
			continue
		}
		seen[bucket] = struct{}{}
		buckets = append(buckets, bucket)
	}
	return buckets
}

func (r *traceBucketRouter) bucketForDestination(
	destination traceDestination,
	shard uint32,
) string {
	switch destination {
	case traceDestinationCore:
		if len(r.traceShardBuckets) > 0 {
			return r.traceShardBuckets[shard%uint32(len(r.traceShardBuckets))]
		}
		if r.traceCoreBucket != "" {
			return r.traceCoreBucket
		}
	case traceDestinationDetail:
		if r.traceDetailBucket != "" {
			return r.traceDetailBucket
		}
	case traceDestinationError:
		if r.traceErrorBucket != "" {
			return r.traceErrorBucket
		}
	}
	return r.traceBucket
}

func (r *traceBucketRouter) TraceBuckets() []string {
	seen := make(map[string]struct{})
	add := func(bucket string) {
		if bucket != "" {
			seen[bucket] = struct{}{}
		}
	}
	add(r.traceBucket)
	add(r.traceCoreBucket)
	add(r.traceDetailBucket)
	add(r.traceErrorBucket)
	for _, bucket := range r.traceShardBuckets {
		add(bucket)
	}
	buckets := make([]string, 0, len(seen))
	for bucket := range seen {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)
	return buckets
}
