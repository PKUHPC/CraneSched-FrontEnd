package db

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/plugin/monitor/pkg/types"
)

type DBInterface interface {
	SaveNodeEnergy(*types.NodeData) error
	SaveJobEnergy(*types.JobData) error
	SaveNodeEvents([]*protos.CranedEventInfo) error
	SaveLicenseUsage([]*protos.LicenseInfo) error
	SaveSpans([]*protos.SpanInfo) error
	SaveSpansToBucket(string, []*protos.SpanInfo) error
	TraceBucketForSpan(*protos.SpanInfo) string
	TraceBucketsForSpan(*protos.SpanInfo) []string
	Close() error
}
