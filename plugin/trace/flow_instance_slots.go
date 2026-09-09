package main

import "hash/fnv"

const flowInstanceSlots = 64

// flowInstanceSlot is a bounded, stateless collision dimension for InfluxDB
// point identity. The trace plugin does not own the flow lifecycle, so keeping
// per-flow assignments would either leak forever or require unsafe eviction.
// Hashing the stable producer identity keeps cardinality fixed and makes the
// mapping deterministic across batches, goroutines, and plugin restarts.
//
// The gate is the producer identity, not flow_id. Heartbeat and pipeline fault
// points carry no flow_id, and they are exactly where the cross-instance
// collision set is widest: every Supervisor on one node shares the "service"
// tag, because that tag is the tracer service name and does not include the
// pid. Falling back to slot zero for them would leave their Influx point
// identity separated only by sequence slot and timestamp, so a same-nanosecond
// pair would silently overwrite. The evidence layer already distinguishes those
// producers by service_instance; this keeps storage able to as well.
func flowInstanceSlot(flow *executionFlowEnvelope) uint8 {
	if flow == nil || flow.serviceInstance == "" {
		return 0
	}
	hasher := fnv.New32a()
	for _, component := range []string{
		flow.producer,
		flow.serviceLogicalInstance,
		flow.serviceInstance,
	} {
		_, _ = hasher.Write([]byte(component))
		_, _ = hasher.Write([]byte{0})
	}
	return uint8(hasher.Sum32() % flowInstanceSlots)
}
