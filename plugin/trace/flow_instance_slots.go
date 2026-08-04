package main

import "hash/fnv"

const flowInstanceSlots = 64

// flowInstanceSlot is a bounded, stateless collision dimension for InfluxDB
// point identity. The trace plugin does not own the flow lifecycle, so keeping
// per-flow assignments would either leak forever or require unsafe eviction.
// Hashing the stable producer identity keeps cardinality fixed and makes the
// mapping deterministic across batches, goroutines, and plugin restarts.
func flowInstanceSlot(flow *executionFlowEnvelope) uint8 {
	if flow == nil || flow.flowID == "" {
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
