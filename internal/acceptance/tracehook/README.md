# Execution-flow TraceHook acceptance fixture

This build-tagged helper is not included in normal FrontEnd builds or packages.
It submits one intentionally invalid execution-flow span through the existing
`CranePluginD.TraceHook` Unix-socket RPC. The loaded trace plugin must convert
the rejected span into a sanitized `flow/v1/pipeline/fault` and persist it
through its normal writer. The helper never writes to InfluxDB itself.

Build and invoke it only from an isolated acceptance environment. The explicit
Make target is deliberately not a dependency of `build`, `package`, or `all`:

```bash
make execution-flow-acceptance-helper

CRANE_EXECUTION_FLOW_ACCEPTANCE=1 build/bin/crane-flow-fault-fixture \
  --socket /var/cranetest/cplugind/cplugind.sock \
  --environment-id <run-id.shard-id> \
  --json
```

The command permits only an absolute, non-symlink Unix socket path. Its
request contains a fixed invalid flow-ID canary, but neither command output nor
the generated pipeline fault may contain that rejected value. A successful RPC
only reports `{"ok":true}` after cplugind accepts the request; the acceptance test must query
the environment-scoped Influx data through the Go validator and require
`trace_pipeline_inconclusive`.
