# Standalone sinfo

`sinfo` is a separate executable that queries a Crane controller. Its command
interface targets Slurm 26.05.4. It does not invoke `cinfo` or `cwrapper`.
Native Crane commands remain available for Crane-specific features.

## Configuration and authentication

The command reads `/etc/crane/config.yaml` and uses the same controller and TLS
configuration as native Crane commands. It queries as the real caller and uses
the existing user-certificate mechanism. There is no `--config` or `-C` option.
For isolated environments, mount the environment's configuration at the default
path.

The controller must implement the versioned `QuerySinfoInfo` API. An older
controller produces an explicit upgrade diagnostic. A missing data field is
reported as a data-contract error rather than replaced with a fabricated value.

## Build and installation

The normal frontend `make build` includes `build/bin/sinfo`. Source installation
uses the existing `PREFIX` and `DESTDIR` options. For example, installing with
`PREFIX=/opt/crane` places the command at `/opt/crane/bin/sinfo`; users can select
it through PATH or invoke it by absolute path.

RPM and DEB builds include an optional `cranesched-slurm-compat` package that
owns `/usr/bin/sinfo`. The native frontend package does not require it. A Slurm
client package may already own this file; package ownership conflicts must be
resolved through the package manager. For coexistence, use a separate source
installation prefix. Do not force-overwrite Slurm-owned files.

Remove an existing `alias sinfo='cwrapper sinfo'` from shell startup files before
selecting the new executable. In an existing shell, `unalias sinfo` removes that
alias. `type -a sinfo` shows which command will execute. Other cwrapper aliases
can remain in use.

`sinfo --version` prints `slurm 26.05.4` as the compatibility version. The package
version and build metadata identify the actual Crane product. Node `Version`
fields retain the real Crane daemon version.

## Behavior and support

The command parses Slurm options, environment variables and output formats.
Requests for features absent from Crane produce an explicit diagnostic and a
nonzero status. Parsing support alone does not imply the underlying scheduling
feature exists. Help and version actions work without controller access.

Output selection controls grouping, and a node may appear in multiple
partitions. Partition availability is administrative availability; node outages
do not change it to `down`. An unlimited partition time limit describes the
partition layer, independently of account, QoS or job time limits.

Authenticated users can read reservation metadata through `sinfo -T`, following
Slurm's default public reservation visibility. This does not grant permission to
create, change, delete or use a reservation. Native reservation queries retain
their existing authorization rules.

JSON/YAML supports only `data_parser/v0.0.45`, the Slurm 26.05 default, including
serializer options and the unset/infinite distinction. Older and newer parser
versions are explicitly rejected; `--json=list` and `--yaml=list` list only v45.
The schema is generated at runtime from the Go types used to serialize reports.

Retired fields are omitted from both the schema and data model. Relative to
`sinfo[].partition`, these are `defaults.memory_per_cpu`,
`maximums.memory_per_cpu`, `maximums.shares`, and
`maximums.oversubscribe.{jobs,flags}` (including the empty parent object).
Slurm v45 still emits placeholders for these fields; this implementation does
not provide that legacy compatibility. Active replacement fields remain.

Full data reports are currently rejected: the schema implicitly requests features
Crane does not have, including temporary disk capacity, node features and scheduling weight.
Selecting fewer text fields with `-o` cannot remove those implicit schema fields.
`--json=v0.0.45+spec_only` remains available. `-T` selects reservation text even
when a structured output option is also supplied.

The supported text fields cover partitions, node identity, configured CPU/memory
capacity, CPU and node state counts, scheduling topology, load/free-memory
observations, GRES, allocation, reasons, reservation names and daemon versions.
The field registry in `compat/sinfo/format.go` is authoritative. Requests for
node comments/extra attributes/features/weight/temporary disk, partition default
time/Unix groups/oversubscription/size/root restriction/priority tier and other
absent policies produce an explicit unsupported diagnostic. Consequently, a
preset such as `--long` that requests an absent policy is also rejected.
Multi-cluster/federation and future-node queries are not supported.

Topology describes configured scheduling CPUs and sockets, with one thread per
logical scheduling core. It is not a report of the host's physical SMT layout.
An indivisible configured CPU/socket tuple is reported as unrepresentable.
Load uses the five-minute Linux load average; free memory uses Linux freeram in
MiB. Missing observations from a registered daemon are data-contract errors.

GRES ordering follows configuration, with GPU indices in natural device-file
order and generic resource indices in declaration order. Slurm shared GPU
accounting (`mps`/`shard`) is unavailable. A missing or inconsistent resource
inventory produces a diagnostic instead of fabricated indices.

Reservation text follows creation order and shows actual assigned nodes. The
controller persists creation order for new reservations. Legacy reservations
without this metadata get a deterministic name order on first recovery; their
original historical order cannot be reconstructed.

Crane allows fractional CPU allocations. A fractional allocation cannot be
represented by Slurm's integer CPU statistics; requesting those statistics
produces a data-representation diagnostic rather than silently truncating them.

The query copies nodes, partitions and reservations under their existing locks;
it does not promise a cross-domain atomic snapshot.

## Development layout

`cmd/sinfo` is the executable entry point; `compat/sinfo` owns the command's
options, execution plan, query adapter, grouping and rendering. The query source
accepts only query conditions and returns a snapshot, so the same command logic
can run against a controller or an in-memory test source.

`compat/utils` is one package containing the shared option scanner, Crane
connection lifecycle, hostlists, time utilities and ordered JSON/YAML/schema
encoding. It reuses existing configuration and authentication helpers. Keep
command-specific defaults, repeated-option rules and diagnostics in the command.
Additional packages can be considered when more compatibility commands provide
concrete reuse requirements.

`compat/protocol/v45` contains the Go report types and schema metadata. The
encoder accepts these values without depending on a command or Crane protobufs.
Future commands add their own query and presentation logic, plus the protocol
types they need; they do not invoke another command's implementation.
