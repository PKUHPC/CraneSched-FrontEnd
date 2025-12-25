# Meta CNI plugin for CraneSched

This directory contains a CNI "meta" plugin that chains multiple CNI plugins
and lets you adjust their runtime parameters (CNI_ARGS, environment, and basic
runtime fields) before each delegate is invoked.

## What it does

- Runs a list of delegate CNI plugins in order on ADD and CHECK.
- Runs delegates in reverse order on DEL.
- Applies a configurable runtime override (global, then per delegate) to the
  CNI runtime environment before invoking each delegate.
- Returns the last non-nil result from ADD. If no delegate returns a result,
  it emits an empty result for the configured cniVersion.

## Build and install

Build with the top-level Makefile:

```sh
make tool
```

The binary will be at `build/tool/meta-cni`. For CNI to discover it, place the
binary in your CNI_PATH and name it to match the `type` field in your config
(`type` is the plugin binary name). For example:

```sh
sudo install -m 0755 build/tool/meta-cni /opt/cni/bin/crane-meta
```

## Configuration

The meta plugin reads JSON config from stdin like any other CNI plugin. The
fields below are in addition to standard CNI fields such as `cniVersion`,
`name`, and `type`.

### Top-level fields

- `logLevel` (string): `trace`, `debug`, or `info`. Defaults to info.
- `timeoutSeconds` (int): When > 0, a single timeout for the entire action
  (ADD/CHECK/DEL) across all delegates.
- `resultMode` (string): Controls whether delegate results are passed to the
  next plugin. Supported values are `none` (default), `chained`, and `merged`.
- `runtimeOverride` (object): Global runtime override applied to every delegate.
- `delegates` (array, required): List of delegate plugins to invoke.

### Delegate fields

Each entry in `delegates` supports:

- `name` (string, optional): Identifier, also used as a default `name` in
  generated delegate configs.
- `type` (string, optional): Delegate plugin type (binary name).
- `conf` (object, optional): Delegate plugin configuration. If omitted, the
  meta plugin will build a minimal config using `type`, `name`, and the parent
  `cniVersion`.
- `runtimeOverride` (object, optional): Per-delegate override (takes precedence
  over the global override).
- `annotations` (object, optional): Reserved for future use.

When `conf` is provided, the meta plugin ensures `type` and `cniVersion` are
set in the resulting delegate config if they are missing.

## Runtime override

Runtime overrides apply to the environment used for each delegate invocation.
The base values come from the incoming CNI request:

- `CNI_CONTAINERID`
- `CNI_NETNS`
- `CNI_IFNAME`
- `CNI_ARGS`
- `CNI_PATH`

Override fields:

- `containerID`, `netns`, `ifName`, `cniPath`: Replace the corresponding
  CNI runtime values.
- `args`: A list of manipulator expressions for `CNI_ARGS`.
- `envs`: A list of manipulator expressions for environment variables.

Manipulator expression syntax:

- Set or update: `KEY=value`
- Delete: `-KEY`
- Values may be single- or double-quoted to include spaces.

Examples:

```
args: ["PodIP=10.0.0.10", "-IgnoreUnknown"]
envs: ["NO_PROXY=svc.local", "A=''", "-HTTP_PROXY"]
```

`CNI_ARGS` is a semicolon-separated list of `key=value` pairs. The meta plugin
merges overrides into the current `CNI_ARGS` and sorts keys in the final
string. If all entries are removed, `CNI_ARGS` is cleared.

Note: `A=''` produces an empty value. When applied, an empty value unsets that
environment variable.

## Execution model and results

- ADD/CHECK: delegates run in list order.
- DEL: delegates run in reverse order.
- Each delegate is invoked with its own adjusted environment. The environment
  is restored after the call.
- `resultMode` controls how previous results are handled for ADD:
  - `none`: no prevResult is passed to delegates.
  - `chained`: the last delegate result is passed as prevResult to the next.
  - `merged`: delegate results are merged and the merged result is passed on.
- The last non-nil result from ADD is returned. In `merged` mode, the merged
  result is returned instead. If every delegate returns nil, the plugin emits
  an empty result with the configured cniVersion.

## Example

See `tool/meta-cni/config/00-meta.example.conf` for a full configuration example with
two delegates and runtime overrides. The key parts are:

- Top-level `type` matching your installed binary name (example uses
  `crane-meta`).
- `delegates` list with a full config for the first delegate and a minimal
  type-only config for the second.
