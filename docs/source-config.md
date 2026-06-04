# Source Configuration Reference

Source-side configuration is assembled from modular files. This document
describes the file layout, schemas, and field semantics.

**Scope:** this doc owns the source + stream + connection config schema,
the `endpoint_ref` structure, and replication / incremental semantics.
For the rest see the siblings: the engine pipeline in
[`engine-architecture.md`](engine-architecture.md), field mapping in
[`mapping-and-transformations.md`](mapping-and-transformations.md), and
the CDK / connector design in
[`connector-module-architecture.md`](connector-module-architecture.md).

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PIPELINE_ID` | Yes | Pipeline ID matching an entry in `pipelines/manifest.json` |
| `RUN_MODE` | No | `source` (default) or `destination` |
| `LOG_LEVEL` | No | `INFO` (default) |
| `ENV` | No | `loc` (default) — `loc` skips the remote config fetch; configs must be on disk |
| `DESTINATION_GRPC_HOST` | No | Hostname of the destination gRPC server (when running with a remote destination) |
| `DESTINATION_GRPC_PORT` | No | `50051` (default) |

The engine is cloud-agnostic: there are no AWS / GCP / Azure SDK
dependencies and no cloud-specific environment variables. State, logs,
DLQ, and metrics use the local filesystem.

## File Layout

```
project_root/
├── pipelines/
│   ├── manifest.json                       # central index of all pipelines
│   └── {pipeline_id}/
│       ├── pipeline.json                   # pipeline-level config
│       └── streams/
│           └── {stream_id}.json            # one file per stream
├── connectors/
│   └── {slug}/definition/
│       ├── connector.json                  # connector definition
│       └── endpoints/
│           └── {endpoint_name}.json        # public endpoint schemas
└── connections/
    └── {alias}/
        ├── connection.json                 # user-created connection
        ├── .secrets/credentials.json       # secret values (gitignored)
        └── definition/
            └── endpoints/
                └── {endpoint_name}.json    # private endpoints (e.g. DB tables)
```

Connection identity is id-based: the directory name under `connections/`
is the `connection_id`, and streams reference a connection through
`endpoint_ref.connection_id`. `manifest.json` is authoritative: only
pipelines with `status: "active"` are executable.

## Pipeline Manifest

**File:** `pipelines/manifest.json`

```json
{
  "pipelines": [
    {
      "pipeline_id": "wise-to-postgresql",
      "name": "Wise to PostgreSQL",
      "path": "wise-to-postgresql/pipeline.json",
      "status": "active",
      "streams": ["wise-transfers"]
    }
  ]
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `pipeline_id` | Yes | Unique pipeline ID (matches `PIPELINE_ID` env) |
| `path` | Yes | Path to pipeline.json relative to `pipelines/` |
| `status` | Yes | `active` to enable execution; any other value disables |
| `streams` | No | Documentary list of stream IDs in the pipeline |

## Pipeline File

**File:** `pipelines/{pipeline_id}/pipeline.json`

```json
{
  "pipeline": {
    "name": "Wise to PostgreSQL",
    "status": "active",
    "version": 1,
    "connections": {
      "source": "my-wise",
      "destinations": ["my-postgres"]
    },
    "streams": ["wise-transfers"],
    "schedule": { "type": "manual" },
    "engine": { "vcpu": 1, "memory": 8192 },
    "runtime": {
      "buffer_size": 5000,
      "batching": { "batch_size": 200, "max_concurrent_batches": 3 },
      "logging": { "log_level": "INFO", "metrics_enabled": true },
      "error_handling": { "strategy": "dlq", "max_retries": 3, "retry_delay": 5 }
    }
  },
  "streams": []
}
```

`connections.source` and entries in `connections.destinations` are
**connection aliases** (directory names under `connections/`). The
`streams` array lists stream IDs that resolve to
`pipelines/{pipeline_id}/streams/{stream_id}.json`.

## Stream — Source Section

**File:** `pipelines/{pipeline_id}/streams/{stream_id}.json`

```json
{
  "stream_id": "wise-transfers",
  "status": "active",
  "source": {
    "endpoint_ref": {
      "scope": "connector",
      "connection_id": "my-wise",
      "endpoint_id": "transfers"
    },
    "primary_keys": ["id"],
    "replication": {
      "method": "incremental",
      "cursor_field": "created"
    }
  },
  "destinations": [ /* see destination-config.md */ ],
  "mapping":      { /* see mapping-and-transformations.md */ }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `status` | Yes | `active` enables the stream; the runtime derives `is_enabled` from `status == "active"` |
| `source.endpoint_ref` | Yes | Reference to the endpoint to read (object form, see below). Its `connection_id` identifies the connection to read from |
| `source.primary_keys` | No | List of fields used for deduplication and as fallback record IDs |
| `source.replication.method` | No | `full_refresh` (default) or `incremental` |
| `source.replication.cursor_field` | When `incremental` | Name of the cursor field, a single string (e.g. `"created"`) |
| `source.replication.safety_window_seconds` | No | Subtracted from cursor for late-arriving data |
| `source.replication.tie_breaker_fields` | No | Used for deterministic ordering when cursor values tie |

> `is_enabled` and `source.connection_ref` are runtime convenience keys
> added by `pipeline_config_prep` (it computes `is_enabled` from `status`
> and copies `endpoint_ref.connection_id` onto the source block). Neither
> appears in the authored stream document.

### Endpoint references

`endpoint_ref` is always an object — there is no string form:

```json
"endpoint_ref": { "scope": "connector", "connection_id": "my-wise", "endpoint_id": "transfers" }
```

| Key | Description |
|-----|-------------|
| `scope` | `connector` (public endpoint, resolved from the connection's connector) or `connection` (private endpoint, e.g. a DB table) |
| `connection_id` | The connection being read from — always present, regardless of scope |
| `endpoint_id` | The endpoint name within that connector/connection |

Optional `x-*` extension keys are accepted verbatim; any other key is
rejected.

## Connection File

**File:** `connections/{connection_id}/connection.json`

The connection references its connector by `connector_id` (resolved by
`src/config/connection_loader.py`). Source-side example:

```json
{
  "$schema": "https://schemas.analitiq.ai/connection/latest.json",
  "connection_id": "my-wise",
  "display_name": "My Wise",
  "connector_id": "wise",
  "parameters": {},
  "secret_refs": { "api_key": "connections/my-wise/api_key" }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `connection_id` | Yes | Connection identity (= directory name under `connections/`) |
| `connector_id` | Yes | Resolves the connector definition under `connectors/{connector_id}/` |
| `display_name` | No | Human-readable label |
| `parameters` | Yes | Non-secret user inputs (host, port, database, etc.) |
| `secret_refs` | No | Map of input name → opaque secret reference resolved by the secrets resolver |

Secret references resolve to entries in
`connections/{connection_id}/.secrets/credentials.json` for the local
file resolver. Inputs declared as secret in the connector definition
MUST be supplied via `secret_refs`, not `parameters`.

## Connector Definition (Source Reading)

**File:** `connectors/{connector_id}/definition/connector.json`

The connector defines `connector_type`, `connection_contract`,
`derived` values, and one or more `transports`. The runtime selects a
transport by `default_transport` (or per-endpoint override) and resolves
its expression tree via the spec resolver.

Connector types accepted by the runtime: `api`, `database`, `file`,
`s3`, `stdout`. The destination handler registry maps these directly
(see `destination-config.md`).

For the CDK / connector design (transports, derived, expression markers,
ssl context, rate-limit shape) see
[`connector-module-architecture.md`](connector-module-architecture.md).

## Endpoint Files

**Public (connector-scoped):** `connectors/{connector_id}/definition/endpoints/{name}.json`
**Private (connection-scoped):** `connections/{connection_id}/definition/endpoints/{name}.json`

Public endpoints describe a connector's API surface (HTTP path, method,
filters, pagination, response schema). Private endpoints describe
connection-specific resources (e.g. a database table belonging to one
connection).

### API endpoint (public example)

```json
{
  "endpoint": "/v1/transfers",
  "method": "GET",
  "version": 1,
  "endpoint_schema": { /* JSON Schema for response items */ },
  "filters": {
    "profile": {
      "type": "integer",
      "operators": ["eq"],
      "required": true,
      "default": "${profile_id}"
    },
    "createdDateStart": {
      "type": "string",
      "operators": ["gte"],
      "required": false
    }
  },
  "pagination": {
    "type": "offset",
    "params": { "limit_param": "limit", "offset_param": "offset" }
  },
  "replication_filter_mapping": {
    "created": "createdDateStart"
  }
}
```

| Field | Description |
|-------|-------------|
| `endpoint` | Path appended to the transport's `base_url` |
| `method` | HTTP method (default `GET`) |
| `endpoint_schema` | JSON Schema describing the response payload |
| `filters` | Per-filter type, allowed operators, default values (defaults can reference `connection.selections` via `${name}` placeholders) |
| `pagination.type` | `offset`, `cursor`, `page`, `time`, or `link` |
| `replication_filter_mapping` | Maps a stream's `cursor_field` to an API filter name |

### Database endpoint (private example)

```json
{
  "endpoint_name": "public-wise_transfers",
  "table": "wise_transfers",
  "schema": "public",
  "columns": [
    { "name": "id", "native_type": "bigint", "nullable": false },
    { "name": "created", "native_type": "timestamptz", "nullable": false }
  ],
  "primary_key": ["id"]
}
```

Private endpoints rely on the connection's `type-map-read.json` (or the
connector's) to convert `native_type` into canonical Arrow types for
vectorized casting.

## See Also

- [`destination-config.md`](destination-config.md) — destination-side config
- [`mapping-and-transformations.md`](mapping-and-transformations.md) — `mapping.assignments` AST
- [`engine-architecture.md`](engine-architecture.md) — module layout and pipeline lifecycle
- [`grpc-streaming-architecture.md`](grpc-streaming-architecture.md) — engine ↔ destination protocol
- [`connector-module-architecture.md`](connector-module-architecture.md) — CDK / connector design
