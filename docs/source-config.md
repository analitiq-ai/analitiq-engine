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
| `LOG_LEVEL` | No | `INFO` (default). The level before the pipeline is loaded, and the default for a pipeline that declares no `runtime.logging.log_level` |
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
│   └── {connector_id}/definition/
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
      "batching": { "batch_size": 200 },
      "logging": { "log_level": "INFO" },
      "error_handling": { "strategy": "dlq", "max_retries": 3, "retry_delay_seconds": 5 }
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
| `source.replication.safety_window_seconds` | No | Subtracted from the stored cursor to cover late-arriving data. Operational policy the engine owns: it fills the default on an incremental stream before the config crosses to the connector, so a connector never invents one |
| `source.replication.tie_breaker_fields` | No | Used for deterministic ordering when cursor values tie |
| `source.database_pagination.order_by_field` | No | Declared page ordering for database reads; systems that require an ordering for paged reads (e.g. MSSQL) need this on full-refresh streams. On incremental streams it must equal `cursor_field` (cursor checkpointing requires cursor-ordered pages); any other value is rejected |

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
  "secret_refs": { "api_key": "env:WISE_API_KEY" }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `connection_id` | Yes | Connection identity (= directory name under `connections/`) |
| `connector_id` | Yes | Resolves the connector definition under `connectors/{connector_id}/` |
| `display_name` | No | Human-readable label |
| `parameters` | Yes | Non-secret user inputs (host, port, database, etc.) |
| `secret_refs` | No | Map of input name → scheme-prefixed secret reference |

Inputs declared as secret in the connector definition MUST be supplied via
`secret_refs`, not `parameters`.

### Secret reference schemes

Each `secret_refs.<name>` value carries an explicit scheme that names *where*
its secret comes from. A bare token (a pasted raw secret) is rejected — secret
material never belongs in a config file.

| Value | Resolves to |
|-------|-------------|
| `env:VAR` | environment variable `VAR` |
| `file:./path` | contents of a local file, relative to the connection directory |
| `sidecar:<name>` | entry `<name>` in `connections/{connection_id}/.secrets/credentials.json` |
| `s3://bucket/key` | an object in S3 / an S3-compatible store (needs the `[s3]` extra) |

`env:`, `file:` and `sidecar:` are built-in and need no extra install. `s3://`
lazily imports `boto3` (installed with `pip install 'analitiq-core[s3]'`) and
honours `AWS_ENDPOINT_URL_S3` / `AWS_REGION` so an S3-compatible store (e.g.
MinIO) works. An unresolvable ref — missing env var, missing file/object,
missing sidecar entry, unsupported scheme — fails loud; the engine never falls
back to an empty secret. `file:` paths are scope-checked, so a `..` sequence
cannot read outside the connection directory. A file/object payload has a single
trailing newline stripped; the value is otherwise verbatim.

The `sidecar:` scheme keeps the local-development flow: put secrets in a flat
`connections/{connection_id}/.secrets/credentials.json` (gitignored) and point
each ref at its key.

```json
{ "PG_PASSWORD": "postgres", "api_key": "sk-..." }
```

Minimal local-Postgres connection supplying its password from the environment —
no sidecar file needed:

```json
{
  "$schema": "https://schemas.analitiq.ai/connection/latest.json",
  "connection_id": "my-postgres",
  "connector_id": "postgresql",
  "display_name": "My Postgres",
  "parameters": {
    "host": "localhost", "port": 5432, "database": "postgres",
    "username": "postgres", "ssl_mode": "prefer"
  },
  "secret_refs": { "password": "env:PG_PASSWORD" }
}
```

```shell
PG_PASSWORD=... PIPELINE_ID=my-pipeline docker compose run --rm source_engine
```

## Connector Definition (Source Reading)

**File:** `connectors/{connector_id}/definition/connector.json`

The connector defines `connector_type`, `connection_contract`,
`derived` values, and one or more `transports`. An operation dispatches
through the transport its `request.transport_ref` names, or through
`default_transport` when it names none; the runtime resolves that
transport's expression tree via the spec resolver.

Everything that follows from a transport travels with it — the session,
the base URL, the rate limiter, and the header names the connection owns
(which is what an operation's own `request.headers` may not shadow). An
operation is judged and sent against **its** transport's facts, never the
default's.

Every URL an operation produces — a next-page link included — must land on
the origin of **the transport that operation dispatches through**. A link
off that origin is refused rather than sent, because the session carries
that transport's credentials and the endpoint's own `request.headers` were
declared for that host.

Containment is per-transport, not per-connection: a read on `files` is
contained to the files origin and may not follow a link back to the api
origin, and vice versa. This is what makes the file-download shape work —
one system, two origins — while keeping one credential, one header map and
one rate limiter describing a whole read.

What it does not support is a single endpoint paginating across hosts. A
traversal that changed transport mid-read would have to decide what to do
with the endpoint's own declared headers at the second host, and there is
no correct answer: they are the endpoint's, bound once, and they belong to
the origin they were written for. Such a link is refused, loudly, naming
the origin the read is contained to.

Connector types accepted by the runtime: `api`, `database`, `file`,
`s3`, `stdout`. The destination handler registry maps these directly
(see `destination-config.md`).

For the CDK / connector design (transports, derived, expression markers,
ssl context, rate-limit shape) see
[`connector-module-architecture.md`](connector-module-architecture.md).

## Endpoint Files

**Public (connector-scoped):** `connectors/{connector_id}/definition/endpoints/{name}.json`
**Private (connection-scoped):** `connections/{connection_id}/definition/endpoints/{name}.json`

Public endpoints describe a connector's API surface (HTTP request, declared
params, pagination, response schema). Private endpoints describe
connection-specific resources (e.g. a database table belonging to one
connection).

### API endpoint (public example)

The authoritative shape is the `api-endpoint` JSON Schema at
`schemas.analitiq.ai`; the engine validates every endpoint document against
it before the document crosses into the connector process.

```json
{
  "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
  "endpoint_id": "transfers",
  "operations": {
    "read": {
      "request": {
        "method": "GET",
        "path": "/v1/transfers",
        "query": {
          "profile": { "from_param": "profile" },
          "limit": { "from_param": "limit" },
          "offset": { "from_param": "offset" },
          "createdDateStart": { "from_param": "createdDateStart" }
        }
      },
      "params": {
        "profile": {
          "in": "query", "type": "integer", "required": true,
          "default": { "ref": "connection.selections.profile_id" }
        },
        "limit": {
          "in": "query", "type": "integer", "required": false,
          "controlled_by": "pagination"
        },
        "offset": {
          "in": "query", "type": "integer", "required": false,
          "controlled_by": "pagination"
        },
        "createdDateStart": {
          "in": "query", "type": "string", "required": false,
          "controlled_by": "replication"
        }
      },
      "response": {
        "schema": {
          "type": "object",
          "properties": {
            "records": {
              "type": "array",
              "items": {
                "type": "object",
                "properties": {
                  "id": { "type": "integer" },
                  "created": { "type": "string", "format": "date-time" }
                }
              }
            }
          }
        },
        "records": { "ref": "response.body.records" }
      },
      "pagination": {
        "type": "offset",
        "offset": {
          "param": "offset", "initial": 0,
          "increment_by": { "ref": "response.record_count" }
        },
        "limit": {
          "param": "limit",
          "default": { "ref": "runtime.batch_size" },
          "max": 500
        },
        "stop_when": { "empty": { "ref": "response.body.records" } }
      },
      "replication": {
        "supported_methods": ["full_refresh", "incremental"],
        "cursor_mappings": [
          { "cursor_field": "created", "param": "createdDateStart", "operator": "gte" }
        ]
      }
    }
  }
}
```

| Field | Description |
|-------|-------------|
| `operations.read.request.method` / `.path` | HTTP verb, and the path appended to the transport's `base_url` |
| `operations.read.request.query` / `.headers` / `.body` | Where each declared param lands on the wire. Every declared param must be bound exactly once, and every binding must name a declared param — the contract refuses a document that breaks either rule |
| `operations.read.params.<name>` | One declared param: `in` (`query` / `header` / `body` / `path`), `type`, `required`, an optional `default` value expression (`literal` / `ref` / `template` / `function`), and `controlled_by` (`pagination` or `replication`) for a param whose value a loop sets rather than the author. A `required` param that resolves to nothing fails the read: the request would otherwise go out without it and the provider would answer the collection the param was narrowing. A `controlled_by` param is exempt from `required` — page one of a cursor scheme carries no cursor by construction |
| `operations.read.params.<name>` constraints | `enum`, `format`, `pattern`, `minimum`, `maximum`, `minLength`, `maxLength`, `minItems`, `maxItems` — JSON Schema keywords, checked as such against the resolved value of every page before the request goes out, so a value the declaration does not admit fails the read instead of the provider. `format` is enforced for the formats the engine has a checker for (`date`, `email`, `idn-email`, `ipv4`, `ipv6`, `regex`, `uuid`); every other name -- `date-time`, `uri`, `json-pointer` and `idn-hostname` included, whose checkers need packages the engine does not install, `time`, whose always-registered checker is the Draft 3 one that refuses a zone offset, and provider-specific names like `int64` -- is carried as documentation |
| `operations.read.params.<name>.operators` | The comparison the param stands for, as a subset of the stream filter vocabulary (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `contains`, `starts_with`, `ends_with`). A stream filter on this param must name one of them; a param declaring no `operators` is not stream-filterable and a filter aimed at it fails the read. The operator is never rendered -- `?updated_since=X` has no spelling of "greater than" the engine could choose -- so this declaration is how an endpoint says what comparing its param means. It cannot tell two members of one set apart: a param declaring `["gt", "lt"]` admits both and binds both the same way, so at most one is truthful and only the document can say which. Some sets are honest (`["eq", "in"]` is told apart by the value's own shape), which is why this is not a cardinality rule. Two filters on ONE param is refused: a param carries one value, so the second would overwrite the first |
| `operations.read.response.schema` | JSON Schema for the response body. Each record field's canonical Arrow type is resolved from the endpoint's read type-map unless the field declares `arrow_type` itself; a JSON type with no rule in that type-map fails the read naming the field |
| `operations.read.response.records.ref` | `response.body`, or `response.body.<field>[.<field>...]` — where a page's records sit in the decoded body. A ref anchored anywhere else, or one that addresses a value carrying no records, fails the read naming the ref |
| `operations.read.pagination.type` | One of `offset`, `page`, `cursor`, `keyset`, `link`. The union is closed: an unrecognised value fails loud rather than reading one page |
| `operations.read.pagination.stop_when` | Required on every strategy. The authoritative end-of-pages condition, evaluated against the page's own body |
| `operations.read.pagination.limit` | Optional on every strategy: `param` (where the page size lands), `default` (a value expression; `runtime.batch_size` is in scope), and `max`, the provider's cap, which clamps whatever the default produced. Under `link` it binds to the first request only — a followed `next_url` carries the provider's own query |
| `operations.read.replication.cursor_mappings` | Maps a stream's `cursor_field` to declared params. The single form (`param` plus a `gt`/`gte` `operator`) binds the stored cursor, moved back by the safety window, as the read's lower bound; the window form (`start_param` / `end_param` / `start_operator` / `end_operator`) binds that lower bound and the run's upper bound (now, UTC). Both render their bounds in the mapping's `format` (`date-time`, `date`, `epoch_seconds`, `epoch_milliseconds`; an undeclared format keeps the cursor's own vocabulary: an epoch record field sends epoch ticks in its unit, a string moment sends ISO). Under `date` or an epoch format a `gt` lower bound goes out one unit earlier, so the provider's exclusive comparison cannot skip the unit the cursor sits in. A single mapping whose operator is `lt`/`lte`, or a window whose ends face the wrong way, fails the read rather than binding a range the author did not declare |

All five strategies run on one loop, `cdk.api.PageLoop`, with one adapter per
scheme. The loop stops on an empty page, on the strategy having nowhere left
to go, or on the declared `stop_when` — and on nothing else. See
[ADR 0002](adr/0002-one-stop-rule-for-every-paging-scheme.md).

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
