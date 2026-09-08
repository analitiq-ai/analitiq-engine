# Source Configuration: Design and Semantics

**Scope:** this doc specifies the source + stream + connection config
*design* — identity rules, scoping, replication semantics, and secret
resolution. It is not the schema reference: the authoritative field-level
shape of every document named below is the published JSON Schema at
`schemas.analitiq.ai` (generated from the Pydantic contract models in the
plugins repo — see the schema-contracts ownership map) and the CDK's own
Pydantic models where a document is engine-internal. This document exists
because the schema alone doesn't carry *why* a field resolves the way it
does; where this doc and the schema could both state a fact, the schema
wins and this doc points at it instead of repeating it.

For the rest see the siblings: the engine pipeline in
[`engine-architecture.md`](../architecture/engine-architecture.md), field
mapping in
[`mapping-and-transformations.md`](../data-path/mapping-and-transformations.md),
and the CDK / connector design in
[`connector-module-architecture.md`](../architecture/connector-module-architecture.md).
Environment variables and engine settings are specified in
[`settings-reference.md`](settings-reference.md).

## File layout

```
project_root/
├── pipelines/
│   ├── manifest.json                  # index of pipelines
│   └── {pipeline_id}/
│       ├── pipeline.json              # pipeline-level config
│       └── streams/{stream_id}.json   # one file per stream
├── connectors/{connector_id}/definition/
│   ├── connector.json                 # connector definition
│   └── endpoints/{endpoint_id}.json   # public endpoint documents
└── connections/{connection_id}/
    ├── connection.json                # user-created connection
    ├── .secrets/credentials.json      # secret values (gitignored)
    └── definition/endpoints/{endpoint_id}.json  # private endpoint documents
```

**Identity is directory-based, not a declared field.** The directory name
under `connections/` *is* the `connection_id`; a stream reaches a connection
through `endpoint_ref.connection_id`, never through a field authored inside
`connection.json` diverging from its own directory. `manifest.json` is
authoritative for which pipelines run: only an entry with `status: "active"`
is executable.

## Endpoint references

`endpoint_ref` is always an object — there is no string shorthand form —
with two scopes, and the scope changes what identifies the endpoint:

- **`connector`** — a public endpoint, resolved from the connection's
  connector. `endpoint_id` names it directly.
- **`connection`** — a private endpoint (e.g. a database table) that
  belongs to one connection. Its identity is `database_object`
  (catalog/schema/name); `endpoint_id` is derived server-side from
  `database_object`, not authored by the client, and the document's
  `endpoint_id` field must agree with that derivation where both are
  present.

`connection_id` is always present regardless of scope. Optional `x-*`
extension keys are accepted verbatim; any other unrecognised key is
rejected — the document is closed, not permissive.

## Replication semantics

A stream's `source.replication` block declares `method` (`full_refresh` or
`incremental`) and, when incremental, a `cursor_field`. Three points are
not derivable from the schema alone:

- **The safety window is engine policy, not connector input.**
  `safety_window_seconds` — subtracted from the stored cursor to cover
  late-arriving data — is filled with its default by the engine before the
  config crosses to the connector. A connector never invents its own
  default; the value the connector sees is always the engine's resolved
  one.
- **`database_pagination.order_by_field` is constrained by replication
  method.** Systems that require an explicit ordering for paged reads
  (e.g. MSSQL) declare it on full-refresh streams freely; on an incremental
  stream it must equal `cursor_field`, because cursor checkpointing depends
  on cursor-ordered pages — any other value is rejected.
- **`is_enabled` and `source.connection_ref` are runtime-computed, never
  authored.** `pipeline_config_prep` derives `is_enabled` from `status ==
  "active"` and copies `endpoint_ref.connection_id` onto the source block
  as a convenience key. Neither appears in the document a user or plugin
  writes.

## Connector definitions and transport containment

A connector declares one or more named `transports`; an operation
dispatches through the transport its `request.transport_ref` names, or
through `default_transport` when it names none. Everything that follows
from a transport travels with it — session, base URL, rate limiter, and
the header names the connection owns, which an operation's own
`request.headers` may not shadow. An operation is judged and sent against
**its** transport's facts, never the default's.

**Containment is per-transport, not per-connection.** Every URL an
operation produces — a next-page link included — must land on the origin
of the transport that operation dispatches through; a link off that origin
is refused rather than sent, because the session carries that transport's
credentials. This is what makes a file-download shape work (one connector,
two origins, e.g. `api` and `files`) while keeping one credential, one
header map, and one rate limiter describing a whole read. The converse is
also enforced: no single endpoint can paginate across transports, because a
traversal that changed host mid-read would have no correct answer for
which transport's headers apply at the second host.

Connector `kind` is a closed enum (the contract model is authoritative for
its members); the destination handler registry maps kinds to handler
classes — see
[`connector-module-architecture.md`](../architecture/connector-module-architecture.md)
and [`destination-config.md`](destination-config.md#handler-registry).

## API endpoint operations: binding and pagination semantics

The authoritative shape of an endpoint document is the `api-endpoint` JSON
Schema at `schemas.analitiq.ai`; the engine validates every endpoint
document against it before the document crosses into the connector
process. What follows is the *binding and pagination behavior* the schema
alone does not state:

- Every declared param must be bound exactly once (in `query`, `headers`,
  or `body`), and every binding must name a declared param — the contract
  refuses a document that breaks either rule.
- A `required` param that resolves to nothing fails the read before its
  first request: a read that silently drops the narrowing would return the
  whole collection and report success. A param a pagination or replication
  loop owns is exempt from `required` only until its loop first produces a
  value.
- The JSON-Schema value keywords on a param (`enum`, `format`, `pattern`,
  numeric and length bounds) are enforced with the reference JSON Schema
  implementation, at the same version the published schema is written
  for — never a second, hand-rolled validator. No refusal renders the
  offending value in its message, since a param can carry a credential or
  continuation token.
- `pagination.type` is a closed union (`offset`, `page`, `cursor`,
  `keyset`, `link`); an unrecognised value fails loud rather than reading
  one page. `stop_when` is required on every strategy — there is no
  default, and no page-size heuristic decides when a read ends. All five
  strategies run on one loop, `cdk.api.PageLoop`, with one adapter per
  scheme; see [ADR 0002](../adr/0002-one-stop-rule-for-every-paging-scheme.md)
  for why the loop, not the scheme, owns termination.
- `replication.cursor_mappings` maps a stream's `cursor_field` to declared
  params, and binds both the stored-cursor lower bound and (for a window
  form) the run's upper bound, rendered in the mapping's declared format.
  An incremental stream whose `cursor_field` no mapping names fails before
  its first request, rather than silently re-reading the whole collection
  every run.

## Secret references

Each `secret_refs.<name>` value carries an explicit scheme naming *where*
its secret comes from; a bare token (a pasted raw secret) is rejected —
secret material never belongs in a config file. Schemes: `env:VAR`,
`file:./path` (scope-checked against the connection directory, so `..`
cannot escape it), `sidecar:<name>` (an entry in
`connections/{connection_id}/.secrets/credentials.json`, the local-dev
flow), and `s3://bucket/key` (lazily imports `boto3`, needs the `[s3]`
extra, honours `AWS_ENDPOINT_URL_S3` / `AWS_REGION` for an S3-compatible
store). An unresolvable ref — missing env var, file, object, or sidecar
entry, or an unsupported scheme — fails loud; the engine never falls back
to an empty secret. A resolved file or object payload has exactly one
trailing newline stripped; the value is otherwise verbatim.

Inputs the connector definition declares as secret MUST be supplied via
`secret_refs`, never `parameters`.

## See Also

- [`destination-config.md`](destination-config.md) — destination-side config
- [`mapping-and-transformations.md`](../data-path/mapping-and-transformations.md) — `mapping.assignments` AST
- [`engine-architecture.md`](../architecture/engine-architecture.md) — module layout and pipeline lifecycle
- [`grpc-streaming-architecture.md`](../architecture/grpc-streaming-architecture.md) — engine ↔ destination protocol
- [`connector-module-architecture.md`](../architecture/connector-module-architecture.md) — CDK / connector design
