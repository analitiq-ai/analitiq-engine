# Source Configuration: Design and Semantics

**Scope:** what the engine does at runtime with a source, stream and
connection once the validator has passed the run's workspace. Document
shape, layout and validity are the published contract's and
`analitiq-validator`'s; this doc does not restate them.

For the rest see the siblings: the engine pipeline in
[`engine-architecture.md`](../architecture/engine-architecture.md), field
mapping in
[`mapping-and-transformations.md`](../data-path/mapping-and-transformations.md),
and the CDK / connector design in
[`connector-module-architecture.md`](../architecture/connector-module-architecture.md).
The environment-variable catalogue is
[`src/config/settings.py`](../../src/config/settings.py) (see also
[README.md](../../README.md#environment-variables)); resolution order and
layering rules are in [`settings-reference.md`](settings-reference.md).

## Replication semantics

- **The safety window is engine policy, not connector input.**
  `safety_window_seconds` — subtracted from the stored cursor to cover
  late-arriving data — is filled with its default by the engine before the
  config crosses to the connector. A connector never invents its own
  default; the value the connector sees is always the engine's resolved
  one.
- **A connection is identified by its directory name.** The engine keys
  each connection by the name of its directory under `connections/`, which
  is what a stream's `endpoint_ref.connection_id` reaches; it does not read
  the `connection_id` inside `connection.json`.
- **`source.connection_ref` is runtime-computed, never authored.**
  `pipeline_config_prep` copies `endpoint_ref.connection_id` onto the
  source block as a convenience key; it does not appear in the document a
  user or plugin writes.

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

The destination handler registry maps connector kinds to handler classes —
see
[`connector-module-architecture.md`](../architecture/connector-module-architecture.md)
and [`destination-config.md`](destination-config.md#handler-registry).

## API endpoint operations: binding and pagination semantics

- A `required` param that resolves to nothing fails the read before its
  first request: a read that silently drops the narrowing would return the
  whole collection and report success. A param a pagination or replication
  loop owns is exempt from `required` only until its loop first produces a
  value.
- The JSON-Schema value keywords on a param (`enum`, `pattern`, numeric
  and length bounds) are enforced with the reference JSON Schema
  implementation, at the same version the published schema is written
  for — never a second, hand-rolled validator. `format` is enforced only
  for the names the engine ships a checker for (`_ENFORCED_FORMATS`,
  `cdk/cdk/api/param_rules.py`); any other declared `format` is accepted
  as an annotation, not validated — the contract intentionally allows
  this rather than pulling in every format library transitively. No
  refusal renders the offending value in its message, since a param can
  carry a credential or continuation token.
- Every pagination scheme runs on one loop, `cdk.api.PageLoop`, with one
  adapter per scheme, and no page-size heuristic decides when a read ends;
  see [ADR 0002](../adr/0002-one-stop-rule-for-every-paging-scheme.md) for
  why the loop, not the scheme, owns termination.
- The engine binds both the stored-cursor lower bound and (for a window
  form) the run's upper bound through `replication.cursor_mappings`,
  rendered in the mapping's declared format. An incremental stream whose
  `cursor_field` no mapping names fails before its first request, rather
  than silently re-reading the whole collection every run.

## Secret references

`SchemeSecretsResolver` (`cdk/cdk/secrets/resolvers/scheme.py`) resolves
each `secret_refs.<name>` engine-side, by the scheme the contract gives it.
A `file:` path is scope-checked against the connection directory, so `..`
cannot escape it. The `s3://` scheme lazily imports `boto3`, needs the
`[s3]` extra, and honours `AWS_ENDPOINT_URL_S3` / `AWS_REGION` for an
S3-compatible store. An unresolvable ref (a missing env var, file, object
or sidecar entry, or an unsupported scheme) fails loud; the engine never
falls back to an empty secret. A resolved file or object payload has
exactly one trailing newline stripped; the value is otherwise verbatim.

## See Also

- [`destination-config.md`](destination-config.md) — destination-side config
- [`mapping-and-transformations.md`](../data-path/mapping-and-transformations.md) — `mapping.assignments` AST
- [`engine-architecture.md`](../architecture/engine-architecture.md) — module layout and pipeline lifecycle
- [`grpc-streaming-architecture.md`](../architecture/grpc-streaming-architecture.md) — engine ↔ destination protocol
- [`connector-module-architecture.md`](../architecture/connector-module-architecture.md) — CDK / connector design
