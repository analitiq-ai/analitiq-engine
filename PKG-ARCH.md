# PKG-ARCH: connector-package architecture implementation

Working note for the connector-brings-its-own-package build. Delete when done.

## Decisions (user-confirmed 2026-06-04)

1. **Full sweep** — every per-system branch leaves `GenericSQLConnector`; packages for
   postgres, mysql, mariadb, snowflake, bigquery (+ redshift thin if no distinct quirks).
2. **Driver-free engine** — asyncpg/aiomysql/adbc-*/snowflake-sqlalchemy/sqlalchemy-bigquery
   leave engine pyproject + docker/requirements.txt; Docker entrypoint installs each attached
   connector (requirements.txt driver + package when pyproject present) at container start.
3. **Isolation IS in scope (Phase B)** — connector code runs as a sandboxed worker subprocess
   speaking the CDK contract over gRPC; engine side resolves secrets, passes resolved values
   only over the local channel. Contract must stay container-ready for SaaS placement.
4. **Package homes** — real packages live in gitignored `connectors/{connector_id}/` (their
   home is the DIP registry repos; user pushes). Engine repo tracks only a synthetic fixture
   connector under tests/ for registry/discovery tests.

## Target design

- **Registry (cdk/cdk/registry.py)**: two-level resolution.
  - `_defaults: {kind: cls}` seeded by host built-ins (GenericSQLConnector for `database`,
    APIConnector for `api`, handlers for file/stdout/s3).
  - `_specific: {connector_id: cls}` from entry points; entry-point NAME = connector_id.
  - `resolve(kind, connector_id)`: specific match first, else kind default, else
    ConnectorNotRegisteredError. No legacy single-key API.
  - Groups unchanged: `analitiq.source_connectors` / `analitiq.destination_connectors`.
- **ConnectionRuntime** gains `connector_id` (from connection.json), plumbed via config prep.
- **GenericSQLConnector** = ANSI-neutral base + named overridable hooks:
  supports_upsert, default schema, SA upsert statement builder, ADBC DDL renderer,
  ADBC timestamp/binary/commit-timestamp types, ADBC temp-table SQL, merge quirks,
  bigquery dataset/paging quirks. Defaults: portable behavior or explicit
  UnsupportedOperation (e.g. ADBC write without a connector class). NEVER branches on
  driver/connector_id.
- **Shared dialect helpers stay in CDK** (`cdk/sql/dialects.py`) so mysql + mariadb can share
  ON DUPLICATE KEY logic without importing each other (connectors never import connectors).
- **Per-system type renderers** (native_to_postgres/snowflake/bigquery, arrow_to_*_native in
  cdk/cdk/sql_types.py) move into their connector packages.
- **Connector package standard** (`connectors/{connector_id}/`):
  definition/ (connector.json with connector_id set; async drivers — mariadb -> asyncmy),
  connector.py (subclass of CDK base carrying that system's hooks),
  requirements.txt (driver only; single source of truth),
  pyproject.toml (name analitiq-connector-{connector_id}; dynamic dependencies read from
  requirements.txt; entry points in both groups, name = connector_id).

## Constraints

- No backward compatibility / legacy paths. Update all callers.
- No workarounds: logical inconsistencies go back to the user.
- Engine repo tests: generic/ANSI + fixture-based registry tests stay; per-system behavior
  tests (snowflake/bigquery ADBC DDL etc.) move with the code toward connector packages.
- Run tests via `.venv/bin/python -m pytest` (broken pytest shebang).
- Public docs must stay free of SaaS/competitive framing.

## Phase B design — isolated connector workers (locked)

**Two-channel rule.** Credentials never travel the data plane (engine<->destination
gRPC, cross-container) — unchanged. Resolved values reach a connector worker once,
at launch, over the local bootstrap channel (stdin), never as RPC payloads.

**Worker model.** Every connector (all kinds, uniformly) runs as a subprocess of
its shell process, same image. The shell owns config loading, the secret store,
secret resolution, and supervision; the worker owns the connector class, the
driver, and every external connection. Registry seeding/discovery moves into the
worker (src/worker). The contract is container-ready: in SaaS the same worker
becomes its own container and the bootstrap becomes a launch-time secret mount.

**Bootstrap (stdin, one-shot JSON, redacted from logs).** Self-contained so the
worker needs NO config volume and NO secret store:
  role (source|destination), kind, connector_id, connection_id,
  resolved transport spec (JSON-safe: dsn, db_kwargs, tls {mode, ca_pem},
  engine_kwargs, base_url, headers, rate_limit), resolved_config (file/stdout),
  type-map rules (connector + connection, raw JSON),
  endpoint_refs + stream_endpoints (destination) / endpoint document + stream
  source config + initial cursor (source), uds_path.

**Channels.**
- Destination: worker hosts the EXISTING DestinationService on a UDS
  (server gains unix: binding). The shell's TCP server wraps WorkerProxyHandler
  (BaseDestinationHandler) which forwards configure_schema/write_batch/health
  over per-stream UDS clients and caches GetCapabilities at connect. TCP side
  byte-compatible with today's engine client.
- Source: new SourceService.ReadStream(ReadRequest) -> stream ReadResponse over
  UDS. ReadResponse is oneof {record_batch (Arrow IPC), cursor_save
  (JSON cursor state), read_error {message, fatal}}. Checkpoint store stays
  engine-side: WorkerReadable (implements Readable) relays cursor_save into
  CheckpointStore; initial cursor rides ReadRequest.

**Resolve/build split (CDK).** transport_factory splits per transport into
resolve_*_spec(spec, resolver) -> JSON-safe dict (engine-side; secrets in
values) and build_*_from_spec(resolved, dialect) -> transport (worker-side;
constructs AsyncEngine/SSLContext/ADBC closure/aiohttp session).
ConnectionRuntime gains resolve_spec() (shell) and from_resolved_spec()
(worker). TLS connect-args move behind SqlDialect hooks
(build_tls_connect_args(mode, ca_pem)): postgres/mysql/mariadb packages carry
their materializers; base raises UnsupportedDialectOperationError when tls is
declared. _ADBC_DRIVER_MODULES is replaced by the adbc_driver_{driver}.dbapi
convention (published schema enum remains the validator).

**Supervision (shell).** spawn: python -m src.worker; stdin=bootstrap pipe;
clean env (PATH/PYTHONPATH/HOME only); cwd /app; own session (setsid);
preexec rlimits: CORE=0, NOFILE, optional AS via WORKER_RLIMIT_AS_MB;
stderr piped through a redacting logger; UDS dir 0700 under a per-worker tmp
dir; readiness = socket connect with deadline; shutdown = rpc then SIGTERM to
the process group, SIGKILL after grace. Worker crash => stream fails
retryable; shell survives.

**Honest local-mode limit.** Same-container subprocesses share uid + fs: the
worker COULD read the mounted .secrets volume. Process isolation here buys
crash/limits/contract containment; fs/network isolation (per-worker egress,
no volume) arrives with container placement in SaaS — enforcement point is the
orchestrator, the contract built here is what makes it possible.

## Type-map unification (user decision, shipped)

ONE declarative surface per direction, every transport:
`definition/type-map-read.json` (native->arrow) + `definition/type-map-write.json`
(arrow->native, REQUIRED for kind: database). SQLAlchemy DDL, ADBC DDL, and the
control-plane create_table all render through dialect.render_column_type whose
base is the write map; SA DML binding comes from post-DDL table REFLECTION.
Deleted: cdk/sql_types.py (arrow_to_sqlalchemy + per-system renderers), the five
adbc_* type hooks, _create_table_from_schema, _build_adbc_*_ddl. Remaining code
overrides (code-when-needed): bigquery render_column_type (NUMERIC/BIGNUMERIC
ranges), mysql/mariadb batch_commits_key_type (VARCHAR(255); TEXT cannot key) and
current_timestamp_default (CURRENT_TIMESTAMP(6) precision match — found by
fresh-DDL e2e). Six write maps authored/extended (postgres+snowflake gained UInt
rules; mysql, mariadb, redshift, bigquery new). Validated: full local matrix
green on FRESH DDL through isolated workers; suite 1015 passed.

## Open problems

- (none currently — raise with user as found)

## Progress

- [x] Branch `feature/connector-packages` off docs/connector-architecture-principles
- [x] Task 2: CDK registry two-level resolution (65b825d)
- [x] Task 3: connector_id plumbing (65b825d; postgres/mysql connector.json got connector_id)
- [x] Task 4: engine/destination resolution call sites (65b825d)
- [x] Task 5: hook extraction full sweep (fc905af) — SqlDialect is the surface;
      residual driver-string infra left: query_builder._SQLA_DIALECTS,
      transport_factory TLS modes + _ADBC_DRIVER_MODULES
- [x] Task 6: connector packages authored in connectors/{id}/ (gitignored — push to
      DIP registry repos): postgres, mysql, mariadb (driver fixed -> mariadb+aiomysql),
      snowflake, bigquery (overrides _record_batch_commit_via_adbc), redshift
      (definition fixed -> adbc/postgresql). Wheel build verified (postgres).
- [x] Task 7: driver-free engine — drivers removed from pyproject + docker/requirements
      (poetry.lock left stale, pre-existing); kind-keyed engine entry points removed;
      docker/entrypoint.sh installs attached connectors at container start
- [x] Task 9: docker e2e validation — driver-free image, attach-time installs;
      green: pg->mysql, mysql->pg, pg->pg, pg->mariadb (datetime(6) microseconds
      verified), mariadb->pg; registry proof: package classes win, sqlite falls
      back to generic. Cloud paths (snowflake/bigquery/redshift) install cleanly
      but need credentials to run.
- [x] Task 8: unit suite migrated (f673e97) — 1046 passed, 3 env skips
- [x] Task 10 (Phase B): worker isolation SHIPPED — resolve/build transport split
      (d72b8d5), worker runtime + UDS + supervision (5a0d97b), shells wired
      (engine WorkerReadable + destination WorkerProxyHandler). Full local matrix
      green THROUGH isolated workers: pg->mysql, mysql->pg, pg->pg, pg->mariadb,
      mariadb->pg (5 rows each); no credentials in shell logs (redactor verified);
      suite 1049 passed. Secrets question resolved: two-channel rule (launch-time
      stdin injection; never on the data plane).
      Historical note — the question as originally posed:
      the documented principle says the connector worker receives resolved
      values over the contract, but the current destination model deliberately
      loads credentials locally and never sends them over gRPC. Where the
      isolation boundary sits (subprocess-per-connector inside each container
      vs connector-process replacing the in-process handler) and what crosses
      it must be ruled on before building.
