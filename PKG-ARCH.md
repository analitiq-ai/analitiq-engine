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
- [ ] Task 10 (Phase B): worker isolation — OPEN DESIGN QUESTION for user:
      the documented principle says the connector worker receives resolved
      values over the contract, but the current destination model deliberately
      loads credentials locally and never sends them over gRPC. Where the
      isolation boundary sits (subprocess-per-connector inside each container
      vs connector-process replacing the in-process handler) and what crosses
      it must be ruled on before building.
