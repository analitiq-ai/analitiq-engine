# Connector Conformance Kit

The CDK ships an acceptance suite for connector packages
(`cdk.conformance`, installed with the `conformance` extra). Every
connector repo runs it in its own CI against the pinned CDK version, so
a CDK change that breaks a connector — or a connector change that
breaks its contract — turns that connector's CI red before release, not
in a customer pipeline (issue #391; ADR
[sql-write-path-v2](sql-write-path-v2.md) §10).

## What each tier certifies

**Tier 1 — contract tests** (`cdk.conformance.tier1`, no live database):

- **Rendering matches declaration.** The stage DDL carries the
  temporary form iff `sql_capabilities.stage.scope` is `temp`; the
  upsert statement carries exactly the declared `merge_form`; the
  target-emptying statement is DELETE-shaped, never `TRUNCATE`;
  identical batches build identical plans (self-healing retries) and
  distinct batches never share a stage name.
- **Refusals fire.** Upsert with empty `conflict_keys`, catalog
  addressing without a declaration (or against `catalog: "none"`) — a
  loud error, never a guessed default.
- **The override surface is the sanctioned one.** The connector class
  carries `dialect_class` and nothing else; the dialect overrides only
  the public `SqlDialect` surface, with base-compatible signatures.
  Overriding a private CDK internal fails with the member named.
- **Declared and implemented agree, both ways.** A declared
  `merge_form` needs `merge_statement_sql`; a `bulk_land` override
  needs a declared `bulk_load` mechanism; a write-capable connector
  (one shipping `type-map-write.json`) needs `sql_capabilities` and
  `stage_table_sql`.
- **Type maps are round-trip stable.** Every native type the write map
  renders must be readable by the read map (a table the connector
  creates stays discoverable), and one write/read round must reach a
  fixed point — `write(read(write(x))) == write(x)` — so re-creating a
  logically identical table never changes its column types. The literal
  `read(write(x)) == x` is deliberately **not** asserted: most systems
  have no unsigned or 8-bit types, so `Int8 -> SMALLINT -> Int16` is
  correct authoring, not a defect.
- **The read path compiles.** The CDK's QueryBuilder resolves the
  connector's dialect flavour (`sqlalchemy_registry_name`) against the
  connector's own installed requirements, and cursor reads order by the
  cursor field — the precondition for monotonic checkpoints.

**Tier 2 — live tests** (`cdk.conformance.tier2`, the connector's
system as a CI service container): all three write modes end-to-end
through `connect` / `configure_schema` / `write_batch`, read-back and
incremental resume through `read_batches`, and replay — each phase on a
fresh connector instance over a fresh connection, so every test also
certifies a restart. A replayed batch must leave the target unchanged
for the exactly-once modes.

Cloud warehouses with no containerizable server (Snowflake, BigQuery,
Redshift) run tier 1 only — the accepted residual risk recorded in
issue #391.

## Wiring a connector repo

The suite needs three inputs: the connector checkout
(`--connector-dir`, holding `definition/connector.json`), the connector
class (resolved from the installed package's entry points; overridable
with `--connector-class package.module:Class`), and — for tier 2 — a
live connection document (`--live-connection`). Each option doubles as
an environment variable (`ANALITIQ_CONNECTOR_DIR`,
`ANALITIQ_CONNECTOR_CLASS`, `ANALITIQ_LIVE_CONNECTION`).

The live connection document is a saved-connection-shaped JSON whose
secrets come through the standard `secret_refs` schemes (`env:` /
`file:` / `sidecar:`), plus the schema the suite creates its
(uniquely-named, dropped-afterwards) tables in:

```json
{
  "connection_id": "conformance-live",
  "schema": "public",
  "config": {
    "parameters": {
      "host": "127.0.0.1", "port": "5432",
      "database": "conformance", "username": "conformance"
    },
    "secret_refs": { "password": "env:CONFORMANCE_DB_PASSWORD" }
  }
}
```

A connector repo's CI job (the Docker service-container pattern):

```yaml
jobs:
  conformance:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: conformance
          POSTGRES_PASSWORD: conformance
          POSTGRES_DB: conformance
        ports: ["5432:5432"]
        options: >-
          --health-cmd "pg_isready -U conformance"
          --health-interval 5s --health-timeout 5s --health-retries 10
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - name: Install the pinned CDK with the suite, then this connector
        run: |
          pip install "analitiq-cdk[conformance] @ git+https://github.com/analitiq-ai/analitiq-core.git@<pinned-tag>#subdirectory=cdk"
          pip install .
      - name: Tier 1 (contract)
        run: pytest --pyargs cdk.conformance.tier1 --connector-dir .
      - name: Tier 2 (live)
        env:
          CONFORMANCE_DB_PASSWORD: conformance
          ANALITIQ_CONFORMANCE_REQUIRE_LIVE: "1"
        run: >-
          pytest --pyargs cdk.conformance.tier2 --connector-dir .
          --live-connection ci/live-connection.json
```

Systems without a service container drop the tier-2 step; the tier-1
step is mandatory in every connector repo. A job that *does* provision
a container should also set `ANALITIQ_CONFORMANCE_REQUIRE_LIVE=1`
(as the snippet's engine-side counterpart does): with it, a missing
live connection fails the job instead of skipping, so a typo'd
variable can never silently retire the live tier while CI stays green.

The checks are also plain importable functions
(`cdk.conformance.check_override_surface`,
`check_declaration_consistency`, `check_type_map_round_trip`) for repos
that want them inside their own harness.

## How the kit itself is certified

`tests/conformance_kit/` in the engine repo runs the kit against a
postgres-shaped reference connector on the sanctioned v2 surface
(`tests/conformance_kit/reference_connector.py`): tier 1 passes in
every CI run; tier 2 passes against the `conformance-live` postgres
service container job in `ci.yml`; and `test_kit_breaks.py` proves that
a bent hook signature, a private-internal override, an
undeclared-capability use, and a declared-but-unimplemented capability
each fail with a message naming the offending member.
