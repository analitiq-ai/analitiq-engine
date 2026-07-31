# Connector Conformance Kit

The CDK ships an acceptance suite for connector packages
(`cdk.conformance`, installed with the `conformance` extra). Every
connector repo runs it in its own CI against the pinned CDK version, so
a CDK change that breaks a connector — or a connector change that
breaks its contract — turns that connector's CI red before release, not
in a customer pipeline (spec
[sql-write-path](sql-write-path.md) §10).

## What each tier certifies

**Tier 1 — contract tests** (`cdk.conformance.tier1`, no live database):

- **Rendering matches declaration.** The stage DDL carries the
  temporary form iff `sql_capabilities.stage.scope` is `temp`; the
  upsert statement carries exactly the declared `merge_form` and states
  the stream's `conflict_keys` where that form names its match keys; the
  target-emptying statement is DELETE-shaped, never `TRUNCATE`;
  identical batches build identical plans (self-healing retries) and
  distinct batches never share a stage name.
- **Refusals fire.** Upsert with empty `conflict_keys`, catalog
  addressing without a declaration (or against `catalog: "none"`) — a
  loud error, never a guessed default.
- **The override surface is the sanctioned one.** The connector class
  carries `dialect_class` and nothing else; the dialect's public
  namespace is exactly the public `SqlDialect` surface, minus the
  framework-owned members (`capabilities`, `for_runtime`,
  `table_address`), with base-compatible signatures. The two that decide
  the declaration — `capabilities` and `for_runtime` — are refused by
  `SqlDialect` itself where the subclass is written, so the kit's finding
  is a backstop for the rest, not the only thing standing between a
  connector and its own binding site. Overriding a private CDK internal fails
  with the member named, and so does a public addition of the
  dialect's own — connector helpers live under a leading underscore,
  which is what keeps stale hooks from older write paths from riding
  along unnoticed. The write primitive's own protocol,
  `StageConnection`, is implementable but is not connector surface: the
  transports that satisfy it are the CDK's, so a connector growing its
  members fails as the public addition it is.
- **Declared and implemented agree, both ways.** A declared
  `merge_form` needs `merge_statement_sql`; a `bulk_land` override
  needs a declared `bulk_load` mechanism; a write-capable connector
  (one shipping `type-map-write.json`) needs `sql_capabilities` and
  `stage_table_sql`.
- **Every connector states its type vocabulary.**
  `definition/type-map-read.json` is what the engine canonicalizes
  discovered source types through, whatever the connector's kind: a
  database canonicalizes the native types discovery returns, an API the
  JSON `type`/`format` its endpoint fields declare.
- **Canonical types are in the published grammar.** Every literal
  canonical a rule names must belong to a family the engine can parse —
  checked for read rules whether or not the connector ships a write map,
  since a source-only connector still emits canonicals from discovery.
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

The database checks above audit a connector *class*. An API connector has
none — the CDK's generic API path executes its definition directly, and
api registry repos ship no `.py` file — so its checks audit the
definition against what that path can execute from it. Every one applies
with no connector code installed:

- **The transport materializes.** Every declared transport resolves through
  the CDK's own http resolve phase, against the connection the connector's
  `connection_contract` promises, and every `transport_type` is one the CDK
  registers. A `${secrets.api_key}` header with no input declaring
  `storage: secrets` under that name is a connection that can never be
  built. So is one reading an input the contract declares *optional with no
  default* — a user may leave it blank, and an http field resolves
  strictly. Stand-in values carry the input's declared type, so the
  resolver's type-sensitive rules apply: an object-typed input substituted
  into a URL template is refused here exactly as it would be at connect.
  And resolving is not the last word — a resolved `rate_limit` must hold
  positive values, since the rate limiter is constructed in the later build
  phase and refuses anything else.
- **The transport a read opens is the one it declares.** The API path
  materializes one connection at connect time from `default_transport`,
  which must therefore name a declared transport of type `http` — another
  http transport elsewhere does not save a non-http default. And no read
  may name a `transport_ref` other than that one: the contract allows the
  selection, the CDK's API path does not make it, so such a read would go
  out on the wrong origin with the wrong headers and still succeed.
- **The declared auth reaches the wire.** The only auth behaviour the CDK
  executes is resolving credential material into the request the transport
  opens (`authorize` / `token_exchange` / `refresh` are the control
  plane's). Checked on the default transport alone: a credential resolved
  into an auth-operation or discovery transport authenticates none of the
  requests a read sends. So `type: "none"` while reading a secret, and any
  other type while the default transport reads none, are both reported.
- **Request expressions resolve at request time.** Declared param defaults
  and the request body resolve against the request-phase scopes —
  `connection.parameters` / `selections` / `discovered` and `runtime`, and
  deliberately *not* secrets, which never cross to where per-request
  resolution runs, nor `response`, which does not exist yet. An
  unresolvable expression omits its param or field, so the request silently
  goes out without it. A request body binds against the param table
  `build_request` receives, so the probe supplies exactly what the runtime
  guarantees is in it — the resolved defaults plus the pagination- and
  replication-controlled params — each carrying its declared type, since a
  strict expression around one is type-sensitive. Replication-controlled
  params are not among them: a full-refresh stream never writes one, and an
  incremental stream's first run has no stored cursor to write.
- **A read sends the query keys it declares.** The contract lets
  `request.query` map a query key to a param; the CDK's API path does not
  materialize that map, sending every non-body param under the param's own
  name. A binding whose key matches the param it names is a harmless no-op;
  any other is a key the provider never sees, on a request that goes out
  anyway.
- **Paging resolves when the loop reads it, and survives what it must.**
  Two facts decide each field. *When*: `limit.default` and
  `page.increment_by` are resolved once, before the first request, so they
  see the request-time scopes and no response; everything else resolves per
  page, against `response.body` and `response.record_count` on top of
  those. *How hard*: a continuation — `next_cursor`, `next_url`, either
  `increment_by` — is certified against the connection the contract
  *guarantees*, because the engine rejects a step it cannot parse and ends
  the loop the moment a cursor resolves to nothing. The survivable rest
  (`limit.default` falls back to the batch size, a `stop_when` operand
  makes the predicate false) is certified against the widest connection.
  An authored page size or step must be a positive integer, whether written
  bare or as a `{"literal": …}` node; `link.next_url` must resolve to a
  string — an input declared any non-string type never becomes one — and
  must stay on the connection's origin, since the loop refuses to send the
  connection's headers to another host; and `keyset.order_by_field`
  must name a field of the record the response schema declares, since the
  loop reads it from each page's last record before yielding the page.
- **The records ref addresses the declared schema, and the schema builds.**
  The engine builds the Arrow schema it emits by walking `response.schema`
  along `response.records.ref`, resolving every record field's `arrow_type`
  through `type-map-read.json`, and constructing the record schema from the
  result. A ref naming a field the schema does not declare, a field whose
  JSON `type`/`format` has no rule in the read map, and a hand-annotated
  `arrow_type` that does not parse all fail the read before its first
  request.

**Tier 2 — live tests** (`cdk.conformance.tier2`, the connector's
system as a CI service container): all three write modes end-to-end
through `connect` / `configure_schema` / `write_batch`, read-back and
incremental resume through `read_batches`, and replay — each phase on a
fresh connector instance over a fresh connection, so every test also
certifies a restart. A replayed batch must leave the target unchanged
for the exactly-once modes. For a connector declaring a bulk mechanism,
the same batch is landed once through the declared mechanism and once
through executemany (a probe whose declaration is doctored to no bulk),
and the two targets must be identical — landing is a pure speed slot, certified
where it can actually execute (native bulk protocols cannot run against
generic fakes, so this assertion lives in the live tier, not the
contract tier).

Cloud warehouses with no containerizable server (Snowflake, BigQuery,
Redshift) run tier 1 only; that is an accepted residual risk.

## What it cannot assess, it does not pass

Every behavioural check is scoped to a kind — it renders SQL through a
dialect, drives the write primitive, or resolves an API definition.
Pointed at a connector of a kind the suite carries no checks for, it
would collect nothing but skips and still exit zero, reporting *not
assessed* as *passed*. That is the one outcome a required status check
must never produce, and the fix belongs in the kit rather than in a kind
branch in every connector repo's CI.

So a run that collects no check for its target's kind fails, naming it:

```
[kind-applicability] no check in this run applies to connector kind
'stdout', so this connector is ungated: the checks collected here apply
to kind 'api', 'database'.
```

A check module states the kinds it applies to once (`APPLIES_TO_KINDS`),
and that single statement does both jobs: it skips the module for every
other kind, and it is what the run reads back to decide whether it
assessed anything. A module of checks for a new kind therefore clears
the gate for that kind the day it lands, with no list anywhere to keep
in step.

Until the suite carries checks for a kind, a connector of that kind has
no conformance gate to wire: its tier-1 job is red, and red is the
accurate report.

## Wiring a connector repo

The suite needs three inputs: the connector checkout
(`--connector-dir`, holding `definition/connector.json`), the connector
class (resolved from the installed package's entry points; overridable
with `--connector-class package.module:Class`), and — for tier 2 — a
live connection document (`--live-connection`). The flags come from an
options plugin loaded explicitly (`-p cdk.conformance.plugin` — it is
deliberately not a `pytest11` entry point, so installing the CDK never
changes unrelated pytest runs); each option doubles as an environment
variable (`ANALITIQ_CONNECTOR_DIR`, `ANALITIQ_CONNECTOR_CLASS`,
`ANALITIQ_LIVE_CONNECTION`), so a plugin-less run works identically.

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
          pip install "analitiq-cdk[conformance]==<pinned-version>"
          pip install .
      - name: Tier 1 (contract)
        run: >-
          pytest -p cdk.conformance.plugin
          --pyargs cdk.conformance.tier1 --connector-dir .
      - name: Tier 2 (live)
        env:
          CONFORMANCE_DB_PASSWORD: conformance
          ANALITIQ_CONFORMANCE_REQUIRE_LIVE: "1"
        run: >-
          pytest -p cdk.conformance.plugin
          --pyargs cdk.conformance.tier2 --connector-dir .
          --live-connection ci/live-connection.json
```

Systems without a service container drop the tier-2 step; the tier-1
step is mandatory in every connector repo whose kind the suite assesses. A job that *does* provision
a container should also set `ANALITIQ_CONFORMANCE_REQUIRE_LIVE=1`
(as the snippet's engine-side counterpart does): with it, a missing
live connection fails the job instead of skipping, so a typo'd
variable can never silently retire the live tier while CI stays green.

An API connector repo runs the tier-1 step and nothing else: there is no
class to install, no service container to provision, and no live tier.

```yaml
      - name: Install the pinned CDK with the suite
        run: pip install "analitiq-cdk[conformance]==<pinned-version>"
      - name: Tier 1 (contract)
        run: >-
          pytest -p cdk.conformance.plugin
          --pyargs cdk.conformance.tier1 --connector-dir .
```

The checks are also plain importable functions
(`cdk.conformance.check_override_surface`,
`check_declaration_consistency`, `check_type_map_grammar`,
`check_type_map_round_trip`, `check_api_transport`, `check_api_auth`,
`check_api_request_expressions`, `check_api_pagination`,
`check_api_response_records`) for repos that want them inside their own
harness.

## How the kit itself is certified

`tests/conformance_kit/` in the engine repo runs the kit against a
postgres-shaped reference connector on the sanctioned v2 surface
(`tests/conformance_kit/reference_connector.py`): tier 1 passes in
every CI run; tier 2 passes against the `conformance-live` postgres
service container job in `ci.yml`; and `test_kit_breaks.py` proves that
a bent hook signature, a private-internal override, an
undeclared-capability use, and a declared-but-unimplemented capability
each fail with a message naming the offending member.

An API reference connector (`tests/conformance_kit/fixtures/api/`) does
the same for the API checks, with no class installed at all: tier 1 runs
green against it, and each deliberate break — an undeclared secret in a
header, `auth: none` beside a credential read, a param default reading
secrets, a `stop_when` on a scope no page carries, a records ref the
response schema does not declare — fails with the offending path named.
A `stdout` fixture holds the other half of the invariant: a kind the
suite carries no checks for fails on applicability alone.
