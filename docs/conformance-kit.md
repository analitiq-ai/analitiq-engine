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
- **The read path compiles.** For a database, the CDK's QueryBuilder
  resolves the connector's dialect flavour (`sqlalchemy_registry_name`)
  against the connector's own installed requirements, and cursor reads
  order by the cursor field — the precondition for monotonic checkpoints.
- **The api read path runs, with nothing sent.** An api read compiles into
  a `PageRequest` the way a database read compiles into SQL, so each
  endpoint document is driven as far as a definition can be driven: the
  first request is built — path placeholders substituted, declared query
  and headers bound — a scripted page is advanced past, the request *after*
  that page is built too, the author's `stop_when` is evaluated against the
  page, and the declared records become an Arrow schema. Every drive goes
  through `build_read_strategy`, `RequestBuilder` and `stop_condition` —
  the same functions the read itself calls — so a paging scheme with
  nowhere to go, a next link the engine's own `follow_url` refuses, a body
  that binds on page one and not on page two, or a stop condition that
  raises mid-traversal fails here rather than in a pipeline. Nothing is
  fetched and no HTTP client is needed, which is why the `conformance`
  extra pulls no transport.

  What a definition cannot supply is deferred rather than guessed at or
  reported. The engine fills a path placeholder from a param default
  resolved against a real connection, from the stream's filters, or from
  the replication cursor, and substitutes the path only once the
  incremental filter has bound; a definition-only run has none of the
  three, so a placeholder bound to a declared param gets a stand-in segment
  and the drives carry on. Only a placeholder nothing could ever bind is a
  finding: one with no binding at all, one bound to a param the endpoint
  does not declare, and one bound to an expression no run fills — it reads
  no scope at all, or it reads `secrets`/`auth`, which request-time
  resolution never supplies (they resolve once, engine-side, at transport
  materialization). Each fails for every connection and every stream. The
  same shape governs every deferral, with the scope set matched to its
  phase: a request slot defers only what `connection.*` supplies, the
  transport's `base_url` and headers defer what materialization supplies
  (connection, secrets, auth), and in both phases the node's grammar is
  judged always, a mixed node is refused by the path no phase supplies,
  and what a definition settles by itself is resolved rather than
  deferred.
- **There is a read to drive, on transports that can open.** Every api
  check iterates the read operations, so a connector shipping none — or
  only write-only endpoints — would satisfy all of them by having nothing
  to fail, and the applicability gate would not notice because those
  modules do apply to the kind. That is the kit's own founding rule one
  level down, so it is a failure.

  So is a transport a read dispatches through that is not `http`, or
  whose `base_url` does not resolve to a non-empty string when it reads
  no connection scope — the `default_transport` and every transport a
  request's `transport_ref` names. The two differ in what they stop, and
  the finding says which: the default is opened at connect time, so no
  stream reaches its first request without it, while a named one is
  opened by the first read that dispatches through it and stops exactly
  those reads.

  Whether a named `transport_ref` resolves to a transport the sibling
  connector.json declares is *not* checked here. It is decidable from the
  two documents alone, which makes it the package validator's
  `endpoint-transport-ref`; a second, differently worded verdict would
  give the author two findings for one defect.

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

There is no live tier for `kind: api`, and that is a statement about the
tier rather than a gap in it. The live tier's whole value is a round trip
against the real system: a public CI carries no provider credentials, and
a stub HTTP server would certify the connector's own fixtures rather than
the provider — worse than nothing, because it reads green. So an api
connector's tier-2 run skips, naming that reason, and the applicability
gate below does not fire. Kind `api` is assessed in full at tier 1, where
the read path is executed.

## What it cannot assess, it does not pass

Every behavioural check gates itself on the connector kinds it applies
to. Pointed at a connector of a kind nothing covers, the suite would
collect nothing but skips and still exit zero, reporting *not assessed*
as *passed*. That is the one outcome a required status check must never
produce, and the fix belongs in the kit rather than in a kind branch in
every connector repo's CI.

So a run that collects no check for its target's kind fails, naming it:

```
[kind-applicability] no check in this run applies to connector kind
'file', so this connector is ungated: the checks collected here apply to
kind 'api', 'database'.
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
class, and — for tier 2 — a live connection document
(`--live-connection`).

The class is resolved the way the engine registry resolves it, from the
same `cdk.registry.KIND_DEFAULTS` table, so what the suite audits is what
production loads: an explicit `--connector-class package.module:Class`
wins (for running the suite before the package is installed), then the
installed package's entry points, then the CDK's generic default for the
connector's kind — the thin path, for every kind the CDK ships a default
for. Both entry-point groups are read and must name the same class; a
connector that registers a different class per role is refused at load,
because a split there is how the two directions drift apart while the
suite stays green. A kind the CDK ships no default for resolves no class
and its class-level checks skip, so a genuinely new kind loads rather
than failing. A kind whose default the install carries no transport for
resolves no class either, and the reason travels on the target so a check
that needs the class reports "not installed here" naming the extra —
rather than skipping as though the kind were inapplicable. The api checks
never ask for the class: they read the endpoint documents and drive the
CDK's own read path, so they answer the same verdict whether or not a
connector package is installed. The flags come from an
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

An api connector's job is the tier-1 step alone, and it needs no service
container and no `[api]` extra: the suite carries no live tier for the
kind, and the checks it does carry run from the definition with no HTTP
client installed.

Systems without a service container drop the tier-2 step; the tier-1
step is mandatory in every connector repo whose kind the suite assesses. A job that *does* provision
a container should also set `ANALITIQ_CONFORMANCE_REQUIRE_LIVE=1`
(as the snippet's engine-side counterpart does): with it, a missing
live connection fails the job instead of skipping, so a typo'd
variable can never silently retire the live tier while CI stays green.

The checks are also plain importable functions
(`cdk.conformance.check_override_surface`,
`check_declaration_consistency`, `check_type_map_grammar`,
`check_type_map_round_trip`) for repos
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

An api-shaped reference connector (one endpoint document per paging
scheme) does the same job for the api drives: a bent document must fail
the drive that executes it — an unknown paging scheme, a path placeholder
nothing could bind, a header the connection's transport declares, a stop
condition written the wrong way round, a body that builds on page one and
not on page two. The
clean cases are pinned just as hard, because a check that fails a correct
connector is the more expensive defect: a link the connector derives into
a relative URL, a base URL the connection supplies, a path segment a
stream's filters supply, a stop operand the response schema reaches but
types only through composition.

A refusal the engine itself enforces needs one test more. Every connector
the kit ships passes the origin guard and the keyset guard, because the
engine refuses before the kit can judge — so "the check found nothing"
says the same thing whether the drive ran or was never armed, which is
the silent non-coverage the drives exist to remove. Each of those drives
is therefore also pointed at a traversal whose guard has been taken out
from under it, and required to report. What replaces a guard still reads
what the drive planted — the link stand-in follows the URL it was handed,
and the keyset stand-in substitutes a value only where the record carried
none — so taking the planting away fails those tests, which is the whole
point of writing them.
