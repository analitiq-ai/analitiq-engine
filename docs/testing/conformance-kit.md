# Connector Conformance Kit

The CDK ships an acceptance suite for connector packages
(`cdk.conformance`, installed with the `conformance` extra), run in a
connector repo's own CI (see
[ADR 0021](../adr/0021-connectors-are-certified-by-the-cdk-conformance-kit-in-their-own-ci.md)).
What it asserts about the SQL write primitive is in
[sql-write-path](../data-path/sql-write-path.md) §10.

## What each tier certifies

**Tier 1 — contract tests** (`cdk.conformance.tier1`, no live database):

- **Rendering matches declaration.** The stage DDL carries the
  temporary form iff `sql_capabilities.stage.scope` is `temp`; the
  upsert statement carries exactly the declared `merge_form` and states
  the stream's `conflict_keys` where that form names its match keys; the
  target-emptying statement is DELETE-shaped, never `TRUNCATE`;
  identical batches build identical plans (self-healing retries) and
  distinct batches never share a stage name.
- **Refusals fire.** Upsert with empty `conflict_keys`, and catalog
  addressing without a declaration (or against `catalog: "none"`), both
  raise (see
  [ADR 0005](../adr/0005-stage-then-merge-is-the-single-sql-write-primitive.md)).
- **The override surface is the sanctioned one** (see
  [ADR 0019](../adr/0019-resolve-a-connector-by-kind-then-connector-id-with-a-generic-class-per-kind.md)).
  The dialect's public namespace is exactly the public `SqlDialect`
  surface, minus the framework-owned members (`capabilities`,
  `for_runtime`, `table_address`), with base-compatible signatures. The
  two that decide the declaration — `capabilities` and `for_runtime` — are
  refused by `SqlDialect` itself where the subclass is written, so the
  kit's finding is a backstop for the rest, not the only thing standing
  between a connector and its own binding site. Overriding a private CDK
  internal fails with the member named, and so does a public addition of
  the dialect's own; connector helpers live under a leading underscore.
  The write primitive's own protocol, `StageConnection`, is implementable
  but is not connector surface: the transports that satisfy it are the
  CDK's, so a connector growing its members fails as the public addition
  it is.
- **Declared and implemented agree, both ways.** A declared `merge_form`
  needs `merge_statement_sql`; a `bulk_land` override needs a declared
  `bulk_load` mechanism; a write-capable connector (a database connector
  that ships a write type map) needs a `sql_capabilities` block and
  `stage_table_sql` (see
  [ADR 0005](../adr/0005-stage-then-merge-is-the-single-sql-write-primitive.md)).
- **Every connector states its type vocabulary.**
  The read map is what the engine maps discovered source types through,
  whatever the connector's kind: a database maps the `native_type`s
  discovery returns, an API the JSON `type`/`format` its endpoint fields
  declare.
- **Type maps are round-trip stable.** Every `native_type` the write map
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

  A declared param's value keywords travel with the table every request is
  built from, so the kit judges them exactly as the run does: a value the
  author's own `enum`, `pattern`, bound or length rules do not admit is a
  finding wherever it comes from. Presence is the one thing it does not
  judge -- a definition-only run has no connection and no secrets, so a
  `required` param resolving to nothing here says nothing about the run.

  What a definition cannot supply is deferred rather than guessed at or
  reported. The engine fills a path placeholder from a param default
  resolved against a real connection, from the stream's filters, or from
  the replication cursor, and substitutes the path only once the
  incremental filter has bound; a definition-only run has none of the
  three, so a placeholder bound to a declared param gets a stand-in segment
  and the drives carry on. A placeholder is a finding when it
  is bound to an expression no run fills — it reads no scope at all, or it
  reads `secrets`/`auth`, which request-time resolution never supplies (they
  resolve once, engine-side, at transport materialization) — or when the
  definition settles it to an empty string, which the path substitution
  refuses. Either fails for every connection and every stream. The
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

**Tier 2 — live tests** (`cdk.conformance.tier2`, the connector's system
as a CI service container; see
[ADR 0021](../adr/0021-connectors-are-certified-by-the-cdk-conformance-kit-in-their-own-ci.md)):
`insert`, `truncate_insert` and, unless the connector declares `merge_form`
`none`, `upsert`, end-to-end through `connect` / `configure_schema` /
`write_batch`, read-back and incremental resume through `read_batches`,
and replay — each phase on a fresh connector instance over a fresh
connection, so a test that spans two phases also certifies a restart. A
replayed batch must leave the target unchanged for the exactly-once modes.
For a connector declaring a bulk mechanism for the transport the live
connection uses, the same batch is written once with that declaration in
place and once through executemany (a probe whose declaration is doctored
to no bulk), and the two targets must be identical (see
[ADR 0005](../adr/0005-stage-then-merge-is-the-single-sql-write-primitive.md));
native bulk protocols cannot run against generic fakes, so this check is
live-tier only.

Which systems run tier 1 only, and why `kind: api` has no live tier, is
recorded in
[ADR 0021](../adr/0021-connectors-are-certified-by-the-cdk-conformance-kit-in-their-own-ci.md).
Kind `api` is
assessed at tier 1, where the read path is executed.

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
(`--connector-dir`), the connector
class, and — for tier 2 — a live connection document
(`--live-connection`).

The class is resolved from the same `cdk.registry.KIND_DEFAULTS` table the
engine registry reads (see
[ADR 0019](../adr/0019-resolve-a-connector-by-kind-then-connector-id-with-a-generic-class-per-kind.md)),
but the precedence order differs from the engine's: an explicit
`--connector-class package.module:Class` wins (for running the suite
before the package is installed), then the installed package's entry
points, then the CDK's generic default for the connector's kind — the thin
path, for every kind the CDK ships a default for. (The engine registry
itself has no `--connector-class` override; see
[`connector-module-architecture.md`](../architecture/connector-module-architecture.md)
for its own seeding order.) Both entry-point groups are read, and a class
registered in both must be the same class (see
[ADR 0020](../adr/0020-one-connector-class-serves-both-read-and-write.md)). With
no explicit class and no entry point, a kind the CDK ships no default for
resolves no class, and so does a kind whose default the install carries no
transport for. The api checks never use
the class: they read the endpoint documents and drive the CDK's own read
path. The flags come from an options plugin loaded explicitly (`-p
cdk.conformance.plugin` — it is deliberately not a `pytest11` entry point,
so installing the CDK never changes unrelated pytest runs); each option
doubles as an environment variable (`ANALITIQ_CONNECTOR_DIR`,
`ANALITIQ_CONNECTOR_CLASS`, `ANALITIQ_LIVE_CONNECTION`), so a plugin-less
run works identically.

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

An api connector's job needs no service container and no `[api]` extra: it
has no tier-2 step (see
[ADR 0021](../adr/0021-connectors-are-certified-by-the-cdk-conformance-kit-in-their-own-ci.md)),
and the tier-1 checks run from the definition with no HTTP client
installed.

Systems without a service container drop the tier-2 step (see
[ADR 0021](../adr/0021-connectors-are-certified-by-the-cdk-conformance-kit-in-their-own-ci.md)).
A job that *does* provision a container should also set
`ANALITIQ_CONFORMANCE_REQUIRE_LIVE=1` (as the snippet's engine-side
counterpart does): with it, a missing live connection fails the job
instead of skipping.

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
