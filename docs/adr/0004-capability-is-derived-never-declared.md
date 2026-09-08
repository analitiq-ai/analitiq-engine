# ADR 0004: Capability is derived, never declared

`connector.json` carries no capability block. Whether a connector can
discover schemas, create tables, read, or write is never a static flag a
document asserts — it is derived at each of two different moments, from
two different sources, because "can this connector do X" is actually two
unrelated questions wearing one name.

## The two questions a capability flag would conflate

**Protocol conformance** — does the connector's code implement the
operation? — is a property of the code, decided once, and derivable by
`isinstance` against the CDK's `runtime_checkable` Protocols
(`Discoverable`, `TableCreator`, `Readable`, `Writable`). It varies only
across `kind`: an API connector may not implement `TableCreator`; a
stdout destination implements only `Writable`. Code is the single source
of truth for this question, so it cannot drift from a hand-maintained
list the way a declared flag would.

**Authorization** — may *this* credential do the operation on *this*
instance? — is a property of the connection's credentials and database
grants, enforced by the database itself at runtime. A read-only role gets
a permission error on `create_table` whatever the connector's code
supports. This is never knowable ahead of the call, so it is never
declared; it surfaces as a permission error, or is checked with a
preflight probe.

A single `capabilities` block in `connector.json` would have to answer
both questions with one static assertion, and neither question is
actually static: conformance is a property of the shipped code, and
authorization is a property of a specific connection's grants, checked
each time. Declaring it once, in the document a connector's author writes
before either fact is known, would drift from the code on the first
refactor and would lie about authorization on the first credential
rotation.

## What follows, and where this claim stops

Where a consumer decides whether to *offer* an optional operation (does
this connector support discovery, so the UI should show a "browse
schemas" action), it checks the connector object against the relevant
Protocol, never a flag:

```python
if isinstance(connector, Discoverable):
    schemas = await connector.list_schemas(runtime)
```

This governs two things, and only two: the CDK's own kind defaults (`is`
this generic class really `Readable`, checked once via `issubclass` when
a kind default is loaded — see
[`engine-architecture.md`](../architecture/engine-architecture.md#connector-registries));
and any caller deciding whether to *offer* an optional capability before
trying it. It does **not** govern whether an externally-installed
connector package's declared role is honoured. That role — source,
destination, or both — is declared by which entry-point group
(`analitiq.source_connectors` / `analitiq.destination_connectors`) the
package registers under; the worker invokes the resolved class directly,
with no Protocol check at that call. A connector package that registers
under a group it doesn't actually implement fails at first invocation,
not at selection — the entry-point group is itself a declaration, just
not a static flag inside `connector.json`.

This is a different rule from the SQL-shape facts in
[`sql-write-path.md`](../data-path/sql-write-path.md) §5
(`sql_capabilities`, `merge_form`, stage scope, and the rest): those *are*
declared data, because they are facts about the target system that no
amount of reading the connector's Python can derive — nothing in the code
tells you whether Postgres supports a session-temp stage. Capability, by
contrast, is entirely readable from the code that already exists. Declaring
it anyway would be a second copy of a fact the class already states.

See [`connector-module-architecture.md`](../architecture/connector-module-architecture.md)
§4 for the contract this decision produces.
