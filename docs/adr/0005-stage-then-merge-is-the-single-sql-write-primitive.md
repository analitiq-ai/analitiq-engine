# ADR 0005: Stage-then-merge is the single SQL write primitive

Every SQL write, on every transport, every dialect, every write mode,
lands the batch in a stage table first and then runs exactly one mode
statement from stage to target. There is no second write shape.

## The failure mode this closes off

The alternative is the one every per-dialect SQL layer drifts toward
without deciding to: the same author intent — "upsert this batch" —
taking a different primitive depending on which transport happened to
run it. A direct dialect statement on one transport, a stage table plus
`MERGE` on another. Once that split exists, every write-path rule has to
be stated twice, the two copies drift, and each new database multiplies
the divergence instead of adding to it. Per-dialect divergence was the
fastest-growing defect class this design replaces, and its mechanism was
always the same: a guessed base-class default, right for the database
family it was written against and silently wrong for the next one.

Stage-then-merge removes the fork before it can start. Because insert,
upsert, and truncate_insert are each exactly one statement from stage to
target, and because both the SQLAlchemy and ADBC backends execute the
identical plan through one shared `StageCycle`, there is no transport
axis a rule could fork along. A system with a native bulk protocol
reaches it through a declared, conformance-certified hook
(`bulk_land`) — never by overriding CDK internals, which is the coupling
that breaks silently on the next refactor.

## Why the facts are declared, not guessed

What a target system can do — its merge form, whether it has a usable
session-temp stage, its bulk-load mechanism — is a fact about that
system, not something derivable from a well-behaved-looking base class.
Guessed defaults are exactly the mechanism of the divergence this design
removes, so every SQL-shape fact a connector needs is validated data in
`connector.json`: the JSON declares *whether* the system has a shape, and
a small dialect class renders *how* to write it. A needed-but-undeclared
capability refuses loudly, at config or handshake time, instead of a base
class quietly filling in a guess.

## Why the split is facade, cycle, and backend — not two write paths

`GenericSQLConnector` stays the single semantic owner of write modes,
identity rules, and retry verdicts, defined once. `StageCycle` owns the
step order once, above both transports, so a transactional shape held per
transport can never become a second copy that drifts from the first.
`SqlAlchemyBackend` and `AdbcBackend` own only mechanics — connections,
cursors, commit calls — behind one shared interface. Every rule that would
otherwise exist twice, for two transports, exists once instead.

The full mechanism — the write primitive, the capability declarations,
stage lifecycle, transaction boundaries, and what the conformance kit
certifies — is specified in [`sql-write-path.md`](../data-path/sql-write-path.md).
