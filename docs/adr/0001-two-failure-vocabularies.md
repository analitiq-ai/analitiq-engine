# ADR 0001: Two failure vocabularies, one adapter

The engine carries two enums for failure. `FailureCategory` crosses the wire on
a `BatchAck` or a `SchemaAck`; `ErrorCode` reaches the customer through the
run-status endpoint. They are not merged, and the mapping between them runs one
way only: `code_for_declared_category` turns a declared category into a code,
and nothing turns a code back into a category.

## What each one is for

`FailureCategory` is what an untrusted peer says about one write attempt. Its
sender may be a CDK base class, an engine-owned handler, or a connector — code
that a customer supplied and an AI may have authored. Its members answer *who
owns the fix* for that attempt: a user-fixable configuration defect, a write the
destination attempted and rejected, a handler that was never ready, or a bug in
the engine or the connector.

`ErrorCode` is what the customer is told about the whole run. It is a published
contract, coordinated with the control plane's error-code catalog, and it names
a run-level outcome: source authentication, source reachability, rate limiting,
destination write failure, invalid configuration, internal fault.

## Why they stay separate

**Trust.** A destination handler must not be able to declare that the *source*
rejected the pipeline's credentials, and merging the enums would give it exactly
that. Half of `ErrorCode` is source-side. Keeping the vocabularies apart means
the worst a defective or hostile destination connector can do is misreport its
own write, and the engine still owns every code the customer sees.

**Scope.** A category describes one batch; a code describes a run. A run that
dead-letters some batches and completes reports `partial` with a dominant code
across many categories — a relationship that only exists because the two are
different kinds of thing.

**Coverage.** `NOT_READY` and `WRITE_REJECTED` drive different dispositions and
have no separate `ErrorCode` counterpart: both resolve to `INTERNAL` and
`DESTINATION_WRITE_FAILED` respectively, and `NOT_READY`'s distinction — that
nothing was attempted at all — would be lost in a merged enum. Going the other
way, `RATE_LIMITED` and `SOURCE_AUTH_FAILED` have no category counterpart,
because no destination write attempt can establish them.

This is the split Airbyte draws between the protocol's `failure_type`, which a
connector emits, and the platform's own failure surfaces, which it does not.

## How a code is chosen

In order, and with no step that inspects an exception's type or message:

1. **A declared category**, when the sender set one. It is range-checked off the
   wire; an unrecognised integer degrades to `UNSPECIFIED` and is logged, so a
   declaration is signal without being trust.
2. **The raising stage's own default**, via `default_code_for_stage`. Every
   stage boundary tags unconditionally, so this always answers.

The stage defaults are asymmetric on purpose. A destination-load failure defaults
to `DESTINATION_WRITE_FAILED` because the stage establishes the whole of that
claim: the write did not happen. A source-extract failure defaults to `INTERNAL`,
not to `SOURCE_UNREACHABLE`, because each source-side code names a *mechanism* —
the host did not answer, the credentials were refused, the quota ran out — and
the stage establishes none of them. A connector that declares an `error_map`
supplies the mechanism; one that does not leaves a gap, and `INTERNAL` reports
the gap instead of papering over it with the most likely-looking cause. The side
is not lost either way: it rides the failure tag's stage label into
`error_detail`, which reads `source_extract/INTERNAL:ReadError`.

An exception that reaches `classify_exception` with no tag at all is a raise site
missing its boundary. That is an engine defect, logged at ERROR and reported as
`INTERNAL` — findable, rather than absorbed into a plausible customer-facing
code.
