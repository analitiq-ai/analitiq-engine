---
schemaVersion: 0.2.0
id: "0001"
title: "ADR 0001: Two failure vocabularies, one adapter"
status: accepted
date: 2026-07-31
deciders: ["@Analitiq-Bot-Wonka"]
consulted: []
informed: []
tags: []
scope: component
reversibility: unknown
blastRadius: component
supersedes: []
relatesTo: []
conflictsWith: []
affects:
  - type: path
    pattern: "cdk/cdk/api/dialects.py"
  - type: path
    pattern: "cdk/cdk/api/generic.py"
  - type: path
    pattern: "cdk/cdk/api/http.py"
  - type: path
    pattern: "cdk/cdk/api/verdicts.py"
  - type: path
    pattern: "cdk/cdk/base_handler.py"
  - type: path
    pattern: "cdk/cdk/conformance/surface.py"
  - type: path
    pattern: "cdk/cdk/declarations.py"
  - type: path
    pattern: "cdk/cdk/file/generic.py"
  - type: path
    pattern: "cdk/cdk/sql/adbc_backend.py"
  - type: path
    pattern: "cdk/cdk/sql/generic.py"
  - type: path
    pattern: "cdk/cdk/stdout/generic.py"
  - type: path
    pattern: "cdk/cdk/types.py"
  - type: path
    pattern: "proto/analitiq/v1/stream.proto"
  - type: path
    pattern: "src/config/utils.py"
  - type: path
    pattern: "src/destination/server.py"
  - type: path
    pattern: "src/engine/batch_policy.py"
  - type: path
    pattern: "src/engine/exceptions.py"
  - type: path
    pattern: "src/engine/stream_processor.py"
  - type: path
    pattern: "src/grpc/client.py"
  - type: path
    pattern: "src/runner.py"
  - type: path
    pattern: "src/state/error_classification.py"
  - type: path
    pattern: "src/worker/proxy.py"
  - type: path
    pattern: "src/worker/readable.py"
  - type: path
    pattern: "src/worker/source_service.py"
assertions: []
provenance:
  authoredBy: human
  importedFrom:
    sourceKind: madr
    sourceRef: docs/adr/0001-two-failure-vocabularies.md
    fingerprint: fcb9a0ada462655d0cefd807a5cc7fb7cef2f4907332597be85ae4ba27f21bba
externalRefs: []
complianceControls: []
---
# ADR 0001: Two failure vocabularies, one adapter

The engine carries two enums for failure. `FailureCategory` crosses the wire on
a `BatchAck` or a `SchemaAck`; `ErrorCode` reaches the customer through the
run-status endpoint. They are not merged, and the mapping between them runs one
way only: `code_for_declared_category` turns a declared category into a code,
and nothing turns a code back into a category. A connector's error
declarations, such as its `error_map`, use a third vocabulary, the
contract's `ErrorCategory`, and it too maps one way only. On a write,
`DECLARED_WRITE_VERDICTS` turns it into a `FailureCategory`, which then
reaches a code through `code_for_declared_category`. On a read, where no
`FailureCategory` exists, `source_code_for_declared_category` turns it into a
code directly.

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
way, `RATE_LIMITED` and `SOURCE_AUTH_FAILED` have no `FailureCategory`
counterpart, because no destination write attempt can establish them. Only a
source's declared `rate_limited` or `auth` category names them. A destination
that declares `rate_limited` or `auth` gets `WRITE_REJECTED` or
`CONFIG_DEFECT` respectively.

This is the split Airbyte draws between the protocol's `failure_type`, which a
connector emits, and the platform's own failure surfaces, which it does not.

## How a code is chosen

In order, and with no step that inspects an exception's type or message:

1. **A declared category**, when the sender set one. It is range-checked off the
   wire. An unrecognised value is logged and counts as no declaration: a
   `FailureCategory` integer degrades to `UNSPECIFIED`, and a source category
   string names no code. A declaration is signal without being trust.
2. **A fact the raising site observed**, when nothing was declared. A source
   worker's `deterministic` flag names `CONFIG_INVALID`. A failed destination
   handshake names the code for its `SchemaHandshakeOutcome`, via
   `classify_handshake_failure`. A config file the engine could not read names
   `INTERNAL`. `tag_failure` never overwrites, so the stage boundary that
   follows cannot replace this answer.
3. **The raising stage's own default**, via `default_code_for_stage`. Every
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
