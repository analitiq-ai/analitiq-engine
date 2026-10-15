---
schemaVersion: 0.2.0
id: "0023"
title: "Customer error text is built only from allowlisted tokens"
status: accepted
date: 2026-06-24
deciders: ["@Analitiq-Bot-Wonka"]
tags: [error-reporting, metrics, security]
scope: component
reversibility: two-way-door
blastRadius: cross-team
relatesTo: ["0001", "0008", "0022"]
affects:
  - type: path
    pattern: "src/state/error_classification.py"
  - type: path
    pattern: "src/state/metrics_storage.py"
provenance:
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/architecture/engine-architecture.md
---

# ADR-0023: Customer error text is built only from allowlisted tokens

## Context

A failed run reports why it failed in the `PipelineMetricsRecord` it logs at INFO as a metrics marker ([ADR 0007](0007-the-engine-carries-no-cloud-sdk.md)), and the control plane forwards part of that record to external, API-key customers. The terminating exception is the obvious source of that text, but its message is written by connectors and drivers: untrusted, possibly AI-authored code (see [ADR 0008](0008-connector-code-runs-only-in-an-isolated-worker.md)). That text can embed query fragments, driver internals, or post-expansion connection values such as a password inside a DSN. Before this decision the record carried `str(exception)`, and the next step was a regex scrubber over that text; a scrubber only removes the credential shapes someone thought to list.

## Decision

We build `error_message` and `error_detail` from fixed engine strings, stage and code tokens, and exception class names, never from exception message text, so there is no message text to scrub. The allowlist is of token kinds: class names are emitted as they are, not checked against a list.

- `error_message` is `customer_message(code)`: one fixed message per `ErrorCode`.
- `error_detail` is `build_error_detail(exc)`: per member of the exception chain, a `stage/CODE:ExceptionType` token from the `FailureTag` stamped by `tag_failure`, or the bare class name (`type(member).__name__`) when untagged. It is capped in length and is internal-only; the control plane must not forward it.
- The pipeline-level record (runner) and the stream-level record (stream processor) both classify a terminating exception with `classify_for_metrics`, which returns `(error_code, error_message, error_detail)`.
- The full message and traceback are logged at ERROR, never put into the record.

Known exception: a partial run with no terminating exception builds `error_detail` with `detail_for_code`, which appends a fixed, developer-authored reason phrase (for example "records dead-lettered after retries") to the stage and code. The phrase is engine text, never connector or driver text.

## Options considered

### Option A: Allowlisted tokens only (chosen)
Pro: no exception message text enters either field. Con: the record cannot say more than stage, code and exception class.

### Option B: Forward `str(exception)`
Pro: maximum diagnostic detail. Con: leaks whatever the connector or driver put in the message, credentials included.

### Option C: Forward the message after redacting credential shapes
Pro: keeps most of the diagnostic text. Con: incomplete by nature; a credential in a shape the patterns miss reaches the record, and every new driver brings new shapes.

## Trade-offs

Debugging a failed run needs the engine logs, which hold the exception message and traceback the record leaves out. Class names are emitted verbatim, so the guarantee is "no message text", not "no free-form identifiers": they are the classes of exceptions in the engine process, library classes included. Customers get one fixed message per code, so their message is only as precise as the `ErrorCode` set, which is coordinated with the control plane's catalog (see [ADR 0001](0001-two-failure-vocabularies.md)).

## Consequences

- Easier: adding a connector or driver never needs a new redaction rule; reviewing a leak means reading one table of fixed strings, one token builder and the reason phrases passed to `detail_for_code`.
- Harder: support cannot diagnose from the run status alone; any richer customer message must be a new fixed per-code string or a new code.
- **How we would know this was wrong:** operators routinely cannot act on `error_detail` and pull logs for most failures, or customers repeatedly ask for detail the fixed messages cannot give.
- Revisit if: the engine gains a structured, engine-validated failure field (for example a declared error_map entry name, see [ADR 0022](0022-connectors-declare-driver-failures-as-facts.md)) that could be added to the token set without admitting free text.
