---
schemaVersion: 0.2.0
id: "0019"
title: "Resolve a connector by kind, then connector_id, with a generic class per kind"
status: accepted
date: 2026-06-03
deciders: ["@Analitiq-Bot-Wonka"]
tags: [connector-registry, cdk, connector-packages]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0003", "0004", "0008", "0020"]
affects:
  - type: path
    pattern: "cdk/cdk/registry.py"
  - type: path
    pattern: "src/worker/**"
provenance:
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/architecture/connector-module-architecture.md
---

# ADR-0019: Resolve a connector by kind, then connector_id, with a generic class per kind

## Context

Most connectors are declarative config and ship no code; a quirky system ships a class. The engine has to turn a connection's `(kind, connector_id)` into a class without carrying per-system code ([ADR 0003](0003-the-cdk-is-a-toolbox-not-a-gatekeeper.md)), without keeping its own list of kinds beside the contract's, and without forcing every transport's extra onto a worker that speaks one. Connector packages are released on their own, so attaching one must not edit or rebuild the engine.

## Decision

We resolve a connector in `cdk.registry.ConnectorRegistry.resolve`: a class an installed package registered under its `connector_id` wins; otherwise the kind's CDK generic class named in `KIND_DEFAULTS` serves it (the thin path); otherwise resolution raises `ConnectorNotRegisteredError`. Packages register through the `analitiq.source_connectors` / `analitiq.destination_connectors` entry-point groups, keyed by `connector_id` and discovered additively by `build_registries`; a package that fails to load or collides on `connector_id` is logged and skipped. `KIND_DEFAULTS` holds `module:Class` strings with declared roles, imported on first resolve and checked against `Readable` / `Writable` then. Generic classes never branch on `connector_id`; a SQL connector's per-system code goes only in `dialect_class` and `classify_error`, the surface the conformance kit checks, and facts it can declare, such as an `error_map`, go in its `connector.json`.

## Options considered

### Option A: connector_id class first, kind generic class as fallback (chosen)
- Pro: declarative connectors run with no code; per-system code lives only in the connector's class.
- Con: two lookups, and two populations (entry points, kind table) to reason about.

### Option B: every connector ships a class
- Pro: one lookup.
- Con: ends declarative-first; every connector carries boilerplate.

### Option C: generic classes branch on connector_id
- Pro: no package needed for small quirks.
- Con: vendor logic in the CDK; a new system waits for a CDK release.

### Option D: a registry-owned kind list, or classes imported eagerly at build
- Pro: roles read straight off each class.
- Con: a second kind vocabulary that drifts from the contract; every worker pays every transport's import.

## Trade-offs

Log-and-skip discovery means a broken package surfaces as a fallback to the generic class or a `ConnectorNotRegisteredError` at resolve, not as a startup failure. The roles in `KIND_DEFAULTS` are declared, the one place a capability is stated rather than derived ([ADR 0004](0004-capability-is-derived-never-declared.md)); the first-load check is what keeps it honest.

Known exceptions in code:
- The registry does not stop an entry point adding a kind: `resolve` matches `connector_id` whatever the kind, and the runner passes a source kind outside `KIND_DEFAULTS` through with a warning (a destination kind, with none). The bound is the contract's closed kind enum.
- The conformance kit checks that surface for `kind: database` only; the registry loads any class.

## Consequences

- Easier: adding a connector is installing a package; a declarative connector needs none.
- Harder: per-system code outside the dialect has no sanctioned home.
- **How we would know this was wrong:** generic classes growing `connector_id` checks, or most packages shipping classes that override beyond the dialect.
- Revisit if: the contract's kind enum opens to kinds the CDK has no generic class for.
