---
schemaVersion: 0.2.0
id: "0020"
title: "One connector class serves both read and write"
status: accepted
date: 2026-08-01
deciders: ["@Analitiq-Bot-Wonka"]
tags: [connector-registry, cdk, connector-packages]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0015", "0019", "0021"]
affects:
  - type: path
    pattern: "cdk/cdk/sql/generic.py"
  - type: path
    pattern: "cdk/cdk/api/**"
  - type: path
    pattern: "cdk/cdk/registry.py"
  - type: path
    pattern: "cdk/cdk/conformance/target.py"
provenance:
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/config/destination-config.md
---

# ADR-0020: One connector class serves both read and write

## Context

The engine keeps one registry per role, and a package registers under one entry-point group per role, so nothing structural stops a system from having a reader class and a writer class. When the API family had two such classes they drifted on separate questions: which statuses count as success, which a retry can heal, whether a declared error mapping is honoured, whether a decimal survives the body, what an empty records path means. Each of those is a property of HTTP or of the endpoint document, not of the direction data flows.

## Decision

We give each connector family one CDK class that serves both roles: `GenericSQLConnector` for databases over both transports, `GenericAPIConnector` for APIs, each listed in `KIND_DEFAULTS` with roles `("source", "destination")`. What differs between reading and writing an API is declared in the endpoint document's `operations` block (`operations.read`, `operations.write.<mode>`) and run by role methods on the one class. A connector package registers the same class in both entry-point groups, and the conformance kit refuses at load a package whose two groups name different classes.

## Options considered

### Option A: one class per family, both roles (chosen)
- Pro: auth and request binding are written once, so read and write cannot disagree on them.
- Con: one larger class that must satisfy both Protocols.

### Option B: separate source and destination classes per family (Airbyte/Singer style)
- Pro: each class is smaller and a one-direction connector carries no code for the other.
- Con: every shared mechanism exists twice and the copies drift, which is what happened to the API family.

## Trade-offs

A change to a shared mechanism reaches both directions at once, for good or ill. Direction-specific behaviour has to be expressible as endpoint-document data or as role methods on the one class.

Known exceptions in code:
- The engine registry does not refuse a split: `build_registries` fills two registries from two groups and would resolve different classes per role. Only the conformance kit's target loading checks the rule, and only when both groups name a class.
- A package that registers its class in one group only passes the kit, which audits that class; production resolves the `KIND_DEFAULTS` generic class for the other role. A run given `--connector-class` reads no entry points at all.
- `GenericSQLConnector` opens its connection on two paths, `connect` for writes and inline in `read_batches` for reads, so the same connect failure surfaces as `ConnectionError` on one and `ReadError` on the other.
- Kinds whose generic class serves one role (`file`, `s3`, `stdout` are destination-only) have one class for that role; the rule binds families that serve both.

## Consequences

- Easier: a fix to auth or request binding lands in both directions in one change.
- Harder: a system whose read and write genuinely need different code must hold both in one class and one dialect.
- **How we would know this was wrong:** role branches accumulating inside the generic classes for behaviour the endpoint document cannot express.
- Revisit if: a family appears whose read and write share no transport or classification logic.
