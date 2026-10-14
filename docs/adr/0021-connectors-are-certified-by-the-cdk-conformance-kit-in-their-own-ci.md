---
schemaVersion: 0.2.0
id: "0021"
title: "Connectors are certified by the CDK conformance kit in their own CI"
status: accepted
date: 2026-07-24
deciders: ["@Analitiq-Bot-Wonka"]
tags: [conformance, connector-packages, testing]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0016", "0019", "0020"]
affects:
  - type: path
    pattern: "cdk/cdk/conformance/**"
provenance:
  authoredBy: human
  sourceArtifact: docs/testing/conformance-kit.md
---

# ADR-0021: Connectors are certified by the CDK conformance kit in their own CI

## Context

Connectors are released independently of the CDK and of each other, and anyone can author one. A CDK change can break a connector, and a connector change can break its contract; either has to turn something red before release, not in a customer pipeline. Some behaviour (bulk landing, replay, incremental resume) is only certifiable against the real system, and a public CI holds no credentials for cloud warehouses or API providers.

## Decision

We ship the acceptance suite in the CDK as `cdk.conformance`, installed with the `conformance` extra, and every connector repo runs it in its own CI against its pinned CDK. Tier 1 (`cdk.conformance.tier1`) needs no live system and is mandatory for every kind the suite assesses. Tier 2 (`cdk.conformance.tier2`) runs the write modes, read-back, resume and replay against the connector's system in a CI service container, where a containerizable server exists. Cloud warehouses (Snowflake, BigQuery, Redshift) are certified at tier 1 only, as an accepted residual risk. Kind `api` has no live tier (`NO_LIVE_TIER`): its tier-2 run skips naming the reason, and tier 1 executes its read path from the endpoint documents. Apart from that exemption, a run that collects no check for its target's kind fails rather than passing.

## Options considered

### Option A: the kit in the CDK, run in each connector's CI (chosen)
- Pro: breakage shows in the connector's own CI at its pinned CDK; scales to user-authored connectors.
- Con: every connector repo carries the CI wiring.

### Option B: central certification in engine CI over all connectors
- Pro: one place to look.
- Con: couples engine and connector releases and cannot reach connectors the engine team never sees.

### Option C: a stub HTTP server as the live tier for api connectors
- Pro: a tier-2 job for every kind.
- Con: certifies the connector's own fixtures, not the provider, and reads green.

### Option D: live cloud-warehouse tests in CI with provider credentials
- Pro: closes the warehouse gap.
- Con: credentials and cost in public CI.

## Trade-offs

Warehouse write regressions and API provider-side behaviour are never live-certified by the kit; they surface only in end-to-end runs. Tier 1 has to go deep enough on the read path to carry api connectors alone. The kit resolves the connector class through the same `KIND_DEFAULTS` table the engine registry seeds ([ADR 0019](0019-resolve-a-connector-by-kind-then-connector-id-with-a-generic-class-per-kind.md)), so what it audits is what production loads.

Known exceptions in code:
- Tier 2 applies only to `kind: database` (`APPLIES_TO_KINDS`). Kinds with no tier-1 checks (`file`, `s3`, `stdout`) fail the kind-applicability gate, so "mandatory tier 1" means a red job for them until checks exist.
- Which database systems run tier 1 only is set by each connector repo's CI wiring: without a live connection, the live checks skip.

## Consequences

- Easier: a CDK release that breaks a connector is caught in that connector's CI.
- Harder: every new kind needs its own check modules before its connectors can pass.
- **How we would know this was wrong:** connectors green in the kit failing in customer pipelines on behaviour tier 1 claims to cover.
- Revisit if: containerizable emulators or credentialed CI become available for warehouses or API providers.
