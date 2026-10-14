---
schemaVersion: 0.2.0
id: "0008"
title: "Connector code runs only in an isolated worker; secrets and transports resolve engine-side"
status: accepted
date: 2026-06-04
deciders: ["@Analitiq-Bot-Wonka"]
tags: [connector-isolation, secrets, worker, security]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0007", "0022"]
affects:
  - type: path
    pattern: "src/worker/**"
  - type: path
    pattern: "cdk/cdk/connection_runtime.py"
  - type: path
    pattern: "cdk/cdk/transport_factory.py"
  - type: path
    pattern: "cdk/cdk/secrets/**"
provenance:
  authoredBy: human
  sourceArtifact: docs/architecture/engine-architecture.md
---

# ADR-0008: Connector code runs only in an isolated worker; secrets and transports resolve engine-side

## Context

Connectors are untrusted, often AI-authored code, run for many tenants. Loaded in-process, a connector could read the secret store, the config volume and engine state, and a crash or runaway allocation would take the engine down. Yet a connector needs credentials to open its connections, and live objects such as SQLAlchemy engines, HTTP sessions and SSL contexts cannot cross a process boundary.

## Decision

We run connector classes only inside a worker subprocess and keep everything secret on the trusted side of that boundary:

- The worker (`python -m src.worker`) is the only caller of `build_registries`. The engine process holds a `WorkerReadable` client and the destination service a `WorkerProxyHandler`; neither imports a connector class. `spawn_worker` starts each worker with a clean environment, its own session, resource limits and a Unix domain socket in a private directory.
- Secret refs resolve engine-side through `SchemeSecretsResolver` (env, file, sidecar, s3). A `file:` path is scope-checked to the connection directory, and an unresolvable ref raises (`SecretNotFoundError`, `UnsupportedSecretRefScheme`, `SecretResolutionError`); there is no empty-secret fallback.
- Transports are split into a trusted-side resolve step (`resolve_transport_specs`, a JSON-safe spec with secrets in place) and a connector-side build step (`build_transport_from_spec`). `ConnectionRuntime.resolve_spec` resolves the default transport plus those the run's endpoint documents name (`endpoint_transport_refs`, through `build_bootstrap`), not every transport the connector declares.
- The worker receives one bootstrap on stdin: the resolved payload, type maps, endpoint documents, and the declared `sql_capabilities`, `error_map` and `concurrency` blocks. It rebuilds its runtime with `ConnectionRuntime.from_resolved_payload` and never reads `connector.json` or a secret store.

## Options considered

### Option A: Worker subprocess, engine-side resolution (chosen)
Pro: tenant isolation, resource limits and crash containment; a connector never holds more credentials than its run uses. Con: every read and write crosses a gRPC hop, and every transport kind needs a resolver/builder pair.

### Option B: Load connectors in-process in the engine and destination service
Pro: no IPC, simplest debugging. Con: untrusted code shares memory with the secret store and engine state, and its crash is the engine's crash.

### Option C: Connector-side secret lookup
Pro: the worker resolves what it needs, lazily. Con: the sandbox must hold secret-store access, which is what isolation exists to deny.

### Option D: Build live transports engine-side and share them
Pro: one place builds connections. Con: live objects cannot cross a process boundary, so this only works with in-process connectors.

### Option E: Resolve every transport the connector declares
Pro: any transport can be opened mid-run. Con: a run carries the auth, login and discovery credentials it never uses, and fails when one of them is absent from its connection.

## Trade-offs

A worker crash becomes a retryable channel error instead of a stack trace in the engine, which is harder to debug. A transport not named up front cannot be opened mid-run. The bootstrap carries resolved credentials across a local pipe, so it must never be logged, and worker stderr is passed through a DSN redactor. The proxy has to relay capabilities and failure categories across the extra hop.

## Consequences

- Easier: running AI-authored connectors next to other tenants' data; reasoning about which credentials a run exposes, since it is the set its endpoints name.
- Harder: anything a connector needs at runtime must be folded into the resolved payload by the trusted side; a new transport kind needs both a resolver and a builder.
- **How we would know this was wrong:** worker spawn and IPC overhead dominate small runs, or connectors repeatedly need data that cannot be resolved ahead of the run.
- Revisit if: connectors move to a different sandbox (container or VM per connector) that changes where the boundary sits.
