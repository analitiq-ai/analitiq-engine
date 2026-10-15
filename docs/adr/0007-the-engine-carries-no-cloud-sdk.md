---
schemaVersion: 0.2.0
id: "0007"
title: "The engine carries no cloud SDK; state and metrics leave via local files and stdout"
status: accepted
date: 2026-06-04
deciders: ["@Analitiq-Bot-Wonka"]
tags: [cloud-agnostic, state, observability, deployment]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0008"]
affects:
  - type: path
    pattern: "src/state/**"
  - type: path
    pattern: "src/runtime_archive.py"
  - type: path
    pattern: "cdk/cdk/secrets/resolvers/scheme.py"
provenance:
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/architecture/engine-architecture.md
---

# ADR-0007: The engine carries no cloud SDK; state and metrics leave via local files and stdout

## Context

The engine is public, open-source and self-hostable, and it also runs as a task under a SaaS control plane. Run state (resume cursors), logs, dead-lettered records and metrics have to outlive a cloud task whose filesystem starts empty and is discarded at exit. The direct route is to write them to a cloud store through its SDK, which ties the engine to one provider and puts cloud-specific code in a public repo. Resume cursors were for a short time injected through a `RESUME_STATE` env var, which grows with the stream count and runs into the size limits a deployment puts on task launch parameters.

## Decision

We keep cloud SDKs out of the engine's core dependencies. State, logs, DLQ and metrics go to the local filesystem and stdout, and harvesting them is the deployment's job:

- When a destination ACK reports that this run committed a batch, `StateManager.save_stream_checkpoint` writes an incremental stream's cursor to its own file, `state/{pipeline_id}/{stream_id}.json` (`CursorStore`), and emits an `ANALITIQ_STATE::` stdout line for the deployment to harvest.
- Metrics leave as `ANALITIQ_METRICS::` lines (`save_pipeline_metrics`); DLQ payloads go to local JSONL (`LocalDLQStorage`) with only an `ANALITIQ_DLQ::` count summary on stdout.
- The next run gets its cursors back as the same per-stream files inside its config bundle. `hydrate_archive` fetches a bundle from a local path or a plain `http(s)` URL (a presigned object-store URL is opaque HTTPS), so no env var and no engine-side cloud read is involved.

Known exception: `boto3` is an optional `s3` extra of both the engine and the CDK, imported lazily by `SchemeSecretsResolver` only when a connection uses an `s3://` secret ref. It is not a core dependency and the Docker image's requirements do not install it.

## Options considered

### Option A: Local files and stdout, harvested by the deployment (chosen)
Pro: one code path for local and cloud runs, a lean image, a public repo with no cloud code. Con: correctness of resume depends on a deployment pipeline outside this repo.

### Option B: Write state, DLQ and metrics directly to cloud stores through an SDK
Pro: durable with no external shipper. Con: ties the engine to one provider and its credentials model, and makes local runs a second code path.

### Option C: A pluggable storage-backend interface with cloud implementations in the engine
Pro: provider choice per deployment. Con: every backend ships in the public engine and has to be maintained and tested there.

### Option D: Deliver resume cursors in a `RESUME_STATE` env var
Pro: no bundle change. Con: one variable for all streams grows with the stream count and can exceed launch-parameter size limits; it was shipped and then replaced by bundle files.

## Trade-offs

The engine cannot verify that its state was harvested; a deployment that drops the `ANALITIQ_STATE` lines or forgets to repackage the files makes the next run do a full re-scan, which costs time but not data. A checkpoint file that cannot be written is logged as a warning and does not fail the load; its `ANALITIQ_STATE` line has already been emitted. Observability depends on an external log shipper parsing stdout markers, so those marker formats are a contract with the deployment. The `s3` extra is a bounded leak of cloud code into the public repo, accepted to support one secret scheme.

## Consequences

- Easier: running the engine anywhere with a filesystem; local runs and cloud runs read the same files; concurrent streams never contend on a shared state file.
- Harder: every new kind of durable output needs a stdout marker or a bundle directory plus a matching change in the deployment's harvester.
- **How we would know this was wrong:** resume or metrics loss traced to the harvest step recurs, or deployments routinely build their own engine fork to write state directly.
- Revisit if: a second cloud scheme is requested for secrets or bundles, which would turn the lazy `s3` exception into a pattern.
