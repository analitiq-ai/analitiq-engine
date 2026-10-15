---
schemaVersion: 0.2.0
id: "0009"
title: "Engine and destination are separate services joined by gRPC carrying Arrow IPC"
status: accepted
date: 2026-04-06
deciders: ["@Analitiq-Bot-Wonka"]
tags: [grpc, topology, arrow, config]
scope: domain
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0008", "0016"]
affects:
  - type: path
    pattern: "src/main.py"
  - type: path
    pattern: "src/destination/**"
  - type: path
    pattern: "src/grpc/**"
  - type: path
    pattern: "proto/analitiq/v1/**"
provenance:
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/architecture/grpc-streaming-architecture.md
---

# ADR-0009: Engine and destination are separate services joined by gRPC carrying Arrow IPC

## Context

Extraction and loading have different failure modes and scaling needs, and a destination should be writable in any language that speaks gRPC. Splitting them into two processes means deciding what crosses the wire: the destination needs its connection, credentials and each stream's target shape, and either the engine sends them or the destination loads them itself. Two separately built artifacts would also risk version skew between the sides. Every batch is serialized onto the wire, and the engine's typed values (timestamp precision, time zones, decimals) must survive the hop.

## Decision

We run the engine and the destination as separate services from one Docker image, selected by `RUN_MODE` (`source` or `destination`), joined by the bidirectional `StreamRecords` RPC of `DestinationService` (`DestinationGRPCClient` to `DestinationGRPCServer`).

Both services load the same pipeline through `PipelineConfigPrep` from the same `PIPELINE_ID`; the destination picks its connection by `DESTINATION_INDEX` and resolves that connection's credentials itself. The `SchemaMessage` handshake therefore carries only `stream_id`, `version`, `write_mode` and `ack_timeout_seconds`, and the destination looks up its preloaded endpoint document by `stream_id`. Endpoint documents, key definitions, the target table and credentials do not cross the wire; a `RecordBatch` carries the Arrow payload, record ids, the cursor and batch metadata.

The data travels only as Arrow IPC: one `pa.RecordBatch` per `RecordBatch` message, schema in the same buffer. The destination refuses any other `PayloadFormat`, and the source worker's `ReadResponse` uses the same encoding.

Inside the destination service, a `WorkerProxyHandler` forwards to a connector worker over a Unix socket using the same `DestinationService` contract (see [ADR 0008](0008-connector-code-runs-only-in-an-isolated-worker.md)), so the engine sends no connection credentials over the cross-container TCP channel.

## Options considered

### Option A: Separate services, one image, slim handshake (chosen)
Pro: independent failure and scaling, language-neutral destinations, one artifact, and a data plane the engine puts no connection credentials on. Con: a wire protocol, a handshake and ack machinery to maintain.

### Option B: Load in-process inside the engine
Pro: no protocol, no second container. Con: source and destination share failure and scaling, and destinations must be Python.

### Option C: Separate images for source and destination
Pro: smaller per-role images. Con: two artifacts to version and keep compatible.

### Option D: Fat handshake carrying schema and credentials
Pro: the destination needs no config volume. Con: a second copy of the validated contract on the wire that can diverge from the first, and credentials in the network data plane.

### Option E: Protobuf or JSON rows instead of Arrow IPC
Pro: native to gRPC or readable, no Arrow library. Con: types lost or coordinated out of band, larger payloads, a row round-trip on both sides.

### Option F: A message broker between the stages
Pro: buffering and replay. Con: a new piece of infrastructure, and ack-per-batch cursor commit has to be rebuilt on top of it.

## Trade-offs

Both containers must mount the same config and resolve the same `PIPELINE_ID`, and the destination repeats config loading and validation. Nothing on the handshake compares the two config copies. Failure text on the acks (`failure_summary`, the `SchemaAck` message) is sent back unredacted and can carry exception text from the connector or its driver. Every destination language needs an Arrow IPC implementation and its own loader for the pipeline config. `write_mode` reaches the destination twice: on the handshake, and from its own config, where it scopes which transports the worker receives.

## Consequences

- Easier: restarting or scaling a destination without touching extraction; a captured gRPC stream holds no connection credentials sent by the engine.
- Harder: every engine-destination interaction is a protocol change with regenerated bindings; deployment must deliver identical config to two containers.
- **How we would know this was wrong:** incidents traced to the two services loading different config, or the handshake repeatedly growing fields the destination could have read from config, or destinations blocked on a language with no Arrow IPC reader.
- Revisit if: destinations are expected to run without access to the pipeline config, which would force config onto the wire.
