---
schemaVersion: 0.2.0
id: "0015"
title: "Every SQL destination uses one handler over two transports"
status: accepted
date: 2026-05-13
deciders: ["@Analitiq-Bot-Wonka"]
tags: [sql-destinations, transports, adbc, sqlalchemy]
scope: component
reversibility: one-way-door
blastRadius: cross-team
relatesTo: ["0005", "0014", "0019"]
affects:
  - type: path
    pattern: "cdk/cdk/sql/**"
  - type: path
    pattern: "cdk/cdk/connection_runtime.py"
provenance:
  authoredBy: agent
  ratifiedBy: "@Analitiq-Bot-Wonka"
  sourceArtifact: docs/data-path/arrow-and-transport-strategy.md
---

# ADR-0015: Every SQL destination uses one handler over two transports

## Context

SQL destinations pull in two directions. High-volume warehouses (Snowflake, BigQuery) need columnar bulk throughput that row-oriented drivers do not give. Every other database needs breadth: any reasonable system should be a destination on day one, including ones with no columnar driver (Oracle, MSSQL, MariaDB, ClickHouse, niche dialects). No single driver layer covers both. Writing a destination per database covers both but multiplies the write path once per system, and each copy drifts.

## Decision

We serve every SQL destination through one handler, `GenericSQLConnector`, over two transports, chosen by the `transport_type` of the connector's default transport: `sqlalchemy` for breadth and `adbc` for high-volume warehouses. Both execute the same stage-then-merge plans ([ADR 0005](0005-stage-then-merge-is-the-single-sql-write-primitive.md)) through the two `TransportBackend` implementations, `SqlAlchemyBackend` and `AdbcBackend`. The SQL transports are `SQL_TRANSPORT_TYPES`, derived from the contract's `SqlBulkLoad` fields. We add no third transport and no per-database destination: a database connector's own class is bounded as [ADR 0019](0019-resolve-a-connector-by-kind-then-connector-id-with-a-generic-class-per-kind.md) records.

## Options considered

### Option A: One handler, SQLAlchemy plus ADBC (chosen)
- Pro: breadth and warehouse throughput from two driver layers; write-mode, identity and retry rules defined once.
- Con: two transports to keep at parity; ADBC driver maturity per system is an outside dependency.

### Option B: SQLAlchemy only
- Pro: one transport, widest dialect coverage.
- Con: modest throughput on high-volume warehouses.

### Option C: ADBC only
- Pro: one columnar transport, high throughput.
- Con: no coverage for Oracle, MSSQL, MariaDB, ClickHouse or niche dialects.

### Option D: A custom destination per database
- Pro: each system tuned freely.
- Con: one write path per database, each drifting from the others.

## Trade-offs

Every write-path rule must hold on both transports, which is only tractable because the write primitive is shared. A system whose best driver is neither SQLAlchemy nor ADBC waits for one of them to cover it.

Known exceptions in code:

- The registry does not require a database connector's own class to subclass `GenericSQLConnector`; an installed class wins over it on `connector_id`, and only the conformance kit refuses one that does not extend it ([ADR 0019](0019-resolve-a-connector-by-kind-then-connector-id-with-a-generic-class-per-kind.md)).

## Consequences

- Easier: a new database is a connector definition plus a dialect, not a new destination; conformance certifies one handler.
- Harder: `transport_type` is part of every SQL connector definition and of the capability declarations (bulk load per transport), so the set is costly to change.
- **How we would know this was wrong:** connectors keep overriding write internals to reach performance or semantics neither transport gives, or ADBC coverage stalls for a warehouse users need.
- Revisit if: a third driver layer would cover a large class of systems that neither transport reaches.

The SQLAlchemy transport runs in two flavours, async engine and sync engine for sync-only drivers; both go through `SqlAlchemyBackend`, so it remains one transport.
