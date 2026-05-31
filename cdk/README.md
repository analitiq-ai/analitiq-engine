# analitiq-cdk

The **Connector Development Kit**: a vendor-neutral, transport-neutral toolbox of
reusable building blocks a connector uses. It is consumed by both the OSS engine
(bulk streaming) and the cloud control-plane Lambdas (synchronous discover /
create-table).

See `docs/architecture/connector-modules-engine-adr.md` (in the engine repo) for
the design of record.

## The one rule

The dependency points **engine → CDK, never back**. No CDK module may import
anything engine-side (`src/grpc`, `src/state`, `src/models`, `src/source`,
`src/engine/{engine,orchestrator,pipeline}`, `server.py`, `runner.py`,
`main.py`). Anything that must cross from engine to CDK crosses as a plain value
or a CDK-owned type — never an engine object. A connectivity capability whose
implementation is engine- or deployment-specific is expressed as a CDK-owned
seam (Protocol / ABC) the other side implements (`SecretsResolver`,
`CheckpointStore`).

## What lives here

- `cdk/types.py` — CDK-native value types (`AckStatus`, `Cursor`, `SchemaSpec`,
  `BatchWriteResult`, `EndpointScope`, `CheckpointStore`).
- `cdk/contract.py` — capability Protocols (`Readable`, `Writable`,
  `Discoverable`, `TableCreator`, `ColumnDef`).
- transports + `ConnectionRuntime`, the ADBC sub-registry, shared DB helpers,
  the query builder, the rate limiter.
- the expression resolver + derived functions.
- the type-map engine (`type_map/`).
- the secrets seam + local/in-memory resolvers (`secrets/`).
- the abstract destination base (`base_handler.py`).

## Install (control-plane)

```
pip install "git+https://github.com/analitiq-ai/analitiq-engine@vX.Y.Z#subdirectory=cdk"
```

The engine consumes it in-tree (it is on `PYTHONPATH` alongside `src/`).
