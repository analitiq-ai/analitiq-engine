"""Capability contracts — the structural Protocols a connector implements.

A connector implements **one or more** of these. Runtimes select by
``isinstance`` (the Protocols are ``runtime_checkable``), never by a declared
capabilities block. Method names mirror the engine's existing source/destination
methods where those already exist; ``Discoverable`` / ``TableCreator`` are the
new control-plane operations (implemented in a later phase — declared here so the
contract is frozen).

Annotations are deferred (``from __future__ import annotations``) and the heavy
references (``pyarrow``, ``ConnectionRuntime``) are typing-only, so importing
this module is cheap and creates no cycle back into the transport stack.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from .types import BatchWriteResult, CheckpointStore, Cursor, SchemaSpec

if TYPE_CHECKING:
    import pyarrow as pa

    from .connection_runtime import ConnectionRuntime


@dataclass(frozen=True)
class ColumnDef:
    """A single column in a table definition.

    ``canonical_type`` is the Apache Arrow canonical type string (e.g.
    ``"Int64"``, ``"Decimal128(38, 9)"``, ``"Timestamp(MICROSECOND, UTC)"``) —
    not the raw native string — so it is symmetric with the type-map and is what
    ``create_table`` consumes downstream.
    """

    name: str
    canonical_type: str
    nullable: bool = True
    primary_key: bool = False
    #: Optional SQL DEFAULT expression, passed through verbatim into DDL
    #: (e.g. ``"now()"``, ``"CURRENT_TIMESTAMP"``). Never a Python value.
    default: str | None = None


# ---- DISCOVER (control-plane reads; implemented in a later phase) ----------
@runtime_checkable
class Discoverable(Protocol):
    async def list_schemas(self, runtime: ConnectionRuntime) -> list[str]:
        ...

    async def list_tables(self, runtime: ConnectionRuntime, schema: str) -> list[str]:
        ...

    async def list_columns(
        self, runtime: ConnectionRuntime, schema: str, table: str
    ) -> tuple[list[ColumnDef], list[str]]:  # (columns, primary_keys)
        ...


# ---- CREATE (control-plane DDL; standalone; implemented in a later phase) ---
@runtime_checkable
class TableCreator(Protocol):
    async def create_table(
        self,
        runtime: ConnectionRuntime,
        schema: str,
        table: str,
        columns: list[ColumnDef],
        primary_keys: list[str],
    ) -> None:
        ...


# ---- READ (engine source) --------------------------------------------------
@runtime_checkable
class Readable(Protocol):
    # Not `async def`: implementors are async generators, so calling
    # read_batches returns the AsyncIterator directly. Declaring it `async def`
    # would type the call as Coroutine[..., AsyncIterator], breaking
    # `async for` at the call sites and the structural match for WorkerReadable.
    def read_batches(
        self,
        runtime: ConnectionRuntime,
        config: dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        ...


# ---- WRITE (engine destination) --------------------------------------------
@runtime_checkable
class Writable(Protocol):
    async def connect(self, runtime: ConnectionRuntime) -> None:
        ...

    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        ...

    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: list[str],
        cursor: Cursor,
    ) -> BatchWriteResult:
        ...

    async def disconnect(self) -> None:
        ...

    async def health_check(self) -> bool:
        ...
