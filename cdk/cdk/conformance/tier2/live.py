"""The live-tier harness: drive the connector through the CDK contract.

Each phase (connect, configure, write, read, drop) goes through exactly
the public surface the engine uses — ``connect`` / ``configure_schema``
/ ``write_batch`` / ``read_batches`` — with a fresh connector instance
and a fresh runtime per phase, so every test inherently certifies a
restart: nothing survives between phases except the database itself.

The live connection document is JSON::

    {
      "connection_id": "conformance-live",
      "schema": "public",
      "config": {
        "host": "localhost", "port": 5432, "database": "conformance",
        "username": "conformance",
        "secret_refs": {"password": "env:CONFORMANCE_DB_PASSWORD"}
      }
    }

``config`` is the connection's transport config exactly as a saved
connection carries it (``secret_refs`` included; ``env:`` / ``file:`` /
``sidecar:`` schemes resolve relative to the document's directory).
``schema`` names the namespace the suite creates its tables in.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from cdk.connection_runtime import ConnectionRuntime
from cdk.secrets.resolvers.scheme import SchemeSecretsResolver
from cdk.sql.dialects import SqlDialect
from cdk.sql.generic import GenericSQLConnector
from cdk.types import BatchWriteResult, Cursor, SchemaSpec, WriteMode

from ..fakes import MemoryCheckpointStore
from ..target import ConformanceSetupError, ConformanceTarget

STREAM_ID = "conformance-stream"
RUN_ID = "conformance-run"

WRITE_MODES = {
    "insert": WriteMode.WRITE_MODE_INSERT,
    "upsert": WriteMode.WRITE_MODE_UPSERT,
    "truncate_insert": WriteMode.WRITE_MODE_TRUNCATE_INSERT,
}

_BATCH_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("val", pa.string()),
        pa.field("seq", pa.int64()),
    ]
)


def rows_batch(rows: list[tuple[int, str, int]]) -> pa.RecordBatch:
    """Build an Arrow batch in the suite's fixed (id, val, seq) shape."""
    ids, vals, seqs = zip(*rows)
    return pa.RecordBatch.from_arrays(
        [
            pa.array(ids, type=pa.int64()),
            pa.array(vals, type=pa.string()),
            pa.array(seqs, type=pa.int64()),
        ],
        schema=_BATCH_SCHEMA,
    )


@dataclass
class LiveHarness:
    """One live table's lifecycle against the connector under test."""

    target: ConformanceTarget
    document_path: Path
    schema: str
    table: str
    _created: bool = field(default=False, init=False)

    @classmethod
    def load(cls, target: ConformanceTarget, document_path: Path) -> LiveHarness:
        """Parse the live connection document and mint a unique table name."""
        try:
            document = json.loads(document_path.read_text())
        except (OSError, json.JSONDecodeError) as err:
            raise ConformanceSetupError(
                f"cannot read live connection document {document_path}: {err}"
            ) from err
        if not isinstance(document, dict) or not isinstance(
            document.get("config"), dict
        ):
            raise ConformanceSetupError(
                f"live connection document {document_path} must be a JSON "
                f"object with a 'config' object"
            )
        schema = document.get("schema")
        if not isinstance(schema, str) or not schema:
            raise ConformanceSetupError(
                f"live connection document {document_path} must name the "
                f"'schema' the suite creates its tables in"
            )
        return cls(
            target=target,
            document_path=document_path,
            schema=schema,
            table=f"conformance_{uuid.uuid4().hex[:12]}",
        )

    # ------------------------------------------------------------------
    # Building blocks
    # ------------------------------------------------------------------

    @property
    def document(self) -> dict[str, Any]:
        """Return the parsed live connection document, re-read per access."""
        document: dict[str, Any] = json.loads(self.document_path.read_text())
        return document

    def runtime(self) -> ConnectionRuntime:
        """Build a fresh trusted-side runtime for one phase."""
        document = self.document
        return ConnectionRuntime(
            raw_config=dict(document["config"]),
            connection_id=str(document.get("connection_id") or "conformance-live"),
            connector_id=self.target.connector_id,
            connector_type=self.target.kind,
            resolver=SchemeSecretsResolver(self.document_path.parent),
            connector_definition=self.target.definition,
            connector_type_mapper=self.target.type_mapper,
        )

    def connector(self) -> GenericSQLConnector:
        """Build a fresh connector instance, as the worker registry would."""
        cls = self.target.connector_class
        if cls is None or not issubclass(cls, GenericSQLConnector):
            raise ConformanceSetupError(
                "no GenericSQLConnector subclass resolved for the live tier"
            )
        connector = cls()
        if not isinstance(connector, GenericSQLConnector):
            raise ConformanceSetupError(
                f"{cls.__name__}() did not produce a GenericSQLConnector"
            )
        return connector

    def endpoint_document(self) -> dict[str, Any]:
        """Return the suite table's contract endpoint document."""
        return {
            "database_object": {"name": self.table, "schema": self.schema},
            "columns": [
                {"name": "id", "arrow_type": "Int64", "nullable": False},
                {"name": "val", "arrow_type": "Utf8", "nullable": True},
                {"name": "seq", "arrow_type": "Int64", "nullable": True},
            ],
            "primary_keys": ["id"],
        }

    # ------------------------------------------------------------------
    # Phases (each: fresh connector + fresh runtime = an engine restart)
    # ------------------------------------------------------------------

    async def write_phase(
        self,
        mode: str,
        batches: list[tuple[int, pa.RecordBatch]],
        *,
        conflict_keys: list[str] | None = None,
    ) -> list[BatchWriteResult]:
        """Configure the stream and write *batches* as one destination session."""
        connector = self.connector()
        await connector.connect(self.runtime())
        try:
            connector.set_endpoint_refs(
                {
                    STREAM_ID: {
                        "scope": "connector",
                        "connection_id": "conformance-live",
                        "endpoint_id": self.table,
                    }
                }
            )
            connector.set_stream_endpoints({STREAM_ID: self.endpoint_document()})
            if conflict_keys is not None:
                connector.set_stream_conflict_keys({STREAM_ID: conflict_keys})
            # Marked before the handshake: configure_schema may create the
            # table and then fail, and the teardown drop (IF EXISTS,
            # idempotent) must still run for that partial state.
            self._created = True
            configured = await connector.configure_schema(
                SchemaSpec(
                    stream_id=STREAM_ID,
                    version=1,
                    write_mode=WRITE_MODES[mode],
                    ack_timeout_seconds=60,
                )
            )
            if not configured:
                pytest.fail(
                    f"configure_schema rejected the {mode} stream for "
                    f"{self.schema}.{self.table}"
                )
            results: list[BatchWriteResult] = []
            for batch_seq, batch in batches:
                results.append(
                    await connector.write_batch(
                        RUN_ID,
                        STREAM_ID,
                        batch_seq,
                        batch,
                        record_ids=[
                            f"{STREAM_ID}-{batch_seq}-{i}"
                            for i in range(batch.num_rows)
                        ],
                        cursor=Cursor(token=f"seq-{batch_seq}".encode()),
                        emitted_at=datetime.now(timezone.utc),
                    )
                )
            return results
        finally:
            await connector.disconnect()

    async def read_phase(
        self,
        *,
        cursor_field: str | None = None,
        checkpoint: MemoryCheckpointStore | None = None,
        batch_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """Read the suite table back through the connector's read path."""
        connector = self.connector()
        runtime = self.runtime()
        replication: dict[str, Any] = (
            {"method": "incremental", "cursor_field": cursor_field}
            if cursor_field
            else {"method": "full_refresh"}
        )
        config = {
            "endpoint_document": self.endpoint_document(),
            "stream_source": {"replication": replication},
        }
        rows: list[dict[str, Any]] = []
        try:
            async for batch in connector.read_batches(
                runtime,
                config,
                checkpoint=checkpoint or MemoryCheckpointStore(),
                stream_name=STREAM_ID,
                batch_size=batch_size,
            ):
                rows.extend(batch.to_pylist())
        finally:
            await runtime.close()
        return rows

    async def drop_phase(self) -> None:
        """Drop the suite table through the runtime's own transport."""
        if not self._created:
            return
        dialect = self.target.dialect
        assert dialect is not None
        address = dialect.table_address(self.table, schema=self.schema)
        statement = f"DROP TABLE IF EXISTS {dialect.quote_table(address)}"
        runtime = self.runtime()
        try:
            await runtime.materialize(sql_dialect=dialect)
            await _run_statement(runtime, statement)
        finally:
            await runtime.close()


async def _run_statement(runtime: ConnectionRuntime, statement: str) -> None:
    """Execute one committed statement on whichever transport materialized."""
    if runtime.is_adbc:
        connection = runtime.open_adbc_connection()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(statement)
            finally:
                cursor.close()
            connection.commit()
        finally:
            connection.close()
        return
    if runtime.is_sync_sqlalchemy:
        with runtime.sync_engine.begin() as connection:
            connection.exec_driver_sql(statement)
        return
    async with runtime.engine.begin() as connection:
        await connection.exec_driver_sql(statement)


def expect_success(results: list[BatchWriteResult], context: str) -> None:
    """Fail with the destination's own summary when any batch was rejected."""
    for i, result in enumerate(results, start=1):
        if not result.success:
            pytest.fail(
                f"{context}: batch {i} rejected "
                f"(status={result.status!r}): {result.failure_summary}"
            )


def by_id(rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    """Index rows by ``id``, failing on an unexpected duplicate."""
    indexed: dict[int, dict[str, Any]] = {}
    for row in rows:
        row_id = int(row["id"])
        assert row_id not in indexed, f"duplicate id {row_id} in {rows!r}"
        indexed[row_id] = row
    return indexed


def dialect_of(harness: LiveHarness) -> SqlDialect:
    """Return the harness target's dialect (present by construction)."""
    dialect = harness.target.dialect
    assert dialect is not None
    return dialect
