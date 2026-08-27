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

import copy
import dataclasses
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from analitiq.contracts.endpoints import DatabaseEndpointDoc

from cdk.connection_runtime import ConnectionRuntime
from cdk.secrets.resolvers.scheme import SchemeSecretsResolver
from cdk.sql.execution import execute_ddl
from cdk.sql.generic import GenericSQLConnector
from cdk.types import BatchWriteResult, Cursor, SchemaSpec, WriteMode

from ..fakes import MemoryCheckpointStore
from ..target import ConformanceSetupError, ConformanceTarget, schema_url_of

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
        """Return the live connection document, parsed fresh per access.

        Fresh so every phase's runtime owns an independent config tree —
        one phase scrubbing or mutating its resolved state can never
        leak into the next phase's connection.
        """
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

    def executemany_forced(self) -> LiveHarness:
        """Build a sibling harness (own table) that lands via executemany.

        The probe target's declaration is doctored to an empty
        ``bulk_load`` mapping — in the parsed capabilities and in the
        definition the runtime is built from — so no backend consults
        any bulk mechanism: the ADBC backend's own ``adbc_ingest``
        landing and the ``bulk_land`` hook are both off, and every batch
        takes the executemany path the comparison certifies. The connector under
        test is never modified; the doctoring is suite-side data.
        """
        caps = self.target.declared_capabilities
        if caps is None:
            raise ConformanceSetupError(
                "executemany_forced needs declared sql_capabilities"
            )
        definition = copy.deepcopy(self.target.definition)
        definition["sql_capabilities"]["bulk_load"] = {}
        return LiveHarness(
            target=dataclasses.replace(
                self.target,
                definition=definition,
                declared_capabilities=dataclasses.replace(caps, bulk_load={}),
            ),
            document_path=self.document_path,
            schema=self.schema,
            table=f"conformance_{uuid.uuid4().hex[:12]}",
        )

    def endpoint_document(self) -> dict[str, Any]:
        """Return the suite table's contract endpoint document.

        JSON, because ``set_stream_endpoints`` takes JSON: the connector
        parses it against ``DatabaseEndpointDoc`` on the way in, the same
        funnel the engine hands a saved document through, so a fixture
        short of the contract fails there rather than being quietly
        accepted by a suite the engine would refuse.
        """
        return {
            "$schema": schema_url_of(DatabaseEndpointDoc),
            "endpoint_id": self.table,
            "database_object": {"name": self.table, "schema": self.schema},
            "columns": [
                self._column("id", "Int64", nullable=False),
                self._column("val", "Utf8", nullable=True),
                self._column("seq", "Int64", nullable=True),
            ],
            "primary_keys": ["id"],
        }

    def _column(self, name: str, arrow_type: str, *, nullable: bool) -> dict[str, Any]:
        """Declare one column of the suite table.

        The contract requires a ``native_type`` beside the Arrow one. Here
        it is asked of the connector's own write map rather than invented,
        because this table is one the suite CREATES through that same map:
        the DDL renders each Arrow type through ``to_native_type``, so
        that is what the column actually is in the database the read phase
        then reads back.
        """
        mapper = self.target.type_mapper
        if mapper is None:
            raise ConformanceSetupError(
                "the live tier writes and reads its own table, so the "
                "connector must ship type maps; this one ships none"
            )
        return {
            "name": name,
            "native_type": mapper.to_native_type(arrow_type),
            "arrow_type": arrow_type,
            "nullable": nullable,
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
            await execute_ddl(runtime, statement)
        finally:
            await runtime.close()


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
