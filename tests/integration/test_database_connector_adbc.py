"""Integration tests for the ADBC fast path in DatabaseDestinationHandler.

Skipped automatically when either PostgreSQL is unreachable or the
``adbc_driver_postgresql`` package is not importable. With both present
this exercises the real ``adbc_ingest`` call against a live database.
"""

from __future__ import annotations

import importlib.util
import os
import socket
import uuid
from typing import Any, Dict

import pyarrow as pa
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from src.destination.connectors.database import (
    DatabaseDestinationHandler,
    _StreamState,
)
from src.destination.schema_contract import SchemaContract
from src.grpc.generated.analitiq.v1 import AckStatus, Cursor


def _postgres_available() -> bool:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _adbc_available() -> bool:
    return importlib.util.find_spec("adbc_driver_postgresql") is not None


pytestmark = [
    pytest.mark.skipif(
        not _postgres_available(), reason="PostgreSQL not reachable"
    ),
    pytest.mark.skipif(
        not _adbc_available(),
        reason="adbc_driver_postgresql not installed",
    ),
]


def _pg_url(driver: str = "postgresql+asyncpg") -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    db = os.getenv("POSTGRES_DB", "analitiq_test")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    cred = user + (f":{password}" if password else "")
    return f"{driver}://{cred}@{host}:{port}/{db}"


def _endpoint_document(table_name: str) -> Dict[str, Any]:
    """Minimal database endpoint document for an int-id + string-name table."""
    return {
        "database_object": {"schema": "public", "name": table_name},
        "primary_keys": ["id"],
        "columns": [
            {"name": "id", "native_type": "INTEGER", "arrow_type": "Int32",
             "nullable": False},
            {"name": "name", "native_type": "TEXT", "arrow_type": "Utf8",
             "nullable": True},
        ],
    }


@pytest.fixture
async def pg_engine():
    """Real AsyncEngine pointed at the local Postgres."""
    engine = create_async_engine(_pg_url(), echo=False)
    yield engine
    await engine.dispose()


@pytest.fixture
def table_name() -> str:
    return f"adbc_test_{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def prepared_handler(pg_engine, table_name, monkeypatch):
    """Build a handler with a real engine + pre-created target table.

    The integration test exercises only the ADBC fast path, which assumes
    the target table already exists (``configure_schema`` would normally
    have emitted DDL via SQLAlchemy). Here we just create the table
    directly to keep the test focused on the ADBC ingest call.
    """
    monkeypatch.setenv("ADBC_FAST_PATH", "1")

    # Create the target + batch_commits tables.
    async with pg_engine.begin() as conn:
        await conn.execute(
            text(f"CREATE TABLE public.{table_name} (id INTEGER PRIMARY KEY, name TEXT)")
        )
        await conn.execute(
            text(
                f"CREATE TABLE public.{table_name}_commits ("
                "  run_id VARCHAR(255), stream_id VARCHAR(255),"
                "  batch_seq BIGINT, committed_cursor BYTEA,"
                "  records_written INTEGER, committed_at TIMESTAMP,"
                "  PRIMARY KEY (run_id, stream_id, batch_seq)"
                ")"
            )
        )

    handler = DatabaseDestinationHandler()
    handler._engine = pg_engine
    handler._driver = "postgresql"
    handler._connected = True

    state = _StreamState(
        schema_name="public",
        table_name=table_name,
        write_mode="insert",
        primary_keys=["id"],
    )
    state.schema_contract = SchemaContract(_endpoint_document(table_name))

    # Build SA Table objects matching what was created above so the
    # batch-commit recorder can use them.
    from sqlalchemy import (
        Column, Integer, MetaData, String, Table, BigInteger,
        LargeBinary, DateTime,
    )
    metadata = MetaData()
    state.table = Table(
        table_name, metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        schema="public",
    )
    state.batch_commits_table = Table(
        f"{table_name}_commits", metadata,
        Column("run_id", String(255), primary_key=True),
        Column("stream_id", String(255), primary_key=True),
        Column("batch_seq", BigInteger, primary_key=True),
        Column("committed_cursor", LargeBinary),
        Column("records_written", Integer),
        Column("committed_at", DateTime),
        schema="public",
    )
    handler._streams["s1"] = state

    yield handler

    async with pg_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS public.{table_name}"))
        await conn.execute(text(f"DROP TABLE IF EXISTS public.{table_name}_commits"))
    # Close ADBC connection if opened. Test teardown — close failures
    # are intentionally swallowed so a flaky close doesn't mask the
    # real test result.
    if handler._adbc_conn is not None:
        try:
            handler._adbc_conn.close()
        except Exception:
            pass


class TestAdbcRealIngest:
    @pytest.mark.asyncio
    async def test_adbc_ingest_round_trip(self, prepared_handler, pg_engine, table_name):
        """ADBC ingest writes rows; SQLAlchemy SELECT reads them back."""
        batch = pa.RecordBatch.from_pydict({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["alpha", "beta", "gamma"], type=pa.string()),
        })

        result = await prepared_handler.write_batch(
            run_id="r1",
            stream_id="s1",
            batch_seq=0,
            record_batch=batch,
            record_ids=["1", "2", "3"],
            cursor=Cursor(token=b"cursor-0"),
        )

        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.records_written == 3

        async with pg_engine.begin() as conn:
            rows = (
                await conn.execute(
                    text(f"SELECT id, name FROM public.{table_name} ORDER BY id")
                )
            ).fetchall()
        assert [(r.id, r.name) for r in rows] == [
            (1, "alpha"), (2, "beta"), (3, "gamma"),
        ]

    @pytest.mark.asyncio
    async def test_idempotency_replay_skips_reingest(
        self, prepared_handler, pg_engine, table_name
    ):
        """Re-running the same (run_id, batch_seq) does not re-insert."""
        batch = pa.RecordBatch.from_pydict({
            "id": pa.array([1], type=pa.int32()),
            "name": pa.array(["alpha"], type=pa.string()),
        })

        first = await prepared_handler.write_batch(
            run_id="r1", stream_id="s1", batch_seq=0,
            record_batch=batch, record_ids=["1"],
            cursor=Cursor(token=b"cursor-0"),
        )
        assert first.status == AckStatus.ACK_STATUS_SUCCESS

        second = await prepared_handler.write_batch(
            run_id="r1", stream_id="s1", batch_seq=0,
            record_batch=batch, record_ids=["1"],
            cursor=Cursor(token=b"cursor-0"),
        )
        # Second call should be deduped by the pre-flight check.
        assert second.status == AckStatus.ACK_STATUS_ALREADY_COMMITTED

        async with pg_engine.begin() as conn:
            count = (
                await conn.execute(
                    text(f"SELECT COUNT(*) AS c FROM public.{table_name}")
                )
            ).scalar()
        assert count == 1
