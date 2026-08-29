"""An SQL upsert refuses a batch whose record carries no conflict key (#467).

``ON CONFLICT`` and ``MERGE`` never match a NULL key, so such a record would
insert again on every run. The refusal is the shared one the API path uses,
applied after the cast and before anything is staged: whole batch, fatal.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.generic import GenericSQLConnector, _StreamState
from cdk.types import AckStatus, Cursor

pytestmark = pytest.mark.unit

STREAM_ID = "stream-keys"
SCHEMA = pa.schema([("id", pa.int64()), ("tenant", pa.string()), ("v", pa.string())])


class _MergeDialect(SqlDialect):
    name = "mergefix"

    def stage_table_sql(self, stage, target, *, temp):
        return f"CREATE TABLE {self.quote_table(stage)} LIKE {self.quote_table(target)}"

    def merge_statement_sql(self, stage, target, conflict_keys, columns):
        return "MERGE"


def _upsert_handler() -> GenericSQLConnector:
    handler = GenericSQLConnector()
    handler._connected = True
    handler._adbc_only = True
    handler.dialect = _MergeDialect()
    handler._capabilities = SqlCapabilities.from_declaration(
        {
            "catalog": "none",
            "session_targeting": "per_statement",
            "merge_form": "merge",
            "bulk_load": {"adbc": "adbc_ingest"},
            "stage": {"scope": "real", "schema": "target", "transactional_ddl": False},
        },
        source="<test>",
    )
    contract = MagicMock()
    # The real cast fills an absent column with typed nulls; mirror that.
    contract.cast_arrow_batch.side_effect = lambda rb: pa.RecordBatch.from_arrays(
        [
            rb.column(f.name)
            if f.name in rb.schema.names
            else pa.nulls(rb.num_rows, f.type)
            for f in SCHEMA
        ],
        schema=SCHEMA,
    )
    handler._streams = {
        STREAM_ID: _StreamState(
            address=TableAddress(table="t"),
            write_mode="upsert",
            primary_keys=["id", "tenant"],
            conflict_keys=["id", "tenant"],
            schema_contract=contract,
        )
    }
    handler._backend = MagicMock()
    handler._backend.execute_write = AsyncMock(return_value=None)
    return handler


async def _write(handler: GenericSQLConnector, rows: list[dict]):
    return await handler.write_batch(
        run_id="run-keys",
        stream_id=STREAM_ID,
        batch_seq=2,
        record_batch=pa.RecordBatch.from_pylist(rows),
        record_ids=[str(i) for i in range(len(rows))],
        cursor=Cursor(token=b""),
        emitted_at=datetime(2026, 8, 28, 9, 0, 0, tzinfo=timezone.utc),
    )


async def test_complete_keys_land():
    handler = _upsert_handler()
    result = await _write(handler, [{"id": 1, "tenant": "a", "v": "x"}])
    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    handler._backend.execute_write.assert_awaited_once()


async def test_null_key_refuses_the_whole_batch_before_staging():
    handler = _upsert_handler()
    result = await _write(
        handler,
        [{"id": 1, "tenant": "a", "v": "x"}, {"id": 2, "tenant": None, "v": "y"}],
    )
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert result.records_written == 0
    assert result.failure_summary.startswith("conflict-key:")
    assert "['tenant']" in result.failure_summary
    handler._backend.execute_write.assert_not_awaited()


async def test_absent_key_column_refuses_the_whole_batch_before_staging():
    handler = _upsert_handler()
    result = await _write(handler, [{"id": 1, "v": "x"}])
    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert "['tenant']" in result.failure_summary
    handler._backend.execute_write.assert_not_awaited()


async def test_insert_never_checks_keys():
    handler = _upsert_handler()
    handler._streams[STREAM_ID].write_mode = "insert"
    handler._streams[STREAM_ID].conflict_keys = []
    handler._streams[STREAM_ID].primary_keys = ["id"]
    result = await _write(handler, [{"id": 1, "tenant": None, "v": "x"}])
    assert result.status == AckStatus.ACK_STATUS_SUCCESS
