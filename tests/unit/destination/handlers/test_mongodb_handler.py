"""Unit tests for MongoDbDestinationHandler.

Covers:
* connect / health_check
* configure_schema (collection + index creation)
* write_batch: insert, upsert, truncate-insert modes
* Idempotency: ALREADY_COMMITTED on replayed (run_id, stream_id, batch_seq)
* Empty batch handling
* Missing stream configuration path (FATAL_FAILURE)
"""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from cdk.types import AckStatus, Cursor, SchemaSpec, WriteMode
from src.destination.connectors.mongodb import (
    MongoDbDestinationHandler,
    _arrow_to_doc_list,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_runtime(db_name: str = "testdb") -> MagicMock:
    rt = MagicMock()
    rt.acquire = MagicMock()
    rt.materialize = AsyncMock()
    rt.close = AsyncMock()
    rt.mongo_default_database = db_name

    # Mock client + db + collections
    commits_coll = MagicMock()
    commits_coll.find_one = AsyncMock(return_value=None)
    commits_coll.insert_one = AsyncMock()
    commits_coll.create_index = AsyncMock()

    target_coll = MagicMock()
    target_coll.create_index = AsyncMock()
    target_coll.insert_many = AsyncMock(return_value=MagicMock(inserted_ids=["a", "b"]))
    target_coll.bulk_write = AsyncMock(
        return_value=MagicMock(upserted_count=1, modified_count=1)
    )
    target_coll.delete_many = AsyncMock()

    db_mock = MagicMock()

    def db_getitem(name):
        if name == "_batch_commits":
            return commits_coll
        return target_coll

    db_mock.__getitem__ = MagicMock(side_effect=db_getitem)
    db_mock.create_collection = AsyncMock()

    client_mock = MagicMock()
    client_mock.admin = MagicMock()
    client_mock.admin.command = AsyncMock()
    client_mock.__getitem__ = MagicMock(return_value=db_mock)

    rt.mongo_client = client_mock
    return rt, commits_coll, target_coll, db_mock


def _make_schema_spec(
    stream_id: str = "s1",
    write_mode: WriteMode = WriteMode.WRITE_MODE_INSERT,
) -> SchemaSpec:
    return SchemaSpec(
        stream_id=stream_id,
        version=1,
        write_mode=write_mode,
        ack_timeout_seconds=30,
    )


def _make_batch(*dicts) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(list(dicts))


def _cursor() -> Cursor:
    return Cursor(token=b"tok1")


# ---------------------------------------------------------------------------
# _arrow_to_doc_list
# ---------------------------------------------------------------------------

class TestArrowToDocList:
    def test_basic(self):
        batch = pa.RecordBatch.from_pylist([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ])
        docs = _arrow_to_doc_list(batch)
        assert len(docs) == 2
        assert docs[0] == {"id": 1, "name": "Alice"}
        assert docs[1] == {"id": 2, "name": "Bob"}

    def test_empty_batch(self):
        batch = pa.RecordBatch.from_pylist([])
        assert _arrow_to_doc_list(batch) == []


# ---------------------------------------------------------------------------
# Handler lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect_sets_connected():
    rt, *_ = _make_runtime()
    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    assert handler._connected is True


@pytest.mark.asyncio
async def test_health_check_pings_admin():
    rt, *_ = _make_runtime()
    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    result = await handler.health_check()
    assert result is True
    rt.mongo_client.admin.command.assert_called_with("ping")


@pytest.mark.asyncio
async def test_health_check_returns_false_when_not_connected():
    handler = MongoDbDestinationHandler()
    assert await handler.health_check() is False


@pytest.mark.asyncio
async def test_disconnect_closes_runtime():
    rt, *_ = _make_runtime()
    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    await handler.disconnect()
    rt.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# configure_schema
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_configure_schema_insert_mode():
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    handler.set_stream_endpoints({
        "s1": {
            "collection": "orders",
            "database": "testdb",
            "operations": {"write": {}},
        }
    })

    spec = _make_schema_spec("s1", WriteMode.WRITE_MODE_INSERT)
    result = await handler.configure_schema(spec)

    assert result is True
    assert handler._stream_collections["s1"] == ("testdb", "orders")
    assert handler._stream_modes["s1"] == WriteMode.WRITE_MODE_INSERT


@pytest.mark.asyncio
async def test_configure_schema_upsert_creates_index():
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    handler.set_stream_endpoints({
        "s1": {
            "collection": "users",
            "database": "testdb",
            "operations": {"write": {"conflict_keys": ["email"]}},
        }
    })

    spec = _make_schema_spec("s1", WriteMode.WRITE_MODE_UPSERT)
    await handler.configure_schema(spec)

    target_coll.create_index.assert_awaited_once()
    call_args = target_coll.create_index.call_args
    assert call_args[0][0] == [("email", 1)]


@pytest.mark.asyncio
async def test_configure_schema_missing_endpoint_returns_false():
    rt, *_ = _make_runtime()
    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    # No endpoints registered

    spec = _make_schema_spec("s1")
    result = await handler.configure_schema(spec)
    assert result is False


# ---------------------------------------------------------------------------
# write_batch — insert mode
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_batch_insert_success():
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    target_coll.insert_many = AsyncMock(
        return_value=MagicMock(inserted_ids=["id1", "id2"])
    )

    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    handler._stream_collections["s1"] = ("testdb", "orders")
    handler._stream_modes["s1"] = WriteMode.WRITE_MODE_INSERT
    handler._stream_conflict_keys["s1"] = []
    handler._batch_commits_ready = True

    batch = _make_batch({"id": 1, "amount": 100}, {"id": 2, "amount": 200})
    result = await handler.write_batch(
        run_id="run1", stream_id="s1", batch_seq=0,
        record_batch=batch, record_ids=["r1", "r2"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    assert result.records_written == 2
    assert result.committed_cursor == _cursor()
    target_coll.insert_many.assert_awaited_once()


@pytest.mark.asyncio
async def test_write_batch_already_committed():
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    commits_coll.find_one = AsyncMock(
        return_value={"run_id": "run1", "stream_id": "s1", "batch_seq": 0, "cursor_token": b"old"}
    )

    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    handler._stream_collections["s1"] = ("testdb", "orders")
    handler._stream_modes["s1"] = WriteMode.WRITE_MODE_INSERT
    handler._stream_conflict_keys["s1"] = []
    handler._batch_commits_ready = True

    batch = _make_batch({"id": 1})
    result = await handler.write_batch(
        run_id="run1", stream_id="s1", batch_seq=0,
        record_batch=batch, record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_ALREADY_COMMITTED
    assert result.committed_cursor == Cursor(token=b"old")
    target_coll.insert_many.assert_not_called()


@pytest.mark.asyncio
async def test_write_batch_upsert():
    rt, commits_coll, target_coll, db_mock = _make_runtime()

    # Stub pymongo.UpdateOne as it may not be installed
    with patch.dict(sys.modules, {
        "pymongo": MagicMock(UpdateOne=lambda *a, **kw: ("update_op", a, kw)),
    }):
        handler = MongoDbDestinationHandler()
        await handler.connect(rt)
        handler._stream_collections["s1"] = ("testdb", "users")
        handler._stream_modes["s1"] = WriteMode.WRITE_MODE_UPSERT
        handler._stream_conflict_keys["s1"] = ["email"]
        handler._batch_commits_ready = True

        batch = _make_batch({"email": "a@b.com", "name": "Alice"})
        result = await handler.write_batch(
            run_id="run1", stream_id="s1", batch_seq=1,
            record_batch=batch, record_ids=["r1"], cursor=_cursor(),
        )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    target_coll.bulk_write.assert_awaited_once()


@pytest.mark.asyncio
async def test_write_batch_truncate_insert():
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    target_coll.insert_many = AsyncMock(
        return_value=MagicMock(inserted_ids=["x"])
    )

    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    handler._stream_collections["s1"] = ("testdb", "snapshots")
    handler._stream_modes["s1"] = WriteMode.WRITE_MODE_TRUNCATE_INSERT
    handler._stream_conflict_keys["s1"] = []
    handler._batch_commits_ready = True

    batch = _make_batch({"v": 42})
    result = await handler.write_batch(
        run_id="run1", stream_id="s1", batch_seq=2,
        record_batch=batch, record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    target_coll.delete_many.assert_awaited_once_with({})
    target_coll.insert_many.assert_awaited_once()


@pytest.mark.asyncio
async def test_write_batch_empty_records():
    rt, commits_coll, target_coll, db_mock = _make_runtime()

    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    handler._stream_collections["s1"] = ("testdb", "items")
    handler._stream_modes["s1"] = WriteMode.WRITE_MODE_INSERT
    handler._stream_conflict_keys["s1"] = []
    handler._batch_commits_ready = True

    empty_batch = pa.RecordBatch.from_pylist([])
    result = await handler.write_batch(
        run_id="run1", stream_id="s1", batch_seq=3,
        record_batch=empty_batch, record_ids=[], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    assert result.records_written == 0
    target_coll.insert_many.assert_not_called()
    commits_coll.insert_one.assert_awaited_once()


@pytest.mark.asyncio
async def test_write_batch_unconfigured_stream():
    rt, *_ = _make_runtime()
    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    # No configure_schema called

    batch = _make_batch({"id": 1})
    result = await handler.write_batch(
        run_id="run1", stream_id="missing", batch_seq=0,
        record_batch=batch, record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE


@pytest.mark.asyncio
async def test_write_batch_retryable_on_insert_error():
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    target_coll.insert_many = AsyncMock(side_effect=RuntimeError("network error"))

    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    handler._stream_collections["s1"] = ("testdb", "items")
    handler._stream_modes["s1"] = WriteMode.WRITE_MODE_INSERT
    handler._stream_conflict_keys["s1"] = []
    handler._batch_commits_ready = True

    batch = _make_batch({"id": 99})
    result = await handler.write_batch(
        run_id="run1", stream_id="s1", batch_seq=5,
        record_batch=batch, record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    assert "network error" in result.failure_summary


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

def test_connector_type():
    handler = MongoDbDestinationHandler()
    assert handler.connector_type == "mongodb"


def test_supports_properties():
    handler = MongoDbDestinationHandler()
    assert handler.supports_upsert is True
    assert handler.supports_transactions is False
    assert handler.supports_bulk_load is False
