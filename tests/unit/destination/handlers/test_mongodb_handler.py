"""Unit tests for MongoDbDestinationHandler.

Covers:
* connect / health_check (including failure path)
* configure_schema: collection + index creation, missing endpoint, create_collection error
* write_batch: insert, upsert (with and without conflict_keys), truncate-insert
* Idempotency: ALREADY_COMMITTED on replayed (run_id, stream_id, batch_seq)
* Empty batch, unconfigured stream (RETRYABLE_FAILURE), insert error (RETRYABLE)
* Commit-record failure returns RETRYABLE_FAILURE for all write modes
* Per-database _batch_commits_ready flag
"""

from __future__ import annotations

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from cdk.types import AckStatus, Cursor, SchemaSpec, WriteMode
from src.destination.connectors.mongodb import (
    MongoDbDestinationHandler,
    _MongoStreamState,
    _arrow_to_doc_list,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_runtime(db_name: str = "testdb"):
    rt = MagicMock()
    rt.acquire = MagicMock()
    rt.materialize = AsyncMock()
    rt.close = AsyncMock()
    rt.mongo_default_database = db_name

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


def _schema_spec(
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


def _endpoint(
    collection: str = "orders",
    db: str = "testdb",
    conflict_keys: list | None = None,
) -> dict:
    return {
        "collection": collection,
        "database": db,
        "operations": {"write": {"conflict_keys": conflict_keys or []}},
    }


async def _connected_handler(rt=None):
    if rt is None:
        rt, *_ = _make_runtime()
    handler = MongoDbDestinationHandler()
    await handler.connect(rt)
    return handler, rt


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
# Lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect_sets_connected():
    rt, *_ = _make_runtime()
    handler, _ = await _connected_handler(rt)
    assert handler._connected is True


@pytest.mark.asyncio
async def test_health_check_pings_admin():
    rt, *_ = _make_runtime()
    handler, _ = await _connected_handler(rt)
    assert await handler.health_check() is True
    rt.mongo_client.admin.command.assert_called_with("ping")


@pytest.mark.asyncio
async def test_health_check_returns_false_on_ping_failure():
    rt, *_ = _make_runtime()
    rt.mongo_client.admin.command = AsyncMock(side_effect=Exception("timeout"))
    handler, _ = await _connected_handler(rt)
    assert await handler.health_check() is False


@pytest.mark.asyncio
async def test_health_check_returns_false_when_not_connected():
    handler = MongoDbDestinationHandler()
    assert await handler.health_check() is False


@pytest.mark.asyncio
async def test_disconnect_clears_runtime():
    rt, *_ = _make_runtime()
    handler, _ = await _connected_handler(rt)
    await handler.disconnect()
    rt.close.assert_awaited_once()
    assert handler._runtime is None


# ---------------------------------------------------------------------------
# configure_schema
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_configure_schema_insert_mode():
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    handler, _ = await _connected_handler(rt)
    handler.set_stream_endpoints({"s1": _endpoint()})

    result = await handler.configure_schema(_schema_spec("s1", WriteMode.WRITE_MODE_INSERT))

    assert result is True
    assert handler._streams["s1"].database_name == "testdb"
    assert handler._streams["s1"].collection_name == "orders"
    assert handler._streams["s1"].write_mode == WriteMode.WRITE_MODE_INSERT


@pytest.mark.asyncio
async def test_configure_schema_upsert_creates_index():
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    handler, _ = await _connected_handler(rt)
    handler.set_stream_endpoints({"s1": _endpoint(conflict_keys=["email"])})

    await handler.configure_schema(_schema_spec("s1", WriteMode.WRITE_MODE_UPSERT))

    target_coll.create_index.assert_awaited_once()
    assert target_coll.create_index.call_args[0][0] == [("email", 1)]


@pytest.mark.asyncio
async def test_configure_schema_missing_endpoint_returns_false():
    rt, *_ = _make_runtime()
    handler, _ = await _connected_handler(rt)

    result = await handler.configure_schema(_schema_spec("unknown"))
    assert result is False


@pytest.mark.asyncio
async def test_configure_schema_before_connect_returns_false():
    handler = MongoDbDestinationHandler()
    handler.set_stream_endpoints({"s1": _endpoint()})
    result = await handler.configure_schema(_schema_spec("s1"))
    assert result is False


@pytest.mark.asyncio
async def test_configure_schema_create_collection_error_returns_false():
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    db_mock.create_collection = AsyncMock(side_effect=RuntimeError("permission denied"))
    handler, _ = await _connected_handler(rt)
    handler.set_stream_endpoints({"s1": _endpoint()})

    result = await handler.configure_schema(_schema_spec("s1"))
    assert result is False


@pytest.mark.asyncio
async def test_configure_schema_per_database_batch_commits():
    """Two streams on different databases each get their own _batch_commits."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    handler, _ = await _connected_handler(rt)
    handler.set_stream_endpoints({
        "s1": _endpoint(db="db_a"),
        "s2": _endpoint(db="db_b"),
    })

    await handler.configure_schema(_schema_spec("s1"))
    await handler.configure_schema(_schema_spec("s2"))

    assert "db_a" in handler._batch_commits_ready
    assert "db_b" in handler._batch_commits_ready


# ---------------------------------------------------------------------------
# write_batch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_batch_insert_success():
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    target_coll.insert_many = AsyncMock(
        return_value=MagicMock(inserted_ids=["id1", "id2"])
    )
    handler, _ = await _connected_handler(rt)
    handler._streams["s1"] = _MongoStreamState("testdb", "orders", WriteMode.WRITE_MODE_INSERT)
    handler._batch_commits_ready.add("testdb")

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
        return_value={"run_id": "r", "stream_id": "s1", "batch_seq": 0, "cursor_token": b"old"}
    )
    handler, _ = await _connected_handler(rt)
    handler._streams["s1"] = _MongoStreamState("testdb", "orders", WriteMode.WRITE_MODE_INSERT)
    handler._batch_commits_ready.add("testdb")

    result = await handler.write_batch(
        run_id="r", stream_id="s1", batch_seq=0,
        record_batch=_make_batch({"id": 1}), record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_ALREADY_COMMITTED
    assert result.committed_cursor == Cursor(token=b"old")
    target_coll.insert_many.assert_not_called()


@pytest.mark.asyncio
async def test_write_batch_upsert_with_conflict_keys():
    rt, commits_coll, target_coll, db_mock = _make_runtime()

    with patch.dict(sys.modules, {
        "pymongo": MagicMock(UpdateOne=lambda filter, update, **kw: (filter, update)),
    }):
        handler, _ = await _connected_handler(rt)
        handler._streams["s1"] = _MongoStreamState(
            "testdb", "users", WriteMode.WRITE_MODE_UPSERT, conflict_keys=["email"]
        )
        handler._batch_commits_ready.add("testdb")

        batch = _make_batch({"email": "a@b.com", "name": "Alice"})
        result = await handler.write_batch(
            run_id="r", stream_id="s1", batch_seq=1,
            record_batch=batch, record_ids=["r1"], cursor=_cursor(),
        )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    target_coll.bulk_write.assert_awaited_once()


@pytest.mark.asyncio
async def test_write_batch_upsert_without_conflict_keys_returns_retryable():
    """Upsert mode with no conflict_keys must fail loudly, not silently insert."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()

    handler, _ = await _connected_handler(rt)
    handler._streams["s1"] = _MongoStreamState(
        "testdb", "items", WriteMode.WRITE_MODE_UPSERT, conflict_keys=[]
    )
    handler._batch_commits_ready.add("testdb")

    batch = _make_batch({"v": 1})
    result = await handler.write_batch(
        run_id="r", stream_id="s1", batch_seq=0,
        record_batch=batch, record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    target_coll.insert_many.assert_not_called()
    target_coll.bulk_write.assert_not_called()


@pytest.mark.asyncio
async def test_write_batch_truncate_insert():
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    target_coll.insert_many = AsyncMock(return_value=MagicMock(inserted_ids=["x"]))

    handler, _ = await _connected_handler(rt)
    handler._streams["s1"] = _MongoStreamState(
        "testdb", "snapshots", WriteMode.WRITE_MODE_TRUNCATE_INSERT
    )
    handler._batch_commits_ready.add("testdb")

    batch = _make_batch({"v": 42})
    result = await handler.write_batch(
        run_id="r", stream_id="s1", batch_seq=2,
        record_batch=batch, record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    target_coll.delete_many.assert_awaited_once_with({})
    target_coll.insert_many.assert_awaited_once()


@pytest.mark.asyncio
async def test_write_batch_truncate_insert_commit_failure_returns_retryable():
    """Truncate-insert: if commit record fails, return RETRYABLE not SUCCESS."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    target_coll.insert_many = AsyncMock(return_value=MagicMock(inserted_ids=["x"]))
    commits_coll.insert_one = AsyncMock(side_effect=RuntimeError("write conflict"))

    handler, _ = await _connected_handler(rt)
    handler._streams["s1"] = _MongoStreamState(
        "testdb", "snaps", WriteMode.WRITE_MODE_TRUNCATE_INSERT
    )
    handler._batch_commits_ready.add("testdb")

    batch = _make_batch({"v": 1})
    result = await handler.write_batch(
        run_id="r", stream_id="s1", batch_seq=0,
        record_batch=batch, record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE


@pytest.mark.asyncio
async def test_write_batch_empty_records():
    rt, commits_coll, target_coll, db_mock = _make_runtime()

    handler, _ = await _connected_handler(rt)
    handler._streams["s1"] = _MongoStreamState("testdb", "items", WriteMode.WRITE_MODE_INSERT)
    handler._batch_commits_ready.add("testdb")

    result = await handler.write_batch(
        run_id="r", stream_id="s1", batch_seq=3,
        record_batch=pa.RecordBatch.from_pylist([]),
        record_ids=[], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    assert result.records_written == 0
    target_coll.insert_many.assert_not_called()
    commits_coll.insert_one.assert_awaited_once()


@pytest.mark.asyncio
async def test_write_batch_unconfigured_stream():
    rt, *_ = _make_runtime()
    handler, _ = await _connected_handler(rt)

    result = await handler.write_batch(
        run_id="r", stream_id="missing", batch_seq=0,
        record_batch=_make_batch({"id": 1}), record_ids=["r1"], cursor=_cursor(),
    )
    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE


@pytest.mark.asyncio
async def test_write_batch_retryable_on_insert_error():
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    target_coll.insert_many = AsyncMock(side_effect=RuntimeError("network error"))

    handler, _ = await _connected_handler(rt)
    handler._streams["s1"] = _MongoStreamState("testdb", "items", WriteMode.WRITE_MODE_INSERT)
    handler._batch_commits_ready.add("testdb")

    result = await handler.write_batch(
        run_id="r", stream_id="s1", batch_seq=5,
        record_batch=_make_batch({"id": 99}), record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    assert "network error" in result.failure_summary


@pytest.mark.asyncio
async def test_write_batch_insert_commit_failure_returns_retryable():
    """A lost commit record must return RETRYABLE for all modes — without it a
    replay would re-execute the write without the idempotency guard stopping it."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    target_coll.insert_many = AsyncMock(return_value=MagicMock(inserted_ids=["x"]))
    commits_coll.insert_one = AsyncMock(side_effect=RuntimeError("timeout"))

    handler, _ = await _connected_handler(rt)
    handler._streams["s1"] = _MongoStreamState("testdb", "orders", WriteMode.WRITE_MODE_INSERT)
    handler._batch_commits_ready.add("testdb")

    result = await handler.write_batch(
        run_id="r", stream_id="s1", batch_seq=0,
        record_batch=_make_batch({"id": 1}), record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE


@pytest.mark.asyncio
async def test_write_batch_empty_records_commit_failure_returns_retryable():
    """Empty-batch commit_one failure must return RETRYABLE, not raise."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    commits_coll.insert_one = AsyncMock(side_effect=RuntimeError("write timeout"))

    handler, _ = await _connected_handler(rt)
    handler._streams["s1"] = _MongoStreamState("testdb", "items", WriteMode.WRITE_MODE_INSERT)
    handler._batch_commits_ready.add("testdb")

    result = await handler.write_batch(
        run_id="r", stream_id="s1", batch_seq=9,
        record_batch=pa.RecordBatch.from_pylist([]),
        record_ids=[], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    assert "write timeout" in result.failure_summary


@pytest.mark.asyncio
async def test_configure_schema_upsert_without_conflict_keys_returns_false():
    """UPSERT with no conflict_keys must fail configure_schema — no silent INSERT fallback."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    handler, _ = await _connected_handler(rt)
    handler.set_stream_endpoints({"s1": _endpoint(conflict_keys=[])})

    result = await handler.configure_schema(_schema_spec("s1", WriteMode.WRITE_MODE_UPSERT))

    assert result is False
    assert "s1" not in handler._streams


@pytest.mark.asyncio
async def test_write_batch_all_duplicate_insert_returns_zero_written():
    """All-duplicate BulkWriteError should be treated as idempotent success, not RETRYABLE."""
    import sys
    from types import ModuleType

    rt, commits_coll, target_coll, db_mock = _make_runtime()

    # Build a fake BulkWriteError whose details indicate 0 inserted, all dup-key
    class FakeBulkWriteError(Exception):
        def __init__(self, details):
            self.details = details

    fake_pymongo_errors = ModuleType("pymongo.errors")
    fake_pymongo_errors.BulkWriteError = FakeBulkWriteError
    fake_pymongo = ModuleType("pymongo")
    fake_pymongo.errors = fake_pymongo_errors

    with patch.dict(sys.modules, {"pymongo": fake_pymongo, "pymongo.errors": fake_pymongo_errors}):
        bulk_exc = FakeBulkWriteError(
            {"nInserted": 0, "writeErrors": [{"code": 11000, "errmsg": "dup"}]}
        )
        target_coll.insert_many = AsyncMock(side_effect=bulk_exc)

        handler, _ = await _connected_handler(rt)
        handler._streams["s1"] = _MongoStreamState("testdb", "items", WriteMode.WRITE_MODE_INSERT)
        handler._batch_commits_ready.add("testdb")

        result = await handler.write_batch(
            run_id="r", stream_id="s1", batch_seq=0,
            record_batch=_make_batch({"id": 1}), record_ids=["r1"], cursor=_cursor(),
        )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    assert result.records_written == 0


@pytest.mark.asyncio
async def test_write_batch_partial_duplicate_insert_returns_n_inserted():
    """Partial BulkWriteError (some dup, some inserted) returns the inserted count."""
    import sys
    from types import ModuleType

    rt, commits_coll, target_coll, db_mock = _make_runtime()

    class FakeBulkWriteError(Exception):
        def __init__(self, details):
            self.details = details

    fake_pymongo_errors = ModuleType("pymongo.errors")
    fake_pymongo_errors.BulkWriteError = FakeBulkWriteError
    fake_pymongo = ModuleType("pymongo")
    fake_pymongo.errors = fake_pymongo_errors

    with patch.dict(sys.modules, {"pymongo": fake_pymongo, "pymongo.errors": fake_pymongo_errors}):
        bulk_exc = FakeBulkWriteError(
            {"nInserted": 1, "writeErrors": [{"code": 11000, "errmsg": "dup"}]}
        )
        target_coll.insert_many = AsyncMock(side_effect=bulk_exc)

        handler, _ = await _connected_handler(rt)
        handler._streams["s1"] = _MongoStreamState("testdb", "items", WriteMode.WRITE_MODE_INSERT)
        handler._batch_commits_ready.add("testdb")

        result = await handler.write_batch(
            run_id="r", stream_id="s1", batch_seq=0,
            record_batch=_make_batch({"id": 1}, {"id": 2}),
            record_ids=["r1", "r2"], cursor=_cursor(),
        )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    assert result.records_written == 1


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

def test_connector_type():
    assert MongoDbDestinationHandler().connector_type == "mongodb"


def test_supports_properties():
    handler = MongoDbDestinationHandler()
    assert handler.supports_upsert is True
    assert handler.supports_transactions is False
    assert handler.supports_bulk_load is False


# ---------------------------------------------------------------------------
# _write_conflict_keys preference
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_configure_schema_uses_enriched_conflict_keys():
    """The engine stores resolved conflict keys under _write_conflict_keys; the
    handler must prefer that over the static endpoint declaration."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    handler, _ = await _connected_handler(rt)

    endpoint = {
        "collection": "orders",
        "database": "testdb",
        "operations": {"write": {"conflict_keys": ["old_key"]}},
        "_write_conflict_keys": ["resolved_key"],
    }
    handler.set_stream_endpoints({"s1": endpoint})

    await handler.configure_schema(_schema_spec("s1", WriteMode.WRITE_MODE_UPSERT))

    assert handler._streams["s1"].conflict_keys == ["resolved_key"]


# ---------------------------------------------------------------------------
# Upsert: reject missing conflict keys and exclude _id from $set
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_batch_upsert_missing_conflict_key_returns_retryable():
    """A document missing a conflict key must return RETRYABLE, not crash."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()

    with patch.dict(sys.modules, {
        "pymongo": MagicMock(UpdateOne=lambda f, u, **kw: (f, u)),
    }):
        handler, _ = await _connected_handler(rt)
        handler._streams["s1"] = _MongoStreamState(
            "testdb", "users", WriteMode.WRITE_MODE_UPSERT, conflict_keys=["email"]
        )
        handler._batch_commits_ready.add("testdb")

        # Document is missing the "email" conflict key
        batch = _make_batch({"name": "Alice"})
        result = await handler.write_batch(
            run_id="r", stream_id="s1", batch_seq=0,
            record_batch=batch, record_ids=["r1"], cursor=_cursor(),
        )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE


# ---------------------------------------------------------------------------
# JSON field decoding
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_configure_schema_collects_json_fields():
    """Fields declared with arrow_type: "Json" must be collected into json_fields."""
    rt, *_ = _make_runtime()
    handler, _ = await _connected_handler(rt)

    endpoint = {
        "collection": "events",
        "database": "testdb",
        "properties": {
            "id": {"arrow_type": "string"},
            "payload": {"arrow_type": "Json"},
        },
    }
    handler.set_stream_endpoints({"s1": endpoint})
    await handler.configure_schema(_schema_spec("s1"))

    assert handler._streams["s1"].json_fields == {"payload"}


@pytest.mark.asyncio
async def test_write_batch_decodes_json_fields():
    """Json-typed fields encoded as strings must be decoded before writing."""
    import json as _json

    rt, commits_coll, target_coll, db_mock = _make_runtime()
    written_docs = []

    async def capture_insert(docs, **kwargs):
        written_docs.extend(docs)
        return MagicMock(inserted_ids=[f"id{i}" for i in range(len(docs))])

    target_coll.insert_many = AsyncMock(side_effect=capture_insert)

    handler, _ = await _connected_handler(rt)
    handler._streams["s1"] = _MongoStreamState(
        "testdb", "events", WriteMode.WRITE_MODE_INSERT,
        json_fields={"payload"},
    )
    handler._batch_commits_ready.add("testdb")

    batch = _make_batch({"id": "e1", "payload": _json.dumps({"key": "value"})})
    result = await handler.write_batch(
        run_id="r", stream_id="s1", batch_seq=0,
        record_batch=batch, record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    assert written_docs[0]["payload"] == {"key": "value"}


# ---------------------------------------------------------------------------
# find_one error handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_batch_find_one_failure_returns_retryable():
    """A network error during the idempotency find_one must return RETRYABLE,
    not propagate as an unhandled exception that bypasses BatchWriteResult."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    commits_coll.find_one = AsyncMock(side_effect=RuntimeError("connection reset"))

    handler, _ = await _connected_handler(rt)
    handler._streams["s1"] = _MongoStreamState("testdb", "orders", WriteMode.WRITE_MODE_INSERT)
    handler._batch_commits_ready.add("testdb")

    result = await handler.write_batch(
        run_id="r", stream_id="s1", batch_seq=0,
        record_batch=_make_batch({"id": 1}), record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    assert "Idempotency check failed" in result.failure_summary
    target_coll.insert_many.assert_not_called()


# ---------------------------------------------------------------------------
# Malformed JSON field returns FATAL
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_batch_malformed_json_field_returns_fatal():
    """A batch with a malformed Json column must return FATAL_FAILURE — it
    cannot be fixed by retrying."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()

    handler, _ = await _connected_handler(rt)
    handler._streams["s1"] = _MongoStreamState(
        "testdb", "events", WriteMode.WRITE_MODE_INSERT,
        json_fields={"payload"},
    )
    handler._batch_commits_ready.add("testdb")

    batch = _make_batch({"id": "e1", "payload": "not-valid-json{"})
    result = await handler.write_batch(
        run_id="r", stream_id="s1", batch_seq=0,
        record_batch=batch, record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    target_coll.insert_many.assert_not_called()


# ---------------------------------------------------------------------------
# UPSERT with empty conflict_keys at write time
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_batch_upsert_empty_conflict_keys_at_write_time_returns_retryable():
    """If UPSERT stream state somehow has empty conflict_keys at write time,
    _write_docs must raise rather than silently falling through to insert."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()

    handler, _ = await _connected_handler(rt)
    # Inject inconsistent state directly (bypassing configure_schema validation)
    handler._streams["s1"] = _MongoStreamState(
        "testdb", "items", WriteMode.WRITE_MODE_UPSERT, conflict_keys=[]
    )
    handler._batch_commits_ready.add("testdb")

    batch = _make_batch({"id": 1})
    result = await handler.write_batch(
        run_id="r", stream_id="s1", batch_seq=0,
        record_batch=batch, record_ids=["r1"], cursor=_cursor(),
    )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    target_coll.bulk_write.assert_not_called()
    target_coll.insert_many.assert_not_called()


# ---------------------------------------------------------------------------
# connect() failure path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connect_failure_leaves_handler_not_connected():
    """A materialize() failure must propagate; _connected must remain False."""
    rt, *_ = _make_runtime()
    rt.materialize = AsyncMock(side_effect=RuntimeError("auth failed"))

    handler = MongoDbDestinationHandler()
    with pytest.raises(RuntimeError, match="auth failed"):
        await handler.connect(rt)

    assert handler._connected is False


# ---------------------------------------------------------------------------
# UPSERT commit-record failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_batch_upsert_commit_failure_returns_retryable():
    """Upsert mode: a lost commit record must return RETRYABLE, same as INSERT."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    commits_coll.insert_one = AsyncMock(side_effect=RuntimeError("write conflict"))

    with patch.dict(sys.modules, {
        "pymongo": MagicMock(UpdateOne=lambda f, u, **kw: (f, u)),
    }):
        handler, _ = await _connected_handler(rt)
        handler._streams["s1"] = _MongoStreamState(
            "testdb", "users", WriteMode.WRITE_MODE_UPSERT, conflict_keys=["email"]
        )
        handler._batch_commits_ready.add("testdb")

        batch = _make_batch({"email": "a@b.com", "name": "Alice"})
        result = await handler.write_batch(
            run_id="r", stream_id="s1", batch_seq=0,
            record_batch=batch, record_ids=["r1"], cursor=_cursor(),
        )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE


# ---------------------------------------------------------------------------
# Upsert correctness: _id immutability, null keys, duplicate-key failures
# (issue #92 review P1s)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_batch_upsert_excludes_id_from_set_when_id_is_conflict_key():
    """_id must never appear in $set, even when it is the conflict key — Mongo
    rejects an in-place _id update with ImmutableField."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()

    with patch.dict(sys.modules, {
        "pymongo": MagicMock(UpdateOne=lambda f, u, **kw: (f, u)),
    }):
        handler, _ = await _connected_handler(rt)
        handler._streams["s1"] = _MongoStreamState(
            "testdb", "users", WriteMode.WRITE_MODE_UPSERT, conflict_keys=["_id"]
        )
        handler._batch_commits_ready.add("testdb")

        batch = _make_batch({"_id": "u1", "name": "Alice"})
        result = await handler.write_batch(
            run_id="r", stream_id="s1", batch_seq=0,
            record_batch=batch, record_ids=["r1"], cursor=_cursor(),
        )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    ops = target_coll.bulk_write.call_args[0][0]
    filter_doc, update = ops[0]
    assert filter_doc == {"_id": "u1"}
    assert update == {"$set": {"name": "Alice"}}  # _id excluded


@pytest.mark.asyncio
async def test_write_batch_upsert_id_only_doc_uses_set_on_insert():
    """A doc carrying only its _id conflict key has nothing to $set; it must use
    $setOnInsert so the upsert is still valid ($set rejects an empty document)."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()

    with patch.dict(sys.modules, {
        "pymongo": MagicMock(UpdateOne=lambda f, u, **kw: (f, u)),
    }):
        handler, _ = await _connected_handler(rt)
        handler._streams["s1"] = _MongoStreamState(
            "testdb", "users", WriteMode.WRITE_MODE_UPSERT, conflict_keys=["_id"]
        )
        handler._batch_commits_ready.add("testdb")

        batch = _make_batch({"_id": "u1"})
        result = await handler.write_batch(
            run_id="r", stream_id="s1", batch_seq=0,
            record_batch=batch, record_ids=["r1"], cursor=_cursor(),
        )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    ops = target_coll.bulk_write.call_args[0][0]
    filter_doc, update = ops[0]
    assert filter_doc == {"_id": "u1"}
    assert update == {"$setOnInsert": {"_id": "u1"}}


@pytest.mark.asyncio
async def test_write_batch_upsert_null_conflict_key_returns_retryable():
    """A present-but-null conflict key must be rejected: {k: None} also matches
    missing-field docs in Mongo and would collapse unrelated records."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()

    with patch.dict(sys.modules, {
        "pymongo": MagicMock(UpdateOne=lambda f, u, **kw: (f, u)),
    }):
        handler, _ = await _connected_handler(rt)
        handler._streams["s1"] = _MongoStreamState(
            "testdb", "users", WriteMode.WRITE_MODE_UPSERT, conflict_keys=["email"]
        )
        handler._batch_commits_ready.add("testdb")

        batch = _make_batch({"email": None, "name": "Alice"})
        result = await handler.write_batch(
            run_id="r", stream_id="s1", batch_seq=0,
            record_batch=batch, record_ids=["r1"], cursor=_cursor(),
        )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    target_coll.bulk_write.assert_not_called()


@pytest.mark.asyncio
async def test_write_batch_upsert_duplicate_key_returns_retryable_not_success():
    """A duplicate-key failure on an unordered upsert means a racing op's $set was
    dropped; the batch must fail (retryable) rather than be committed as success."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()
    target_coll.bulk_write = AsyncMock(
        side_effect=RuntimeError("E11000 duplicate key error")
    )

    with patch.dict(sys.modules, {
        "pymongo": MagicMock(UpdateOne=lambda f, u, **kw: (f, u)),
    }):
        handler, _ = await _connected_handler(rt)
        handler._streams["s1"] = _MongoStreamState(
            "testdb", "users", WriteMode.WRITE_MODE_UPSERT, conflict_keys=["email"]
        )
        handler._batch_commits_ready.add("testdb")

        batch = _make_batch({"email": "a@b.com", "name": "Alice"})
        result = await handler.write_batch(
            run_id="r", stream_id="s1", batch_seq=0,
            record_batch=batch, record_ids=["r1"], cursor=_cursor(),
        )

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    assert "duplicate key" in result.failure_summary
    commits_coll.insert_one.assert_not_awaited()  # no commit recorded


@pytest.mark.asyncio
async def test_write_batch_upsert_preserves_source_id_on_insert():
    """When _id is present but is NOT a conflict key, it must be pinned via
    $setOnInsert (preserved on insert) while staying out of $set (immutable on
    update) — otherwise an upsert insert would mint a fresh _id and drop the
    source's."""
    rt, commits_coll, target_coll, db_mock = _make_runtime()

    with patch.dict(sys.modules, {
        "pymongo": MagicMock(UpdateOne=lambda f, u, **kw: (f, u)),
    }):
        handler, _ = await _connected_handler(rt)
        handler._streams["s1"] = _MongoStreamState(
            "testdb", "users", WriteMode.WRITE_MODE_UPSERT, conflict_keys=["email"]
        )
        handler._batch_commits_ready.add("testdb")

        batch = _make_batch({"_id": "x1", "email": "a@b.com", "name": "Alice"})
        result = await handler.write_batch(
            run_id="r", stream_id="s1", batch_seq=0,
            record_batch=batch, record_ids=["r1"], cursor=_cursor(),
        )

    assert result.status == AckStatus.ACK_STATUS_SUCCESS
    ops = target_coll.bulk_write.call_args[0][0]
    filter_doc, update = ops[0]
    assert filter_doc == {"email": "a@b.com"}
    assert update["$set"] == {"email": "a@b.com", "name": "Alice"}  # _id not in $set
    assert update["$setOnInsert"] == {"_id": "x1"}  # source _id preserved on insert
