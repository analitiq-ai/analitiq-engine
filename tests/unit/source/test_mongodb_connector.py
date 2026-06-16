"""Unit tests for MongoDbSourceConnector.

Tests cover:
* BSON-type coercion (ObjectId, Decimal128, Binary, nested) without a live
  Mongo instance — the bson module is stubbed via a pytest fixture.
* Full-refresh and incremental read paths using an AsyncMock motor client.
* Checkpoint save/load for incremental replication (AsyncMock, matching the
  CheckpointStore Protocol's async interface).
* Safety-window rollback and cutoff enforcement.
* Edge cases: empty collection, missing endpoint_document, missing collection.
"""

from __future__ import annotations

import datetime
import sys
from types import ModuleType
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest


# ---------------------------------------------------------------------------
# Helpers: build a stub bson module so tests run without installing motor
# ---------------------------------------------------------------------------

def _make_bson_stub() -> ModuleType:
    """Return a minimal bson stub with ObjectId, Decimal128, Binary."""
    mod = ModuleType("bson")

    class ObjectId:
        def __init__(self, hex_str: str = "000000000000000000000001"):
            self._hex = hex_str

        def __str__(self) -> str:
            return self._hex

        def __gt__(self, other: Any) -> bool:
            return self._hex > str(other)

    class Decimal128:
        def __init__(self, val: str = "1.5"):
            self._val = val

        def __str__(self) -> str:
            return self._val

    class Binary(bytes):
        pass

    mod.ObjectId = ObjectId
    mod.Decimal128 = Decimal128
    mod.Binary = Binary
    return mod


@pytest.fixture(autouse=True)
def _inject_bson_stub(monkeypatch):
    if "bson" not in sys.modules:
        stub = _make_bson_stub()
        monkeypatch.setitem(sys.modules, "bson", stub)


# ---------------------------------------------------------------------------
# Import under test (after bson stub is in place)
# ---------------------------------------------------------------------------

from src.source.connectors.mongodb import (
    MongoDbSourceConnector,
    _coerce_bson,
    _coerce_document,
    _subtract_safety_window,
)


# ---------------------------------------------------------------------------
# _coerce_bson unit tests
# ---------------------------------------------------------------------------

class TestCoerceBson:
    def test_object_id_to_str(self):
        bson = sys.modules["bson"]
        oid = bson.ObjectId("507f1f77bcf86cd799439011")
        assert _coerce_bson(oid) == "507f1f77bcf86cd799439011"

    def test_decimal128_to_str(self):
        bson = sys.modules["bson"]
        dec = bson.Decimal128("3.14159")
        assert _coerce_bson(dec) == "3.14159"

    def test_binary_to_bytes(self):
        bson = sys.modules["bson"]
        blob = bson.Binary(b"\x00\x01\x02")
        result = _coerce_bson(blob)
        assert isinstance(result, bytes)
        assert result == b"\x00\x01\x02"

    def test_passthrough_scalars(self):
        assert _coerce_bson(42) == 42
        assert _coerce_bson("hello") == "hello"
        assert _coerce_bson(3.14) == 3.14
        assert _coerce_bson(None) is None

    def test_nested_dict(self):
        bson = sys.modules["bson"]
        doc = {"id": bson.ObjectId("abc123abc123abc123abc123"), "value": 1}
        result = _coerce_bson(doc)
        assert result["id"] == "abc123abc123abc123abc123"
        assert result["value"] == 1

    def test_nested_list(self):
        bson = sys.modules["bson"]
        lst = [bson.ObjectId("aaa000aaa000aaa000aaa000"), "string", 99]
        result = _coerce_bson(lst)
        assert result[0] == "aaa000aaa000aaa000aaa000"
        assert result[1] == "string"
        assert result[2] == 99


class TestCoerceDocument:
    def test_full_document(self):
        bson = sys.modules["bson"]
        doc = {
            "_id": bson.ObjectId("507f191e810c19729de860ea"),
            "name": "Alice",
            "score": bson.Decimal128("9.8"),
            "blob": bson.Binary(b"\xff"),
        }
        result = _coerce_document(doc)
        assert result["_id"] == "507f191e810c19729de860ea"
        assert result["name"] == "Alice"
        assert result["score"] == "9.8"
        assert result["blob"] == b"\xff"


# ---------------------------------------------------------------------------
# _subtract_safety_window
# ---------------------------------------------------------------------------

class TestSubtractSafetyWindow:
    def test_tz_aware_input(self):
        base = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        result = _subtract_safety_window(base, 3600)
        assert result == datetime.datetime(2024, 1, 1, 11, 0, 0, tzinfo=datetime.timezone.utc)

    def test_naive_input_gets_utc(self):
        base = datetime.datetime(2024, 6, 15, 10, 0, 0)
        result = _subtract_safety_window(base, 60)
        assert result.tzinfo == datetime.timezone.utc
        assert result.minute == 59


# ---------------------------------------------------------------------------
# Motor cursor / client stubs
# ---------------------------------------------------------------------------

def _make_runtime(db_name: str = "mydb"):
    rt = MagicMock()
    rt.acquire = MagicMock()
    rt.materialize = AsyncMock()
    rt.close = AsyncMock()
    rt.mongo_default_database = db_name
    return rt


def _make_motor_client(pages: list[list[dict]]):
    """Build a stub motor client that returns pages of documents sequentially."""
    page_iter = iter(pages)

    class FakeCursor:
        def __init__(self, docs):
            self._docs = docs
            self._idx = 0

        def sort(self, *args, **kwargs):
            return self

        def limit(self, n):
            return self

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self._idx >= len(self._docs):
                raise StopAsyncIteration
            doc = self._docs[self._idx]
            self._idx += 1
            return doc

    def find_side_effect(*args, **kwargs):
        try:
            page = next(page_iter)
        except StopIteration:
            page = []
        return FakeCursor(page)

    collection_mock = MagicMock()
    collection_mock.find.side_effect = find_side_effect

    db_mock = MagicMock()
    db_mock.__getitem__ = MagicMock(return_value=collection_mock)

    client_mock = MagicMock()
    client_mock.__getitem__ = MagicMock(return_value=db_mock)

    return client_mock, collection_mock


def _make_checkpoint(cursor_value=None):
    """Build an async-correct CheckpointStore mock."""
    cp = MagicMock()
    # CheckpointStore.get_cursor and save_cursor are async — use AsyncMock.
    stored = {"cursor": cursor_value} if cursor_value is not None else None
    cp.get_cursor = AsyncMock(return_value=stored)
    cp.save_cursor = AsyncMock()
    return cp


# ---------------------------------------------------------------------------
# Full-refresh reads
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_read_batches_full_refresh_single_page():
    bson = sys.modules["bson"]
    docs = [
        {"_id": bson.ObjectId("507f191e810c19729de860e1"), "name": "Alice", "age": 30},
        {"_id": bson.ObjectId("507f191e810c19729de860e2"), "name": "Bob", "age": 25},
    ]
    client, _ = _make_motor_client([docs, []])

    runtime = _make_runtime()
    runtime.mongo_client = client

    config = {
        "endpoint_document": {"collection": "users", "database": "mydb"},
        "stream_source": {"replication": {"method": "full_refresh"}},
    }

    connector = MongoDbSourceConnector()
    batches = []
    async for batch in connector.read_batches(
        runtime, config, checkpoint=_make_checkpoint(), stream_name="users"
    ):
        batches.append(batch)

    assert len(batches) == 1
    assert batches[0].num_rows == 2
    assert batches[0].column("name").to_pylist() == ["Alice", "Bob"]


@pytest.mark.asyncio
async def test_read_batches_multi_page():
    bson = sys.modules["bson"]
    page1 = [{"_id": bson.ObjectId(f"507f191e810c19729de8600{i}"), "v": i} for i in range(3)]
    page2 = [{"_id": bson.ObjectId(f"507f191e810c19729de8601{i}"), "v": i + 10} for i in range(2)]

    client, _ = _make_motor_client([page1, page2, []])

    runtime = _make_runtime()
    runtime.mongo_client = client

    config = {
        "endpoint_document": {"collection": "items", "database": "mydb"},
        "stream_source": {"replication": {"method": "full_refresh"}},
    }

    connector = MongoDbSourceConnector()
    batches = []
    async for batch in connector.read_batches(
        runtime, config, checkpoint=_make_checkpoint(), stream_name="items", batch_size=3
    ):
        batches.append(batch)

    assert len(batches) == 2
    assert batches[0].num_rows == 3
    assert batches[1].num_rows == 2


@pytest.mark.asyncio
async def test_read_batches_empty_collection_yields_no_batches():
    """Empty first page → no batches, no save_cursor call."""
    client, _ = _make_motor_client([[]])

    runtime = _make_runtime()
    runtime.mongo_client = client

    config = {
        "endpoint_document": {"collection": "empty", "database": "mydb"},
        "stream_source": {"replication": {"method": "full_refresh"}},
    }

    cp = _make_checkpoint()
    connector = MongoDbSourceConnector()
    batches = []
    async for batch in connector.read_batches(
        runtime, config, checkpoint=cp, stream_name="empty"
    ):
        batches.append(batch)

    assert batches == []
    cp.save_cursor.assert_not_called()


# ---------------------------------------------------------------------------
# Incremental reads + checkpoint protocol
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_read_batches_incremental_saves_cursor():
    """Incremental: checkpoint.get_cursor and save_cursor are awaited."""
    bson = sys.modules["bson"]
    now = datetime.datetime(2024, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    docs = [
        {"_id": bson.ObjectId("507f191e810c19729de860e1"), "updated_at": now, "v": 1},
    ]

    client, _ = _make_motor_client([docs, []])
    runtime = _make_runtime()
    runtime.mongo_client = client

    config = {
        "endpoint_document": {"collection": "events", "database": "mydb"},
        "stream_source": {
            "replication": {"method": "incremental", "cursor_field": "updated_at"}
        },
    }

    cp = _make_checkpoint()
    connector = MongoDbSourceConnector()
    batches = []
    async for batch in connector.read_batches(
        runtime, config, checkpoint=cp, stream_name="events"
    ):
        batches.append(batch)

    assert len(batches) == 1
    cp.get_cursor.assert_awaited_once_with("events", None)
    # save_cursor should be awaited with the high-water mark wrapped in a dict
    cp.save_cursor.assert_awaited_once()
    call_args = cp.save_cursor.call_args
    assert call_args[0][0] == "events"   # stream_name
    assert call_args[0][1] is None        # partition
    saved = call_args[0][2]
    assert isinstance(saved, dict) and saved.get("cursor") == now


@pytest.mark.asyncio
async def test_read_batches_incremental_uses_stored_cursor():
    """Stored cursor is unpacked from {"cursor": value} and used in the filter."""
    bson = sys.modules["bson"]
    prev_cursor = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    docs = [{"_id": bson.ObjectId("507f191e810c19729de860aa"), "updated_at": prev_cursor, "v": 5}]

    client, coll = _make_motor_client([docs, []])
    runtime = _make_runtime()
    runtime.mongo_client = client

    config = {
        "endpoint_document": {"collection": "ev2", "database": "mydb"},
        "stream_source": {
            "replication": {"method": "incremental", "cursor_field": "updated_at"}
        },
    }

    cp = _make_checkpoint(cursor_value=prev_cursor)
    connector = MongoDbSourceConnector()
    async for _ in connector.read_batches(
        runtime, config, checkpoint=cp, stream_name="ev2"
    ):
        pass

    # The first find call's filter should include the $gte clause
    first_call_filter = coll.find.call_args_list[0][0][0]
    assert "updated_at" in first_call_filter
    assert "$gte" in first_call_filter["updated_at"]


@pytest.mark.asyncio
async def test_read_batches_incremental_safety_window_caps_saved_cursor():
    """Safety window: cursor is rolled back for querying; saved value is capped at cutoff."""
    bson = sys.modules["bson"]
    base_cursor = datetime.datetime(2024, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    future_val = datetime.datetime(2024, 6, 1, 13, 0, 0, tzinfo=datetime.timezone.utc)
    docs = [{"_id": bson.ObjectId("507f191e810c19729de860bb"), "ts": future_val, "v": 1}]

    client, _ = _make_motor_client([docs, []])
    runtime = _make_runtime()
    runtime.mongo_client = client

    config = {
        "endpoint_document": {"collection": "ev3", "database": "mydb"},
        "stream_source": {
            "replication": {
                "method": "incremental",
                "cursor_field": "ts",
                "safety_window_seconds": 300,
            }
        },
    }

    cp = _make_checkpoint(cursor_value=base_cursor)
    connector = MongoDbSourceConnector()
    async for _ in connector.read_batches(
        runtime, config, checkpoint=cp, stream_name="ev3"
    ):
        pass

    cp.save_cursor.assert_awaited_once()
    saved_cursor_val = cp.save_cursor.call_args[0][2]["cursor"]
    # The saved cursor is capped at cutoff (approx. now), not at future_val
    assert saved_cursor_val <= datetime.datetime.now(tz=datetime.timezone.utc)


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_read_batches_missing_endpoint_raises():
    runtime = _make_runtime()
    runtime.mongo_client = MagicMock()

    config = {}
    connector = MongoDbSourceConnector()
    from src.source.connectors.base import ReadError
    with pytest.raises(ReadError, match="endpoint_document"):
        async for _ in connector.read_batches(
            runtime, config, checkpoint=_make_checkpoint(), stream_name="x"
        ):
            pass


@pytest.mark.asyncio
async def test_read_batches_missing_collection_raises():
    runtime = _make_runtime()
    runtime.mongo_client = MagicMock()

    config = {
        "endpoint_document": {"database": "mydb"},  # no collection
        "stream_source": {},
    }
    connector = MongoDbSourceConnector()
    from src.source.connectors.base import ReadError
    with pytest.raises(ReadError, match="collection"):
        async for _ in connector.read_batches(
            runtime, config, checkpoint=_make_checkpoint(), stream_name="x"
        ):
            pass


@pytest.mark.asyncio
async def test_incremental_tz_naive_cursor_no_crash_with_safety_window():
    """Motor returns tz-naive datetimes by default; after safety-window rollback
    max_cursor_seen becomes tz-aware. Comparison must not raise TypeError."""
    bson = sys.modules["bson"]
    prev_cursor = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    # Motor returns tz-naive datetimes (no tzinfo)
    tz_naive_ts = datetime.datetime(2024, 1, 1, 1, 0, 0)
    docs = [{"_id": bson.ObjectId("507f191e810c19729de860cc"), "ts": tz_naive_ts, "v": 1}]

    client, _ = _make_motor_client([docs, []])
    runtime = _make_runtime()
    runtime.mongo_client = client

    config = {
        "endpoint_document": {"collection": "ev4", "database": "mydb"},
        "stream_source": {
            "replication": {
                "method": "incremental",
                "cursor_field": "ts",
                "safety_window_seconds": 300,
            }
        },
    }

    cp = _make_checkpoint(cursor_value=prev_cursor)
    connector = MongoDbSourceConnector()
    # Should not raise TypeError from tz-aware vs tz-naive comparison
    batches = []
    async for batch in connector.read_batches(
        runtime, config, checkpoint=cp, stream_name="ev4"
    ):
        batches.append(batch)

    assert len(batches) == 1
    cp.save_cursor.assert_awaited_once()
    saved = cp.save_cursor.call_args[0][2]["cursor"]
    assert saved.tzinfo is not None
