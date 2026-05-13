"""Unit tests for the ADBC fast path in DatabaseDestinationHandler.

These tests exercise the gating predicate, the dispatch wiring inside
``write_batch``, and the ``adbc_ingest`` invocation. ADBC itself is
mocked — no real ADBC driver or database is required.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from src.destination.connectors import database as database_module
from src.destination.connectors.database import (
    DatabaseDestinationHandler,
    _StreamState,
)
from src.grpc.generated.analitiq.v1 import AckStatus, Cursor


def _state_with_contract(
    write_mode: str = "insert",
    schema_name: str = "public",
    table_name: str = "events",
) -> _StreamState:
    state = _StreamState(
        schema_name=schema_name,
        table_name=table_name,
        write_mode=write_mode,
    )
    # Provide a stub schema contract that returns the batch unchanged so
    # the gating + dispatch tests don't depend on cast semantics.
    contract = MagicMock()
    contract.cast_arrow_batch = MagicMock(side_effect=lambda b: b)
    state.schema_contract = contract
    state.table = MagicMock()
    state.batch_commits_table = MagicMock()
    return state


def _build_engine_url_mock(
    backend: str = "postgresql",
    host: str = "db.example.com",
    port: int = 5432,
    username: str = "u",
    password: str = "p@ss/word",
    database: str = "warehouse",
) -> MagicMock:
    url = SimpleNamespace(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        get_backend_name=lambda: backend,
    )
    engine = MagicMock()
    engine.url = url
    return engine


@pytest.fixture
def handler() -> DatabaseDestinationHandler:
    h = DatabaseDestinationHandler()
    h._driver = "postgresql"
    h._engine = _build_engine_url_mock()
    h._connected = True
    return h


@pytest.fixture
def adbc_module_stub() -> MagicMock:
    """Stand-in for ``adbc_driver_postgresql.dbapi``."""
    module = MagicMock()
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    module.connect.return_value = conn
    return module


@pytest.fixture(autouse=True)
def _disable_adbc_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default the env var to off so each test opts in explicitly."""
    monkeypatch.delenv("ADBC_FAST_PATH", raising=False)


class TestCanUseAdbc:
    def test_default_flag_off_returns_false(self, handler):
        state = _state_with_contract(write_mode="insert")
        assert handler._can_use_adbc(state) is False

    def test_upsert_returns_false(self, handler, monkeypatch, adbc_module_stub):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        with patch.object(
            database_module.importlib,
            "import_module",
            return_value=adbc_module_stub,
        ):
            state = _state_with_contract(write_mode="upsert")
            assert handler._can_use_adbc(state) is False

    def test_truncate_insert_returns_false(self, handler, monkeypatch, adbc_module_stub):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        with patch.object(
            database_module.importlib,
            "import_module",
            return_value=adbc_module_stub,
        ):
            state = _state_with_contract(write_mode="truncate_insert")
            assert handler._can_use_adbc(state) is False

    def test_unsupported_driver_returns_false(
        self, handler, monkeypatch, adbc_module_stub
    ):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        handler._driver = "mysql"  # no ADBC module registered
        with patch.object(
            database_module.importlib,
            "import_module",
            return_value=adbc_module_stub,
        ):
            state = _state_with_contract(write_mode="insert")
            assert handler._can_use_adbc(state) is False

    def test_import_failure_returns_false(self, handler, monkeypatch):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        with patch.object(
            database_module.importlib,
            "import_module",
            side_effect=ImportError("boom"),
        ):
            state = _state_with_contract(write_mode="insert")
            assert handler._can_use_adbc(state) is False
            # Cached as False so repeated calls don't re-import.
            assert handler._adbc_module is False

    def test_all_conditions_met_returns_true(
        self, handler, monkeypatch, adbc_module_stub
    ):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        with patch.object(
            database_module.importlib,
            "import_module",
            return_value=adbc_module_stub,
        ):
            state = _state_with_contract(write_mode="insert")
            assert handler._can_use_adbc(state) is True

    def test_postgres_alias_dialect_matches(
        self, handler, monkeypatch, adbc_module_stub
    ):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        handler._driver = "postgres"  # alias of postgresql
        with patch.object(
            database_module.importlib,
            "import_module",
            return_value=adbc_module_stub,
        ):
            state = _state_with_contract(write_mode="insert")
            assert handler._can_use_adbc(state) is True


class TestBuildAdbcUri:
    def test_renders_libpq_uri_with_quoted_password(self, handler):
        # Password contains characters that must be percent-encoded.
        handler._engine = _build_engine_url_mock(password="p@ss/word")
        uri = handler._build_adbc_uri()
        assert uri == "postgresql://u:p%40ss%2Fword@db.example.com:5432/warehouse"

    def test_returns_none_for_unsupported_backend(self, handler):
        handler._engine = _build_engine_url_mock(backend="sqlite")
        assert handler._build_adbc_uri() is None

    def test_returns_none_when_host_missing(self, handler):
        handler._engine = _build_engine_url_mock(host=None)
        assert handler._build_adbc_uri() is None


class TestWriteViaAdbc:
    @pytest.mark.asyncio
    async def test_cast_then_ingest(self, handler, adbc_module_stub):
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract()
        batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})

        await handler._write_via_adbc(state, batch)

        # cast happens exactly once
        state.schema_contract.cast_arrow_batch.assert_called_once_with(batch)

        # ADBC connect uses the URI built from the engine URL
        adbc_module_stub.connect.assert_called_once()
        connect_uri = adbc_module_stub.connect.call_args.args[0]
        assert connect_uri.startswith("postgresql://")

        # Ingest used append mode with the cast batch and table info
        cursor = adbc_module_stub.connect.return_value.cursor.return_value
        cursor.adbc_ingest.assert_called_once()
        kwargs = cursor.adbc_ingest.call_args.kwargs
        args = cursor.adbc_ingest.call_args.args
        assert args[0] == "events"
        assert args[1] is batch  # cast was a no-op stub
        assert kwargs["mode"] == "append"
        assert kwargs["db_schema_name"] == "public"

        # Commit + cursor close
        adbc_module_stub.connect.return_value.commit.assert_called_once()
        cursor.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_is_reused_across_batches(
        self, handler, adbc_module_stub
    ):
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract()
        batch = pa.RecordBatch.from_pydict({"id": [1]})

        await handler._write_via_adbc(state, batch)
        await handler._write_via_adbc(state, batch)

        # One open, two ingest calls
        assert adbc_module_stub.connect.call_count == 1
        cursor = adbc_module_stub.connect.return_value.cursor.return_value
        assert cursor.adbc_ingest.call_count == 2


class TestWriteBatchDispatch:
    @pytest.mark.asyncio
    async def test_dispatches_to_adbc_when_gated_on(
        self, handler, monkeypatch, adbc_module_stub
    ):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        handler._adbc_module = adbc_module_stub

        state = _state_with_contract(write_mode="insert")
        handler._streams["s1"] = state

        # Idempotency check + commit recorder are both isolated SA paths;
        # patch them so we don't need a real engine.
        handler._check_batch_committed = AsyncMock(return_value=None)
        handler._record_batch_commit = AsyncMock()

        batch = pa.RecordBatch.from_pydict({"id": [1, 2]})
        result = await handler.write_batch(
            run_id="r1",
            stream_id="s1",
            batch_seq=0,
            record_batch=batch,
            record_ids=["1", "2"],
            cursor=Cursor(token=b"tok"),
        )

        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.records_written == 2
        # ADBC ingest fired; SA path was NOT touched (no engine.begin)
        cursor = adbc_module_stub.connect.return_value.cursor.return_value
        cursor.adbc_ingest.assert_called_once()
        handler._record_batch_commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_falls_back_to_sqlalchemy_when_flag_off(
        self, handler, adbc_module_stub
    ):
        # Flag intentionally not set.
        handler._adbc_module = adbc_module_stub

        state = _state_with_contract(write_mode="insert")
        handler._streams["s1"] = state

        handler._check_batch_committed = AsyncMock(return_value=None)

        # Mock the SA path: replace _prepare_for_sqlalchemy and _insert_records.
        handler._prepare_for_sqlalchemy = MagicMock(return_value=[{"id": 1}])
        handler._insert_records = AsyncMock()
        handler._record_batch_commit_in_txn = AsyncMock()

        # Mock the engine.begin() async context.
        conn = AsyncMock()
        begin_ctx = AsyncMock()
        begin_ctx.__aenter__.return_value = conn
        begin_ctx.__aexit__.return_value = None
        handler._engine.begin = MagicMock(return_value=begin_ctx)

        batch = pa.RecordBatch.from_pydict({"id": [1]})
        result = await handler.write_batch(
            run_id="r1",
            stream_id="s1",
            batch_seq=0,
            record_batch=batch,
            record_ids=["1"],
            cursor=Cursor(token=b"tok"),
        )

        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        # ADBC path was never touched
        adbc_module_stub.connect.assert_not_called()
        # SA path ran
        handler._insert_records.assert_awaited_once()


class TestDisconnectClosesAdbc:
    @pytest.mark.asyncio
    async def test_closes_adbc_connection(self, handler):
        adbc_conn = MagicMock()
        handler._adbc_conn = adbc_conn
        handler._runtime = AsyncMock()
        handler._runtime.close = AsyncMock()

        await handler.disconnect()

        adbc_conn.close.assert_called_once()
        assert handler._adbc_conn is None
        assert handler._connected is False

    @pytest.mark.asyncio
    async def test_disconnect_tolerates_adbc_close_failure(self, handler):
        adbc_conn = MagicMock()
        adbc_conn.close.side_effect = RuntimeError("already closed")
        handler._adbc_conn = adbc_conn
        handler._runtime = AsyncMock()
        handler._runtime.close = AsyncMock()

        # Should not raise.
        await handler.disconnect()

        assert handler._adbc_conn is None
        assert handler._connected is False
