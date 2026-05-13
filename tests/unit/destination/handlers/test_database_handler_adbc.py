"""Unit tests for the ADBC fast path in DatabaseDestinationHandler.

These tests exercise the gating predicate, the dispatch wiring inside
``write_batch``, idempotency-divergence logging, error classification,
and the disconnect/cleanup contract. ADBC itself is mocked — no real
ADBC driver or database is required.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from src.destination.connectors import database as database_module
from src.destination.connectors.database import (
    _ADBC_IMPORT_FAILED,
    AdbcConfigurationError,
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
    query: dict | None = None,
) -> MagicMock:
    url = SimpleNamespace(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        query=query or {},
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
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract(write_mode="upsert")
        assert handler._can_use_adbc(state) is False

    def test_truncate_insert_returns_false(self, handler, monkeypatch, adbc_module_stub):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract(write_mode="truncate_insert")
        assert handler._can_use_adbc(state) is False

    def test_unsupported_driver_returns_false(self, handler, monkeypatch):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        handler._driver = "mysql"
        state = _state_with_contract(write_mode="insert")
        assert handler._can_use_adbc(state) is False
        # _load_adbc_module cached the import-failed sentinel for the
        # unsupported dialect rather than attempting an import.
        assert handler._adbc_module is _ADBC_IMPORT_FAILED

    def test_import_failure_caches_sentinel(self, handler, monkeypatch):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        with patch.object(
            database_module.importlib,
            "import_module",
            side_effect=ImportError("boom"),
        ):
            state = _state_with_contract(write_mode="insert")
            assert handler._can_use_adbc(state) is False
            assert handler._adbc_module is _ADBC_IMPORT_FAILED

    def test_import_failure_logs_warning_when_flag_on(
        self, handler, monkeypatch, caplog
    ):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        with patch.object(
            database_module.importlib,
            "import_module",
            side_effect=ImportError("boom"),
        ):
            with caplog.at_level(logging.WARNING, logger=database_module.logger.name):
                handler._can_use_adbc(_state_with_contract(write_mode="insert"))
        msgs = [r for r in caplog.records if "not importable" in r.message]
        assert msgs, "expected a WARNING when the opted-in driver is missing"
        assert msgs[0].levelno == logging.WARNING

    def test_import_failure_logs_debug_when_flag_off(
        self, handler, monkeypatch, caplog
    ):
        # No env var set. We still call _load_adbc_module directly to
        # check the level: opt-out demotion is mundane.
        with patch.object(
            database_module.importlib,
            "import_module",
            side_effect=ImportError("boom"),
        ):
            with caplog.at_level(logging.DEBUG, logger=database_module.logger.name):
                handler._load_adbc_module()
        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert not warnings

    def test_all_conditions_met_returns_true(
        self, handler, monkeypatch, adbc_module_stub
    ):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract(write_mode="insert")
        assert handler._can_use_adbc(state) is True

    def test_postgres_alias_dialect_matches(
        self, handler, monkeypatch, adbc_module_stub
    ):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        handler._driver = "postgres"  # `postgres+psycopg2` strips to `postgres`
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract(write_mode="insert")
        assert handler._can_use_adbc(state) is True

    def test_unbuildable_uri_returns_false(self, handler, monkeypatch, adbc_module_stub):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        handler._adbc_module = adbc_module_stub
        handler._engine = _build_engine_url_mock(host=None)
        state = _state_with_contract(write_mode="insert")
        assert handler._can_use_adbc(state) is False


class TestDemotionLogging:
    def test_logs_once_per_stream_and_reason(
        self, handler, monkeypatch, caplog
    ):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        handler._driver = "mysql"  # not in _ADBC_MODULES
        state = _state_with_contract(write_mode="insert", table_name="events")
        with caplog.at_level(logging.INFO, logger=database_module.logger.name):
            for _ in range(5):
                assert handler._can_use_adbc(state) is False
        demotion_msgs = [
            r for r in caplog.records
            if "fast path disabled" in r.message and "no ADBC driver" in r.message
        ]
        assert len(demotion_msgs) == 1

    def test_silent_when_flag_off(self, handler, caplog):
        # Flag intentionally not set.
        state = _state_with_contract(write_mode="upsert")
        with caplog.at_level(logging.INFO, logger=database_module.logger.name):
            handler._can_use_adbc(state)
        assert not any("fast path disabled" in r.message for r in caplog.records)


class TestBuildAdbcUri:
    def test_renders_libpq_uri_with_quoted_password(self, handler):
        handler._engine = _build_engine_url_mock(password="p@ss/word")
        uri = handler._build_adbc_uri()
        assert uri == "postgresql://u:p%40ss%2Fword@db.example.com:5432/warehouse"

    def test_returns_none_for_unsupported_backend(self, handler):
        handler._engine = _build_engine_url_mock(backend="sqlite")
        assert handler._build_adbc_uri() is None

    def test_returns_none_when_host_missing(self, handler):
        handler._engine = _build_engine_url_mock(host=None)
        assert handler._build_adbc_uri() is None

    def test_forwards_query_params(self, handler):
        handler._engine = _build_engine_url_mock(
            query={"sslmode": "require", "application_name": "engine"}
        )
        uri = handler._build_adbc_uri()
        assert uri is not None
        assert uri.startswith("postgresql://u:p%40ss%2Fword@db.example.com:5432/warehouse?")
        # Order-insensitive: both keys present, URL-encoded.
        assert "sslmode=require" in uri
        assert "application_name=engine" in uri


class TestWriteViaAdbc:
    @pytest.mark.asyncio
    async def test_passes_cast_output_not_raw_input(self, handler, adbc_module_stub):
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract()
        # Replace cast with one that returns a distinct object so we
        # can prove the cast result (not the raw input) reached ADBC.
        raw = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})
        cast_out = pa.RecordBatch.from_pydict({"id": [9, 9, 9]})
        state.schema_contract.cast_arrow_batch = MagicMock(return_value=cast_out)

        await handler._write_via_adbc(state, raw)

        cursor = adbc_module_stub.connect.return_value.cursor.return_value
        args = cursor.adbc_ingest.call_args.args
        assert args[0] == "events"
        assert args[1] is cast_out
        kwargs = cursor.adbc_ingest.call_args.kwargs
        assert kwargs["mode"] == "append"
        assert kwargs["db_schema_name"] == "public"

    @pytest.mark.asyncio
    async def test_connection_is_reused_across_batches(self, handler, adbc_module_stub):
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract()
        batch = pa.RecordBatch.from_pydict({"id": [1]})

        await handler._write_via_adbc(state, batch)
        await handler._write_via_adbc(state, batch)

        assert adbc_module_stub.connect.call_count == 1
        cursor = adbc_module_stub.connect.return_value.cursor.return_value
        assert cursor.adbc_ingest.call_count == 2

    @pytest.mark.asyncio
    async def test_raises_when_schema_contract_missing(self, handler, adbc_module_stub):
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract()
        state.schema_contract = None
        with pytest.raises(AdbcConfigurationError):
            await handler._write_via_adbc(
                state, pa.RecordBatch.from_pydict({"id": [1]})
            )

    @pytest.mark.asyncio
    async def test_connect_failure_propagates_and_leaves_conn_unset(
        self, handler, adbc_module_stub
    ):
        adbc_module_stub.connect.side_effect = RuntimeError("auth failed")
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract()
        with pytest.raises(RuntimeError, match="auth failed"):
            await handler._write_via_adbc(
                state, pa.RecordBatch.from_pydict({"id": [1]})
            )
        assert handler._adbc_conn is None

    @pytest.mark.asyncio
    async def test_ingest_failure_invalidates_cached_connection(
        self, handler, adbc_module_stub
    ):
        handler._adbc_module = adbc_module_stub
        cursor = adbc_module_stub.connect.return_value.cursor.return_value
        cursor.adbc_ingest.side_effect = RuntimeError("backend exploded")

        state = _state_with_contract()
        with pytest.raises(RuntimeError, match="backend exploded"):
            await handler._write_via_adbc(
                state, pa.RecordBatch.from_pydict({"id": [1]})
            )

        # Poisoned connection nulled + best-effort close attempted.
        assert handler._adbc_conn is None
        adbc_module_stub.connect.return_value.close.assert_called_once()

        # A second batch reconnects rather than reusing the broken handle.
        cursor.adbc_ingest.side_effect = None
        await handler._write_via_adbc(
            state, pa.RecordBatch.from_pydict({"id": [2]})
        )
        assert adbc_module_stub.connect.call_count == 2

    @pytest.mark.asyncio
    async def test_open_raises_adbc_error_when_module_missing(self, handler):
        handler._driver = "mysql"  # not registered
        state = _state_with_contract()
        with pytest.raises(AdbcConfigurationError):
            await handler._write_via_adbc(
                state, pa.RecordBatch.from_pydict({"id": [1]})
            )


class TestWriteBatchDispatch:
    @pytest.mark.asyncio
    async def test_dispatches_to_adbc_when_gated_on(
        self, handler, monkeypatch, adbc_module_stub
    ):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract(write_mode="insert")
        handler._streams["s1"] = state
        handler._check_batch_committed = AsyncMock(return_value=None)
        handler._record_batch_commit = AsyncMock()

        batch = pa.RecordBatch.from_pydict({"id": [1, 2]})
        result = await handler.write_batch(
            run_id="r1", stream_id="s1", batch_seq=0,
            record_batch=batch, record_ids=["1", "2"],
            cursor=Cursor(token=b"tok"),
        )

        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.records_written == 2
        cursor = adbc_module_stub.connect.return_value.cursor.return_value
        cursor.adbc_ingest.assert_called_once()
        handler._record_batch_commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_falls_back_to_sqlalchemy_when_flag_off(
        self, handler, adbc_module_stub
    ):
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract(write_mode="insert")
        handler._streams["s1"] = state
        handler._check_batch_committed = AsyncMock(return_value=None)
        handler._prepare_for_sqlalchemy = MagicMock(return_value=[{"id": 1}])
        handler._insert_records = AsyncMock()
        handler._record_batch_commit_in_txn = AsyncMock()
        conn = AsyncMock()
        begin_ctx = AsyncMock()
        begin_ctx.__aenter__.return_value = conn
        begin_ctx.__aexit__.return_value = None
        handler._engine.begin = MagicMock(return_value=begin_ctx)

        batch = pa.RecordBatch.from_pydict({"id": [1]})
        result = await handler.write_batch(
            run_id="r1", stream_id="s1", batch_seq=0,
            record_batch=batch, record_ids=["1"],
            cursor=Cursor(token=b"tok"),
        )

        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        adbc_module_stub.connect.assert_not_called()
        handler._insert_records.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_commit_record_failure_after_ingest_logs_and_returns_retryable(
        self, handler, monkeypatch, adbc_module_stub, caplog
    ):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract(write_mode="insert")
        handler._streams["s1"] = state
        handler._check_batch_committed = AsyncMock(return_value=None)
        handler._record_batch_commit = AsyncMock(
            side_effect=RuntimeError("commit-record table missing")
        )

        batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})
        with caplog.at_level(logging.ERROR, logger=database_module.logger.name):
            result = await handler.write_batch(
                run_id="r1", stream_id="s1", batch_seq=0,
                record_batch=batch, record_ids=["1", "2", "3"],
                cursor=Cursor(token=b"tok"),
            )

        # Outer except converts to retryable failure (engine retries it).
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        # Ingest happened.
        cursor = adbc_module_stub.connect.return_value.cursor.return_value
        cursor.adbc_ingest.assert_called_once()
        # Explicit warning about the duplication window.
        divergence = [
            r for r in caplog.records
            if "retry will duplicate" in r.message
        ]
        assert divergence, "expected an ERROR-level log when commit-record fails after ingest"
        assert divergence[0].levelno == logging.ERROR

    @pytest.mark.asyncio
    async def test_adbc_config_error_returns_fatal(
        self, handler, monkeypatch, adbc_module_stub
    ):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        # All gating passes (driver imported, URI fine, write_mode insert)
        # but the schema contract is missing — a deterministic config error.
        handler._adbc_module = adbc_module_stub
        state = _state_with_contract(write_mode="insert")
        state.schema_contract = None
        handler._streams["s1"] = state
        handler._check_batch_committed = AsyncMock(return_value=None)

        batch = pa.RecordBatch.from_pydict({"id": [1]})
        result = await handler.write_batch(
            run_id="r1", stream_id="s1", batch_seq=0,
            record_batch=batch, record_ids=["1"],
            cursor=Cursor(token=b"tok"),
        )
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "adbc:" in (result.failure_summary or "")


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
        handler._runtime.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_failure_logged_at_error_and_runtime_still_released(
        self, handler, caplog
    ):
        adbc_conn = MagicMock()
        adbc_conn.close.side_effect = RuntimeError("already closed")
        handler._adbc_conn = adbc_conn
        handler._runtime = AsyncMock()
        handler._runtime.close = AsyncMock()

        with caplog.at_level(logging.ERROR, logger=database_module.logger.name):
            await handler.disconnect()

        # ERROR with exc_info on a resource leak (was WARNING before).
        errors = [
            r for r in caplog.records
            if "Failed to close ADBC connection" in r.message
        ]
        assert errors and errors[0].levelno == logging.ERROR
        assert errors[0].exc_info is not None

        assert handler._adbc_conn is None
        assert handler._connected is False
        # Engine is still released so we don't leak it on top of the
        # ADBC handle.
        handler._runtime.close.assert_awaited_once()
