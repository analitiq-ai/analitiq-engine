"""Unit tests for the source-side ADBC fast path.

Verifies the eligibility predicate and the connection / page-fetch
shape of ``src.source.drivers.adbc_reader``. The actual ADBC driver
is mocked — the focus is on the dispatch and the per-page Arrow
materialization.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from src.source.drivers import adbc_reader
from src.source.drivers.adbc_reader import (
    open_session,
    source_adbc_eligible,
)
from src.shared.adbc_registry import AdbcConfigurationError


def _engine(backend: str = "postgresql") -> MagicMock:
    url = SimpleNamespace(
        host="db.example.com",
        port=5432,
        username="u",
        password="pw",
        database="warehouse",
        query={},
        get_backend_name=lambda: backend,
    )
    engine = MagicMock()
    engine.url = url
    return engine


@pytest.fixture(autouse=True)
def _disable_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ADBC_FAST_PATH", raising=False)
    # Demotion-log dedupe is process-wide; reset between tests so each
    # eligibility scenario observes its own first-time log.
    adbc_reader._demotion_logged.clear()


class TestDemotionLogging:
    def test_silent_when_flag_off(self, caplog):
        import logging as _logging
        with caplog.at_level(_logging.INFO, logger=adbc_reader.logger.name):
            assert source_adbc_eligible("postgresql", _engine()) is False
        # Flag off → demotion is the default; no signal.
        assert not any("fast path disabled" in r.message for r in caplog.records)

    def test_logs_once_per_reason_when_flag_on(self, monkeypatch, caplog):
        import logging as _logging
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        with patch.object(
            adbc_reader,
            "load_adbc_module",
            return_value=adbc_reader._ADBC_IMPORT_FAILED,
        ):
            with caplog.at_level(_logging.INFO, logger=adbc_reader.logger.name):
                # Two calls, same reason — only one demotion log.
                source_adbc_eligible("postgresql", _engine())
                source_adbc_eligible("postgresql", _engine())
        demotion = [
            r for r in caplog.records
            if "fast path disabled" in r.message
        ]
        assert len(demotion) == 1

    def test_different_reasons_each_log(self, monkeypatch, caplog):
        import logging as _logging
        monkeypatch.setenv("ADBC_FAST_PATH", "1")

        with caplog.at_level(_logging.INFO, logger=adbc_reader.logger.name):
            # Reason 1: unknown dialect → no URI builder
            source_adbc_eligible("nonexistent", _engine())
            # Reason 2: known dialect but module unavailable
            with patch.object(
                adbc_reader,
                "load_adbc_module",
                return_value=adbc_reader._ADBC_IMPORT_FAILED,
            ):
                source_adbc_eligible("postgresql", _engine())

        demotion = [r for r in caplog.records if "fast path disabled" in r.message]
        assert len(demotion) == 2


class TestEligibility:
    def test_disabled_by_default(self):
        assert source_adbc_eligible("postgresql", _engine()) is False

    def test_enabled_with_flag_and_supported_dialect(self, monkeypatch):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        with patch.object(adbc_reader, "load_adbc_module", return_value=MagicMock()):
            assert source_adbc_eligible("postgresql", _engine()) is True

    def test_disabled_when_module_unavailable(self, monkeypatch):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        with patch.object(
            adbc_reader,
            "load_adbc_module",
            return_value=adbc_reader._ADBC_IMPORT_FAILED,
        ):
            assert source_adbc_eligible("postgresql", _engine()) is False

    def test_disabled_for_dialect_without_uri_builder(self, monkeypatch):
        # Snowflake is in _ADBC_MODULES but has no URI builder, so
        # eligibility must short-circuit before importing the driver.
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        assert source_adbc_eligible("snowflake", _engine("snowflake")) is False

    def test_disabled_for_unknown_dialect(self, monkeypatch):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        assert source_adbc_eligible("foobardb", _engine()) is False

    def test_disabled_when_engine_is_none(self, monkeypatch):
        monkeypatch.setenv("ADBC_FAST_PATH", "1")
        assert source_adbc_eligible("postgresql", None) is False


class TestOpenSession:
    @pytest.mark.asyncio
    async def test_raises_when_module_missing(self):
        with patch.object(
            adbc_reader,
            "load_adbc_module",
            return_value=adbc_reader._ADBC_IMPORT_FAILED,
        ):
            with pytest.raises(AdbcConfigurationError, match="not available"):
                async with open_session("postgresql", _engine()):
                    pass

    @pytest.mark.asyncio
    async def test_raises_when_uri_unbuildable(self):
        with patch.object(adbc_reader, "load_adbc_module", return_value=MagicMock()):
            with patch.object(adbc_reader, "build_adbc_uri", return_value=None):
                with pytest.raises(AdbcConfigurationError, match="URI"):
                    async with open_session("postgresql", _engine()):
                        pass

    @pytest.mark.asyncio
    async def test_yields_session_and_closes_connection(self):
        module = MagicMock()
        conn = MagicMock()
        module.connect.return_value = conn
        with patch.object(adbc_reader, "load_adbc_module", return_value=module):
            with patch.object(
                adbc_reader, "build_adbc_uri", return_value="postgresql://x"
            ):
                async with open_session("postgresql", _engine()) as session:
                    assert session.dialect == "postgresql"
                    assert session._conn is conn
        module.connect.assert_called_once_with("postgresql://x")
        conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_failure_swallowed(self):
        module = MagicMock()
        conn = MagicMock()
        conn.close.side_effect = RuntimeError("already closed")
        module.connect.return_value = conn
        with patch.object(adbc_reader, "load_adbc_module", return_value=module):
            with patch.object(
                adbc_reader, "build_adbc_uri", return_value="postgresql://x"
            ):
                # The context manager exit must not propagate the close
                # error -- the engine has the original work product, the
                # close failure only leaves a debug-level trace.
                async with open_session("postgresql", _engine()):
                    pass


class TestFetchPage:
    def _make_session_with_table(self, table: pa.Table):
        module = MagicMock()
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetch_arrow_table.return_value = table
        conn.cursor.return_value = cursor
        module.connect.return_value = conn
        return module, conn, cursor

    @pytest.mark.asyncio
    async def test_returns_batches_for_non_empty_result(self):
        table = pa.Table.from_pydict({"id": [1, 2, 3]})
        module, _, cursor = self._make_session_with_table(table)
        with patch.object(adbc_reader, "load_adbc_module", return_value=module):
            with patch.object(
                adbc_reader, "build_adbc_uri", return_value="postgresql://x"
            ):
                async with open_session("postgresql", _engine()) as session:
                    batches = await session.fetch_page(
                        "SELECT * FROM events WHERE id > $1", [0]
                    )
        cursor.execute.assert_called_once_with(
            "SELECT * FROM events WHERE id > $1", [0]
        )
        assert batches
        assert sum(b.num_rows for b in batches) == 3

    @pytest.mark.asyncio
    async def test_empty_result_returns_empty_list(self):
        table = pa.Table.from_pydict({"id": pa.array([], type=pa.int64())})
        module, _, _ = self._make_session_with_table(table)
        with patch.object(adbc_reader, "load_adbc_module", return_value=module):
            with patch.object(
                adbc_reader, "build_adbc_uri", return_value="postgresql://x"
            ):
                async with open_session("postgresql", _engine()) as session:
                    batches = await session.fetch_page("SELECT 1", None)
        assert batches == []

    @pytest.mark.asyncio
    async def test_execute_called_without_params_when_none(self):
        table = pa.Table.from_pydict({"id": [1]})
        module, _, cursor = self._make_session_with_table(table)
        with patch.object(adbc_reader, "load_adbc_module", return_value=module):
            with patch.object(
                adbc_reader, "build_adbc_uri", return_value="postgresql://x"
            ):
                async with open_session("postgresql", _engine()) as session:
                    await session.fetch_page("SELECT 1", None)
        cursor.execute.assert_called_once_with("SELECT 1")

    @pytest.mark.asyncio
    async def test_cursor_closed_after_fetch(self):
        table = pa.Table.from_pydict({"id": [1]})
        module, _, cursor = self._make_session_with_table(table)
        with patch.object(adbc_reader, "load_adbc_module", return_value=module):
            with patch.object(
                adbc_reader, "build_adbc_uri", return_value="postgresql://x"
            ):
                async with open_session("postgresql", _engine()) as session:
                    await session.fetch_page("SELECT 1", None)
        cursor.close.assert_called_once()


class TestDatabaseConnectorAdbcDispatch:
    """End-to-end test that ``DatabaseConnector.read_batches`` actually
    routes through ``_read_via_adbc`` when the ADBC fast path is
    eligible, and through the SQLAlchemy path when it isn't.
    """

    def _endpoint_doc(self):
        return {
            "database_object": {"schema": "public", "name": "events"},
            "columns": [
                {"name": "id", "native_type": "INTEGER", "arrow_type": "Int64", "nullable": False},
                {"name": "name", "native_type": "TEXT", "arrow_type": "Utf8", "nullable": True},
            ],
        }

    def _config(self):
        return {
            "endpoint_document": self._endpoint_doc(),
            "stream_source": {
                "replication": {"method": "full_refresh"},
            },
        }

    @pytest.mark.asyncio
    async def test_dispatches_to_adbc_when_eligible(self, monkeypatch):
        from src.source.connectors.database import DatabaseConnector
        from unittest.mock import AsyncMock

        connector = DatabaseConnector()
        connector._engine = MagicMock()
        connector._driver = "postgresql"
        connector._initialized = True
        connector._runtime = MagicMock()

        state_manager = MagicMock()
        state_manager.get_cursor = AsyncMock(return_value={})
        state_manager.save_cursor = AsyncMock()

        adbc_called = False

        async def _fake_adbc(self, **kwargs):
            nonlocal adbc_called
            adbc_called = True
            yield pa.RecordBatch.from_pydict(
                {"id": [1, 2], "name": ["a", "b"]},
                schema=pa.schema([
                    ("id", pa.int64()),
                    ("name", pa.utf8()),
                ]),
            )

        monkeypatch.setattr(
            "src.source.connectors.database.source_adbc_eligible",
            lambda *a, **kw: True,
        )
        monkeypatch.setattr(DatabaseConnector, "_read_via_adbc", _fake_adbc)

        batches = []
        async for batch in connector.read_batches(
            self._config(),
            state_manager=state_manager,
            stream_name="s1",
            batch_size=10,
        ):
            batches.append(batch)

        assert adbc_called
        assert len(batches) == 1

    @pytest.mark.asyncio
    async def test_falls_back_to_sqlalchemy_when_not_eligible(self, monkeypatch):
        from src.source.connectors.database import DatabaseConnector
        from unittest.mock import AsyncMock

        connector = DatabaseConnector()
        engine = MagicMock()
        connector._engine = engine
        connector._driver = "postgresql"
        connector._initialized = True
        connector._runtime = MagicMock()

        state_manager = MagicMock()
        state_manager.get_cursor = AsyncMock(return_value={})
        state_manager.save_cursor = AsyncMock()

        async def _adbc_should_not_be_called(self, **kwargs):
            raise AssertionError("ADBC path called when ineligible")
            yield  # never reached

        # Fake the SA driver path so we don't need a live DB.
        mock_conn = AsyncMock()
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        mock_conn.exec_driver_sql = AsyncMock(return_value=mock_result)

        acm = MagicMock()
        acm.__aenter__ = AsyncMock(return_value=mock_conn)
        acm.__aexit__ = AsyncMock(return_value=None)

        monkeypatch.setattr(
            "src.source.connectors.database.source_adbc_eligible",
            lambda *a, **kw: False,
        )
        monkeypatch.setattr(
            "src.source.connectors.database.acquire_connection",
            lambda eng: acm,
        )
        monkeypatch.setattr(
            DatabaseConnector, "_read_via_adbc", _adbc_should_not_be_called
        )

        batches = []
        async for batch in connector.read_batches(
            self._config(),
            state_manager=state_manager,
            stream_name="s1",
            batch_size=10,
        ):
            batches.append(batch)

        # No data → no batches; what matters is the SA path was used,
        # and that ADBC path's assertion didn't fire.
        assert batches == []


class TestCursorFieldMissingWarning:
    @pytest.mark.asyncio
    async def test_warns_when_cursor_field_not_in_selected_columns(
        self, monkeypatch, caplog
    ):
        """Incremental stream with a cursor field that wasn't selected
        would silently never advance — emit a WARNING up-front."""
        import logging as _logging
        from src.source.connectors import database as db_mod
        from src.source.connectors.database import DatabaseConnector
        from unittest.mock import AsyncMock

        connector = DatabaseConnector()
        connector._engine = MagicMock()
        connector._driver = "postgresql"
        connector._initialized = True
        connector._runtime = MagicMock()

        state_manager = MagicMock()
        state_manager.get_cursor = AsyncMock(return_value={})
        state_manager.save_cursor = AsyncMock()

        config = {
            "endpoint_document": {
                "database_object": {"schema": "public", "name": "events"},
                "columns": [
                    {"name": "id", "native_type": "INTEGER", "arrow_type": "Int64", "nullable": False},
                    {"name": "updated_at", "native_type": "TEXT", "arrow_type": "Utf8", "nullable": True},
                ],
            },
            "stream_source": {
                "selected_columns": ["id"],  # updated_at dropped
                "replication": {
                    "method": "incremental",
                    "cursor_field": "updated_at",
                },
            },
        }

        monkeypatch.setattr(
            "src.source.connectors.database.source_adbc_eligible",
            lambda *a, **kw: False,
        )
        # Stub the SA path to terminate immediately.
        mock_conn = AsyncMock()
        mock_result = MagicMock()
        mock_result.__iter__ = lambda self: iter([])
        mock_conn.exec_driver_sql = AsyncMock(return_value=mock_result)
        acm = MagicMock()
        acm.__aenter__ = AsyncMock(return_value=mock_conn)
        acm.__aexit__ = AsyncMock(return_value=None)
        monkeypatch.setattr(
            "src.source.connectors.database.acquire_connection",
            lambda eng: acm,
        )

        with caplog.at_level(_logging.WARNING, logger=db_mod.logger.name):
            async for _ in connector.read_batches(
                config,
                state_manager=state_manager,
                stream_name="s1",
                batch_size=10,
            ):
                pass

        warnings = [
            r for r in caplog.records
            if "cursor_field" in r.message and "not in selected columns" in r.message
        ]
        assert warnings, "expected a WARNING when cursor_field is dropped from projection"
