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
