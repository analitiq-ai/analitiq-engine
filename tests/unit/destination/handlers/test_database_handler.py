"""Unit tests for DatabaseDestinationHandler connection handling.

The handler now obtains its engine through the connector-driven transport
factory; these tests patch :func:`build_transport` to substitute a
mocked :class:`SqlAlchemyTransport`.
"""

import json
import ssl

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table
from unittest.mock import AsyncMock, MagicMock, patch

from src.destination.connectors.database import (
    DatabaseDestinationHandler,
    _StreamState,
    _UPSERT_BUILDERS,
    _pg_upsert_stmt,
    _mysql_upsert_stmt,
)
from src.shared.connection_runtime import ConnectionRuntime
from src.shared.transport_factory import SqlAlchemyTransport


@pytest.fixture
def handler():
    return DatabaseDestinationHandler()


@pytest.fixture
def base_config():
    return {
        "connector_slug": "postgres",
        "parameters": {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
        },
        "secret_refs": {"password": "tests/postgres/password"},
    }


def _connector_def(driver: str = "postgresql+asyncpg") -> dict:
    return {
        "slug": "postgres",
        "connector_type": "database",
        "default_transport": "database",
        "transports": {
            "database": {
                "kind": "sqlalchemy",
                "driver": driver,
                "dsn": {"template": f"{driver}://u:p@h:5432/d"},
            }
        },
    }


def _make_runtime(config, *, connector_def=None):
    resolver = AsyncMock()
    resolver.resolve = AsyncMock(return_value={"password": "test_password"})
    return ConnectionRuntime(
        raw_config=config,
        connection_id="test-conn",
        connector_type="database",
        resolver=resolver,
        connector_definition=connector_def or _connector_def(),
    )


def _patch_transport(*, engine=None, side_effect=None):
    """Patch build_transport with a mocked SqlAlchemyTransport result."""
    if side_effect is not None:
        return patch(
            "src.shared.connection_runtime.build_transport",
            new=AsyncMock(side_effect=side_effect),
        )
    transport = SqlAlchemyTransport(
        engine=engine or AsyncMock(),
        driver="postgresql+asyncpg",
        dialect="postgresql",
    )
    return patch(
        "src.shared.connection_runtime.build_transport",
        new=AsyncMock(return_value=transport),
    )


class TestDatabaseHandlerConnect:
    @pytest.mark.asyncio
    async def test_connect_success(self, handler, base_config):
        mock_engine = AsyncMock()
        runtime = _make_runtime(base_config)
        with _patch_transport(engine=mock_engine):
            await handler.connect(runtime)
        assert handler._connected is True
        assert handler._engine is mock_engine
        assert handler._driver == "postgresql"

    @pytest.mark.asyncio
    async def test_connect_failure_propagates(self, handler, base_config):
        runtime = _make_runtime(base_config)
        with _patch_transport(side_effect=OSError("Connection timed out")):
            with pytest.raises(OSError, match="Connection timed out"):
                await handler.connect(runtime)
        assert handler._connected is False

    @pytest.mark.asyncio
    async def test_connect_ssl_error_propagates(self, handler, base_config):
        runtime = _make_runtime(base_config)
        with _patch_transport(side_effect=ssl.SSLError("SSL handshake failed")):
            with pytest.raises(ConnectionError, match="Database connection failed"):
                await handler.connect(runtime)
        assert handler._connected is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exc",
        [
            pytest.param(
                __import__(
                    "src.engine.type_map", fromlist=["UnmappedTypeError"]
                ).UnmappedTypeError("pg", "forward", "MONEY"),
                id="unmapped-type",
            ),
            pytest.param(
                __import__(
                    "src.engine.type_map", fromlist=["InvalidTypeMapError"]
                ).InvalidTypeMapError("rule 3 invalid"),
                id="invalid-type-map",
            ),
            pytest.param(
                __import__(
                    "src.secrets.exceptions", fromlist=["PlaceholderExpansionError"]
                ).PlaceholderExpansionError(
                    placeholder="password", connection_id="x", detail="not found"
                ),
                id="placeholder",
            ),
            pytest.param(
                ValueError("DSN must resolve to a non-empty string"),
                id="value-error",
            ),
        ],
    )
    async def test_connect_propagates_deterministic_errors_unchanged(
        self, handler, base_config, exc
    ):
        """Deterministic configuration / type-map / secret errors must
        surface with their real type so callers can distinguish them from
        transient connection failures."""
        runtime = _make_runtime(base_config)
        with _patch_transport(side_effect=exc):
            with pytest.raises(type(exc)):
                await handler.connect(runtime)
        assert handler._connected is False

    @pytest.mark.asyncio
    async def test_sqlite_connect(self, handler):
        sqlite_config = {
            "connector_slug": "sqlite",
            "parameters": {"database": ":memory:"},
            "secret_refs": {},
        }
        runtime = _make_runtime(
            sqlite_config,
            connector_def={
                "slug": "sqlite",
                "connector_type": "database",
                "default_transport": "database",
                "transports": {
                    "database": {
                        "kind": "sqlalchemy",
                        "driver": "sqlite+aiosqlite",
                        "dsn": {"template": "sqlite+aiosqlite:///:memory:"},
                    }
                },
            },
        )
        # Reset resolver to return empty dict — sqlite has no secrets.
        runtime._resolver.resolve = AsyncMock(return_value={})
        transport = SqlAlchemyTransport(
            engine=AsyncMock(), driver="sqlite+aiosqlite", dialect="sqlite"
        )
        with patch(
            "src.shared.connection_runtime.build_transport",
            new=AsyncMock(return_value=transport),
        ):
            await handler.connect(runtime)
        assert handler._connected is True
        assert handler._driver == "sqlite"


class TestDatabaseHandlerURLEncoding:
    """The transport factory's URL-encode derived function escapes
    reserved characters in credentials before they hit the SQLAlchemy
    URL parser. Connect should succeed regardless of password content."""

    @pytest.mark.asyncio
    async def test_reserved_char_password_works(self, handler):
        config = {
            "connector_slug": "postgres",
            "parameters": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "user",
            },
            "secret_refs": {"password": "tests/postgres/password"},
        }
        runtime = _make_runtime(config)
        # Override the resolver to return a tricky password.
        runtime._resolver.resolve = AsyncMock(
            return_value={"password": "a@b#c%/d:e"}
        )
        with _patch_transport():
            await handler.connect(runtime)
        assert handler._connected is True


_UPSERT_TABLE = Table(
    "events",
    MetaData(),
    Column("id", Integer),
    Column("name", String),
    Column("value", Integer),
)


class TestUpsertRegistry:
    def test_registry_covers_expected_drivers(self):
        assert set(_UPSERT_BUILDERS) == {"postgresql", "postgres", "mysql", "mariadb"}

    def test_postgres_aliases_share_builder(self):
        assert _UPSERT_BUILDERS["postgresql"] is _UPSERT_BUILDERS["postgres"]

    def test_mysql_aliases_share_builder(self):
        assert _UPSERT_BUILDERS["mysql"] is _UPSERT_BUILDERS["mariadb"]

    def test_supports_upsert_true_for_registered_drivers(self, handler):
        for driver in ("postgresql", "postgres", "mysql", "mariadb"):
            handler._driver = driver
            assert handler.supports_upsert is True

    def test_supports_upsert_false_for_unknown_driver(self, handler):
        handler._driver = "sqlite"
        assert handler.supports_upsert is False

    def test_pg_upsert_stmt_returns_compilable_construct(self):
        records = [{"id": 1, "name": "a", "value": 10}]
        record_columns = set(records[0].keys())
        stmt = _pg_upsert_stmt(_UPSERT_TABLE, records, ["id"], record_columns)
        sql = str(stmt.compile(dialect=__import__("sqlalchemy.dialects.postgresql", fromlist=["dialect"]).dialect()))
        assert "ON CONFLICT" in sql
        assert "DO UPDATE" in sql

    def test_mysql_upsert_stmt_returns_compilable_construct(self):
        records = [{"id": 1, "name": "a", "value": 10}]
        record_columns = set(records[0].keys())
        stmt = _mysql_upsert_stmt(_UPSERT_TABLE, records, ["id"], record_columns)
        sql = str(stmt.compile(dialect=__import__("sqlalchemy.dialects.mysql", fromlist=["dialect"]).dialect()))
        assert "ON DUPLICATE KEY UPDATE" in sql

    def test_pg_upsert_excludes_conflict_keys_from_update(self):
        from sqlalchemy.dialects import postgresql as pg_dialect
        records = [{"id": 1, "name": "a", "value": 10}]
        record_columns = set(records[0].keys())
        stmt = _pg_upsert_stmt(_UPSERT_TABLE, records, ["id"], record_columns)
        sql = str(stmt.compile(dialect=pg_dialect.dialect()))
        update_clause = sql.split("DO UPDATE SET")[1]
        assert "excluded.id" not in update_clause
        assert "name" in update_clause
        assert "value" in update_clause

    def test_mysql_upsert_excludes_conflict_keys_from_update(self):
        from sqlalchemy.dialects import mysql as mysql_dialect
        records = [{"id": 1, "name": "a", "value": 10}]
        record_columns = set(records[0].keys())
        stmt = _mysql_upsert_stmt(_UPSERT_TABLE, records, ["id"], record_columns)
        sql = str(stmt.compile(dialect=mysql_dialect.dialect()))
        update_clause = sql.split("ON DUPLICATE KEY UPDATE")[1]
        assert "`id`" not in update_clause
        assert "name" in update_clause
        assert "value" in update_clause

    @pytest.mark.asyncio
    @pytest.mark.parametrize("driver", ["postgresql", "postgres", "mysql", "mariadb"])
    async def test_upsert_records_dispatches_to_builder(self, handler, driver):
        """_upsert_records calls conn.execute (not _insert_records) for registered drivers."""
        handler._driver = driver
        conn = AsyncMock()
        state = _StreamState(
            table=_UPSERT_TABLE,
            conflict_keys=["id"],
            primary_keys=["id"],
        )
        records = [{"id": 1, "name": "a", "value": 10}]
        with patch.object(handler, "_insert_records", new=AsyncMock()) as mock_insert:
            await handler._upsert_records(conn, state, records)
        conn.execute.assert_called_once()
        mock_insert.assert_not_called()

    @pytest.mark.asyncio
    async def test_upsert_records_falls_back_to_insert_for_unregistered_driver(self, handler, caplog):
        """_upsert_records warns and falls back to INSERT for drivers not in the registry."""
        import logging
        handler._driver = "sqlite"
        conn = AsyncMock()
        state = _StreamState(
            table=_UPSERT_TABLE,
            conflict_keys=["id"],
            primary_keys=["id"],
        )
        records = [{"id": 1, "name": "a", "value": 10}]
        with patch.object(handler, "_insert_records", new=AsyncMock()) as mock_insert:
            with caplog.at_level(logging.WARNING, logger="src.destination.connectors.database"):
                await handler._upsert_records(conn, state, records)
        mock_insert.assert_called_once_with(conn, state, records)
        conn.execute.assert_not_called()
        assert "sqlite" in caplog.text

    @pytest.mark.asyncio
    async def test_upsert_records_falls_back_when_no_conflict_keys(self, handler):
        """_upsert_records falls back to INSERT when conflict_keys and primary_keys are empty."""
        handler._driver = "postgresql"
        conn = AsyncMock()
        state = _StreamState(
            table=_UPSERT_TABLE,
            conflict_keys=[],
            primary_keys=[],
        )
        records = [{"id": 1, "name": "a", "value": 10}]
        with patch.object(handler, "_insert_records", new=AsyncMock()) as mock_insert:
            await handler._upsert_records(conn, state, records)
        mock_insert.assert_called_once()
        conn.execute.assert_not_called()


class TestBatchCommitTracker:
    def test_check_committed_false_before_any_record(self):
        from src.state.batch_commit_tracker import BatchCommitTracker
        tracker = BatchCommitTracker()
        assert tracker.check_committed("stream-a", 1) is False

    def test_check_committed_true_after_record(self):
        from src.state.batch_commit_tracker import BatchCommitTracker
        tracker = BatchCommitTracker()
        tracker.record_commit("stream-a", 1, records_written=5, cursor_bytes=b"\x00")
        assert tracker.check_committed("stream-a", 1) is True

    def test_different_streams_dont_collide(self):
        from src.state.batch_commit_tracker import BatchCommitTracker
        tracker = BatchCommitTracker()
        tracker.record_commit("stream-a", 1, records_written=5, cursor_bytes=b"")
        assert tracker.check_committed("stream-b", 1) is False

    def test_different_batch_seq_dont_collide(self):
        from src.state.batch_commit_tracker import BatchCommitTracker
        tracker = BatchCommitTracker()
        tracker.record_commit("stream-a", 1, records_written=5, cursor_bytes=b"")
        assert tracker.check_committed("stream-a", 2) is False


class TestEmitLog:
    def test_format_and_prefix(self, capsys):
        from src.state.log_emitter import emit_log
        emit_log("dlq", {"type": "dlq", "count": 3})
        out = capsys.readouterr().out
        assert out.startswith("ANALITIQ_DLQ::")
        assert json.loads(out.split("::", 1)[1]) == {"type": "dlq", "count": 3}

    def test_category_is_case_insensitive(self, capsys):
        from src.state.log_emitter import emit_log
        emit_log("metrics", {"x": 1})
        out = capsys.readouterr().out
        assert out.startswith("ANALITIQ_METRICS::")

    def test_non_serialisable_value_does_not_raise(self, capsys):
        from src.state.log_emitter import emit_log
        emit_log("dlq", {"bad": object()})
        out = capsys.readouterr().out
        assert out == ""  # nothing emitted to stdout on serialisation failure


class TestEmitStateLog:
    def test_format_and_all_fields_present(self, capsys):
        from src.state.state_emission import emit_state_log
        emit_state_log(
            run_id="run-1",
            pipeline_id="pipe-1",
            stream_id="stream-1",
            cursor_hex="deadbeef",
            cursor_value="2024-01-01",
        )
        out = capsys.readouterr().out
        assert out.startswith("ANALITIQ_STATE::")
        payload = json.loads(out.split("::", 1)[1])
        assert payload["run_id"] == "run-1"
        assert payload["pipeline_id"] == "pipe-1"
        assert payload["stream_id"] == "stream-1"
        assert payload["cursor_hex"] == "deadbeef"
        assert payload["cursor_value"] == "2024-01-01"

    def test_non_serialisable_value_does_not_raise(self, capsys):
        from src.state.state_emission import emit_state_log
        emit_state_log("r", "p", "s", object(), "v")  # type: ignore[arg-type]
        out = capsys.readouterr().out
        assert out == ""
