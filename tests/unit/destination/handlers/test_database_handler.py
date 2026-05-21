"""Unit tests for DatabaseDestinationHandler connection handling.

The handler now obtains its engine through the connector-driven transport
factory; these tests patch :func:`build_transport` to substitute a
mocked :class:`SqlAlchemyTransport`.
"""

import ssl

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table
from unittest.mock import AsyncMock, patch

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
