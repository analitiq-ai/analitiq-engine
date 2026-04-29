"""Unit tests for DatabaseDestinationHandler connection handling.

The handler now obtains its engine through the connector-driven transport
factory; these tests patch :func:`build_transport` to substitute a
mocked :class:`SqlAlchemyTransport`.
"""

import ssl

import pytest
from unittest.mock import AsyncMock, patch

from src.destination.connectors.database import DatabaseDestinationHandler
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
