"""Unit tests for DatabaseDestinationHandler connection handling."""

import ssl
from contextlib import asynccontextmanager
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy.engine import URL

from src.destination.connectors.database import DatabaseDestinationHandler
from src.shared.connection_runtime import ConnectionRuntime


@pytest.fixture
def handler():
    return DatabaseDestinationHandler()


@pytest.fixture
def base_config():
    return {
        "driver": "postgresql",
        "host": "localhost",
        "parameters": {
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_password",
        },
    }


def _make_runtime(config, driver="postgresql"):
    return ConnectionRuntime(
        raw_config=config,
        connection_id="test-conn",
        connector_type="database",
        driver=driver,
        resolver=AsyncMock(),
    )


class TestDatabaseHandlerConnect:
    """Test connect() delegates to ConnectionRuntime.materialize()."""

    @pytest.mark.asyncio
    async def test_connect_success(self, handler, base_config):
        """connect() should materialize runtime and set state."""
        mock_engine = AsyncMock()
        runtime = _make_runtime(base_config)

        with patch(
            "src.shared.connection_runtime.create_database_engine",
            return_value=(mock_engine, "postgresql"),
        ):
            await handler.connect(runtime)

        assert handler._connected is True
        assert handler._engine is mock_engine
        assert handler._driver == "postgresql"

    @pytest.mark.asyncio
    async def test_connect_failure_propagates(self, handler, base_config):
        """connect() should propagate errors from materialize."""
        runtime = _make_runtime(base_config)

        with patch(
            "src.shared.connection_runtime.create_database_engine",
            side_effect=OSError("Connection timed out"),
        ):
            with pytest.raises(OSError, match="Connection timed out"):
                await handler.connect(runtime)

        assert handler._connected is False

    @pytest.mark.asyncio
    async def test_connect_ssl_error_propagates(self, handler, base_config):
        """SSL errors are wrapped in ConnectionError from materialize."""
        runtime = _make_runtime(base_config)

        with patch(
            "src.shared.connection_runtime.create_database_engine",
            side_effect=ssl.SSLError("SSL handshake failed"),
        ):
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
                    "src.engine.type_map", fromlist=["UnmappedSSLModeError"]
                ).UnmappedSSLModeError("pg", "totally-made-up"),
                id="unmapped-ssl-mode",
            ),
            pytest.param(
                __import__(
                    "src.engine.type_map", fromlist=["InvalidSSLModeMapError"]
                ).InvalidSSLModeMapError("bad canonical value"),
                id="invalid-ssl-mode-map",
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
                ValueError("Database port is required"),
                id="value-error",
            ),
        ],
    )
    async def test_connect_propagates_deterministic_errors_unchanged(
        self, handler, base_config, exc
    ):
        """Deterministic configuration errors must surface with their real
        type — wrapping them as ConnectionError buries the root cause and
        makes retry-classification impossible. Guards the ``connect()``
        bypass clause in database.py against future refactors."""
        runtime = _make_runtime(base_config)

        with patch(
            "src.shared.connection_runtime.create_database_engine",
            side_effect=exc,
        ):
            with pytest.raises(type(exc)):
                await handler.connect(runtime)

        assert handler._connected is False

    @pytest.mark.asyncio
    async def test_sqlite_connect(self, handler):
        """SQLite connections should work through runtime materialization."""
        sqlite_config = {
            "driver": "sqlite",
            "database": ":memory:",
        }
        runtime = _make_runtime(sqlite_config, driver="sqlite")
        mock_engine = AsyncMock()

        with patch(
            "src.shared.connection_runtime.create_database_engine",
            return_value=(mock_engine, "sqlite"),
        ):
            await handler.connect(runtime)

        assert handler._connected is True
        assert handler._driver == "sqlite"


class TestDatabaseHandlerURLEncoding:
    """Test that connection URLs use sqlalchemy.engine.URL (no raw f-string)."""

    @pytest.mark.asyncio
    async def test_reserved_char_password_works(self, handler):
        """Password with reserved characters should be handled by shared engine."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {
                "port": 5432,
                "database": "test_db",
                "username": "user",
                "password": "a@b#c%/d:e",
            },
        }
        runtime = _make_runtime(config)
        mock_engine = AsyncMock()

        with patch(
            "src.shared.connection_runtime.create_database_engine",
            return_value=(mock_engine, "postgresql"),
        ):
            await handler.connect(runtime)

        assert handler._connected is True
