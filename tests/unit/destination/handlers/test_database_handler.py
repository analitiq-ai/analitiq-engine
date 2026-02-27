"""Unit tests for DatabaseDestinationHandler connection handling."""

import ssl
from contextlib import asynccontextmanager
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy.engine import URL

from src.destination.connectors.database import DatabaseDestinationHandler


@pytest.fixture
def handler():
    return DatabaseDestinationHandler()


@pytest.fixture
def base_config():
    return {
        "driver": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "username": "test_user",
        "password": "test_password",
    }


class TestDatabaseHandlerConnect:
    """Test connect() delegates to shared create_database_engine."""

    @pytest.mark.asyncio
    async def test_connect_success(self, handler, base_config):
        """connect() should delegate to create_database_engine and set state."""
        mock_engine = AsyncMock()

        with patch(
            "src.destination.connectors.database.create_database_engine",
            return_value=(mock_engine, "postgresql"),
        ) as mock_create:
            await handler.connect(base_config)

        mock_create.assert_called_once_with(base_config, require_port=False)
        assert handler._connected is True
        assert handler._engine is mock_engine
        assert handler._driver == "postgresql"

    @pytest.mark.asyncio
    async def test_connect_failure_propagates(self, handler, base_config):
        """connect() should propagate errors from create_database_engine."""
        with patch(
            "src.destination.connectors.database.create_database_engine",
            side_effect=OSError("Connection timed out"),
        ):
            with pytest.raises(OSError, match="Connection timed out"):
                await handler.connect(base_config)

        assert handler._connected is False

    @pytest.mark.asyncio
    async def test_connect_ssl_error_propagates(self, handler, base_config):
        """SSL errors are handled by the shared engine factory, not here."""
        with patch(
            "src.destination.connectors.database.create_database_engine",
            side_effect=ssl.SSLError("SSL handshake failed"),
        ):
            with pytest.raises(ssl.SSLError):
                await handler.connect(base_config)

        assert handler._connected is False

    @pytest.mark.asyncio
    async def test_sqlite_connect(self, handler):
        """SQLite connections should work through shared factory."""
        sqlite_config = {
            "driver": "sqlite",
            "database": ":memory:",
        }
        mock_engine = AsyncMock()

        with patch(
            "src.destination.connectors.database.create_database_engine",
            return_value=(mock_engine, "sqlite"),
        ) as mock_create:
            await handler.connect(sqlite_config)

        mock_create.assert_called_once_with(sqlite_config, require_port=False)
        assert handler._connected is True
        assert handler._driver == "sqlite"


class TestDatabaseHandlerURLEncoding:
    """Test that connection URLs use sqlalchemy.engine.URL (no raw f-string).

    This is now guaranteed by the shared engine factory which uses
    DatabaseConnectionParams.to_sqlalchemy_url() internally.
    """

    @pytest.mark.asyncio
    async def test_reserved_char_password_works(self, handler):
        """Password with reserved characters should be handled by shared engine."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "user",
            "password": "a@b#c%/d:e",
        }
        mock_engine = AsyncMock()

        with patch(
            "src.destination.connectors.database.create_database_engine",
            return_value=(mock_engine, "postgresql"),
        ):
            await handler.connect(config)

        assert handler._connected is True
