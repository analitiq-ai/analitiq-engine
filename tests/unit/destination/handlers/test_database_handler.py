"""Unit tests for DatabaseDestinationHandler SSL prefer fallback."""

import ssl
from contextlib import asynccontextmanager
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

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


def _make_engine(connect_side_effect=None):
    """Create a mock engine whose .connect() works as async context manager."""
    engine = AsyncMock()
    conn = AsyncMock()

    if connect_side_effect:
        conn.execute.side_effect = connect_side_effect

    @asynccontextmanager
    async def fake_connect():
        yield conn

    engine.connect = fake_connect
    return engine


class TestDatabaseHandlerSSLPreferFallback:
    """Test SSL prefer fallback behavior in connect()."""

    @pytest.mark.asyncio
    async def test_ssl_prefer_retries_on_ssl_error(self, handler, base_config):
        """ssl_mode=prefer should retry without SSL on handshake error."""
        base_config["ssl_mode"] = "prefer"

        ssl_error = ssl.SSLError("SSL handshake failed")
        engine_fail = _make_engine(connect_side_effect=ssl_error)
        engine_ok = _make_engine()

        engines = [engine_fail, engine_ok]

        with patch(
            "src.destination.connectors.database.create_async_engine",
            side_effect=engines,
        ) as mock_create:
            await handler.connect(base_config)

        assert mock_create.call_count == 2
        engine_fail.dispose.assert_awaited_once()
        # Second engine should explicitly disable ssl
        second_call = mock_create.call_args_list[1]
        connect_args = second_call.kwargs.get("connect_args", {})
        assert connect_args["ssl"] is False
        assert handler._connected is True

    @pytest.mark.asyncio
    async def test_non_ssl_error_does_not_retry(self, handler, base_config):
        """Non-SSL errors should not trigger retry, engine should be disposed."""
        base_config["ssl_mode"] = "prefer"

        engine = _make_engine(connect_side_effect=OSError("Connection timed out"))

        with patch(
            "src.destination.connectors.database.create_async_engine",
            return_value=engine,
        ):
            with pytest.raises(OSError, match="Connection timed out"):
                await handler.connect(base_config)

        engine.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_fallback_retry_failure_disposes_engine(self, handler, base_config):
        """If the plaintext retry also fails, the retry engine should be disposed."""
        base_config["ssl_mode"] = "prefer"

        ssl_error = ssl.SSLError("SSL handshake failed")
        engine_fail_ssl = _make_engine(connect_side_effect=ssl_error)
        engine_fail_plain = _make_engine(connect_side_effect=OSError("DB unreachable"))

        engines = [engine_fail_ssl, engine_fail_plain]

        with patch(
            "src.destination.connectors.database.create_async_engine",
            side_effect=engines,
        ):
            with pytest.raises(OSError, match="DB unreachable"):
                await handler.connect(base_config)

        engine_fail_ssl.dispose.assert_awaited_once()
        engine_fail_plain.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ssl_require_does_not_fallback(self, handler, base_config):
        """ssl_mode=require should NOT retry without SSL."""
        base_config["ssl_mode"] = "require"

        engine = _make_engine(connect_side_effect=ssl.SSLError("SSL handshake failed"))

        with patch(
            "src.destination.connectors.database.create_async_engine",
            return_value=engine,
        ) as mock_create:
            with pytest.raises(ssl.SSLError):
                await handler.connect(base_config)

        assert mock_create.call_count == 1
        engine.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_default_ssl_mode_is_prefer(self, handler, base_config):
        """When ssl_mode is omitted, it should default to 'prefer' behavior."""
        assert "ssl_mode" not in base_config

        ssl_error = ssl.SSLError("SSL handshake failed")
        engine_fail = _make_engine(connect_side_effect=ssl_error)
        engine_ok = _make_engine()

        engines = [engine_fail, engine_ok]

        with patch(
            "src.destination.connectors.database.create_async_engine",
            side_effect=engines,
        ) as mock_create:
            await handler.connect(base_config)

        # Should have retried (default is prefer)
        assert mock_create.call_count == 2
        assert handler._connected is True

    @pytest.mark.asyncio
    async def test_sqlite_does_not_set_ssl(self, handler):
        """SQLite connections should not receive ssl in connect_args."""
        sqlite_config = {
            "driver": "sqlite",
            "database": ":memory:",
        }

        engine = _make_engine()

        with patch(
            "src.destination.connectors.database.create_async_engine",
            return_value=engine,
        ) as mock_create:
            await handler.connect(sqlite_config)

        assert mock_create.call_count == 1
        connect_args = mock_create.call_args.kwargs.get("connect_args", {})
        assert "ssl" not in connect_args
        assert handler._connected is True
