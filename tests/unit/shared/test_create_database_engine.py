"""Unit tests for shared create_database_engine and acquire_connection."""

import ssl
from contextlib import asynccontextmanager

import pytest
from unittest.mock import AsyncMock, patch

from src.shared.database_utils import create_database_engine, acquire_connection


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


def _pg_config(**overrides):
    """Build a standard PostgreSQL config dict with new structure."""
    base = {
        "driver": "postgresql",
        "host": "localhost",
        "parameters": {
            "port": 5432,
            "database": "db",
            "username": "user",
            "password": "pass",
        },
    }
    base.update(overrides)
    return base


def _pg_config_with_ssl(ssl_mode):
    """Build a PostgreSQL config with a specific ssl_mode."""
    config = _pg_config()
    config["parameters"]["ssl_mode"] = ssl_mode
    return config


class TestCreateDatabaseEngine:
    """Test create_database_engine factory."""

    @pytest.mark.asyncio
    async def test_successful_creation(self):
        """Happy path: returns (engine, driver) after successful probe."""
        engine = _make_engine()

        with patch(
            "src.shared.database_utils.create_async_engine",
            return_value=engine,
        ) as mock_create:
            result_engine, driver = await create_database_engine(
                _pg_config(),
                require_port=True,
            )

        assert mock_create.call_count == 1
        assert result_engine is engine
        assert driver == "postgresql"

    @pytest.mark.asyncio
    async def test_ssl_prefer_retries_on_ssl_error(self):
        """ssl_mode=prefer should dispose + retry with ssl=False on SSL error."""
        ssl_error = ssl.SSLError("SSL handshake failed")
        engine_fail = _make_engine(connect_side_effect=ssl_error)
        engine_ok = _make_engine()

        engines = [engine_fail, engine_ok]

        with patch(
            "src.shared.database_utils.create_async_engine",
            side_effect=engines,
        ) as mock_create:
            result_engine, driver = await create_database_engine(
                _pg_config_with_ssl("prefer"),
                require_port=True,
            )

        assert mock_create.call_count == 2
        engine_fail.dispose.assert_awaited_once()
        # Second call should have ssl=False in connect_args
        second_call = mock_create.call_args_list[1]
        connect_args = second_call.kwargs.get("connect_args", {})
        assert connect_args["ssl"] is False
        assert result_engine is engine_ok

    @pytest.mark.asyncio
    async def test_ssl_prefer_retry_failure_disposes(self):
        """If plaintext retry also fails, both engines are disposed."""
        ssl_error = ssl.SSLError("SSL handshake failed")
        engine_fail_ssl = _make_engine(connect_side_effect=ssl_error)
        engine_fail_plain = _make_engine(connect_side_effect=OSError("DB unreachable"))

        engines = [engine_fail_ssl, engine_fail_plain]

        with patch(
            "src.shared.database_utils.create_async_engine",
            side_effect=engines,
        ):
            with pytest.raises(OSError, match="DB unreachable"):
                await create_database_engine(
                    _pg_config_with_ssl("prefer"),
                    require_port=True,
                )

        engine_fail_ssl.dispose.assert_awaited_once()
        engine_fail_plain.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_ssl_error_disposes_and_raises(self):
        """Non-SSL errors should dispose engine and raise without retry."""
        engine = _make_engine(connect_side_effect=OSError("Connection timed out"))

        with patch(
            "src.shared.database_utils.create_async_engine",
            return_value=engine,
        ) as mock_create:
            with pytest.raises(OSError, match="Connection timed out"):
                await create_database_engine(
                    _pg_config_with_ssl("prefer"),
                    require_port=True,
                )

        assert mock_create.call_count == 1
        engine.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ssl_require_does_not_fallback(self):
        """ssl_mode=require should NOT retry without SSL."""
        engine = _make_engine(connect_side_effect=ssl.SSLError("SSL handshake failed"))

        with patch(
            "src.shared.database_utils.create_async_engine",
            return_value=engine,
        ) as mock_create:
            with pytest.raises(ssl.SSLError):
                await create_database_engine(
                    _pg_config_with_ssl("require"),
                    require_port=True,
                )

        assert mock_create.call_count == 1
        engine.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_require_port_raises_before_engine(self):
        """ValueError from missing port should not create any engine."""
        config = {
            "driver": "postgresql",
            "host": "localhost",
            "parameters": {
                "database": "db",
                "username": "user",
                "password": "pass",
            },
        }
        with patch(
            "src.shared.database_utils.create_async_engine",
        ) as mock_create:
            with pytest.raises(ValueError, match="port is required"):
                await create_database_engine(config, require_port=True)

        mock_create.assert_not_called()


class TestAcquireConnection:
    """Test acquire_connection context manager."""

    @pytest.mark.asyncio
    async def test_raises_when_no_engine(self):
        """Should raise RuntimeError when engine is None."""
        with pytest.raises(RuntimeError, match="Engine not initialized"):
            async with acquire_connection(None):
                pass

    @pytest.mark.asyncio
    async def test_yields_connection(self):
        """Should yield a connection from the engine."""
        mock_conn = AsyncMock()
        engine = AsyncMock()

        @asynccontextmanager
        async def fake_connect():
            yield mock_conn

        engine.connect = fake_connect

        async with acquire_connection(engine) as conn:
            assert conn is mock_conn
