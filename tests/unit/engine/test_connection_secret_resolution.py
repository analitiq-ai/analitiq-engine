"""Tests for ConnectionRuntime secret resolution and lifecycle."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.shared.connection_runtime import ConnectionRuntime
from src.secrets.exceptions import PlaceholderExpansionError


class TestConnectionRuntimeMetadata:
    """Test that ConnectionRuntime exposes enriched metadata before materialize."""

    def test_connector_type_available(self):
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost"},
            connection_id="conn-1",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )
        assert runtime.connector_type == "database"
        assert runtime.driver == "postgresql"
        assert runtime.connection_id == "conn-1"

    def test_raw_config_returns_deep_copy(self):
        config = {"host": "localhost", "parameters": {"port": 5432}}
        runtime = ConnectionRuntime(
            raw_config=config,
            connection_id="conn-1",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )
        result = runtime.raw_config
        assert result == config
        assert result is not config
        # Mutating the copy should not affect internal state
        result["host"] = "mutated"
        assert runtime.raw_config["host"] == "localhost"

    def test_invalid_connector_type_raises(self):
        with pytest.raises(ValueError, match="Invalid connector_type"):
            ConnectionRuntime(
                raw_config={"host": "localhost"},
                connection_id="conn-1",
                connector_type="foobar",
                driver=None,
                resolver=AsyncMock(),
            )


class TestConnectionRuntimeResolveSecrets:
    """Test secret resolution inside materialize."""

    @pytest.mark.asyncio
    async def test_resolve_without_placeholders_skips_resolver(self):
        mock_resolver = AsyncMock()
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost", "parameters": {"port": 5432, "password": "literal"}},
            connection_id="conn-no-secrets",
            connector_type="file",
            driver=None,
            resolver=mock_resolver,
        )

        await runtime.materialize()

        mock_resolver.resolve.assert_not_awaited()
        assert runtime.resolved_config["host"] == "localhost"

    @pytest.mark.asyncio
    async def test_resolve_with_placeholders_calls_resolver(self):
        mock_resolver = AsyncMock()
        mock_resolver.resolve.return_value = {"DB_PASS": "secret123"}
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost", "parameters": {"password": "${DB_PASS}"}},
            connection_id="conn-with-secrets",
            connector_type="file",
            driver=None,
            resolver=mock_resolver,
        )

        await runtime.materialize()

        mock_resolver.resolve.assert_awaited_once_with("conn-with-secrets")
        assert runtime.resolved_config["parameters"]["password"] == "secret123"

    @pytest.mark.asyncio
    async def test_missing_secret_key_raises_placeholder_error(self):
        mock_resolver = AsyncMock()
        mock_resolver.resolve.return_value = {"OTHER_KEY": "value"}
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost", "token": "${MISSING_KEY}"},
            connection_id="conn-missing",
            connector_type="file",
            driver=None,
            resolver=mock_resolver,
        )

        with pytest.raises(PlaceholderExpansionError) as exc_info:
            await runtime.materialize()

        assert "MISSING_KEY" in str(exc_info.value)


class TestConnectionRuntimeMaterialize:
    """Test materialize creates correct transport per connector_type."""

    @pytest.mark.asyncio
    async def test_materialize_is_idempotent(self):
        mock_resolver = AsyncMock()
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost", "file_format": "jsonl"},
            connection_id="conn-1",
            connector_type="stdout",
            driver=None,
            resolver=mock_resolver,
        )

        await runtime.materialize()
        first_config = runtime.resolved_config

        # Second call is a no-op
        await runtime.materialize()
        assert runtime.resolved_config is first_config

    @pytest.mark.asyncio
    async def test_transport_accessors_raise_before_materialize(self):
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost"},
            connection_id="conn-1",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )

        with pytest.raises(RuntimeError, match="call materialize"):
            _ = runtime.engine

        with pytest.raises(RuntimeError, match="call materialize"):
            _ = runtime.session

    @pytest.mark.asyncio
    async def test_materialize_database(self):
        mock_resolver = AsyncMock()
        mock_engine = AsyncMock()

        runtime = ConnectionRuntime(
            raw_config={
                "host": "localhost",
                "driver": "postgresql",
                "parameters": {"port": 5432, "database": "test", "username": "user", "password": "pass"},
            },
            connection_id="conn-db",
            connector_type="database",
            driver="postgresql",
            resolver=mock_resolver,
        )

        with patch("src.shared.connection_runtime.create_database_engine", return_value=(mock_engine, "postgresql")) as mock_create:
            await runtime.materialize(require_port=True)

            mock_create.assert_awaited_once()
            assert runtime.engine is mock_engine

    @pytest.mark.asyncio
    async def test_materialize_api(self):
        mock_resolver = AsyncMock()
        mock_session = AsyncMock()
        mock_rate_limiter = MagicMock()

        runtime = ConnectionRuntime(
            raw_config={
                "host": "https://api.example.com",
                "parameters": {"timeout": 30},
            },
            connection_id="conn-api",
            connector_type="api",
            driver=None,
            resolver=mock_resolver,
        )

        with patch(
            "src.shared.connection_runtime._create_api_session",
            return_value=(mock_session, "https://api.example.com", mock_rate_limiter),
        ):
            await runtime.materialize()

            assert runtime.session is mock_session
            assert runtime.base_url == "https://api.example.com"
            assert runtime.rate_limiter is mock_rate_limiter

    @pytest.mark.asyncio
    async def test_secrets_scrubbed_after_materialize_database(self):
        mock_resolver = AsyncMock()
        mock_resolver.resolve.return_value = {"DB_PASS": "secret"}

        runtime = ConnectionRuntime(
            raw_config={
                "host": "localhost",
                "driver": "postgresql",
                "parameters": {"password": "${DB_PASS}"},
            },
            connection_id="conn-db",
            connector_type="database",
            driver="postgresql",
            resolver=mock_resolver,
        )

        with patch("src.shared.connection_runtime.create_database_engine", return_value=(AsyncMock(), "postgresql")):
            await runtime.materialize()

        assert runtime._secrets is None
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_secrets_scrubbed_on_materialize_failure(self):
        mock_resolver = AsyncMock()
        mock_resolver.resolve.return_value = {"DB_PASS": "secret"}

        runtime = ConnectionRuntime(
            raw_config={
                "host": "localhost",
                "driver": "postgresql",
                "parameters": {"password": "${DB_PASS}"},
            },
            connection_id="conn-db",
            connector_type="database",
            driver="postgresql",
            resolver=mock_resolver,
        )

        with patch("src.shared.connection_runtime.create_database_engine", side_effect=Exception("connection failed")):
            with pytest.raises(Exception, match="connection failed"):
                await runtime.materialize()

        # Secrets should still be scrubbed even on failure
        assert runtime._secrets is None


class TestConnectionRuntimeClose:
    """Test close disposes resources."""

    @pytest.mark.asyncio
    async def test_close_disposes_engine(self):
        mock_engine = AsyncMock()
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost", "driver": "postgresql", "parameters": {"port": 5432, "database": "test", "username": "u", "password": "p"}},
            connection_id="conn-db",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )

        with patch("src.shared.connection_runtime.create_database_engine", return_value=(mock_engine, "postgresql")):
            await runtime.materialize()

        await runtime.close()
        mock_engine.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_disposes_session(self):
        mock_session = AsyncMock()
        runtime = ConnectionRuntime(
            raw_config={"host": "https://api.example.com", "parameters": {}},
            connection_id="conn-api",
            connector_type="api",
            driver=None,
            resolver=AsyncMock(),
        )

        with patch("src.shared.connection_runtime._create_api_session", return_value=(mock_session, "https://api.example.com", None)):
            await runtime.materialize()

        await runtime.close()
        mock_session.close.assert_awaited_once()
        assert not runtime._materialized

    @pytest.mark.asyncio
    async def test_double_close_is_safe(self):
        mock_engine = AsyncMock()
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost", "driver": "postgresql", "parameters": {"port": 5432, "database": "test", "username": "u", "password": "p"}},
            connection_id="conn-db",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )

        with patch("src.shared.connection_runtime.create_database_engine", return_value=(mock_engine, "postgresql")):
            await runtime.materialize()

        await runtime.close()
        await runtime.close()  # Should not raise

    @pytest.mark.asyncio
    async def test_close_engine_failure_still_closes_session(self):
        """If engine.dispose() fails, session.close() should still be called."""
        mock_engine = AsyncMock()
        mock_engine.dispose.side_effect = Exception("dispose failed")
        mock_session = AsyncMock()

        runtime = ConnectionRuntime(
            raw_config={"host": "localhost"},
            connection_id="conn-1",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )
        # Manually set state to simulate both engine and session
        runtime._engine = mock_engine
        runtime._session = mock_session
        runtime._materialized = True

        # close() should not raise — it logs the error and continues cleanup
        await runtime.close()

        # Session should still be closed despite engine failure
        mock_session.close.assert_awaited_once()
        assert runtime._materialized is False


class TestScrubResolvedConfig:
    """Test cooperative scrub_resolved_config() lifecycle."""

    @pytest.mark.asyncio
    async def test_scrub_clears_resolved_config_for_file_type(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp/out", "token": "${SECRET}"},
            connection_id="conn-file",
            connector_type="file",
            driver=None,
            resolver=AsyncMock(resolve=AsyncMock(return_value={"SECRET": "s3cr3t"})),
        )
        runtime.acquire()
        await runtime.materialize()

        # Before scrub: resolved config is accessible
        assert runtime.resolved_config["token"] == "s3cr3t"

        runtime.scrub_resolved_config()

        # After scrub: resolved config is cleared
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_scrub_waits_for_all_acquirers(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp/out"},
            connection_id="conn-shared",
            connector_type="file",
            driver=None,
            resolver=AsyncMock(),
        )
        runtime.acquire()
        runtime.acquire()
        await runtime.materialize()

        # First scrub: not all consumers have signalled
        runtime.scrub_resolved_config()
        assert runtime._resolved_config is not None

        # Second scrub: all consumers signalled, now cleared
        runtime.scrub_resolved_config()
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_resolved_config_accessible_before_scrub(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/data", "file_format": "jsonl"},
            connection_id="conn-stdout",
            connector_type="stdout",
            driver=None,
            resolver=AsyncMock(),
        )
        runtime.acquire()
        await runtime.materialize()

        cfg = runtime.resolved_config
        assert cfg["path"] == "/data"
        assert cfg["file_format"] == "jsonl"

    @pytest.mark.asyncio
    async def test_close_resets_scrub_counter(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp"},
            connection_id="conn-reset",
            connector_type="file",
            driver=None,
            resolver=AsyncMock(),
        )
        runtime.acquire()
        await runtime.materialize()
        runtime.scrub_resolved_config()

        # After close, scrub counter is reset
        await runtime.close()
        assert runtime._scrub_requests == 0

    def test_scrub_before_materialize_is_ignored(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp"},
            connection_id="conn-early",
            connector_type="file",
            driver=None,
            resolver=AsyncMock(),
        )
        runtime.acquire()
        # Calling scrub before materialize should not raise
        runtime.scrub_resolved_config()
        assert runtime._scrub_requests == 0

    @pytest.mark.asyncio
    async def test_scrub_with_zero_ref_count_clears_immediately(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp"},
            connection_id="conn-zero-ref",
            connector_type="file",
            driver=None,
            resolver=AsyncMock(),
        )
        # Materialize without acquire (unusual but possible)
        await runtime.materialize()
        assert runtime._resolved_config is not None

        runtime.scrub_resolved_config()
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_scrub_is_noop_on_database_type(self):
        runtime = ConnectionRuntime(
            raw_config={"host": "localhost", "driver": "postgresql", "parameters": {"port": 5432, "database": "test", "username": "u", "password": "p"}},
            connection_id="conn-db-scrub",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )
        runtime.acquire()

        with patch("src.shared.connection_runtime.create_database_engine", return_value=(AsyncMock(), "postgresql")):
            await runtime.materialize()

        # _resolved_config is already None from _scrub_secrets()
        assert runtime._resolved_config is None

        # scrub_resolved_config() should be a safe no-op
        runtime.scrub_resolved_config()
        assert runtime._scrub_requests == 0

    @pytest.mark.asyncio
    async def test_excess_scrub_calls_are_harmless(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp"},
            connection_id="conn-excess",
            connector_type="file",
            driver=None,
            resolver=AsyncMock(),
        )
        runtime.acquire()
        await runtime.materialize()

        # First scrub clears
        runtime.scrub_resolved_config()
        assert runtime._resolved_config is None

        # Extra scrub calls are no-ops (config already None)
        runtime.scrub_resolved_config()
        runtime.scrub_resolved_config()
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_resolved_config_error_message_after_scrub(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp"},
            connection_id="conn-msg",
            connector_type="file",
            driver=None,
            resolver=AsyncMock(),
        )
        runtime.acquire()
        await runtime.materialize()
        runtime.scrub_resolved_config()

        with pytest.raises(RuntimeError, match="already scrubbed"):
            _ = runtime.resolved_config


class TestCreateSourceConnectorUnknownType:
    """Test _create_source_connector with unknown connector_type."""

    def test_unknown_connector_type_raises(self):
        from src.engine.engine import StreamingEngine
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            engine = StreamingEngine(
                pipeline_id="test", dlq_path=tmp
            )
            runtime = ConnectionRuntime(
                raw_config={},
                connection_id="conn-1",
                connector_type="file",
                driver=None,
                resolver=AsyncMock(),
            )
            config = {"_runtime": runtime}
            with pytest.raises(ValueError, match="Unknown connector_type 'file'"):
                engine._create_source_connector(config)
