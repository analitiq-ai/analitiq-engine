"""Tests for ConnectionRuntime materialization and lifecycle.

The runtime now drives transport creation through
:mod:`src.shared.transport_factory` based on the connector definition's
``transports`` block. These tests exercise that contract using mock
transports rather than the legacy ``${PLACEHOLDER}`` expansion path.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.secrets.exceptions import SecretNotFoundError
from src.shared.connection_runtime import ConnectionRuntime
from src.shared.transport_factory import HttpTransport, SqlAlchemyTransport


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolver(value: dict | None = None) -> AsyncMock:
    """Build a resolver mock that returns *value* (or {}) from .resolve()."""
    mock = AsyncMock()
    mock.resolve = AsyncMock(return_value=dict(value or {}))
    return mock


def _db_connector(slug: str = "pg") -> dict:
    """Minimal connector spec with a sqlalchemy transports block."""
    return {
        "slug": slug,
        "connector_type": "database",
        "default_transport": "database",
        "transports": {
            "database": {
                "kind": "sqlalchemy",
                "driver": "postgresql+asyncpg",
                "dsn": {"template": "postgresql+asyncpg://u:p@h:5432/d"},
            }
        },
    }


def _api_connector() -> dict:
    return {
        "slug": "wise-test",
        "connector_type": "api",
        "default_transport": "api",
        "transports": {
            "api": {
                "kind": "http",
                "base_url": "https://api.example.com",
            }
        },
    }


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


class TestConnectionRuntimeMetadata:
    def test_connector_type_available(self):
        runtime = ConnectionRuntime(
            raw_config={"parameters": {"host": "h"}},
            connection_id="conn-1",
            connector_type="database",
            driver="postgresql",
            resolver=AsyncMock(),
        )
        assert runtime.connector_type == "database"
        assert runtime.driver == "postgresql"
        assert runtime.connection_id == "conn-1"

    def test_driver_derived_from_connector_definition(self):
        """When ``driver`` is not passed, it falls out of the connector's
        default sqlalchemy transport."""
        runtime = ConnectionRuntime(
            raw_config={"parameters": {}},
            connection_id="conn-1",
            connector_type="database",
            resolver=AsyncMock(),
            connector_definition=_db_connector(),
        )
        assert runtime.driver == "postgresql"

    def test_raw_config_returns_deep_copy(self):
        config = {"parameters": {"port": 5432}}
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

    def test_invalid_connector_type_raises(self):
        with pytest.raises(ValueError, match="Invalid connector_type"):
            ConnectionRuntime(
                raw_config={},
                connection_id="conn-1",
                connector_type="foobar",
                resolver=AsyncMock(),
            )


# ---------------------------------------------------------------------------
# Materialize: spec-driven path
# ---------------------------------------------------------------------------


class TestConnectionRuntimeMaterialize:
    @pytest.mark.asyncio
    async def test_materialize_database_uses_transport_factory(self):
        engine = AsyncMock()
        transport = SqlAlchemyTransport(
            engine=engine, driver="postgresql+asyncpg", dialect="postgresql"
        )
        runtime = ConnectionRuntime(
            raw_config={"parameters": {"host": "h", "port": 5432}},
            connection_id="conn-db",
            connector_type="database",
            resolver=_resolver({"password": "secret"}),
            connector_definition=_db_connector(),
        )
        with patch(
            "src.shared.connection_runtime.build_transport",
            new=AsyncMock(return_value=transport),
        ) as mock_build:
            await runtime.materialize()
            mock_build.assert_awaited_once()
        assert runtime.engine is engine
        assert runtime.driver == "postgresql"
        assert runtime.driver_string == "postgresql+asyncpg"

    @pytest.mark.asyncio
    async def test_materialize_api_uses_transport_factory(self):
        session = AsyncMock()
        transport = HttpTransport(
            session=session,
            base_url="https://api.example.com",
            headers={"Authorization": "Bearer t"},
            rate_limiter=None,
        )
        runtime = ConnectionRuntime(
            raw_config={"parameters": {}},
            connection_id="conn-api",
            connector_type="api",
            resolver=_resolver({"api_key": "t"}),
            connector_definition=_api_connector(),
        )
        with patch(
            "src.shared.connection_runtime.build_transport",
            new=AsyncMock(return_value=transport),
        ):
            await runtime.materialize()
        assert runtime.session is session
        assert runtime.base_url == "https://api.example.com"

    @pytest.mark.asyncio
    async def test_materialize_is_idempotent(self):
        runtime = ConnectionRuntime(
            raw_config={"file_format": "jsonl"},
            connection_id="conn-1",
            connector_type="stdout",
            resolver=_resolver(),
        )
        await runtime.materialize()
        first_config = runtime.resolved_config
        await runtime.materialize()
        assert runtime.resolved_config is first_config

    @pytest.mark.asyncio
    async def test_transport_accessors_raise_before_materialize(self):
        runtime = ConnectionRuntime(
            raw_config={},
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
    async def test_secret_refs_are_validated_against_secret_store(self):
        """A connection that declares a secret_ref but the store does not
        contain that key must fail materialization."""
        runtime = ConnectionRuntime(
            raw_config={
                "parameters": {},
                "secret_refs": {"password": "connections/x/password"},
            },
            connection_id="conn-missing",
            connector_type="database",
            resolver=_resolver({}),  # no `password` key
            connector_definition=_db_connector(),
        )
        with pytest.raises(SecretNotFoundError):
            await runtime.materialize()


# ---------------------------------------------------------------------------
# Lifecycle: close / disposal
# ---------------------------------------------------------------------------


class TestConnectionRuntimeClose:
    @pytest.mark.asyncio
    async def test_close_disposes_engine(self):
        engine = AsyncMock()
        transport = SqlAlchemyTransport(
            engine=engine, driver="postgresql+asyncpg", dialect="postgresql"
        )
        runtime = ConnectionRuntime(
            raw_config={"parameters": {}},
            connection_id="conn-db",
            connector_type="database",
            resolver=_resolver(),
            connector_definition=_db_connector(),
        )
        with patch(
            "src.shared.connection_runtime.build_transport",
            new=AsyncMock(return_value=transport),
        ):
            await runtime.materialize()
        await runtime.close()
        engine.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_disposes_session(self):
        session = AsyncMock()
        transport = HttpTransport(
            session=session,
            base_url="https://api.example.com",
            headers={},
            rate_limiter=None,
        )
        runtime = ConnectionRuntime(
            raw_config={"parameters": {}},
            connection_id="conn-api",
            connector_type="api",
            resolver=_resolver(),
            connector_definition=_api_connector(),
        )
        with patch(
            "src.shared.connection_runtime.build_transport",
            new=AsyncMock(return_value=transport),
        ):
            await runtime.materialize()
        await runtime.close()
        session.close.assert_awaited_once()
        assert not runtime._materialized

    @pytest.mark.asyncio
    async def test_double_close_is_safe(self):
        engine = AsyncMock()
        transport = SqlAlchemyTransport(
            engine=engine, driver="postgresql+asyncpg", dialect="postgresql"
        )
        runtime = ConnectionRuntime(
            raw_config={"parameters": {}},
            connection_id="conn-db",
            connector_type="database",
            resolver=_resolver(),
            connector_definition=_db_connector(),
        )
        with patch(
            "src.shared.connection_runtime.build_transport",
            new=AsyncMock(return_value=transport),
        ):
            await runtime.materialize()
        await runtime.close()
        await runtime.close()  # second close is a no-op


# ---------------------------------------------------------------------------
# Lifecycle: scrub_resolved_config (file/stdout legacy passthrough)
# ---------------------------------------------------------------------------


class TestScrubResolvedConfig:
    @pytest.mark.asyncio
    async def test_scrub_clears_resolved_config_for_file_type(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp/out"},
            connection_id="conn-file",
            connector_type="file",
            resolver=_resolver({"SECRET": "s3cr3t"}),
        )
        runtime.acquire()
        await runtime.materialize()
        # Secret values are merged into the legacy resolved_config dict
        # (the same passthrough behavior file handlers depend on).
        assert runtime.resolved_config["SECRET"] == "s3cr3t"
        runtime.scrub_resolved_config()
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_scrub_waits_for_all_acquirers(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp/out"},
            connection_id="conn-shared",
            connector_type="file",
            resolver=_resolver(),
        )
        runtime.acquire()
        runtime.acquire()
        await runtime.materialize()
        runtime.scrub_resolved_config()
        assert runtime._resolved_config is not None
        runtime.scrub_resolved_config()
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_resolved_config_accessible_before_scrub(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/data", "file_format": "jsonl"},
            connection_id="conn-stdout",
            connector_type="stdout",
            resolver=_resolver(),
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
            resolver=_resolver(),
        )
        runtime.acquire()
        await runtime.materialize()
        runtime.scrub_resolved_config()
        await runtime.close()
        assert runtime._scrub_requests == 0

    def test_scrub_before_materialize_is_ignored(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp"},
            connection_id="conn-early",
            connector_type="file",
            resolver=_resolver(),
        )
        runtime.acquire()
        runtime.scrub_resolved_config()
        assert runtime._scrub_requests == 0

    @pytest.mark.asyncio
    async def test_scrub_with_zero_ref_count_clears_immediately(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp"},
            connection_id="conn-zero-ref",
            connector_type="file",
            resolver=_resolver(),
        )
        # No acquire() call: scrub should clear the config immediately.
        await runtime.materialize()
        assert runtime.resolved_config is not None
        runtime.scrub_resolved_config()
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_scrub_is_noop_on_database_type(self):
        """Spec-driven connectors never expose ``resolved_config``, so a
        scrub call is a benign no-op even when the runtime has been
        materialized."""
        engine = AsyncMock()
        transport = SqlAlchemyTransport(
            engine=engine, driver="postgresql+asyncpg", dialect="postgresql"
        )
        runtime = ConnectionRuntime(
            raw_config={"parameters": {}},
            connection_id="conn-db",
            connector_type="database",
            resolver=_resolver(),
            connector_definition=_db_connector(),
        )
        with patch(
            "src.shared.connection_runtime.build_transport",
            new=AsyncMock(return_value=transport),
        ):
            await runtime.materialize()
        runtime.scrub_resolved_config()  # no-op
        # resolved_config is *not* available for db connectors, period.
        with pytest.raises(RuntimeError):
            _ = runtime.resolved_config

    @pytest.mark.asyncio
    async def test_excess_scrub_calls_are_harmless(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp"},
            connection_id="conn-excess",
            connector_type="file",
            resolver=_resolver(),
        )
        runtime.acquire()
        await runtime.materialize()
        runtime.scrub_resolved_config()
        runtime.scrub_resolved_config()  # already cleared, no error
        runtime.scrub_resolved_config()  # still no error
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_resolved_config_error_message_after_scrub(self):
        runtime = ConnectionRuntime(
            raw_config={"path": "/tmp"},
            connection_id="conn-msg",
            connector_type="file",
            resolver=_resolver(),
        )
        runtime.acquire()
        await runtime.materialize()
        runtime.scrub_resolved_config()
        with pytest.raises(RuntimeError, match="already scrubbed"):
            _ = runtime.resolved_config
