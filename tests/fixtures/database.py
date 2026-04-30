"""Database fixtures for integration tests.

The legacy ``create_database_engine`` factory has been replaced by the
connector-driven :mod:`src.shared.transport_factory`. The integration
fixture now builds a sqlalchemy transport directly from a synthesized
connector spec, exercising the same code path the engine uses at
runtime.
"""

import os
from unittest.mock import AsyncMock, Mock

import pytest

from src.engine.resolver import ResolutionContext
from src.shared.transport_factory import build_sqlalchemy_transport, resolve_transport_spec


def _postgres_connector_spec(driver_string: str) -> dict:
    """Build a minimal postgres connector definition for fixtures."""
    return {
        "slug": "postgres-test-fixture",
        "connector_type": "database",
        "default_transport": "database",
        "derived": {
            "dsn_username": {
                "function": "url_encode",
                "input": {"ref": "connection.parameters.username"},
            },
            "dsn_password": {
                "function": "url_encode",
                "input": {"ref": "secrets.password"},
            },
        },
        "transports": {
            "database": {
                "kind": "sqlalchemy",
                "driver": driver_string,
                "dsn": {
                    "template": (
                        f"{driver_string}://"
                        "${derived.dsn_username}:${derived.dsn_password}"
                        "@${connection.parameters.host}:${connection.parameters.port}"
                        "/${connection.parameters.database}"
                    )
                },
                "connect_args": {
                    "ssl": {
                        "function": "lookup",
                        "input": {"ref": "connection.parameters.ssl_mode"},
                        "map": {
                            "disable": False,
                            "prefer": {
                                "verify_mode": "CERT_NONE",
                                "check_hostname": False,
                            },
                            "require": {
                                "verify_mode": "CERT_NONE",
                                "check_hostname": False,
                            },
                        },
                    }
                },
            }
        },
    }


@pytest.fixture
async def postgres_driver():
    """Materialize a SQLAlchemy AsyncEngine via the transport factory.

    Yields ``(engine, base_dialect)`` for tests that drive real SQL. Skips
    when no postgres credentials are configured in the environment.
    """
    if not any(
        os.getenv(var)
        for var in ("POSTGRES_HOST", "POSTGRES_PASSWORD", "TEST_POSTGRES_URL")
    ):
        pytest.skip(
            "PostgreSQL connection not configured. Set POSTGRES_PASSWORD or "
            "TEST_POSTGRES_URL in tests/.env"
        )

    parameters: dict
    secrets: dict

    postgres_url = os.getenv("TEST_POSTGRES_URL")
    if postgres_url:
        import urllib.parse
        parsed = urllib.parse.urlparse(postgres_url)
        parameters = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 5432,
            "username": parsed.username or "postgres",
            "database": parsed.path.lstrip("/") or "analitiq_test",
            "ssl_mode": "prefer",
        }
        secrets = {"password": parsed.password or ""}
    else:
        parameters = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "username": os.getenv("POSTGRES_USER", "postgres"),
            "database": os.getenv("POSTGRES_DB", "analitiq_test"),
            "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer"),
        }
        secrets = {"password": os.getenv("POSTGRES_PASSWORD", "")}

    connector = _postgres_connector_spec("postgresql+asyncpg")
    context = ResolutionContext(
        connector=connector,
        connection={"parameters": parameters, "selections": {}, "discovered": {}},
        secrets=secrets,
    )
    spec = resolve_transport_spec(connector, context=context)
    transport = await build_sqlalchemy_transport(spec)
    try:
        yield transport.engine, transport.dialect
    finally:
        await transport.engine.dispose()


@pytest.fixture
def mock_database_connector():
    """Mock database connector for unit tests."""
    connector = AsyncMock()
    connector.connect = AsyncMock()
    connector.disconnect = AsyncMock()
    connector.read_batches = AsyncMock(
        return_value=iter([[{"id": 1, "data": "test"}]])
    )
    connector.write_batch = AsyncMock()
    connector.supports_incremental_read = Mock(return_value=True)
    connector.health_check = AsyncMock(return_value=True)
    return connector


@pytest.fixture
def sample_database_config():
    """Sample database configuration for testing.

    Shape matches the new connection.json schema (``parameters`` plus
    ``secret_refs``); the password lives in a sibling ``secrets`` dict so
    callers can feed both into a :class:`ResolutionContext` if they want
    to drive the transport factory directly.
    """
    return {
        "connector_slug": "postgres",
        "parameters": {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "database": os.getenv("POSTGRES_DB", "analitiq_test"),
            "username": os.getenv("POSTGRES_USER", "postgres"),
            "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer"),
        },
        "secret_refs": {
            "password": "tests/postgres/password",
        },
    }
