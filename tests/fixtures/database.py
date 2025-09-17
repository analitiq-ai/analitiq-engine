"""Database fixtures for integration tests."""

import pytest
import os
from unittest.mock import AsyncMock, Mock

from analitiq_stream.connectors.database.postgresql_driver import PostgreSQLDriver


@pytest.fixture
async def postgres_driver():
    """Create PostgreSQL driver with test database connection."""
    # Check if any PostgreSQL environment variables are set
    postgres_vars = ["POSTGRES_HOST", "POSTGRES_PASSWORD", "TEST_POSTGRES_URL"]
    if not any(os.getenv(var) for var in postgres_vars):
        pytest.skip("PostgreSQL connection not configured. Set POSTGRES_PASSWORD or TEST_POSTGRES_URL in tests/.env")
    
    driver = PostgreSQLDriver()
    
    postgres_url = os.getenv("TEST_POSTGRES_URL")
    if postgres_url:
        import urllib.parse
        parsed = urllib.parse.urlparse(postgres_url)
        config = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 5432,
            "user": parsed.username or "postgres",
            "password": parsed.password or "",
            "database": parsed.path.lstrip('/') or "analitiq_test"
        }
    else:
        config = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", ""),
            "database": os.getenv("POSTGRES_DB", "analitiq_test"),
            "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer"),
            "connection_pool": {
                "min_connections": int(os.getenv("POSTGRES_MIN_CONNECTIONS", "2")),
                "max_connections": int(os.getenv("POSTGRES_MAX_CONNECTIONS", "10")),
                "max_overflow": int(os.getenv("POSTGRES_MAX_OVERFLOW", "20")),
                "pool_timeout": int(os.getenv("POSTGRES_POOL_TIMEOUT", "30")),
                "pool_recycle": int(os.getenv("POSTGRES_POOL_RECYCLE", "3600")),
                "pool_pre_ping": os.getenv("POSTGRES_POOL_PRE_PING", "true").lower() == "true"
            }
        }
    
    try:
        await driver.create_connection_pool(config)
        yield driver
    finally:
        await driver.close_connection_pool()


@pytest.fixture
def mock_database_connector():
    """Mock database connector for unit tests."""
    connector = AsyncMock()
    connector.connect = AsyncMock()
    connector.disconnect = AsyncMock()
    connector.configure = AsyncMock()
    connector.read_batches = AsyncMock(return_value=iter([[{"id": 1, "data": "test"}]]))
    connector.write_batch = AsyncMock()
    connector.supports_incremental_read = Mock(return_value=True)
    connector.supports_upsert = Mock(return_value=True)
    connector.health_check = AsyncMock(return_value=True)
    return connector


@pytest.fixture
def sample_database_config():
    """Sample database configuration for testing."""
    return {
        "driver": "postgresql",
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "analitiq_test"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "test_password"),
        "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer")
    }