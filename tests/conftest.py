"""Pytest configuration and shared fixtures."""

import asyncio
import os
import tempfile
import json
from pathlib import Path
from typing import Dict, Any
from unittest.mock import AsyncMock, Mock

import pytest
from dotenv import load_dotenv

# Load test environment variables from .env file
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    load_dotenv(env_file, override=False)  # Don't override existing env vars

# Import specific fixtures from modules
from tests.fixtures.database import postgres_driver, mock_database_connector, sample_database_config
from tests.fixtures.api import mock_api_connector, sample_api_config, mock_http_responses, sample_api_response


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def sample_wise_record():
    """Sample Wise transfer record for testing."""
    return {
        "id": 123456,
        "created": "2025-08-16T10:30:00Z",
        "targetValue": 100.50,
        "targetCurrency": "EUR",
        "sourceCurrency": "USD",
        "status": "outgoing_payment_sent",
        "details": {
            "reference": "Payment for services",
            "merchant": {
                "name": "Test Merchant"
            }
        }
    }


@pytest.fixture
def sample_pipeline_config():
    """Sample pipeline configuration for testing."""
    return {
        "pipeline_id": "test-pipeline",
        "name": "Test Pipeline",
        "version": "1.0",
        "src": {
            "host_id": "test-src-host",
            "name": "Test Source"
        },
        "dst": {
            "host_id": "test-dst-host", 
            "name": "Test Destination"
        },
        "engine_config": {
            "batch_size": 10,
            "max_concurrent_batches": 2,
            "buffer_size": 100
        },
        "streams": {
            "test-stream-1": {
                "name": "test-stream",
                "src": {
                    "endpoint_id": "test-src-endpoint",
                    "replication_method": "incremental",
                    "replication_key": "created"
                },
                "dst": {
                    "endpoint_id": "test-dst-endpoint",
                    "refresh_mode": "upsert",
                    "batch_support": False
                },
                "mapping": {
                    "field_mappings": {
                        "created": {
                            "target": "valueDate",
                            "transformations": ["iso_to_date"]
                        },
                        "targetValue": {
                            "target": "amount"
                        },
                        "id": {
                            "target": "paymtPurpose"
                        }
                    },
                    "computed_fields": {
                        "objectName": {
                            "expression": "CheckAccountTransaction"
                        },
                        "checkAccount": {
                            "expression": '{"id": "5936402", "objectName": "CheckAccount"}'
                        },
                        "status": {
                            "expression": "100"
                        }
                    }
                }
            }
        }
    }


@pytest.fixture
def mock_connector():
    """Mock connector for testing."""
    connector = AsyncMock()
    connector.configure = AsyncMock()
    connector.health_check = AsyncMock(return_value=True)
    connector.read_batches = AsyncMock(return_value=iter([[{"id": 1, "name": "test"}]]))
    connector.write_batch = AsyncMock()
    connector.supports_incremental_read = Mock(return_value=True)
    connector.close = AsyncMock()
    return connector


@pytest.fixture
def mock_state_manager():
    """Mock state manager for testing."""
    state_manager = Mock()
    state_manager.start_run = Mock(return_value="test-run-id")
    state_manager.finalize_run = Mock()
    state_manager.get_checkpoint = Mock(return_value={})
    state_manager.checkpoint = AsyncMock()
    return state_manager


@pytest.fixture
def database_cleanup():
    """Fixture to cleanup test database tables after tests."""
    cleanup_tables = []
    
    def add_table_for_cleanup(schema: str, table: str):
        """Register a table for cleanup."""
        cleanup_tables.append((schema, table))
    
    yield add_table_for_cleanup
    
    # Cleanup after test
    if cleanup_tables and os.getenv("POSTGRES_PASSWORD"):
        try:
            import asyncio
            from analitiq_stream.connectors.database.postgresql_driver import PostgreSQLDriver
            
            async def cleanup():
                driver = PostgreSQLDriver()
                config = {
                    "host": os.getenv("POSTGRES_HOST", "localhost"),
                    "port": int(os.getenv("POSTGRES_PORT", "5432")),
                    "database": os.getenv("POSTGRES_DB", "analitiq_test"),
                    "user": os.getenv("POSTGRES_USER", "postgres"),
                    "password": os.getenv("POSTGRES_PASSWORD"),
                    "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer")
                }
                
                await driver.create_connection_pool(config)
                try:
                    async with driver.connection_pool.acquire() as conn:
                        for schema, table in cleanup_tables:
                            try:
                                await conn.execute(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE")
                                await conn.execute("COMMIT")
                            except Exception:
                                pass  # Ignore cleanup errors
                finally:
                    await driver.close_connection_pool()
            
            # Run cleanup if there's an event loop
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Schedule cleanup for later if loop is running
                    asyncio.create_task(cleanup())
                else:
                    loop.run_until_complete(cleanup())
            except RuntimeError:
                # No event loop, create one
                asyncio.run(cleanup())
        except Exception:
            pass  # Ignore cleanup errors


@pytest.fixture(autouse=True)
def setup_test_env():
    """Set up test environment variables."""
    test_vars = {
        "TEST_VAR": "test_value",
        "SEVDESK_BANK_ACCOUNT_ID": "5936402",
        "API_TOKEN": "test-token"
    }
    
    original_values = {}
    for key, value in test_vars.items():
        original_values[key] = os.environ.get(key)
        os.environ[key] = value
    
    yield
    
    # Restore original values
    for key, original_value in original_values.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value