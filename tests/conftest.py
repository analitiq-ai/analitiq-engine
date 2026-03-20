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
from tests.fixtures.pipeline_config_prep import (
    sample_wise_host_config, sample_sevdesk_host_config, sample_database_host_config,
    sample_wise_endpoint_config, sample_sevdesk_endpoint_config, sample_database_endpoint_config,
    sample_invalid_pipeline_config, sample_s3_error_responses, environment_variables,
    multi_stream_pipeline_config
)


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
    """Sample pipeline configuration for testing - mimics real-world complexity."""
    return {
        "pipeline_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "name": "Wise Multi-Stream to SevDesk Integration",
        "version": "1.0",
        "source": {
            "connection_id": "0e8b1731-479a-4bc0-b056-244cc5d6a53c",
            "name": "Wise Platform"
        },
        "destination": {
            "connection_id": "7c1a69eb-239f-45d4-b6c2-3ad4c6e89cfa",
            "name": "SevDesk Platform"
        },
        "engine_config": {
            "batch_size": 100,
            "max_concurrent_batches": 3,
            "buffer_size": 5000,
            "schedule": {
                "type": "interval",
                "interval_minutes": 60,
                "timezone": "UTC"
            }
        },
        "streams": {
            "f1a2b3c4-d5e6-7890-abcd-ef1234567891": {
                "name": "wise-transactions",
                "description": "Wise bank transactions sync",
                "source": {
                    "endpoint_id": "5a4b9e21-441f-4bc7-9d5e-41917b4357e6",
                    "replication_method": "incremental",
                    "cursor_field": "created",
                    "cursor_mode": "inclusive",
                    "safety_window_seconds": 120,
                    "primary_key": ["id"],
                    "tie_breaker_fields": ["id"]
                },
                "destination": {
                    "endpoint_id": "1e63d782-4b67-4b7e-b845-4b4de5e4f46e",
                    "refresh_mode": "upsert",
                    "batch_support": False,
                    "batch_size": 1
                },
                "mapping": {
                    "field_mappings": {
                        "created": {
                            "target": "valueDate",
                            "transformations": ["iso_to_date"],
                            "validation": {
                                "rules": [{"type": "not_null"}],
                                "error_action": "dlq"
                            }
                        },
                        "targetValue": {
                            "target": "amount",
                            "validation": {
                                "rules": [{"type": "not_null"}],
                                "error_action": "dlq"
                            }
                        },
                        "details.reference": {
                            "target": "paymtPurpose"
                        }
                    },
                    "computed_fields": {
                        "objectName": {
                            "expression": "CheckAccountTransaction"
                        },
                        "checkAccount": {
                            "expression": "{\"id\": \"5936402\", \"objectName\": \"CheckAccount\"}"
                        },
                        "status": {
                            "expression": "100"
                        }
                    }
                }
            }
        },
        "error_handling": {
            "strategy": "dlq",
            "retry_failed_records": True,
            "max_retries": 3,
            "retry_delay": 5,
            "error_categories": {
                "validation_error": "dlq",
                "transformation_error": "dlq",
                "api_error": "retry",
                "rate_limit_error": "retry_with_backoff"
            }
        },
        "monitoring": {
            "metrics_enabled": True,
            "log_level": "DEBUG",
            "checkpoint_interval": 50,
            "health_check_interval": 300,
            "progress_monitoring": "enabled"
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
            from sqlalchemy import text
            from src.shared.database_utils import create_database_engine

            async def cleanup():
                config = {
                    "driver": "postgresql",
                    "host": os.getenv("POSTGRES_HOST", "localhost"),
                    "parameters": {
                        "port": int(os.getenv("POSTGRES_PORT", "5432")),
                        "database": os.getenv("POSTGRES_DB", "analitiq_test"),
                        "user": os.getenv("POSTGRES_USER", "postgres"),
                        "password": os.getenv("POSTGRES_PASSWORD"),
                        "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer"),
                    },
                }
                engine, _ = await create_database_engine(config, require_port=True)
                try:
                    async with engine.connect() as conn:
                        for schema, table in cleanup_tables:
                            try:
                                await conn.execute(
                                    text(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE")
                                )
                                await conn.commit()
                            except Exception:
                                pass
                finally:
                    await engine.dispose()

            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(cleanup())
                else:
                    loop.run_until_complete(cleanup())
            except RuntimeError:
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