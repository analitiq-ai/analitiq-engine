"""Pytest configuration and fixtures."""

import asyncio
import os
import tempfile
from typing import Dict, Any
from unittest.mock import AsyncMock, Mock

import pytest


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