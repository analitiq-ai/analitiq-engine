"""Pytest configuration and shared fixtures."""

import asyncio
import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
from dotenv import load_dotenv

from tests.fixtures.api import (
    mock_api_connector,
    mock_http_responses,
    sample_api_config,
    sample_api_response,
)

# Import specific fixtures from modules
from tests.fixtures.database import mock_database_connector, postgres_driver
from tests.fixtures.pipeline_config_prep import (
    environment_variables,
    multi_stream_pipeline_config,
    sample_database_endpoint_config,
    sample_database_host_config,
    sample_invalid_pipeline_config,
    sample_s3_error_responses,
    sample_sevdesk_endpoint_config,
    sample_sevdesk_host_config,
    sample_wise_endpoint_config,
    sample_wise_host_config,
)

# Fixtures imported above are re-exported for pytest discovery, not used here.
__all__ = [
    "mock_api_connector",
    "mock_http_responses",
    "sample_api_config",
    "sample_api_response",
    "mock_database_connector",
    "postgres_driver",
    "environment_variables",
    "multi_stream_pipeline_config",
    "sample_database_endpoint_config",
    "sample_database_host_config",
    "sample_invalid_pipeline_config",
    "sample_s3_error_responses",
    "sample_sevdesk_endpoint_config",
    "sample_sevdesk_host_config",
    "sample_wise_endpoint_config",
    "sample_wise_host_config",
]

# Load test environment variables from .env file
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    load_dotenv(env_file, override=False)  # Don't override existing env vars


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
def tmp_project_root(tmp_path, monkeypatch):
    """Chdir into a synthetic project root with a ``pipelines/manifest.json``.

    ``StreamingEngine.__init__`` wires its worker client by walking up from
    CWD to the nearest ``pipelines/manifest.json``. Tests that construct an
    engine must run inside this synthetic root so the walk never escapes into
    an ambient developer checkout (whose gitignored registry would be silently
    picked up) and never fails in a clean checkout such as CI, where no
    manifest exists at all.
    """
    (tmp_path / "pipelines").mkdir()
    (tmp_path / "pipelines" / "manifest.json").write_text('{"pipelines": []}')
    monkeypatch.chdir(tmp_path)
    return tmp_path


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
            "merchant": {"name": "Test Merchant"},
        },
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
    state_manager.get_checkpoint = Mock(return_value={})
    state_manager.checkpoint = AsyncMock()
    return state_manager


@pytest.fixture(autouse=True)
def setup_test_env():
    """Set up test environment variables."""
    test_vars = {
        "TEST_VAR": "test_value",
        "SEVDESK_BANK_ACCOUNT_ID": "5936402",
        "API_TOKEN": "test-token",
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
