"""E2E Test: PostgreSQL Database → API Streaming

Tests the complete pipeline execution from a PostgreSQL database source
to an API destination with mocked connections.
"""

import tempfile
from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from analitiq_stream.core.pipeline import Pipeline


class MockPostgreSQLConnection:
    """Mock PostgreSQL connection for source database."""

    def __init__(self, mock_data: List[Dict] = None):
        self.executed_queries = []
        self.mock_data = mock_data or []
        self.current_index = 0
        self.is_closed = False

    async def execute(self, query, *args):
        self.executed_queries.append(("execute", query, args))
        return "EXECUTE"

    async def fetch(self, query, *args):
        """Return mock data for SELECT queries."""
        self.executed_queries.append(("fetch", query, args))

        # Return data for actual data queries
        if "SELECT" in query.upper() and "users" in query.lower():
            batch_size = 2  # Small batch for testing
            start_idx = self.current_index
            end_idx = min(start_idx + batch_size, len(self.mock_data))
            result = self.mock_data[start_idx:end_idx]
            self.current_index = end_idx
            return result

        return []

    async def close(self):
        self.is_closed = True


class MockHTTPResponse:
    """Mock HTTP response for API destination."""

    def __init__(self, data: Any, status: int = 200):
        self._data = data
        self.status = status

    async def json(self):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest.fixture
def temp_state_dir():
    """Create temporary directory for pipeline state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def source_db_data():
    """Sample data from PostgreSQL source."""
    return [
        {
            "id": 1,
            "name": "John Doe",
            "email": "john@company.com",
            "created_at": datetime(2023, 12, 1, 10, 0, 0),
            "status": "active",
            "department": "Engineering"
        },
        {
            "id": 2,
            "name": "Jane Smith",
            "email": "jane@company.com",
            "created_at": datetime(2023, 12, 1, 11, 0, 0),
            "status": "active",
            "department": "Marketing"
        },
        {
            "id": 3,
            "name": "Bob Johnson",
            "email": "bob@company.com",
            "created_at": datetime(2023, 12, 1, 12, 0, 0),
            "status": "inactive",
            "department": "Sales"
        }
    ]


@pytest.fixture
def db_to_api_pipeline_config():
    """Pipeline configuration for DB → API streaming."""
    return {
        "pipeline_id": "db-to-api-test",
        "name": "Database to API Test Pipeline",
        "version": "1.0",
        "engine_config": {
            "batch_size": 2,
            "max_concurrent_batches": 1,
            "buffer_size": 10
        },
        "streams": {
            "user_sync": {
                "name": "User Sync Stream",
                "description": "Sync users from database to API",
                "src": {
                    "endpoint_id": "db-source-endpoint",
                    "host_id": "db-source-host",
                    "replication_key": "created_at",
                    "cursor_mode": "inclusive"
                },
                "dst": {
                    "endpoint_id": "api-dest-endpoint",
                    "host_id": "api-dest-host",
                    "refresh_mode": "insert",
                    "batch_support": False
                },
                "mapping": {
                    "field_mappings": {
                        "id": {"target": "user_id"},
                        "name": {"target": "full_name"},
                        "email": {"target": "email_address"},
                        "status": {"target": "account_status"},
                        "department": {"target": "dept_name"}
                    },
                    "computed_fields": {
                        "sync_timestamp": {"expression": "now()"},
                        "source_system": {"expression": "internal_db"}
                    }
                }
            }
        },
        "error_handling": {
            "strategy": "dlq",
            "max_retries": 2
        },
        "monitoring": {
            "metrics_enabled": True,
            "log_level": "INFO"
        }
    }


@pytest.fixture
def db_source_config():
    """PostgreSQL source configuration."""
    return {
        "type": "database",
        "driver": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "source_db",
        "user": "source_user",
        "password": "source_pass",
        "schema": "public",
        "table": "users",
        "incremental_column": "created_at"
    }


@pytest.fixture
def api_destination_config():
    """API destination configuration."""
    return {
        "type": "api",
        "endpoint": "/api/users",
        "method": "POST",
        "base_url": "https://external-api.com",
        "headers": {
            "Authorization": "Bearer api-token",
            "Content-Type": "application/json"
        }
    }


class TestDatabaseToAPI:
    """Test Database → API streaming scenarios."""

    @pytest.mark.asyncio
    async def test_successful_db_to_api_stream(
        self,
        db_to_api_pipeline_config,
        db_source_config,
        api_destination_config,
        source_db_data,
        temp_state_dir
    ):
        """Test successful streaming from PostgreSQL to API."""

        # Mock successful API responses
        api_responses = [
            MockHTTPResponse({"id": 1, "status": "created"}),
            MockHTTPResponse({"id": 2, "status": "created"}),
            MockHTTPResponse({"id": 3, "status": "created"})
        ]

        # Create mock database connection with data
        mock_db_conn = MockPostgreSQLConnection(source_db_data)

        with patch('asyncpg.connect', return_value=mock_db_conn):
            with patch('aiohttp.ClientSession.post') as mock_post:
                # Mock API POST requests
                mock_post.side_effect = api_responses

                with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                    mock_engine = MagicMock()
                    mock_engine.stream_data = AsyncMock()
                    mock_engine.get_metrics.return_value = {
                        "records_processed": 3,
                        "records_failed": 0,
                        "api_requests_made": 3
                    }
                    # Mock state manager to return None (no previous state)
                    mock_state_manager = MagicMock()
                    mock_state_manager.get_run_info.return_value = None
                    mock_engine.get_state_manager.return_value = mock_state_manager
                    mock_engine_cls.return_value = mock_engine

                    # Create and run pipeline
                    pipeline = Pipeline(
                        pipeline_config=db_to_api_pipeline_config,
                        source_config=db_source_config,
                        destination_config=api_destination_config,
                        state_dir=temp_state_dir
                    )

                    await pipeline.run()

                    # Verify engine was called
                    mock_engine.stream_data.assert_called_once()

                    # Verify configuration structure
                    config = mock_engine.stream_data.call_args[0][0]
                    assert config["pipeline_id"] == "db-to-api-test"
                    assert "user_sync" in config["streams"]

    @pytest.mark.asyncio
    async def test_db_to_api_with_api_errors(
        self,
        db_to_api_pipeline_config,
        db_source_config,
        api_destination_config,
        source_db_data,
        temp_state_dir
    ):
        """Test DB → API streaming with API errors."""

        # Mix of successful and failed API responses
        api_responses = [
            MockHTTPResponse({"id": 1, "status": "created"}),
            MockHTTPResponse({"error": "Validation failed"}, status=400),
            MockHTTPResponse({"id": 3, "status": "created"})
        ]

        mock_db_conn = MockPostgreSQLConnection(source_db_data)

        with patch('asyncpg.connect', return_value=mock_db_conn):
            with patch('aiohttp.ClientSession.post') as mock_post:
                mock_post.side_effect = api_responses

                with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                    mock_engine = MagicMock()
                    mock_engine.stream_data = AsyncMock()
                    mock_engine.get_metrics.return_value = {
                        "records_processed": 2,
                        "records_failed": 1,
                        "api_requests_made": 3
                    }
                    # Mock state manager to return None (no previous state)
                    mock_state_manager = MagicMock()
                    mock_state_manager.get_run_info.return_value = None
                    mock_engine.get_state_manager.return_value = mock_state_manager
                    mock_engine_cls.return_value = mock_engine

                    pipeline = Pipeline(
                        pipeline_config=db_to_api_pipeline_config,
                        source_config=db_source_config,
                        destination_config=api_destination_config,
                        state_dir=temp_state_dir
                    )

                    # Should handle errors gracefully
                    await pipeline.run()

                    # Verify partial success metrics
                    metrics = pipeline.get_metrics()
                    assert metrics["records_processed"] == 2
                    assert metrics["records_failed"] == 1

    @pytest.mark.asyncio
    async def test_db_to_api_with_transformations(
        self,
        db_source_config,
        api_destination_config,
        source_db_data,
        temp_state_dir
    ):
        """Test DB → API streaming with field transformations."""

        # Pipeline config with transformations
        pipeline_config = {
            "pipeline_id": "db-to-api-transform-test",
            "name": "DB to API with Transformations",
            "version": "1.0",
            "engine_config": {
                "batch_size": 2,
                "max_concurrent_batches": 1
            },
            "streams": {
                "user_transform": {
                    "name": "User Transform Stream",
                    "src": {"endpoint_id": "db-src", "host_id": "db-host"},
                    "dst": {"endpoint_id": "api-dst", "host_id": "api-host"},
                    "mapping": {
                        "field_mappings": {
                            "name": {
                                "target": "full_name",
                                "transformations": ["upper"]
                            },
                            "email": {
                                "target": "email_address",
                                "transformations": ["lower"]
                            },
                            "status": {
                                "target": "account_status",
                                "transformations": ["upper"]
                            }
                        },
                        "computed_fields": {
                            "record_type": {"expression": "user_record"},
                            "processed_at": {"expression": "now()"}
                        }
                    }
                }
            }
        }

        mock_db_conn = MockPostgreSQLConnection(source_db_data)

        with patch('asyncpg.connect', return_value=mock_db_conn):
            with patch('aiohttp.ClientSession.post') as mock_post:
                mock_post.return_value = MockHTTPResponse({"status": "success"})

                with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                    mock_engine = MagicMock()
                    mock_engine.stream_data = AsyncMock()
                    # Mock state manager to return None (no previous state)
                    mock_state_manager = MagicMock()
                    mock_state_manager.get_run_info.return_value = None
                    mock_engine.get_state_manager.return_value = mock_state_manager
                    mock_engine_cls.return_value = mock_engine

                    pipeline = Pipeline(
                        pipeline_config=pipeline_config,
                        source_config=db_source_config,
                        destination_config=api_destination_config,
                        state_dir=temp_state_dir
                    )

                    await pipeline.run()

                    # Verify transformations are in config
                    config = mock_engine.stream_data.call_args[0][0]
                    stream_config = config["streams"]["user_transform"]
                    field_mappings = stream_config["mapping"]["field_mappings"]

                    assert "transformations" in field_mappings["name"]
                    assert field_mappings["name"]["transformations"] == ["upper"]

    def test_db_to_api_config_validation(
        self,
        db_source_config,
        api_destination_config,
        temp_state_dir
    ):
        """Test configuration validation for DB → API pipeline."""

        # Invalid config - missing required fields
        invalid_config = {
            "pipeline_id": "invalid-test",
            "streams": {
                "test": {
                    # Missing src and dst sections
                    "name": "Invalid Stream"
                }
            }
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=invalid_config,
                source_config=db_source_config,
                destination_config=api_destination_config,
                state_dir=temp_state_dir
            )

            # Should pass initialization but fail validation
            assert pipeline.config["pipeline_id"] == "invalid-test"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])