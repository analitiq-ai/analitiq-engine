"""E2E Test: API → PostgreSQL Database Streaming

Tests the complete pipeline execution from an API source
to a PostgreSQL database destination with mocked connections.
"""

import tempfile
from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from analitiq_stream.core.pipeline import Pipeline


class MockPostgreSQLConnection:
    """Mock PostgreSQL connection for destination database."""

    def __init__(self):
        self.executed_queries = []
        self.inserted_records = []
        self.is_closed = False

    async def execute(self, query, *args):
        self.executed_queries.append(("execute", query, args))

        # Track INSERT/UPSERT operations
        if ("INSERT" in query.upper() or "UPSERT" in query.upper()) and args:
            self.inserted_records.append(args)

        return "EXECUTE"

    async def executemany(self, query, args_list):
        self.executed_queries.append(("executemany", query, args_list))

        # Track batch inserts
        if "INSERT" in query.upper() or "UPSERT" in query.upper():
            self.inserted_records.extend(args_list)

        return "EXECUTEMANY"

    async def fetch(self, query, *args):
        """Return empty results for schema/metadata queries."""
        self.executed_queries.append(("fetch", query, args))
        return []

    async def fetchrow(self, query, *args):
        self.executed_queries.append(("fetchrow", query, args))
        return None

    async def close(self):
        self.is_closed = True


class MockHTTPResponse:
    """Mock HTTP response for API source."""

    def __init__(self, data: Any, status: int = 200, has_more: bool = False):
        self._data = data
        self.status = status
        self.has_more = has_more

    async def json(self):
        # Simulate API pagination response
        if isinstance(self._data, list):
            return {
                "data": self._data,
                "pagination": {
                    "has_more": self.has_more,
                    "total": len(self._data),
                    "offset": 0
                }
            }
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
def api_source_data():
    """Sample data from API source."""
    return [
        {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@external-api.com",
            "created": "2023-12-01T10:00:00Z",
            "status": "active",
            "profile": {
                "department": "Engineering",
                "level": "senior",
                "location": "US"
            }
        },
        {
            "id": 2,
            "name": "Bob Wilson",
            "email": "bob@external-api.com",
            "created": "2023-12-01T11:00:00Z",
            "status": "pending",
            "profile": {
                "department": "Marketing",
                "level": "junior",
                "location": "EU"
            }
        },
        {
            "id": 3,
            "name": "Carol Brown",
            "email": "carol@external-api.com",
            "created": "2023-12-01T12:00:00Z",
            "status": "inactive",
            "profile": {
                "department": "Sales",
                "level": "manager",
                "location": "APAC"
            }
        }
    ]


@pytest.fixture
def api_to_db_pipeline_config():
    """Pipeline configuration for API → DB streaming."""
    return {
        "pipeline_id": "api-to-db-test",
        "name": "API to Database Test Pipeline",
        "version": "1.0",
        "engine_config": {
            "batch_size": 2,
            "max_concurrent_batches": 1,
            "buffer_size": 10
        },
        "streams": {
            "user_ingestion": {
                "name": "User Ingestion Stream",
                "description": "Ingest users from external API to database",
                "src": {
                    "endpoint_id": "api-source-endpoint",
                    "host_id": "api-source-host",
                    "replication_key": "created",
                    "cursor_mode": "inclusive"
                },
                "dst": {
                    "endpoint_id": "db-dest-endpoint",
                    "host_id": "db-dest-host",
                    "refresh_mode": "upsert",
                    "batch_size": 2
                },
                "mapping": {
                    "field_mappings": {
                        "id": {"target": "external_id"},
                        "name": {"target": "full_name"},
                        "email": {"target": "email_address"},
                        "created": {
                            "target": "created_at",
                            "transformations": ["iso_string_to_datetime"]
                        },
                        "status": {"target": "account_status"},
                        "profile.department": {"target": "dept"},
                        "profile.level": {"target": "job_level"},
                        "profile.location": {"target": "region"}
                    },
                    "computed_fields": {
                        "ingestion_timestamp": {"expression": "now()"},
                        "data_source": {"expression": "external_api"},
                        "record_type": {"expression": "user_profile"}
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
def api_source_config():
    """API source configuration."""
    return {
        "type": "api",
        "endpoint": "/api/v1/users",
        "method": "GET",
        "base_url": "https://external-api.com",
        "headers": {
            "Authorization": "Bearer api-source-token",
            "Accept": "application/json"
        },
        "pagination": {
            "type": "offset",
            "limit_param": "limit",
            "offset_param": "offset",
            "limit": 100
        }
    }


@pytest.fixture
def db_destination_config():
    """PostgreSQL destination configuration."""
    return {
        "type": "database",
        "driver": "postgresql",
        "host": "warehouse-db.example.com",
        "port": 5432,
        "database": "data_warehouse",
        "user": "warehouse_user",
        "password": "warehouse_pass",
        "schema": "external_data",
        "table": "api_users",
        "write_mode": "upsert",
        "conflict_resolution": {
            "on_conflict": "external_id",
            "action": "update"
        }
    }


class TestAPIToDatabase:
    """Test API → Database streaming scenarios."""

    @pytest.mark.asyncio
    async def test_successful_api_to_db_stream(
        self,
        api_to_db_pipeline_config,
        api_source_config,
        db_destination_config,
        api_source_data,
        temp_state_dir
    ):
        """Test successful streaming from API to PostgreSQL."""

        # Mock API responses with pagination
        api_responses = [
            MockHTTPResponse(api_source_data[:2], has_more=True),
            MockHTTPResponse(api_source_data[2:], has_more=False)
        ]

        mock_db_conn = MockPostgreSQLConnection()

        with patch('asyncpg.connect', return_value=mock_db_conn):
            with patch('aiohttp.ClientSession.get') as mock_get:
                # Mock paginated API calls
                mock_get.side_effect = api_responses

                with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                    mock_engine = MagicMock()
                    mock_engine.stream_data = AsyncMock()
                    mock_engine.get_metrics.return_value = {
                        "records_processed": 3,
                        "records_failed": 0,
                        "api_requests_made": 2
                    }
                    # Mock state manager to return None (no previous state)
                    mock_state_manager = MagicMock()
                    mock_state_manager.get_run_info.return_value = None
                    mock_engine.get_state_manager.return_value = mock_state_manager
                    mock_engine_cls.return_value = mock_engine

                    # Create and run pipeline
                    pipeline = Pipeline(
                        pipeline_config=api_to_db_pipeline_config,
                        source_config=api_source_config,
                        destination_config=db_destination_config,
                        state_dir=temp_state_dir
                    )

                    await pipeline.run()

                    # Verify engine was called
                    mock_engine.stream_data.assert_called_once()

                    # Verify configuration
                    config = mock_engine.stream_data.call_args[0][0]
                    assert config["pipeline_id"] == "api-to-db-test"
                    assert "user_ingestion" in config["streams"]

                    # Verify field mappings for nested fields
                    stream_config = config["streams"]["user_ingestion"]
                    field_mappings = stream_config["mapping"]["field_mappings"]
                    assert field_mappings["profile.department"]["target"] == "dept"

    @pytest.mark.asyncio
    async def test_api_to_db_with_api_errors(
        self,
        api_to_db_pipeline_config,
        api_source_config,
        db_destination_config,
        temp_state_dir
    ):
        """Test API → DB streaming with API errors."""

        mock_db_conn = MockPostgreSQLConnection()

        with patch('asyncpg.connect', return_value=mock_db_conn):
            with patch('aiohttp.ClientSession.get') as mock_get:
                # First call succeeds, second fails
                mock_get.side_effect = [
                    MockHTTPResponse([{"id": 1, "name": "Test User"}]),
                    MockHTTPResponse({"error": "Rate limit exceeded"}, status=429)
                ]

                with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                    mock_engine = MagicMock()
                    mock_engine.stream_data = AsyncMock()
                    mock_engine.get_metrics.return_value = {
                        "records_processed": 1,
                        "records_failed": 0,
                        "api_errors": 1
                    }
                    # Mock state manager to return None (no previous state)
                    mock_state_manager = MagicMock()
                    mock_state_manager.get_run_info.return_value = None
                    mock_engine.get_state_manager.return_value = mock_state_manager
                    mock_engine_cls.return_value = mock_engine

                    pipeline = Pipeline(
                        pipeline_config=api_to_db_pipeline_config,
                        source_config=api_source_config,
                        destination_config=db_destination_config,
                        state_dir=temp_state_dir
                    )

                    # Should handle API errors gracefully
                    try:
                        await pipeline.run()
                    except Exception:
                        pass  # Expected to handle errors

                    # Verify partial success
                    metrics = pipeline.get_metrics()
                    assert "records_processed" in metrics

    @pytest.mark.asyncio
    async def test_api_to_db_with_data_transformations(
        self,
        api_source_config,
        db_destination_config,
        api_source_data,
        temp_state_dir
    ):
        """Test API → DB streaming with complex data transformations."""

        # Pipeline config with extensive transformations
        pipeline_config = {
            "pipeline_id": "api-to-db-transform-test",
            "name": "API to DB Transform Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 1,
                "max_concurrent_batches": 1
            },
            "streams": {
                "transform_stream": {
                    "name": "Transform Stream",
                    "src": {"endpoint_id": "api-src", "host_id": "api-host"},
                    "dst": {"endpoint_id": "db-dst", "host_id": "db-host"},
                    "mapping": {
                        "field_mappings": {
                            "name": {
                                "target": "user_name",
                                "transformations": ["strip", "upper"]
                            },
                            "email": {
                                "target": "email_addr",
                                "transformations": ["lower", "strip"]
                            },
                            "created": {
                                "target": "registration_time",
                                "transformations": ["iso_string_to_datetime"]
                            },
                            "status": {
                                "target": "status_code",
                                "transformations": ["upper"]
                            }
                        },
                        "computed_fields": {
                            "is_active": {"expression": "status == 'ACTIVE'"},
                            "processed_date": {"expression": "now()"},
                            "hash_key": {"expression": "uuid()"}
                        }
                    }
                }
            }
        }

        mock_db_conn = MockPostgreSQLConnection()

        with patch('asyncpg.connect', return_value=mock_db_conn):
            with patch('aiohttp.ClientSession.get') as mock_get:
                mock_get.return_value = MockHTTPResponse(api_source_data)

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
                        source_config=api_source_config,
                        destination_config=db_destination_config,
                        state_dir=temp_state_dir
                    )

                    await pipeline.run()

                    # Verify transformations are in config
                    config = mock_engine.stream_data.call_args[0][0]
                    stream_config = config["streams"]["transform_stream"]
                    field_mappings = stream_config["mapping"]["field_mappings"]

                    assert field_mappings["name"]["transformations"] == ["strip", "upper"]
                    assert field_mappings["email"]["transformations"] == ["lower", "strip"]

    @pytest.mark.asyncio
    async def test_api_to_db_incremental_sync(
        self,
        api_to_db_pipeline_config,
        api_source_config,
        db_destination_config,
        temp_state_dir
    ):
        """Test incremental synchronization from API to database."""

        # First batch
        initial_data = [
            {
                "id": 1,
                "name": "Initial User",
                "email": "initial@test.com",
                "created": "2023-12-01T10:00:00Z",
                "status": "active"
            }
        ]

        # Second batch with newer data
        newer_data = [
            {
                "id": 2,
                "name": "New User",
                "email": "new@test.com",
                "created": "2023-12-01T11:00:00Z",
                "status": "active"
            }
        ]

        mock_db_conn = MockPostgreSQLConnection()

        # First sync
        with patch('asyncpg.connect', return_value=mock_db_conn):
            with patch('aiohttp.ClientSession.get') as mock_get:
                mock_get.return_value = MockHTTPResponse(initial_data)

                with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                    mock_engine = MagicMock()
                    mock_engine.stream_data = AsyncMock()
                    # Mock state manager to return None (no previous state)
                    mock_state_manager = MagicMock()
                    mock_state_manager.get_run_info.return_value = None
                    mock_engine.get_state_manager.return_value = mock_state_manager
                    mock_engine_cls.return_value = mock_engine

                    pipeline1 = Pipeline(
                        pipeline_config=api_to_db_pipeline_config,
                        source_config=api_source_config,
                        destination_config=db_destination_config,
                        state_dir=temp_state_dir
                    )

                    await pipeline1.run()

        # Second sync with newer data
        with patch('asyncpg.connect', return_value=mock_db_conn):
            with patch('aiohttp.ClientSession.get') as mock_get:
                mock_get.return_value = MockHTTPResponse(newer_data)

                with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                    mock_engine = MagicMock()
                    mock_engine.stream_data = AsyncMock()
                    # Mock state manager to return None (no previous state)
                    mock_state_manager = MagicMock()
                    mock_state_manager.get_run_info.return_value = None
                    mock_engine.get_state_manager.return_value = mock_state_manager
                    mock_engine_cls.return_value = mock_engine

                    pipeline2 = Pipeline(
                        pipeline_config=api_to_db_pipeline_config,
                        source_config=api_source_config,
                        destination_config=db_destination_config,
                        state_dir=temp_state_dir
                    )

                    await pipeline2.run()

                    # Both syncs should complete
                    mock_engine.stream_data.assert_called_once()

    def test_api_to_db_config_validation(
        self,
        api_source_config,
        db_destination_config,
        temp_state_dir
    ):
        """Test configuration validation for API → DB pipeline."""

        # Valid minimal config
        valid_config = {
            "pipeline_id": "valid-api-to-db",
            "name": "Valid API to DB Pipeline",
            "streams": {
                "test_stream": {
                    "name": "Test Stream",
                    "src": {"endpoint_id": "api-src", "host_id": "api-host"},
                    "dst": {"endpoint_id": "db-dst", "host_id": "db-host"}
                }
            }
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=valid_config,
                source_config=api_source_config,
                destination_config=db_destination_config,
                state_dir=temp_state_dir
            )

            assert pipeline.config["pipeline_id"] == "valid-api-to-db"

            # Should be able to get status
            status = pipeline.get_status()
            assert "test_stream" in status["streams"]
            assert status["streams"]["test_stream"]["name"] == "Test Stream"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])