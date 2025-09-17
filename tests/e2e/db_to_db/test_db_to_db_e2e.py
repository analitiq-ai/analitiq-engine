"""E2E Test: PostgreSQL Database → PostgreSQL Database Streaming

Tests the complete pipeline execution from a PostgreSQL database source
to another PostgreSQL database destination with mocked connections.
"""

import tempfile
from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from analitiq_stream.core.pipeline import Pipeline


class MockPostgreSQLConnection:
    """Mock PostgreSQL connection for both source and destination."""

    def __init__(self, mock_data: List[Dict] = None, connection_type: str = "source"):
        self.executed_queries = []
        self.mock_data = mock_data or []
        self.current_index = 0
        self.is_closed = False
        self.connection_type = connection_type
        self.inserted_records = []

    async def execute(self, query, *args):
        self.executed_queries.append(("execute", query, args))

        # Track INSERT/UPSERT operations for destination
        if self.connection_type == "destination" and ("INSERT" in query.upper() or "UPSERT" in query.upper()):
            if args:
                self.inserted_records.extend(args)

        return "EXECUTE"

    async def executemany(self, query, args_list):
        self.executed_queries.append(("executemany", query, args_list))

        # Track batch inserts for destination
        if self.connection_type == "destination" and ("INSERT" in query.upper() or "UPSERT" in query.upper()):
            self.inserted_records.extend(args_list)

        return "EXECUTEMANY"

    async def fetch(self, query, *args):
        """Return mock data for SELECT queries."""
        self.executed_queries.append(("fetch", query, args))

        # Return data for source database queries
        if self.connection_type == "source" and "SELECT" in query.upper():
            batch_size = 2  # Small batch for testing
            start_idx = self.current_index
            end_idx = min(start_idx + batch_size, len(self.mock_data))
            result = self.mock_data[start_idx:end_idx]
            self.current_index = end_idx
            return result

        # Return metadata for destination database queries
        return []

    async def fetchrow(self, query, *args):
        results = await self.fetch(query, *args)
        return results[0] if results else None

    async def close(self):
        self.is_closed = True


@pytest.fixture
def temp_state_dir():
    """Create temporary directory for pipeline state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def source_db_data():
    """Sample data from source PostgreSQL database."""
    return [
        {
            "user_id": 1,
            "username": "john_doe",
            "email": "john@source.com",
            "created_at": datetime(2023, 12, 1, 10, 0, 0),
            "status": "active",
            "last_login": datetime(2023, 12, 1, 9, 0, 0)
        },
        {
            "user_id": 2,
            "username": "jane_smith",
            "email": "jane@source.com",
            "created_at": datetime(2023, 12, 1, 11, 0, 0),
            "status": "active",
            "last_login": datetime(2023, 12, 1, 10, 30, 0)
        },
        {
            "user_id": 3,
            "username": "bob_johnson",
            "email": "bob@source.com",
            "created_at": datetime(2023, 12, 1, 12, 0, 0),
            "status": "inactive",
            "last_login": datetime(2023, 11, 30, 15, 0, 0)
        }
    ]


@pytest.fixture
def db_to_db_pipeline_config():
    """Pipeline configuration for DB → DB streaming."""
    return {
        "pipeline_id": "db-to-db-test",
        "name": "Database to Database Test Pipeline",
        "version": "1.0",
        "engine_config": {
            "batch_size": 2,
            "max_concurrent_batches": 1,
            "buffer_size": 10
        },
        "streams": {
            "user_replication": {
                "name": "User Replication Stream",
                "description": "Replicate users from source DB to destination DB",
                "src": {
                    "endpoint_id": "source-db-endpoint",
                    "host_id": "source-db-host",
                    "replication_key": "created_at",
                    "cursor_mode": "inclusive"
                },
                "dst": {
                    "endpoint_id": "dest-db-endpoint",
                    "host_id": "dest-db-host",
                    "refresh_mode": "upsert",
                    "batch_size": 2
                },
                "mapping": {
                    "field_mappings": {
                        "user_id": {"target": "id"},
                        "username": {"target": "login_name"},
                        "email": {"target": "email_address"},
                        "status": {"target": "account_status"},
                        "created_at": {"target": "registration_date"},
                        "last_login": {"target": "last_access_time"}
                    },
                    "computed_fields": {
                        "sync_timestamp": {"expression": "now()"},
                        "source_system": {"expression": "legacy_db"},
                        "record_version": {"expression": "1"}
                    }
                }
            }
        },
        "error_handling": {
            "strategy": "dlq",
            "max_retries": 3
        },
        "monitoring": {
            "metrics_enabled": True,
            "log_level": "INFO"
        }
    }


@pytest.fixture
def source_db_config():
    """Source PostgreSQL database configuration."""
    return {
        "type": "database",
        "driver": "postgresql",
        "host": "source-db.example.com",
        "port": 5432,
        "database": "legacy_system",
        "user": "source_user",
        "password": "source_pass",
        "schema": "public",
        "table": "users",
        "incremental_column": "created_at"
    }


@pytest.fixture
def destination_db_config():
    """Destination PostgreSQL database configuration."""
    return {
        "type": "database",
        "driver": "postgresql",
        "host": "dest-db.example.com",
        "port": 5432,
        "database": "analytics_warehouse",
        "user": "dest_user",
        "password": "dest_pass",
        "schema": "staging",
        "table": "user_profiles",
        "write_mode": "upsert",
        "conflict_resolution": {
            "on_conflict": "id",
            "action": "update"
        }
    }


class TestDatabaseToDatabase:
    """Test Database → Database streaming scenarios."""

    @pytest.mark.asyncio
    async def test_successful_db_to_db_stream(
        self,
        db_to_db_pipeline_config,
        source_db_config,
        destination_db_config,
        source_db_data,
        temp_state_dir
    ):
        """Test successful streaming from PostgreSQL to PostgreSQL."""

        # Create mock connections
        mock_source_conn = MockPostgreSQLConnection(source_db_data, "source")
        mock_dest_conn = MockPostgreSQLConnection([], "destination")

        # Mock connection factory to return appropriate connection
        async def mock_connect(host=None, **kwargs):
            if "source-db" in host:
                return mock_source_conn
            elif "dest-db" in host:
                return mock_dest_conn
            else:
                # Default to source for any other case
                return mock_source_conn

        with patch('asyncpg.connect', side_effect=mock_connect):
            with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                mock_engine = MagicMock()
                mock_engine.stream_data = AsyncMock()
                mock_engine.get_metrics.return_value = {
                    "records_processed": 3,
                    "records_failed": 0,
                    "batches_processed": 2
                }
                # Mock state manager to return None (no previous state)
                mock_state_manager = MagicMock()
                mock_state_manager.get_run_info.return_value = None
                mock_engine.get_state_manager.return_value = mock_state_manager
                mock_engine_cls.return_value = mock_engine

                # Create and run pipeline
                pipeline = Pipeline(
                    pipeline_config=db_to_db_pipeline_config,
                    source_config=source_db_config,
                    destination_config=destination_db_config,
                    state_dir=temp_state_dir
                )

                await pipeline.run()

                # Verify engine was called
                mock_engine.stream_data.assert_called_once()

                # Verify configuration
                config = mock_engine.stream_data.call_args[0][0]
                assert config["pipeline_id"] == "db-to-db-test"
                assert "user_replication" in config["streams"]

                # Verify field mappings are preserved
                stream_config = config["streams"]["user_replication"]
                field_mappings = stream_config["mapping"]["field_mappings"]
                assert field_mappings["user_id"]["target"] == "id"
                assert field_mappings["username"]["target"] == "login_name"

    @pytest.mark.asyncio
    async def test_db_to_db_with_schema_differences(
        self,
        source_db_config,
        destination_db_config,
        source_db_data,
        temp_state_dir
    ):
        """Test DB → DB streaming with schema transformation."""

        # Pipeline config with schema transformation
        pipeline_config = {
            "pipeline_id": "db-to-db-schema-test",
            "name": "DB to DB Schema Transform",
            "version": "1.0",
            "engine_config": {
                "batch_size": 1,
                "max_concurrent_batches": 1
            },
            "streams": {
                "schema_transform": {
                    "name": "Schema Transform Stream",
                    "src": {"endpoint_id": "src-db", "host_id": "src-host"},
                    "dst": {"endpoint_id": "dst-db", "host_id": "dst-host"},
                    "mapping": {
                        "field_mappings": {
                            # Transform data types and formats
                            "user_id": {
                                "target": "external_id",
                                "transformations": ["to_str"]
                            },
                            "created_at": {
                                "target": "created_timestamp",
                                "transformations": ["iso_string_to_datetime"]
                            },
                            "status": {
                                "target": "status_code",
                                "transformations": ["upper"]
                            }
                        },
                        "computed_fields": {
                            "is_active": {"expression": "status == 'ACTIVE'"},
                            "migration_batch": {"expression": "batch_001"},
                            "last_updated": {"expression": "now()"}
                        }
                    }
                }
            }
        }

        mock_source_conn = MockPostgreSQLConnection(source_db_data, "source")
        mock_dest_conn = MockPostgreSQLConnection([], "destination")

        async def mock_connect(host=None, **kwargs):
            return mock_source_conn if "source" in str(host) else mock_dest_conn

        with patch('asyncpg.connect', side_effect=mock_connect):
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
                    source_config=source_db_config,
                    destination_config=destination_db_config,
                    state_dir=temp_state_dir
                )

                await pipeline.run()

                # Verify transformations in config
                config = mock_engine.stream_data.call_args[0][0]
                stream_config = config["streams"]["schema_transform"]
                field_mappings = stream_config["mapping"]["field_mappings"]

                assert field_mappings["user_id"]["transformations"] == ["to_str"]
                assert field_mappings["status"]["transformations"] == ["upper"]

    @pytest.mark.asyncio
    async def test_db_to_db_incremental_sync(
        self,
        db_to_db_pipeline_config,
        source_db_config,
        destination_db_config,
        temp_state_dir
    ):
        """Test incremental synchronization between databases."""

        # First batch of data
        initial_data = [
            {
                "user_id": 1,
                "username": "user1",
                "email": "user1@test.com",
                "created_at": datetime(2023, 12, 1, 10, 0, 0),
                "status": "active"
            }
        ]

        # Second batch with newer data
        newer_data = [
            {
                "user_id": 2,
                "username": "user2",
                "email": "user2@test.com",
                "created_at": datetime(2023, 12, 1, 11, 0, 0),
                "status": "active"
            }
        ]

        # First run
        mock_source_conn1 = MockPostgreSQLConnection(initial_data, "source")
        mock_dest_conn1 = MockPostgreSQLConnection([], "destination")

        async def mock_connect_run1(host=None, **kwargs):
            return mock_source_conn1 if "source" in str(host) else mock_dest_conn1

        with patch('asyncpg.connect', side_effect=mock_connect_run1):
            with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                mock_engine = MagicMock()
                mock_engine.stream_data = AsyncMock()
                # Mock state manager to return None (no previous state)
                mock_state_manager = MagicMock()
                mock_state_manager.get_run_info.return_value = None
                mock_engine.get_state_manager.return_value = mock_state_manager
                mock_engine_cls.return_value = mock_engine

                pipeline1 = Pipeline(
                    pipeline_config=db_to_db_pipeline_config,
                    source_config=source_db_config,
                    destination_config=destination_db_config,
                    state_dir=temp_state_dir
                )

                await pipeline1.run()

        # Second run with newer data
        mock_source_conn2 = MockPostgreSQLConnection(newer_data, "source")
        mock_dest_conn2 = MockPostgreSQLConnection([], "destination")

        async def mock_connect_run2(host=None, **kwargs):
            return mock_source_conn2 if "source" in str(host) else mock_dest_conn2

        with patch('asyncpg.connect', side_effect=mock_connect_run2):
            with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                mock_engine = MagicMock()
                mock_engine.stream_data = AsyncMock()
                # Mock state manager to return None (no previous state)
                mock_state_manager = MagicMock()
                mock_state_manager.get_run_info.return_value = None
                mock_engine.get_state_manager.return_value = mock_state_manager
                mock_engine_cls.return_value = mock_engine

                pipeline2 = Pipeline(
                    pipeline_config=db_to_db_pipeline_config,
                    source_config=source_db_config,
                    destination_config=destination_db_config,
                    state_dir=temp_state_dir
                )

                await pipeline2.run()

                # Both runs should succeed
                mock_engine.stream_data.assert_called_once()

    def test_db_to_db_config_validation(
        self,
        source_db_config,
        destination_db_config,
        temp_state_dir
    ):
        """Test configuration validation for DB → DB pipeline."""

        # Valid config should work
        valid_config = {
            "pipeline_id": "valid-db-to-db",
            "name": "Valid Pipeline",
            "streams": {
                "test_stream": {
                    "name": "Test Stream",
                    "src": {"endpoint_id": "src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "dst", "host_id": "dst-host"}
                }
            }
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine'):
            pipeline = Pipeline(
                pipeline_config=valid_config,
                source_config=source_db_config,
                destination_config=destination_db_config,
                state_dir=temp_state_dir
            )

            assert pipeline.config["pipeline_id"] == "valid-db-to-db"
            status = pipeline.get_status()
            assert "test_stream" in status["streams"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])