"""Integration tests for streaming data to PostgreSQL database."""

import pytest
import asyncio
import os
from typing import List, Dict, Any
from datetime import datetime, timezone
import json

from src.source.connectors.database import DatabaseConnector
from src.engine.engine import StreamingEngine
from src.engine.pipeline import Pipeline


@pytest.fixture
def postgres_config():
    """PostgreSQL configuration from environment."""
    if not os.getenv("POSTGRES_PASSWORD"):
        pytest.skip("PostgreSQL not configured. Set POSTGRES_PASSWORD in tests/.env")
    
    return {
        "driver": "postgresql",
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "analitiq_test"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "ssl_mode": os.getenv("POSTGRES_SSL_MODE", "prefer"),
        "connection_pool": {
            "min_connections": 2,
            "max_connections": 5,
            "pool_timeout": 30
        }
    }


@pytest.fixture
def test_table_config():
    """Test table configuration with auto-creation."""
    return {
        "schema": "analitiq_test",
        "table": "streaming_test_data",
        "primary_key": ["id"],
        "unique_constraints": ["id"],
        "write_mode": "upsert",
        "conflict_resolution": {
            "on_conflict": "id",
            "action": "update",
            "update_columns": ["name", "value", "updated_at"]
        },
        "configure": {
            "auto_create_schema": True,
            "auto_create_table": True,
            "auto_create_indexes": [
                {
                    "name": "idx_streaming_test_updated_at",
                    "columns": ["updated_at"],
                    "type": "btree"
                }
            ]
        },
        "endpoint_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Streaming Test Data Schema",
            "type": "object",
            "properties": {
                "id": {
                    "type": "integer",
                    "database_type": "BIGINT",
                    "nullable": False,
                    "primary_key": True
                },
                "name": {
                    "type": "string",
                    "database_type": "VARCHAR(255)",
                    "nullable": False
                },
                "value": {
                    "type": "number",
                    "database_type": "DECIMAL(10,2)",
                    "nullable": True
                },
                "created_at": {
                    "type": "string",
                    "format": "date-time",
                    "database_type": "TIMESTAMPTZ",
                    "nullable": False
                },
                "updated_at": {
                    "type": "string", 
                    "format": "date-time",
                    "database_type": "TIMESTAMPTZ",
                    "nullable": False
                }
            },
            "required": ["id", "name", "created_at", "updated_at"]
        }
    }


@pytest.fixture
async def database_connector(postgres_config):
    """Database connector with real PostgreSQL connection."""
    connector = DatabaseConnector("TestStreamingConnector")
    await connector.connect(postgres_config)
    yield connector
    await connector.disconnect()


@pytest.fixture
async def prepared_database(database_connector, test_table_config, database_cleanup):
    """Database with test table configured and ready."""
    await database_connector.configure(test_table_config)
    
    # Register table for cleanup
    database_cleanup("analitiq_test", "streaming_test_data")
    
    # Clean up any existing test data
    async with database_connector.driver.connection_pool.acquire() as conn:
        await conn.execute("DELETE FROM analitiq_test.streaming_test_data WHERE 1=1")
        await conn.execute("COMMIT")
    
    yield database_connector


class TestStreamingToPostgreSQL:
    """Test streaming data operations with real PostgreSQL."""

    @pytest.mark.asyncio
    async def test_single_batch_write(self, prepared_database, test_table_config):
        """Test writing a single batch of data to PostgreSQL."""
        connector = prepared_database
        
        # Sample data batch
        test_data = [
            {
                "id": 1,
                "name": "Test Record 1",
                "value": 100.50,
                "created_at": "2025-01-15T10:00:00Z",
                "updated_at": "2025-01-15T10:00:00Z"
            },
            {
                "id": 2,
                "name": "Test Record 2",
                "value": 250.75,
                "created_at": "2025-01-15T10:01:00Z",
                "updated_at": "2025-01-15T10:01:00Z"
            }
        ]
        
        # Write batch
        await connector.write_batch(test_data, test_table_config)
        
        # Verify data was written
        async with connector.driver.connection_pool.acquire() as conn:
            result = await conn.fetch("SELECT * FROM analitiq_test.streaming_test_data ORDER BY id")
            assert len(result) == 2
            assert result[0]['id'] == 1
            assert result[0]['name'] == "Test Record 1"
            assert result[1]['id'] == 2
            assert result[1]['name'] == "Test Record 2"

    @pytest.mark.asyncio
    async def test_multiple_batch_streaming(self, prepared_database, test_table_config):
        """Test streaming multiple batches of data."""
        connector = prepared_database
        
        # Create multiple batches
        batches = [
            [
                {"id": i*10 + j, "name": f"Batch {i} Record {j}", "value": (i*10 + j) * 1.5,
                 "created_at": "2025-01-15T10:00:00Z", "updated_at": "2025-01-15T10:00:00Z"}
                for j in range(1, 4)
            ]
            for i in range(1, 4)
        ]
        
        # Stream batches
        for batch in batches:
            await connector.write_batch(batch, test_table_config)
        
        # Verify all data was written
        async with connector.driver.connection_pool.acquire() as conn:
            result = await conn.fetch("SELECT COUNT(*) as count FROM analitiq_test.streaming_test_data")
            assert result[0]['count'] == 9  # 3 batches × 3 records each

    @pytest.mark.asyncio
    async def test_upsert_behavior(self, prepared_database, test_table_config):
        """Test upsert behavior - insert then update existing records."""
        connector = prepared_database
        
        # Initial data
        initial_data = [
            {
                "id": 100,
                "name": "Original Name",
                "value": 100.00,
                "created_at": "2025-01-15T10:00:00Z",
                "updated_at": "2025-01-15T10:00:00Z"
            }
        ]
        
        # Write initial data
        await connector.write_batch(initial_data, test_table_config)
        
        # Update data with same ID
        updated_data = [
            {
                "id": 100,
                "name": "Updated Name",
                "value": 200.00,
                "created_at": "2025-01-15T10:00:00Z",  # Should not change (not in update_columns)
                "updated_at": "2025-01-15T10:05:00Z"   # Should change (in update_columns)
            }
        ]
        
        # Write updated data (upsert)
        await connector.write_batch(updated_data, test_table_config)
        
        # Verify upsert behavior
        async with connector.driver.connection_pool.acquire() as conn:
            result = await conn.fetch("SELECT * FROM analitiq_test.streaming_test_data WHERE id = 100")
            assert len(result) == 1
            record = result[0]
            assert record['name'] == "Updated Name"  # Should be updated
            assert record['value'] == 200.00        # Should be updated
            assert record['created_at'].isoformat().startswith("2025-01-15T10:00:00")  # Should not change
            assert record['updated_at'].isoformat().startswith("2025-01-15T10:05:00")  # Should change

    @pytest.mark.asyncio
    async def test_incremental_read_streaming(self, prepared_database, test_table_config):
        """Test reading data incrementally for streaming."""
        connector = prepared_database
        
        # Insert test data with different timestamps
        test_data = []
        for i in range(1, 11):  # 10 records
            test_data.append({
                "id": i,
                "name": f"Record {i}",
                "value": i * 10.0,
                "created_at": f"2025-01-15T10:{i:02d}:00Z",
                "updated_at": f"2025-01-15T10:{i:02d}:00Z"
            })
        
        await connector.write_batch(test_data, test_table_config)
        
        # Configure for incremental read
        read_config = {
            **test_table_config,
            "incremental_column": "updated_at"
        }
        
        # Read in batches of 3
        batches_read = []
        batch_count = 0
        async for batch in connector.read_batches(read_config, batch_size=3):
            batches_read.append(batch)
            batch_count += 1
            if batch_count >= 5:  # Safety limit
                break
        
        # Verify we got data in batches
        assert len(batches_read) > 0
        total_records = sum(len(batch) for batch in batches_read)
        assert total_records <= 10  # Should not exceed our test data

    @pytest.mark.asyncio
    async def test_error_handling_during_streaming(self, database_connector, test_table_config):
        """Test error handling during streaming operations."""
        connector = database_connector
        
        # Try to write to non-existent table (no auto-create)
        config_no_auto = {**test_table_config}
        config_no_auto.pop("configure", None)  # Remove auto-create config
        config_no_auto["table"] = "non_existent_table"
        
        invalid_data = [{"id": 1, "name": "Test"}]
        
        # Should handle the error gracefully
        with pytest.raises(Exception):  # Expect some form of error
            await connector.write_batch(invalid_data, config_no_auto)

    @pytest.mark.asyncio 
    async def test_large_batch_streaming(self, prepared_database, test_table_config):
        """Test streaming larger batches of data."""
        connector = prepared_database
        
        # Create a larger batch (1000 records)
        large_batch = []
        for i in range(1, 1001):
            large_batch.append({
                "id": i,
                "name": f"Large Batch Record {i}",
                "value": i * 1.5,
                "created_at": "2025-01-15T10:00:00Z",
                "updated_at": "2025-01-15T10:00:00Z"
            })
        
        # Write large batch
        await connector.write_batch(large_batch, test_table_config)
        
        # Verify count
        async with connector.driver.connection_pool.acquire() as conn:
            result = await conn.fetch("SELECT COUNT(*) as count FROM analitiq_test.streaming_test_data")
            assert result[0]['count'] == 1000

    @pytest.mark.asyncio
    async def test_concurrent_streaming_operations(self, prepared_database, test_table_config):
        """Test concurrent streaming operations."""
        connector = prepared_database
        
        async def write_batch_async(start_id: int, count: int):
            batch = []
            for i in range(start_id, start_id + count):
                batch.append({
                    "id": i,
                    "name": f"Concurrent Record {i}",
                    "value": i * 2.0,
                    "created_at": "2025-01-15T10:00:00Z",
                    "updated_at": "2025-01-15T10:00:00Z"
                })
            await connector.write_batch(batch, test_table_config)
        
        # Run multiple concurrent writes
        tasks = [
            write_batch_async(1, 10),    # Records 1-10
            write_batch_async(11, 10),   # Records 11-20
            write_batch_async(21, 10),   # Records 21-30
        ]
        
        await asyncio.gather(*tasks)
        
        # Verify all records were written
        async with connector.driver.connection_pool.acquire() as conn:
            result = await conn.fetch("SELECT COUNT(*) as count FROM analitiq_test.streaming_test_data")
            assert result[0]['count'] == 30


class TestDatabaseMetrics:
    """Test database connector metrics during streaming."""
    
    @pytest.mark.asyncio
    async def test_metrics_tracking(self, prepared_database, test_table_config):
        """Test that metrics are properly tracked during operations."""
        connector = prepared_database
        
        # Reset metrics
        connector.metrics = {"records_written": 0, "batches_written": 0, "records_read": 0, "batches_read": 0, "errors": 0}
        
        # Write some data
        test_data = [
            {"id": 1, "name": "Metrics Test", "value": 100.0, 
             "created_at": "2025-01-15T10:00:00Z", "updated_at": "2025-01-15T10:00:00Z"}
        ]
        
        await connector.write_batch(test_data, test_table_config)
        
        # Check metrics
        assert connector.metrics["records_written"] == 1
        assert connector.metrics["batches_written"] == 1
        assert connector.metrics["errors"] == 0