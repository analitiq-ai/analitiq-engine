"""
Integration test to reproduce the duplicate record problem.
This test simulates running the same pipeline twice and verifies that
records with the same cursor and tie-breaker are not duplicated.
"""

import asyncio
import json
import logging
import pytest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

from src.connectors.api import APIConnector
from src.fault_tolerance.state_manager import StateManager


class TestDuplicateRecordsIntegration:
    """Integration test for duplicate record prevention."""

    @pytest.fixture
    def temp_state_dir(self):
        """Create a temporary directory for state management."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def sample_pipeline_config(self):
        """Sample pipeline configuration with tie-breaker fields."""
        return {
            "pipeline_id": "test-pipeline",
            "name": "Test Pipeline",
            "version": "1.0",
            "streams": {
                "test-stream-1": {
                    "name": "test-stream",
                    "description": "Test stream for duplicate detection",
                    "source": {
                        "endpoint_id": "test-endpoint",
                        "replication_method": "incremental",
                        "cursor_field": "created",
                        "cursor_mode": "inclusive",
                        "safety_window_seconds": 120,
                        "primary_key": ["id"],
                        "tie_breaker_fields": ["id"]
                    },
                    "destination": {
                        "endpoint_id": "test-dest-endpoint",
                        "refresh_mode": "upsert",
                        "batch_support": False,
                        "batch_size": 1
                    },
                    "mapping": {
                        "field_mappings": {
                            "created": {"target": "valueDate"},
                            "targetValue": {"target": "amount"},
                            "details.reference": {"target": "paymtPurpose"}
                        },
                        "computed_fields": {
                            "objectName": {"expression": "TestTransaction"},
                            "status": {"expression": "100"}
                        }
                    }
                }
            }
        }

    @pytest.fixture  
    def sample_source_config(self):
        """Sample source configuration."""
        return {
            "endpoint": "/api/test",
            "type": "api",
            "method": "GET",
            "host": "https://api.test.com",
            "headers": {"Authorization": "Bearer test-token"},
            "pagination": {
                "type": "offset",
                "params": {
                    "limit_param": "limit",
                    "offset_param": "offset"
                }
            },
            "replication_filter_mapping": {
                "created": "createdDateStart"
            },
            "filters": {
                "createdDateStart": {
                    "type": "string",
                    "required": False,
                    "operators": ["gte"]
                }
            }
        }

    @pytest.fixture
    def sample_destination_config(self):
        """Sample destination configuration."""
        return {
            "endpoint": "/api/dest",
            "type": "api", 
            "method": "POST",
            "host": "https://api.dest.com",
            "headers": {"Authorization": "Bearer dest-token"}
        }

    @pytest.fixture
    def sample_api_response(self):
        """Sample API response data - same record each time to test deduplication."""
        return [
            {
                "id": 12345,
                "created": "2025-08-11T15:58:36Z",
                "targetValue": 100.50,
                "details": {
                    "reference": "TXN-TEST-001"
                },
                "status": "completed"
            }
        ]

    @pytest.fixture
    def unsorted_api_response(self):
        """API response with records in unsorted order to test sorting requirements."""
        return [
            # Records intentionally out of chronological order
            {
                "id": 12347,
                "created": "2025-08-11T16:00:00Z",  # Newest timestamp
                "targetValue": 75.00,
                "details": {"reference": "TXN-TEST-003"},
                "status": "completed"
            },
            {
                "id": 12345,
                "created": "2025-08-11T15:58:36Z",  # Oldest timestamp
                "targetValue": 100.50,
                "details": {"reference": "TXN-TEST-001"},
                "status": "completed"
            },
            {
                "id": 12346,
                "created": "2025-08-11T15:59:30Z",  # Middle timestamp
                "targetValue": 50.25,
                "details": {"reference": "TXN-TEST-002"},
                "status": "completed"
            }
        ]

    @pytest.fixture
    def safety_window_duplicates_response(self):
        """API response that includes records within safety window that should be deduplicated."""
        # This simulates what happens when safety window causes the same record to be fetched again
        return [
            # This is the "new" record from a subsequent API call
            {
                "id": 12348,
                "created": "2025-08-11T16:01:00Z",
                "targetValue": 200.00,
                "details": {"reference": "TXN-TEST-004"},
                "status": "completed"
            },
            # These are duplicates from safety window overlap - should be filtered out
            {
                "id": 12345,  # Same ID as stored in state
                "created": "2025-08-11T15:58:36Z",  # Same timestamp as stored cursor
                "targetValue": 100.50,
                "details": {"reference": "TXN-TEST-001"},
                "status": "completed"
            },
            {
                "id": 12346,
                "created": "2025-08-11T15:59:30Z",  # Within safety window but different ID
                "targetValue": 50.25,
                "details": {"reference": "TXN-TEST-002"},
                "status": "completed"
            }
        ]

    @pytest.fixture
    def same_timestamp_different_ids_response(self):
        """API response with multiple records having same timestamp but different IDs."""
        return [
            {
                "id": 12345,
                "created": "2025-08-11T15:58:36Z",  # Same timestamp
                "targetValue": 100.50,
                "details": {"reference": "TXN-TEST-001"},
                "status": "completed"
            },
            {
                "id": 12346,
                "created": "2025-08-11T15:58:36Z",  # Same timestamp, different ID
                "targetValue": 75.00,
                "details": {"reference": "TXN-TEST-002"},
                "status": "completed"
            },
            {
                "id": 12347,
                "created": "2025-08-11T15:58:36Z",  # Same timestamp, highest ID
                "targetValue": 50.00,
                "details": {"reference": "TXN-TEST-003"},
                "status": "completed"
            }
        ]

    @pytest.mark.asyncio
    async def test_api_connector_deduplication_with_state_manager(
        self,
        temp_state_dir,
        sample_api_response
    ):
        """Test that API connector correctly deduplicates records using state manager."""
        
        # Configuration that matches the wise_to_sevdesk example
        config = {
            "endpoint": "/api/test",
            "method": "GET",
            "host": "https://api.test.com",
            "replication_method": "incremental",
            "cursor_field": "created", 
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "tie_breaker_fields": ["id"],
            "replication_filter_mapping": {
                "created": "createdDateStart"
            },
            "filters": {
                "createdDateStart": {
                    "type": "string",
                    "required": False,
                    "operators": ["gte"],
                    "description": "Starting date filter"
                }
            },
            "pagination": {
                "type": "offset",
                "params": {
                    "limit_param": "limit",
                    "offset_param": "offset"
                }
            }
        }

        # Create API connector
        connector = APIConnector("test-api")
        connector.base_url = "https://api.test.com"
        
        # Connect the API connector to initialize session  
        await connector.connect({
            "host": "https://api.test.com",
            "headers": {"Authorization": "Bearer test-token"},
            "timeout": 30
        })
        
        # Create state manager
        state_manager = StateManager("test-pipeline", str(temp_state_dir))
        
        # Mock the HTTP requests to return the same record each time
        with patch('aiohttp.ClientSession.request') as mock_request:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=sample_api_response)
            mock_request.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_request.return_value.__aexit__ = AsyncMock(return_value=False)

            # First read - should get 1 record
            batches_first_run = []
            async for batch in connector.read_batches(config, state_manager, "test-stream-1", {}, 100):
                batches_first_run.extend(batch)
            
            assert len(batches_first_run) == 1, f"First run should yield 1 record, got {len(batches_first_run)}"
            
            # Second read - should get 0 records (deduplication should prevent it) 
            batches_second_run = []
            async for batch in connector.read_batches(config, state_manager, "test-stream-1", {}, 100):
                batches_second_run.extend(batch)
                
            assert len(batches_second_run) == 0, f"Second run should yield 0 records due to deduplication, got {len(batches_second_run)}"

    @pytest.mark.asyncio
    async def test_tie_breaker_state_is_persisted(
        self,
        temp_state_dir,
        sample_api_response
    ):
        """Test that tie-breaker information is correctly saved to state."""
        
        # Configuration that matches the wise_to_sevdesk example
        config = {
            "endpoint": "/api/test",
            "method": "GET", 
            "host": "https://api.test.com",
            "replication_method": "incremental",
            "cursor_field": "created",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "tie_breaker_fields": ["id"],
            "replication_filter_mapping": {
                "created": "createdDateStart"
            },
            "filters": {
                "createdDateStart": {
                    "type": "string",
                    "required": False,
                    "operators": ["gte"],
                    "description": "Starting date filter"
                }
            },
            "pagination": {
                "type": "offset",
                "params": {
                    "limit_param": "limit",
                    "offset_param": "offset"
                }
            }
        }

        # Create API connector
        connector = APIConnector("test-api")
        connector.base_url = "https://api.test.com"
        
        # Connect the API connector to initialize session
        await connector.connect({
            "host": "https://api.test.com",
            "headers": {"Authorization": "Bearer test-token"},
            "timeout": 30
        })
        
        # Create state manager
        state_manager = StateManager("test-pipeline", str(temp_state_dir))
        
        # Mock the HTTP requests to return the sample record
        with patch('aiohttp.ClientSession.request') as mock_request:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=sample_api_response)
            mock_request.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_request.return_value.__aexit__ = AsyncMock(return_value=False)

            # Process one batch to trigger state saving
            async for batch in connector.read_batches(config, state_manager, "test-stream-1", {}, 100):
                pass  # Just process the batch to trigger state saving
                
            # Check that state was saved with tie-breaker information
            partition_files = list(temp_state_dir.rglob("partition-*.json"))
            assert len(partition_files) > 0, "No partition state files found"
            
            # Read the partition state
            with open(partition_files[0], 'r') as f:
                partition_state = json.load(f)
            
            # Verify cursor information was saved
            cursor = partition_state.get("cursor", {})
            assert cursor is not None, "Cursor information missing from state"
            
            # Check for tie-breaker information
            tiebreakers = cursor.get("tiebreakers")
            assert tiebreakers is not None, "Tiebreakers should not be None"
            assert len(tiebreakers) > 0, "Tiebreakers list should not be empty"
            
            id_tiebreaker = next((tb for tb in tiebreakers if tb.get("field") == "id"), None)
            assert id_tiebreaker is not None, "Should have tie-breaker for 'id' field"
            assert id_tiebreaker.get("value") == 12345, f"Tie-breaker value should be 12345, got {id_tiebreaker.get('value')}"

    @pytest.mark.asyncio 
    async def test_state_loading_includes_tiebreaker_info(
        self,
        temp_state_dir,
        sample_pipeline_config,
        sample_source_config,
        sample_destination_config
    ):
        """Test that state loading correctly includes tie-breaker information for deduplication."""
        
        # Create a state file with tie-breaker information
        state_manager = StateManager("test-pipeline", str(temp_state_dir))
        
        # Simulate a previously saved state with tie-breaker
        from src.models.state import PartitionCursor, CursorField, PartitionStats
        from datetime import datetime, timezone
        
        cursor = PartitionCursor(
            primary=CursorField(field="created", value="2025-08-11T15:58:36Z", inclusive=True),
            tiebreakers=[CursorField(field="id", value=12345, inclusive=True)]
        )
        
        stats = PartitionStats(
            records_synced=1,
            batches_written=1,
            last_checkpoint_at=datetime.now(timezone.utc),
            errors_since_checkpoint=0
        )
        
        # Save initial state
        state_manager.save_stream_checkpoint(
            stream_name="test-stream-1",
            partition={},
            cursor=cursor.model_dump(mode='json'),
            hwm="2025-08-11T15:58:36Z",
            stats=stats.model_dump(mode='json')
        )
        
        # Now test that the API connector loads this state correctly
        from src.connectors.api import APIConnector
        
        connector = APIConnector("test")
        
        # Test state loading - this should include tie-breaker info
        config = sample_source_config.copy()
        config.update({
            "replication_method": "incremental", 
            "cursor_field": "created",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "tie_breaker_fields": ["id"]
        })
        
        loaded_state = connector._load_state_from_state_manager(
            state_manager, "test-stream-1", {}, config
        )
        
        # Verify that tie-breaker information is available in loaded state
        bookmarks = loaded_state.get("bookmarks", [])
        assert len(bookmarks) > 0, "Should have loaded bookmark from state"
        
        bookmark = bookmarks[0]
        aux_data = bookmark.get("aux", {})
        
        # Check that tie-breaker information is present
        tiebreakers = aux_data.get("tiebreakers")
        if tiebreakers:
            assert len(tiebreakers) > 0, "Should have loaded tie-breaker information"
            id_tiebreaker = next((tb for tb in tiebreakers if tb.get("field") == "id"), None)
            assert id_tiebreaker is not None, "Should have tie-breaker for 'id' field"
            assert id_tiebreaker.get("value") == 12345, f"Should have loaded tie-breaker value 12345, got {id_tiebreaker.get('value')}"
        else:
            pytest.fail("Tie-breaker information was not loaded from state - this causes duplicate records!")

    @pytest.mark.asyncio
    async def test_unsorted_api_response_handling(
        self,
        temp_state_dir,
        unsorted_api_response
    ):
        """Test that API connector handles unsorted responses correctly with tie-breakers."""
        
        config = {
            "endpoint": "/api/test",
            "method": "GET",
            "host": "https://api.test.com",
            "replication_method": "incremental",
            "cursor_field": "created",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "tie_breaker_fields": ["id"],
            "replication_filter_mapping": {"created": "createdDateStart"},
            "filters": {"createdDateStart": {"type": "string", "required": False, "operators": ["gte"], "description": "Starting date filter"}},
            "pagination": {"type": "offset", "params": {"limit_param": "limit", "offset_param": "offset"}}
        }

        connector = APIConnector("test-api")
        await connector.connect({
            "host": "https://api.test.com",
            "headers": {"Authorization": "Bearer test-token"},
            "timeout": 30
        })
        
        state_manager = StateManager("test-pipeline", str(temp_state_dir))
        
        with patch('aiohttp.ClientSession.request') as mock_request:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=unsorted_api_response)
            mock_request.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_request.return_value.__aexit__ = AsyncMock(return_value=False)

            # First run with unsorted data - should process all records
            batches_first_run = []
            async for batch in connector.read_batches(config, state_manager, "test-stream-1", {}, 100):
                batches_first_run.extend(batch)
            
            assert len(batches_first_run) == 3, f"First run should yield 3 records from unsorted response, got {len(batches_first_run)}"
            
            # Verify records are processed (even if unsorted)
            record_ids = [record["id"] for record in batches_first_run]
            assert set(record_ids) == {12345, 12346, 12347}, f"Should have all three record IDs, got {record_ids}"
            
        await connector.disconnect()

    @pytest.mark.asyncio
    async def test_safety_window_duplicate_filtering(
        self,
        temp_state_dir,
        safety_window_duplicates_response
    ):
        """Test that records within safety window are correctly deduplicated."""
        
        config = {
            "endpoint": "/api/test",
            "method": "GET",
            "host": "https://api.test.com",
            "replication_method": "incremental",
            "cursor_field": "created",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "tie_breaker_fields": ["id"],
            "replication_filter_mapping": {"created": "createdDateStart"},
            "filters": {"createdDateStart": {"type": "string", "required": False, "operators": ["gte"], "description": "Starting date filter"}},
            "pagination": {"type": "offset", "params": {"limit_param": "limit", "offset_param": "offset"}}
        }

        connector = APIConnector("test-api")
        await connector.connect({
            "host": "https://api.test.com",
            "headers": {"Authorization": "Bearer test-token"},
            "timeout": 30
        })
        
        state_manager = StateManager("test-pipeline", str(temp_state_dir))
        
        # First, simulate having processed some records previously
        from src.models.state import PartitionCursor, CursorField, PartitionStats
        from datetime import datetime, timezone
        
        # Set up state as if we've already processed records up to 12346
        cursor = PartitionCursor(
            primary=CursorField(field="created", value="2025-08-11T15:59:30Z", inclusive=True),
            tiebreakers=[CursorField(field="id", value=12346, inclusive=True)]
        )
        
        stats = PartitionStats(
            records_synced=2,
            batches_written=1,
            last_checkpoint_at=datetime.now(timezone.utc),
            errors_since_checkpoint=0
        )
        
        state_manager.save_stream_checkpoint(
            stream_name="test-stream-1",
            partition={},
            cursor=cursor.model_dump(mode='json'),
            hwm="2025-08-11T15:59:30Z",
            stats=stats.model_dump(mode='json')
        )
        
        with patch('aiohttp.ClientSession.request') as mock_request:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=safety_window_duplicates_response)
            mock_request.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_request.return_value.__aexit__ = AsyncMock(return_value=False)

            # Run with safety window duplicates - should only get new records
            batches = []
            async for batch in connector.read_batches(config, state_manager, "test-stream-1", {}, 100):
                batches.extend(batch)
            
            # Should only get record 12348 (the truly new one)
            # Records 12345 and 12346 should be filtered out as duplicates
            assert len(batches) == 1, f"Should only yield 1 new record, got {len(batches)}"
            assert batches[0]["id"] == 12348, f"Should get record 12348, got {batches[0]['id']}"
            assert batches[0]["created"] == "2025-08-11T16:01:00Z", "Should get the newest record"
            
        await connector.disconnect()

    @pytest.mark.asyncio
    async def test_same_timestamp_tie_breaker_ordering(
        self,
        temp_state_dir,
        same_timestamp_different_ids_response
    ):
        """Test that tie-breaker fields work correctly when records have same timestamp."""
        
        config = {
            "endpoint": "/api/test",
            "method": "GET",
            "host": "https://api.test.com",
            "replication_method": "incremental",
            "cursor_field": "created",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 120,
            "tie_breaker_fields": ["id"],
            "replication_filter_mapping": {"created": "createdDateStart"},
            "filters": {"createdDateStart": {"type": "string", "required": False, "operators": ["gte"], "description": "Starting date filter"}},
            "pagination": {"type": "offset", "params": {"limit_param": "limit", "offset_param": "offset"}}
        }

        connector = APIConnector("test-api")
        await connector.connect({
            "host": "https://api.test.com",
            "headers": {"Authorization": "Bearer test-token"},
            "timeout": 30
        })
        
        state_manager = StateManager("test-pipeline", str(temp_state_dir))
        
        # First run - process all records with same timestamp
        with patch('aiohttp.ClientSession.request') as mock_request:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=same_timestamp_different_ids_response)
            mock_request.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_request.return_value.__aexit__ = AsyncMock(return_value=False)

            batches_first_run = []
            async for batch in connector.read_batches(config, state_manager, "test-stream-1", {}, 100):
                batches_first_run.extend(batch)
            
            assert len(batches_first_run) == 3, f"First run should yield 3 records, got {len(batches_first_run)}"
            
            # Second run - should get no records (all should be deduplicated)
            batches_second_run = []
            async for batch in connector.read_batches(config, state_manager, "test-stream-1", {}, 100):
                batches_second_run.extend(batch)
            
            assert len(batches_second_run) == 0, f"Second run should yield 0 records due to tie-breaker deduplication, got {len(batches_second_run)}"
            
        await connector.disconnect()

    @pytest.mark.asyncio
    async def test_unsorted_response_with_safety_window_overlap(
        self,
        temp_state_dir
    ):
        """Test complex scenario: unsorted response + safety window overlap + tie-breakers."""
        
        # Simulate a complex real-world scenario
        initial_response = [
            {"id": 12345, "created": "2025-08-11T15:58:36Z", "targetValue": 100.50, "details": {"reference": "TXN-001"}, "status": "completed"},
            {"id": 12346, "created": "2025-08-11T15:59:30Z", "targetValue": 50.25, "details": {"reference": "TXN-002"}, "status": "completed"}
        ]
        
        # Second call with safety window overlap + new records, but unsorted
        subsequent_response = [
            {"id": 12349, "created": "2025-08-11T16:02:00Z", "targetValue": 300.00, "details": {"reference": "TXN-005"}, "status": "completed"},  # New
            {"id": 12346, "created": "2025-08-11T15:59:30Z", "targetValue": 50.25, "details": {"reference": "TXN-002"}, "status": "completed"},   # Duplicate
            {"id": 12348, "created": "2025-08-11T16:01:00Z", "targetValue": 200.00, "details": {"reference": "TXN-004"}, "status": "completed"},  # New
            {"id": 12345, "created": "2025-08-11T15:58:36Z", "targetValue": 100.50, "details": {"reference": "TXN-001"}, "status": "completed"}   # Duplicate
        ]
        
        config = {
            "endpoint": "/api/test",
            "method": "GET",
            "host": "https://api.test.com",
            "replication_method": "incremental",
            "cursor_field": "created",
            "cursor_mode": "inclusive",
            "safety_window_seconds": 300,  # Large safety window to ensure overlap
            "tie_breaker_fields": ["id"],
            "replication_filter_mapping": {"created": "createdDateStart"},
            "filters": {"createdDateStart": {"type": "string", "required": False, "operators": ["gte"], "description": "Starting date filter"}},
            "pagination": {"type": "offset", "params": {"limit_param": "limit", "offset_param": "offset"}}
        }

        connector = APIConnector("test-api")
        await connector.connect({
            "host": "https://api.test.com",
            "headers": {"Authorization": "Bearer test-token"},
            "timeout": 30
        })
        
        state_manager = StateManager("test-pipeline", str(temp_state_dir))
        
        with patch('aiohttp.ClientSession.request') as mock_request:
            # First API call
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=initial_response)
            mock_request.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_request.return_value.__aexit__ = AsyncMock(return_value=False)

            batches_first_run = []
            async for batch in connector.read_batches(config, state_manager, "test-stream-1", {}, 100):
                batches_first_run.extend(batch)
            
            assert len(batches_first_run) == 2, f"First run should yield 2 records, got {len(batches_first_run)}"
            
            # Second API call with overlapping records (simulating safety window)
            mock_response.json = AsyncMock(return_value=subsequent_response)
            
            batches_second_run = []
            async for batch in connector.read_batches(config, state_manager, "test-stream-1", {}, 100):
                batches_second_run.extend(batch)
            
            # Should only get the 2 new records (12348, 12349), duplicates should be filtered
            assert len(batches_second_run) == 2, f"Second run should yield 2 new records, got {len(batches_second_run)}"
            
            # Verify we got the right records (new ones only)
            second_run_ids = [record["id"] for record in batches_second_run]
            assert set(second_run_ids) == {12348, 12349}, f"Should get records 12348 and 12349, got {second_run_ids}"
            
        await connector.disconnect()

    @pytest.mark.asyncio
    async def test_null_tiebreaker_scenario_reproduction(
        self,
        temp_state_dir,
        sample_api_response
    ):
        """Test that reproduces the exact scenario where tie-breaker shows as null in state.
        
        This happens when:
        1. First call saves state with tie-breaker info
        2. Second call returns same record (all duplicates)  
        3. State is NOT updated because no new records were processed
        4. Tie-breaker information remains from the first call or gets lost
        """
        
        config = {
            "endpoint": "/api/test",
            "method": "GET",
            "host": "https://api.test.com",
            "replication_method": "incremental",
            "cursor_field": "created",
            "cursor_mode": "inclusive", 
            "safety_window_seconds": 120,
            "tie_breaker_fields": ["id"],
            "replication_filter_mapping": {"created": "createdDateStart"},
            "filters": {"createdDateStart": {"type": "string", "required": False, "operators": ["gte"], "description": "Starting date filter"}},
            "pagination": {"type": "offset", "params": {"limit_param": "limit", "offset_param": "offset"}}
        }

        connector = APIConnector("test-api")
        await connector.connect({
            "host": "https://api.test.com", 
            "headers": {"Authorization": "Bearer test-token"},
            "timeout": 30
        })
        
        state_manager = StateManager("test-pipeline", str(temp_state_dir))
        
        # First API call - should save state with tie-breaker
        with patch('aiohttp.ClientSession.request') as mock_request:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value=sample_api_response)
            mock_request.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_request.return_value.__aexit__ = AsyncMock(return_value=False)

            # First run - should process record and save tie-breaker info
            batches_first = []
            async for batch in connector.read_batches(config, state_manager, "test-stream-1", {}, 100):
                batches_first.extend(batch)
            
            assert len(batches_first) == 1, "First run should process 1 record"
            
            # Check state after first run - should have tie-breaker info
            partition_state = state_manager.get_partition_state("test-stream-1", {})
            cursor = partition_state.get("cursor", {}) if partition_state else {}
            
            # This should NOT be null if everything works correctly
            tiebreakers = cursor.get("tiebreakers")
            
            assert tiebreakers is not None, "First run should save tie-breaker info"
            
            # Second API call - returns exact same record (should be all duplicates)
            batches_second = []
            async for batch in connector.read_batches(config, state_manager, "test-stream-1", {}, 100):
                batches_second.extend(batch)
            
            assert len(batches_second) == 0, "Second run should yield 0 records (all duplicates)"
            
            # BUG REPRODUCTION: Check state after second run
            partition_state_after = state_manager.get_partition_state("test-stream-1", {})
            cursor_after = partition_state_after.get("cursor", {}) if partition_state_after else {}
            
            # The BUG: tie-breaker info should still be present, but might be null
            tiebreakers_after = cursor_after.get("tiebreakers")
            
            # This is the bug scenario - tie-breaker info gets lost
            print(f"DEBUG: cursor_after = {cursor_after}")
            print(f"DEBUG: tiebreakers_after = {tiebreakers_after}")
            
            # The assertion that demonstrates the bug
            # In the buggy version, this will fail because tie-breaker info is lost
            assert tiebreakers_after is not None, \
                "BUG: Tie-breaker information should persist even when no new records are processed!"
            
        await connector.disconnect()

    @pytest.mark.asyncio
    async def test_pipeline_level_tie_breaker_fields_integration(
        self,
        temp_state_dir,
        sample_api_response
    ):
        """Test that Pipeline passes tie_breaker_fields correctly to the StreamingEngine and APIConnector.
        
        This test verifies the complete integration chain:
        1. Pipeline config contains tie_breaker_fields in stream src config
        2. Pipeline passes it to StreamingEngine 
        3. StreamingEngine adds it to source_config
        4. APIConnector receives it and uses it for deduplication
        5. State is saved with proper tie-breaker information
        """
        from src import Pipeline
        
        # Create a full pipeline configuration that matches the wise_to_sevdesk structure
        pipeline_config = {
            "pipeline_id": "test-integration-pipeline",
            "name": "Integration Test Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 100,
                "max_concurrent_batches": 1,
                "buffer_size": 1000
            },
            "streams": {
                "test-stream-001": {
                    "name": "test-integration-stream", 
                    "description": "Test stream for pipeline integration",
                    "source": {
                        "endpoint_id": "test-endpoint",
                        "replication_method": "incremental",
                        "cursor_field": "created",
                        "cursor_mode": "inclusive",
                        "safety_window_seconds": 120,
                        "primary_key": ["id"],
                        "tie_breaker_fields": ["id"]  # This is the key setting being tested
                    },
                    "destination": {
                        "endpoint_id": "test-dest-endpoint",
                        "refresh_mode": "upsert",
                        "batch_support": False,
                        "batch_size": 1
                    },
                    "mapping": {
                        "field_mappings": {
                            "created": {"target": "valueDate"},
                            "targetValue": {"target": "amount"}
                        }
                    }
                }
            }
        }
        
        # Source configuration
        source_config = {
            "endpoint": "/api/test",
            "type": "api",
            "method": "GET", 
            "host": "https://api.test.com",
            "headers": {"Authorization": "Bearer test-token"},
            "replication_filter_mapping": {"created": "createdDateStart"},
            "filters": {"createdDateStart": {"type": "string", "required": False, "operators": ["gte"]}},
            "pagination": {"type": "offset", "params": {"limit_param": "limit", "offset_param": "offset"}}
        }
        
        # Destination configuration
        destination_config = {
            "endpoint": "/api/dest",
            "type": "api",
            "method": "POST",
            "host": "https://api.dest.com", 
            "headers": {"Authorization": "Bearer dest-token"}
        }
        
        # Mock both source and destination API responses
        with patch('aiohttp.ClientSession.request') as mock_request:
            def mock_side_effect(*args, **kwargs):
                mock_response = AsyncMock()
                
                # Check if this is a source or destination request based on URL
                url = args[1] if len(args) > 1 else kwargs.get('url', '')
                method = args[0] if len(args) > 0 else kwargs.get('method', 'GET')
                
                if 'api.test.com' in str(url) and method == 'GET':
                    # Source API request
                    mock_response.status = 200
                    mock_response.json = AsyncMock(return_value=sample_api_response)
                elif 'api.dest.com' in str(url) and method == 'POST':
                    # Destination API request
                    mock_response.status = 200  
                    mock_response.json = AsyncMock(return_value={"id": "12345", "status": "created"})
                else:
                    # Default response
                    mock_response.status = 200
                    mock_response.json = AsyncMock(return_value=[])
                
                mock_response.__aenter__ = AsyncMock(return_value=mock_response)
                mock_response.__aexit__ = AsyncMock(return_value=False)
                return mock_response
            
            mock_request.side_effect = mock_side_effect
            
            # Create and run pipeline 
            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_state_dir)
            )
            
            # Run pipeline twice to test deduplication
            # First run - should process record and save tie-breaker info
            await pipeline.run()
            metrics1 = pipeline.get_metrics()
            assert metrics1.records_processed > 0, "First run should process at least 1 record"
            
            # Check that tie-breaker information was saved
            from src.fault_tolerance.state_manager import StateManager
            state_manager = StateManager("test-integration-pipeline", str(temp_state_dir))
            
            partition_state = state_manager.get_partition_state("stream.test-stream-001", {})
            assert partition_state is not None, "State should be saved after first run"
            
            cursor = partition_state.get("cursor", {})
            tiebreakers = cursor.get("tiebreakers")
            
            # The fix should ensure tie-breaker info is saved
            assert tiebreakers is not None, \
                "Tie-breaker information should be saved after first pipeline run"
                
            print(f"DEBUG: After first run, cursor = {cursor}")
            
            # Second run - should not process duplicate records 
            await pipeline.run()
            metrics2 = pipeline.get_metrics()
            
            # In second run, no new records should be processed (deduplicated)
            # But metrics are cumulative, so we check that no NEW records were added
            assert metrics2.records_processed == metrics1.records_processed, \
                f"Second run should not process additional records due to deduplication. " \
                f"First: {metrics1.records_processed}, Second: {metrics2.records_processed}"
            
            # Verify tie-breaker information persists after second run
            partition_state_after = state_manager.get_partition_state("stream.test-stream-001", {})
            cursor_after = partition_state_after.get("cursor", {}) if partition_state_after else {}
            
            tiebreakers_after = cursor_after.get("tiebreakers")
            
            print(f"DEBUG: After second run, cursor = {cursor_after}")
            
            # Final verification: tie-breaker info should still be present
            assert tiebreakers_after is not None, \
                "Tie-breaker information should persist after second pipeline run"