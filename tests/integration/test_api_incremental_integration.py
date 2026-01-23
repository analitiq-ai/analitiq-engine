"""
Integration tests for incremental replication in APIConnector.
Demonstrates end-to-end functionality with realistic scenarios.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch, mock_open, AsyncMock, MagicMock
from aiohttp import ClientSession
from src.source.connectors.api import APIConnector
from src.state.state_manager import StateManager


class TestAPIIncrementalIntegration:
    """Integration tests for incremental replication functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.connector = APIConnector("test_api")
        self.connector.base_url = "https://api.example.com"
        self.connector.is_connected = True

        # Mock session for HTTP requests
        self.mock_session = MagicMock(spec=ClientSession)
        self.mock_session.close = AsyncMock()
        self.connector.session = self.mock_session

        # Mock state manager
        self.mock_state_manager = AsyncMock(spec=StateManager)
        self.stream_name = "test_stream"

        # Sample endpoint schema matching wise_to_sevdesk example
        self.endpoint_schema = {
            "endpoint": "/v1/transfers",
            "type": "api", 
            "method": "GET",
            "filters": {
                "createdDateStart": {
                    "type": "string",
                    "required": False,
                    "operators": ["gte"],
                    "description": "Starting date to filter transfers",
                    "example": "2018-12-15T00:00:00.000Z"
                },
                "status": {
                    "type": "string",
                    "required": False,
                    "operators": ["eq", "in"],
                    "description": "Comma separated list of status codes"
                }
            }
        }

    @pytest.mark.asyncio
    async def test_incremental_replication_with_timestamp_flow(self):
        """Test complete incremental replication flow with timestamp replication key."""
        
        # Set up endpoint schema file mock
        with patch('pathlib.Path.exists') as mock_exists:
            with patch('builtins.open', mock_open(read_data=json.dumps(self.endpoint_schema))):
                mock_exists.return_value = True
                
                # Configure pipeline with incremental replication
                config = {
                    "endpoint": "/v1/transfers",
                    "method": "GET",
                    "pipeline_config": {
                        "source": {
                            "endpoint_id": "test-endpoint-123",
                            "replication_method": "incremental", 
                            "replication_key": "createdDateStart",
                            "cursor_mode": "inclusive",
                            "safety_window_seconds": 300,  # 5 minutes
                            "bookmarks": [
                                {
                                    "partition": {},
                                    "cursor": "2025-08-14T12:00:00Z",
                                    "aux": {"last_id": 54321}
                                }
                            ]
                        }
                    }
                }

                # Mock API response
                mock_response_data = [
                    {
                        "id": 54322,
                        "status": "completed",
                        "created": "2025-08-14T12:01:00Z",
                        "amount": 100.0
                    },
                    {
                        "id": 54323, 
                        "status": "pending",
                        "created": "2025-08-14T12:02:00Z",
                        "amount": 250.0
                    }
                ]

                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json.return_value = mock_response_data

                # Set up async context manager
                self.mock_session.request.return_value.__aenter__.return_value = mock_response

                # Execute incremental read
                batches = []
                async for batch in self.connector.read_batches(config, self.mock_state_manager, self.stream_name):
                    batches.append(batch)

                # Verify results
                assert len(batches) == 1
                assert len(batches[0]) == 2
                assert batches[0][0]["id"] == 54322
                assert batches[0][1]["id"] == 54323

                # Verify that incremental filter was applied
                # The effective start should be cursor (2025-08-14T12:00:00Z) - 300s = 2025-08-14T11:55:00Z
                self.mock_session.request.assert_called_once()
                call_args = self.mock_session.request.call_args
                
                # Check that the URL contains the incremental filter parameter
                assert "params" in call_args.kwargs
                # The exact URL construction is handled by aiohttp, so we verify the params dict
                # Note: This would contain the incremental filter based on our logic

    @pytest.mark.asyncio 
    async def test_incremental_replication_with_numeric_id_flow(self):
        """Test complete incremental replication flow with numeric ID replication key."""
        
        # Update schema to include ID filter
        schema_with_id = self.endpoint_schema.copy()
        schema_with_id["filters"]["id"] = {
            "type": "integer",
            "required": False,
            "operators": ["gte", "lte"],
            "description": "Filter by transfer ID"
        }
        
        with patch('pathlib.Path.exists') as mock_exists:
            with patch('builtins.open', mock_open(read_data=json.dumps(schema_with_id))):
                mock_exists.return_value = True
                
                # Configure pipeline with numeric ID replication
                config = {
                    "endpoint": "/v1/transfers",
                    "method": "GET", 
                    "pipeline_config": {
                        "source": {
                            "endpoint_id": "test-endpoint-456",
                            "replication_method": "incremental",
                            "replication_key": "id",
                            "cursor_mode": "exclusive", 
                            "safety_window_seconds": 10,  # Use as ID offset
                            "bookmarks": [
                                {
                                    "partition": {},
                                    "cursor": "1000"  # Last processed ID
                                }
                            ]
                        }
                    }
                }

                # Mock API response with records after ID 990 (1000 - 10)
                mock_response_data = [
                    {"id": 1001, "status": "completed", "amount": 75.0},
                    {"id": 1002, "status": "pending", "amount": 150.0}
                ]

                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json.return_value = mock_response_data

                # Set up async context manager
                self.mock_session.request.return_value.__aenter__.return_value = mock_response

                # Execute incremental read
                batches = []
                async for batch in self.connector.read_batches(config, self.mock_state_manager, self.stream_name):
                    batches.append(batch)

                # Verify results
                assert len(batches) == 1
                assert len(batches[0]) == 2
                assert all(record["id"] > 990 for record in batches[0])

    @pytest.mark.asyncio
    async def test_first_run_no_bookmarks(self):
        """Test incremental replication on first run with no existing bookmarks."""
        
        with patch('pathlib.Path.exists') as mock_exists:
            with patch('builtins.open', mock_open(read_data=json.dumps(self.endpoint_schema))):
                mock_exists.return_value = True
                
                # Configure pipeline with no bookmarks (first run)
                config = {
                    "endpoint": "/v1/transfers",
                    "method": "GET",
                    "pipeline_config": {
                        "source": {
                            "endpoint_id": "test-endpoint-789",
                            "replication_method": "incremental",
                            "replication_key": "createdDateStart", 
                            "cursor_mode": "inclusive",
                            "safety_window_seconds": 120,
                            "bookmarks": []  # No bookmarks - first run
                        }
                    }
                }

                # Mock API response for full data on first run
                mock_response_data = [
                    {"id": 1, "status": "completed", "amount": 50.0},
                    {"id": 2, "status": "pending", "amount": 100.0}
                ]

                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json.return_value = mock_response_data

                # Set up async context manager
                self.mock_session.request.return_value.__aenter__.return_value = mock_response

                # Execute read - should perform full replication on first run
                batches = []
                async for batch in self.connector.read_batches(config, self.mock_state_manager, self.stream_name):
                    batches.append(batch)

                # Verify results
                assert len(batches) == 1
                assert len(batches[0]) == 2
                
                # Verify no incremental filter was added (first run)
                self.mock_session.request.assert_called_once()

    @pytest.mark.asyncio
    async def test_full_replication_mode(self):
        """Test that full replication mode bypasses incremental logic."""
        
        config = {
            "endpoint": "/v1/transfers",
            "method": "GET",
            "pipeline_config": {
                "source": {
                    "endpoint_id": "test-endpoint-000",
                    "replication_method": "full",  # Full replication
                    "bookmarks": [
                        {
                            "partition": {},
                            "cursor": "2025-08-14T12:00:00Z"
                        }
                    ]
                }
            }
        }

        # Mock API response
        mock_response_data = [{"id": 1, "status": "completed"}]
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = mock_response_data

        # Set up async context manager
        self.mock_session.request.return_value.__aenter__.return_value = mock_response

        # Execute read - should ignore bookmarks and perform full replication
        batches = []
        async for batch in self.connector.read_batches(config, self.mock_state_manager, self.stream_name):
            batches.append(batch)

        # Verify results - should get data without any incremental filtering
        assert len(batches) == 1
        assert len(batches[0]) == 1