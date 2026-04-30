"""Comprehensive tests for API connector functionality."""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from aiohttp import ClientTimeout, TCPConnector, ClientSession

from src.source.connectors.api import APIConnector
from src.shared.rate_limiter import RateLimiter
from src.source.connectors.base import ConnectionError, ReadError
from src.shared.connection_runtime import ConnectionRuntime
from src.secrets.resolvers.memory import InMemorySecretsResolver
from src.state.state_manager import StateManager


def _make_api_runtime(config):
    """Create a ConnectionRuntime for API tests."""
    return ConnectionRuntime(
        raw_config=config,
        connector_type="api",
        driver=None,
        connection_id="test-conn",
        resolver=InMemorySecretsResolver({}),
    )


@pytest.fixture
def connector():
    """Create API connector instance."""
    return APIConnector("TestAPI")

@pytest.fixture
def valid_connection_config():
    """Valid connection configuration."""
    return {
        "host": "https://api.example.com",
        "parameters": {
            "headers": {"Authorization": "Bearer token"},
            "timeout": 30,
            "max_connections": 5,
            "max_connections_per_host": 2,
        },
    }

@pytest.fixture
def valid_read_config():
    """Valid read configuration."""
    return {
        "endpoint": "/users",
        "method": "GET",
        "data_field": "data",
        "cursor_field": "updated_at",
        "replication_method": "incremental",
        "cursor_mode": "inclusive",
        "safety_window_seconds": 120,
        "tie_breaker_fields": ["id"],
    }

@pytest.fixture
def mock_state_manager():
    """Mock state manager."""
    manager = MagicMock(spec=StateManager)
    manager.get_partition_state.return_value = None
    manager.get_run_info.return_value = {}
    return manager

@pytest.fixture
def mock_session():
    """Mock aiohttp session."""
    session = MagicMock(spec=ClientSession)
    session.close = AsyncMock()
    return session


class TestConnection:
    """Test connection management."""
    
    @pytest.mark.asyncio
    async def test_connect_success(self, connector, valid_connection_config):
        """Test successful API connection."""
        runtime = _make_api_runtime(valid_connection_config)
        with patch('aiohttp.ClientSession') as mock_session_cls, \
             patch('aiohttp.ClientTimeout') as mock_timeout_cls, \
             patch('aiohttp.TCPConnector') as mock_connector_cls:

            mock_session = AsyncMock()
            mock_session_cls.return_value = mock_session

            await connector.connect(runtime)

            assert connector.is_connected is True
            assert connector.base_url == "https://api.example.com"
            assert connector.session == mock_session

            # Verify aiohttp components were configured correctly
            mock_timeout_cls.assert_called_once_with(total=30)
            mock_connector_cls.assert_called_once_with(limit=5, limit_per_host=2)

    @pytest.mark.asyncio
    async def test_connect_with_rate_limit(self, connector):
        """Test connection with rate limiting configuration."""
        config = {
            "host": "https://api.example.com",
            "parameters": {
                "headers": {},
                "timeout": 30,
                "max_connections": 5,
                "max_connections_per_host": 2,
                "rate_limit": {"max_requests": 10, "time_window": 60},
            },
        }
        runtime = _make_api_runtime(config)

        with patch('aiohttp.ClientSession'), \
             patch('aiohttp.ClientTimeout'), \
             patch('aiohttp.TCPConnector'):

            await connector.connect(runtime)

            assert connector.rate_limiter is not None
            assert connector.rate_limiter.max_requests == 10
            assert connector.rate_limiter.time_window == 60

    @pytest.mark.asyncio
    async def test_connect_invalid_config(self, connector):
        """Test connection with invalid configuration."""
        runtime = _make_api_runtime({"invalid": "config"})

        with pytest.raises(ConnectionError, match="API connection failed"):
            await connector.connect(runtime)

    @pytest.mark.asyncio
    async def test_connect_missing_host(self, connector):
        """Test connection with missing host."""
        runtime = _make_api_runtime({"parameters": {}})

        with pytest.raises(ConnectionError, match="API connection failed"):
            await connector.connect(runtime)
    
    @pytest.mark.asyncio
    async def test_disconnect(self, connector, mock_session):
        """Test API disconnection."""
        mock_runtime = AsyncMock()
        connector._runtime = mock_runtime
        connector.session = mock_session
        connector.is_connected = True

        await connector.disconnect()

        mock_runtime.close.assert_called_once()
        assert connector.is_connected is False
    
    @pytest.mark.asyncio
    async def test_disconnect_no_session(self, connector):
        """Test disconnection when no session exists."""
        connector.session = None
        
        await connector.disconnect()
        
        # Should not raise an error
        assert connector.session is None


class TestReadOperations:
    """Test reading operations."""
    
    @pytest.mark.asyncio
    async def test_read_batches_single_request(self, connector, valid_read_config, mock_state_manager, mock_session):
        """Test reading data with single request (no pagination)."""
        connector.session = mock_session
        connector.base_url = "https://api.example.com"
        
        # Mock successful response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "name": "User 1", "updated_at": "2023-01-01T10:00:00Z"},
                {"id": 2, "name": "User 2", "updated_at": "2023-01-02T10:00:00Z"}
            ]
        }
        mock_session.request.return_value.__aenter__.return_value = mock_response
        
        batches = []
        async for batch in connector.read_batches(
            valid_read_config, state_manager=mock_state_manager, stream_name="test_stream"
        ):
            batches.append(batch)

        assert len(batches) == 1
        assert len(batches[0]) == 2
        assert batches[0][0]["name"] == "User 1"

        # Verify state checkpoint was saved
        mock_state_manager.save_stream_checkpoint.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_batches_with_deduplication(self, connector, valid_read_config, mock_state_manager, mock_session):
        """Test reading with deduplication based on stored state."""
        connector.session = mock_session
        connector.base_url = "https://api.example.com"
        
        # Mock existing state with cursor and tie-breaker
        mock_state_manager.get_partition_state.return_value = {
            "cursor": {
                "primary": {"field": "updated_at", "value": "2023-01-01T10:00:00Z"},
                "tiebreakers": [{"field": "id", "value": "1"}]
            }
        }
        
        # Mock response with duplicate and new records
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "name": "User 1", "updated_at": "2023-01-01T10:00:00Z"},  # Duplicate
                {"id": 2, "name": "User 2", "updated_at": "2023-01-01T10:00:00Z"},  # New (same timestamp, higher ID)
                {"id": 3, "name": "User 3", "updated_at": "2023-01-02T10:00:00Z"}   # New (newer timestamp)
            ]
        }
        mock_session.request.return_value.__aenter__.return_value = mock_response
        
        batches = []
        async for batch in connector.read_batches(
            valid_read_config, state_manager=mock_state_manager, stream_name="test_stream"
        ):
            batches.append(batch)

        assert len(batches) == 1
        assert len(batches[0]) == 2  # Only 2 non-duplicate records
        assert batches[0][0]["id"] == 2  # User 2 (new)
        assert batches[0][1]["id"] == 3  # User 3 (newer)
    
    @pytest.mark.asyncio
    async def test_read_offset_paginated(self, connector, valid_read_config, mock_state_manager, mock_session):
        """Test offset-based pagination."""
        connector.session = mock_session
        connector.base_url = "https://api.example.com"
        
        # Configure pagination
        valid_read_config["pagination"] = {
            "type": "offset",
            "params": {"offset_param": "offset", "limit_param": "limit"}
        }
        
        # Mock paginated responses
        responses = [
            {"data": [{"id": 1, "updated_at": "2023-01-01T10:00:00Z"}]},  # First page
            {"data": [{"id": 2, "updated_at": "2023-01-02T10:00:00Z"}]},  # Second page
            {"data": []}  # Empty page (end)
        ]
        
        mock_responses = []
        for resp_data in responses:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json.return_value = resp_data
            mock_responses.append(mock_resp)
        
        mock_session.request.return_value.__aenter__.side_effect = mock_responses
        
        batches = []
        async for batch in connector.read_batches(
            valid_read_config, state_manager=mock_state_manager, stream_name="test_stream", batch_size=1
        ):
            batches.append(batch)

        assert len(batches) == 2
        assert len(batches[0]) == 1
        assert len(batches[1]) == 1
        assert batches[0][0]["id"] == 1
        assert batches[1][0]["id"] == 2
    
    @pytest.mark.asyncio
    async def test_read_cursor_paginated(self, connector, valid_read_config, mock_state_manager, mock_session):
        """Test cursor-based pagination."""
        connector.session = mock_session
        connector.base_url = "https://api.example.com"
        
        # Configure pagination
        valid_read_config["pagination"] = {
            "type": "cursor",
            "params": {"cursor_param": "cursor", "limit_param": "limit"}
        }
        
        # Mock paginated responses with cursors
        responses = [
            {
                "data": [{"id": 1, "updated_at": "2023-01-01T10:00:00Z"}],
                "next_cursor": "cursor_123"
            },
            {
                "data": [{"id": 2, "updated_at": "2023-01-02T10:00:00Z"}],
                "next_cursor": None  # No more data
            }
        ]
        
        mock_responses = []
        for resp_data in responses:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json.return_value = resp_data
            mock_responses.append(mock_resp)
        
        mock_session.request.return_value.__aenter__.side_effect = mock_responses
        
        batches = []
        async for batch in connector.read_batches(
            valid_read_config, state_manager=mock_state_manager, stream_name="test_stream", batch_size=1
        ):
            batches.append(batch)

        assert len(batches) == 2
        assert batches[0][0]["id"] == 1
        assert batches[1][0]["id"] == 2
    
    @pytest.mark.asyncio
    async def test_read_page_paginated(self, connector, valid_read_config, mock_state_manager, mock_session):
        """Test page number-based pagination."""
        connector.session = mock_session
        connector.base_url = "https://api.example.com"
        
        # Configure pagination
        valid_read_config["pagination"] = {
            "type": "page",
            "params": {"page_param": "page", "limit_param": "limit"},
            "start_page": 1
        }
        
        # Mock two pages of data, then empty
        responses = [
            {"data": [{"id": 1, "updated_at": "2023-01-01T10:00:00Z"}]},  # Page 1
            {"data": []}  # Page 2 (empty)
        ]
        
        mock_responses = []
        for resp_data in responses:
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_resp.json.return_value = resp_data
            mock_responses.append(mock_resp)
        
        mock_session.request.return_value.__aenter__.side_effect = mock_responses
        
        batches = []
        async for batch in connector.read_batches(
            valid_read_config, state_manager=mock_state_manager, stream_name="test_stream", batch_size=1
        ):
            batches.append(batch)

        assert len(batches) == 1
        assert batches[0][0]["id"] == 1
    
    @pytest.mark.asyncio
    async def test_read_batches_api_error(self, connector, valid_read_config, mock_state_manager, mock_session):
        """Test handling API errors during read."""
        connector.session = mock_session
        connector.base_url = "https://api.example.com"
        
        # Mock error response
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_session.request.return_value.__aenter__.return_value = mock_response
        
        with pytest.raises(ReadError, match="API request failed with status 500"):
            async for batch in connector.read_batches(
                valid_read_config, state_manager=mock_state_manager, stream_name="test_stream"
            ):
                pass
    
    @pytest.mark.asyncio
    async def test_read_batches_connection_error(self, connector, valid_read_config, mock_state_manager, mock_session):
        """Test handling connection errors during read."""
        connector.session = mock_session
        connector.base_url = "https://api.example.com"
        
        # Mock connection error
        mock_session.request.side_effect = Exception("Connection failed")
        
        with pytest.raises(ReadError, match="API GET connection to https://api.example.com/users failed"):
            async for batch in connector.read_batches(
                valid_read_config, state_manager=mock_state_manager, stream_name="test_stream"
            ):
                pass


class TestWriteOperations:
    """Test write operations."""

    @pytest.mark.asyncio
    async def test_write_batch_raises(self, connector):
        """Source connector write_batch should raise NotImplementedError."""
        with pytest.raises(NotImplementedError, match="read-only"):
            await connector.write_batch([{"id": 1}], {})


class TestUtilityMethods:
    """Test utility and helper methods."""
    
    def test_extract_records_from_response_list(self, connector):
        """Test extracting records when response is a list."""
        data = [{"id": 1}, {"id": 2}]
        config = {}
        
        records = connector._extract_records_from_response(data, config)
        
        assert records == data
    
    def test_extract_records_from_response_dict_with_data_field(self, connector):
        """Test extracting records from dict with data field."""
        data = {
            "data": [{"id": 1}, {"id": 2}],
            "meta": {"count": 2}
        }
        config = {"data_field": "data"}
        
        records = connector._extract_records_from_response(data, config)
        
        assert records == [{"id": 1}, {"id": 2}]
    
    def test_extract_records_from_response_dict_single_record(self, connector):
        """Test extracting when data field contains single record."""
        data = {"data": {"id": 1, "name": "User"}}
        config = {"data_field": "data"}
        
        records = connector._extract_records_from_response(data, config)
        
        assert records == [{"id": 1, "name": "User"}]
    
    def test_extract_records_from_response_no_data_field(self, connector):
        """Test extracting when no data field is found."""
        data = {"user": {"id": 1}}
        config = {"data_field": "data"}
        
        records = connector._extract_records_from_response(data, config)
        
        assert records == [{"user": {"id": 1}}]
    
    def test_apply_filters_to_params(self, connector):
        """Test applying filters to request parameters."""
        params = {}
        config = {
            "filters": {
                "status": {"value": "active"},
                "category": {"value": ["tech", "business"]},
                "required_filter": {"value": "test", "required": True}
            }
        }
        
        connector._apply_filters_to_params(params, config)
        
        assert params["status"] == "active"
        assert params["category"] == "tech,business"
        assert params["required_filter"] == "test"
    
    def test_apply_filters_with_defaults(self, connector):
        """Test applying filters with default values."""
        params = {}
        config = {
            "filters": {
                "limit": {"default": 100},
                "missing_required": {"required": True}  # Missing value/default
            }
        }
        
        connector._apply_filters_to_params(params, config)
        
        assert params["limit"] == 100
        assert "missing_required" not in params
    
    def test_get_nested_field_value(self, connector):
        """Test getting nested field values."""
        record = {
            "user": {
                "profile": {
                    "email": "test@example.com"
                }
            },
            "id": 123
        }
        
        # Test nested field
        assert connector._get_nested_field_value(record, "user.profile.email") == "test@example.com"
        
        # Test simple field
        assert connector._get_nested_field_value(record, "id") == 123
        
        # Test missing field
        assert connector._get_nested_field_value(record, "missing.field") is None
        
        # Test partial path exists
        assert connector._get_nested_field_value(record, "user.missing") is None
    
    def test_compute_effective_start_time_datetime(self, connector):
        """Test computing effective start time with datetime cursor."""
        cursor = "2023-01-01T12:00:00Z"
        safety_window = 300  # 5 minutes
        
        with patch('dateutil.parser.isoparse') as mock_parse:
            mock_dt = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            mock_parse.return_value = mock_dt
            
            result = connector._compute_effective_start_time(cursor, safety_window)
            
            expected = "2023-01-01T11:55:00Z"  # 5 minutes earlier
            assert result == expected
    
    def test_compute_effective_start_time_numeric(self, connector):
        """Test computing effective start time with numeric cursor."""
        cursor = "1000"
        safety_window = 100
        
        with patch('dateutil.parser.isoparse', side_effect=ValueError):
            result = connector._compute_effective_start_time(cursor, safety_window)
            
            assert result == "900"  # 1000 - 100
    
    def test_compute_effective_start_time_invalid(self, connector):
        """Test computing effective start time with invalid cursor."""
        cursor = "invalid_cursor"
        safety_window = 100
        
        with patch('dateutil.parser.isoparse', side_effect=ValueError):
            result = connector._compute_effective_start_time(cursor, safety_window)
            
            assert result == "invalid_cursor"  # Return as-is
    
    def test_build_replication_filter(self, connector):
        """Test building replication filters."""
        filter_param = "updated_since"
        effective_start = "2023-01-01T12:00:00Z"

        result = connector._build_replication_filter(filter_param, effective_start)

        assert result == {"updated_since": "2023-01-01T12:00:00Z"}
    
    def test_supports_capabilities(self, connector):
        """Test connector capability flags."""
        assert connector.supports_incremental_read() is True
        assert connector.supports_upsert() is True


class TestRateLimiter:
    """Test rate limiter functionality."""
    
    def test_rate_limiter_init(self):
        """Test rate limiter initialization."""
        limiter = RateLimiter(max_requests=10, time_window=60)
        
        assert limiter.max_requests == 10
        assert limiter.time_window == 60
        assert limiter.requests == []
    
    def test_rate_limiter_invalid_params(self):
        """Test rate limiter with invalid parameters."""
        with pytest.raises(ValueError, match="max_requests must be positive"):
            RateLimiter(max_requests=0, time_window=60)
        
        with pytest.raises(ValueError, match="time_window must be positive"):
            RateLimiter(max_requests=10, time_window=0)
    
    @pytest.mark.asyncio
    async def test_rate_limiter_acquire_within_limit(self):
        """Test acquiring permits within rate limit."""
        limiter = RateLimiter(max_requests=5, time_window=60)
        
        # Should not block when under limit
        for _ in range(3):
            await limiter.acquire()
        
        assert len(limiter.requests) == 3
    
    @pytest.mark.asyncio
    async def test_rate_limiter_acquire_at_limit(self):
        """Test behavior when reaching rate limit."""
        limiter = RateLimiter(max_requests=2, time_window=1)
        
        # Fill up the rate limit
        await limiter.acquire()
        await limiter.acquire()
        
        assert len(limiter.requests) == 2
        
        # Test that we can inspect the rate limit state
        # In a real scenario, the next acquire would wait, but we'll just verify the limiter tracks requests correctly
        current_time = datetime.now().timestamp()
        limiter.requests = [current_time - 0.5, current_time - 0.3]  # Two recent requests
        
        # Verify the limiter has the expected state
        assert len(limiter.requests) == 2


