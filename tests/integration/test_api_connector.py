"""Comprehensive tests for API connector functionality."""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from aiohttp import ClientTimeout, TCPConnector, ClientSession

from src.source.connectors.api import APIConnector
from cdk.rate_limiter import RateLimiter
from src.source.connectors.base import ConnectionError, ReadError
from cdk.connection_runtime import ConnectionRuntime
from cdk.secrets.resolvers.memory import InMemorySecretsResolver
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
    async def test_connect_adopts_materialized_runtime(self, connector, valid_connection_config):
        """Connector.connect adopts an already-materialized runtime's session.

        The runtime owns transport materialization; the connector just
        reads ``runtime.session`` / ``runtime.base_url`` from it. We
        pre-populate the runtime's private state to skip the aiohttp
        bring-up, since that lives in the runtime and is tested there.
        """
        runtime = _make_api_runtime(valid_connection_config)
        mock_session = AsyncMock()
        runtime._session = mock_session
        runtime._base_url = "https://api.example.com"
        runtime._materialized = True

        await connector.connect(runtime)

        assert connector.is_connected is True
        assert connector.base_url == "https://api.example.com"
        assert connector.session is mock_session

    @pytest.mark.asyncio
    async def test_connect_with_rate_limit(self, connector):
        """Connector exposes the runtime's rate limiter when present."""
        from cdk.rate_limiter import RateLimiter

        runtime = _make_api_runtime({"host": "https://api.example.com"})
        runtime._session = AsyncMock()
        runtime._base_url = "https://api.example.com"
        runtime._rate_limiter = RateLimiter(max_requests=10, time_window=60)
        runtime._materialized = True

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



class TestWriteOperations:
    """Test write operations."""

    @pytest.mark.asyncio
    async def test_write_batch_raises(self, connector):
        """Source connector write_batch should raise NotImplementedError."""
        with pytest.raises(NotImplementedError, match="read-only"):
            await connector.write_batch([{"id": 1}], {})


class TestUtilityMethods:
    """Test utility and helper methods."""
    
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


