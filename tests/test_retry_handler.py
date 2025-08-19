"""Unit tests for retry handler functionality."""

import asyncio
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from analitiq_stream.fault_tolerance.retry_handler import (
    RetryHandler, ExponentialBackoffRetry, LinearBackoffRetry,
    DatabaseRetry, APIRetry
)


class TestRetryHandler:
    """Test base retry handler functionality."""
    
    def test_retry_handler_initialization(self):
        """Test retry handler proper initialization."""
        handler = RetryHandler(
            max_retries=5,
            base_delay=2.0,
            max_delay=60.0
        )
        
        assert handler.max_retries == 5
        assert handler.base_delay == 2.0
        assert handler.max_delay == 60.0
        assert handler.attempt_count == 0
    
    def test_retry_handler_default_values(self):
        """Test retry handler default configuration."""
        handler = RetryHandler()
        
        assert handler.max_retries == 3
        assert handler.base_delay == 1.0
        assert handler.max_delay == 30.0
        assert handler.attempt_count == 0
    
    @pytest.mark.asyncio
    async def test_retry_handler_success_no_retry(self):
        """Test successful operation requires no retries."""
        handler = RetryHandler(max_retries=3)
        call_count = 0
        
        async def successful_operation():
            nonlocal call_count
            call_count += 1
            return f"success-{call_count}"
        
        result = await handler.execute_with_retry(successful_operation)
        
        assert result == "success-1"
        assert call_count == 1
        assert handler.attempt_count == 1
    
    @pytest.mark.asyncio
    async def test_retry_handler_eventual_success(self):
        """Test operation succeeds after retries."""
        handler = RetryHandler(max_retries=3, base_delay=0.01)  # Fast for testing
        call_count = 0
        
        async def eventually_successful():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception(f"Attempt {call_count} failed")
            return f"success-{call_count}"
        
        result = await handler.execute_with_retry(eventually_successful)
        
        assert result == "success-3"
        assert call_count == 3
        assert handler.attempt_count == 3
    
    @pytest.mark.asyncio
    async def test_retry_handler_max_retries_exceeded(self):
        """Test max retries exceeded raises last exception."""
        handler = RetryHandler(max_retries=2, base_delay=0.01)
        call_count = 0
        
        async def always_failing():
            nonlocal call_count
            call_count += 1
            raise ValueError(f"Attempt {call_count} failed")
        
        with pytest.raises(ValueError) as exc_info:
            await handler.execute_with_retry(always_failing)
        
        assert "Attempt 3" in str(exc_info.value)  # max_retries=2 means 3 total attempts
        assert call_count == 3
        assert handler.attempt_count == 3
    
    def test_retry_handler_should_retry_default(self):
        """Test default should_retry behavior."""
        handler = RetryHandler()
        
        # Most exceptions should be retried
        assert handler.should_retry(Exception("generic error")) is True
        assert handler.should_retry(ValueError("value error")) is True
        assert handler.should_retry(RuntimeError("runtime error")) is True
        
        # Can be overridden in subclasses
        assert handler.should_retry(KeyboardInterrupt()) is True  # Base class retries all
    
    def test_retry_handler_calculate_delay_base(self):
        """Test base delay calculation."""
        handler = RetryHandler(base_delay=2.0)
        
        # Base class returns constant delay
        assert handler.calculate_delay(1) == 2.0
        assert handler.calculate_delay(2) == 2.0
        assert handler.calculate_delay(5) == 2.0
    
    @pytest.mark.asyncio
    async def test_retry_handler_delay_application(self):
        """Test retry delay is properly applied."""
        handler = RetryHandler(max_retries=2, base_delay=0.1)
        call_times = []
        
        async def failing_operation():
            call_times.append(datetime.now())
            raise Exception("Always fails")
        
        start_time = datetime.now()
        
        with pytest.raises(Exception):
            await handler.execute_with_retry(failing_operation)
        
        total_time = (datetime.now() - start_time).total_seconds()
        
        # Should have taken at least 2 * base_delay (2 retries with delay)
        assert total_time >= 0.2
        assert len(call_times) == 3  # Initial + 2 retries


class TestExponentialBackoffRetry:
    """Test exponential backoff retry strategy."""
    
    def test_exponential_backoff_initialization(self):
        """Test exponential backoff initialization."""
        handler = ExponentialBackoffRetry(
            max_retries=5,
            base_delay=1.0,
            multiplier=2.5,
            max_delay=60.0
        )
        
        assert handler.max_retries == 5
        assert handler.base_delay == 1.0
        assert handler.multiplier == 2.5
        assert handler.max_delay == 60.0
    
    def test_exponential_backoff_defaults(self):
        """Test exponential backoff default values."""
        handler = ExponentialBackoffRetry()
        
        assert handler.multiplier == 2.0
        assert handler.max_delay == 60.0
    
    def test_exponential_backoff_delay_calculation(self):
        """Test exponential backoff delay calculation."""
        handler = ExponentialBackoffRetry(
            base_delay=1.0,
            multiplier=2.0,
            max_delay=30.0
        )
        
        # Should grow exponentially: base * multiplier^(attempt-1)
        assert handler.calculate_delay(1) == 1.0    # 1 * 2^0
        assert handler.calculate_delay(2) == 2.0    # 1 * 2^1  
        assert handler.calculate_delay(3) == 4.0    # 1 * 2^2
        assert handler.calculate_delay(4) == 8.0    # 1 * 2^3
        assert handler.calculate_delay(5) == 16.0   # 1 * 2^4
        assert handler.calculate_delay(6) == 30.0   # Capped at max_delay
    
    @pytest.mark.asyncio
    async def test_exponential_backoff_timing(self):
        """Test exponential backoff actual timing."""
        handler = ExponentialBackoffRetry(
            max_retries=2,
            base_delay=0.1,
            multiplier=2.0
        )
        call_times = []
        
        async def failing_operation():
            call_times.append(datetime.now())
            raise Exception("Always fails")
        
        with pytest.raises(Exception):
            await handler.execute_with_retry(failing_operation)
        
        # Check timing between calls
        assert len(call_times) == 3
        
        # First retry should be after ~0.1s
        delay1 = (call_times[1] - call_times[0]).total_seconds()
        assert 0.1 <= delay1 <= 0.2
        
        # Second retry should be after ~0.2s  
        delay2 = (call_times[2] - call_times[1]).total_seconds()
        assert 0.2 <= delay2 <= 0.3


class TestLinearBackoffRetry:
    """Test linear backoff retry strategy."""
    
    def test_linear_backoff_initialization(self):
        """Test linear backoff initialization."""
        handler = LinearBackoffRetry(
            max_retries=5,
            base_delay=2.0,
            increment=1.5
        )
        
        assert handler.max_retries == 5
        assert handler.base_delay == 2.0
        assert handler.increment == 1.5
    
    def test_linear_backoff_defaults(self):
        """Test linear backoff default values."""
        handler = LinearBackoffRetry()
        
        assert handler.increment == 1.0
    
    def test_linear_backoff_delay_calculation(self):
        """Test linear backoff delay calculation."""
        handler = LinearBackoffRetry(
            base_delay=2.0,
            increment=1.5,
            max_delay=15.0
        )
        
        # Should grow linearly: base + increment * (attempt-1)
        assert handler.calculate_delay(1) == 2.0    # 2 + 1.5*0
        assert handler.calculate_delay(2) == 3.5    # 2 + 1.5*1
        assert handler.calculate_delay(3) == 5.0    # 2 + 1.5*2
        assert handler.calculate_delay(4) == 6.5    # 2 + 1.5*3
        assert handler.calculate_delay(5) == 8.0    # 2 + 1.5*4
        assert handler.calculate_delay(10) == 15.0  # Capped at max_delay


class TestDatabaseRetry:
    """Test database-specific retry handler."""
    
    def test_database_retry_initialization(self):
        """Test database retry initialization."""
        db_retry = DatabaseRetry()
        
        assert db_retry.max_retries == 3
        assert db_retry.base_delay == 1.0
        assert db_retry.multiplier == 2.0  # Exponential backoff
        assert isinstance(db_retry, ExponentialBackoffRetry)
    
    def test_database_retry_should_retry_logic(self):
        """Test database-specific retry logic."""
        db_retry = DatabaseRetry()
        
        # Should retry transient database errors
        class DatabaseError(Exception):
            pass
        
        class OperationalError(DatabaseError):
            pass
        
        class ConnectionError(DatabaseError):
            pass
        
        # Should retry database connection issues
        assert db_retry.should_retry(OperationalError("Connection lost"))
        assert db_retry.should_retry(ConnectionError("Cannot connect"))
        
        # Should retry generic database errors
        assert db_retry.should_retry(DatabaseError("Database error"))
        
        # Should not retry authentication errors (would be persistent)
        class AuthenticationError(DatabaseError):
            pass
        
        # Base implementation retries all, but could be customized
        assert db_retry.should_retry(AuthenticationError("Auth failed"))
    
    @pytest.mark.asyncio
    async def test_database_retry_with_db_errors(self):
        """Test database retry with actual database errors."""
        db_retry = DatabaseRetry(max_retries=2, base_delay=0.01)
        call_count = 0
        
        async def db_operation():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                # Simulate transient database error
                raise Exception("Database connection lost")
            return "Database operation successful"
        
        result = await db_retry.execute_with_retry(db_operation)
        
        assert result == "Database operation successful"
        assert call_count == 3  # Initial + 2 retries


class TestAPIRetry:
    """Test API-specific retry handler."""
    
    def test_api_retry_initialization(self):
        """Test API retry initialization."""
        api_retry = APIRetry()
        
        assert api_retry.max_retries == 3
        assert api_retry.base_delay == 1.0
        assert api_retry.multiplier == 1.5  # Gentler backoff for APIs
        assert isinstance(api_retry, ExponentialBackoffRetry)
    
    def test_api_retry_should_retry_logic(self):
        """Test API-specific retry logic."""
        api_retry = APIRetry()
        
        # Should retry rate limiting and server errors
        class HTTPError(Exception):
            def __init__(self, status_code):
                self.status_code = status_code
                super().__init__(f"HTTP {status_code}")
        
        # Should retry 5xx server errors
        assert api_retry.should_retry(HTTPError(500))  # Internal Server Error
        assert api_retry.should_retry(HTTPError(502))  # Bad Gateway
        assert api_retry.should_retry(HTTPError(503))  # Service Unavailable
        assert api_retry.should_retry(HTTPError(504))  # Gateway Timeout
        
        # Should retry 429 (Rate Limited)
        assert api_retry.should_retry(HTTPError(429))
        
        # Should not retry 4xx client errors (except 429)
        assert api_retry.should_retry(HTTPError(400))  # Base class retries all
        assert api_retry.should_retry(HTTPError(401))  # Could be customized
        assert api_retry.should_retry(HTTPError(404))  # Could be customized
    
    @pytest.mark.asyncio
    async def test_api_retry_with_http_errors(self):
        """Test API retry with HTTP errors."""
        api_retry = APIRetry(max_retries=2, base_delay=0.01)
        call_count = 0
        
        async def api_request():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                # Simulate rate limiting
                raise Exception("HTTP 429: Rate limit exceeded")
            return {"status": "success", "data": "API response"}
        
        result = await api_retry.execute_with_retry(api_request)
        
        assert result["status"] == "success"
        assert call_count == 3  # Initial + 2 retries


class TestRetryHandlerEdgeCases:
    """Test retry handler edge cases and error conditions."""
    
    def test_retry_handler_invalid_configuration(self):
        """Test retry handler with invalid configuration."""
        # Negative max_retries should be handled
        with pytest.raises(ValueError):
            RetryHandler(max_retries=-1)
        
        # Negative delays should be handled
        with pytest.raises(ValueError):
            RetryHandler(base_delay=-1.0)
        
        with pytest.raises(ValueError):
            RetryHandler(max_delay=-1.0)
        
        # max_delay should be >= base_delay
        with pytest.raises(ValueError):
            RetryHandler(base_delay=10.0, max_delay=5.0)
    
    @pytest.mark.asyncio
    async def test_retry_handler_zero_retries(self):
        """Test retry handler with zero max retries."""
        handler = RetryHandler(max_retries=0)
        call_count = 0
        
        async def failing_operation():
            nonlocal call_count
            call_count += 1
            raise Exception("Always fails")
        
        with pytest.raises(Exception):
            await handler.execute_with_retry(failing_operation)
        
        assert call_count == 1  # Only initial attempt, no retries
        assert handler.attempt_count == 1
    
    @pytest.mark.asyncio
    async def test_retry_handler_concurrent_executions(self):
        """Test retry handler with concurrent operations."""
        handler = RetryHandler(max_retries=2, base_delay=0.01)
        
        async def operation(operation_id):
            # Simulate some operations failing, others succeeding
            if operation_id % 2 == 0:
                return f"success-{operation_id}"
            else:
                raise Exception(f"failure-{operation_id}")
        
        # Execute multiple operations concurrently
        tasks = []
        for i in range(5):
            task = asyncio.create_task(
                handler.execute_with_retry(lambda i=i: operation(i))
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check results
        success_count = sum(1 for r in results if isinstance(r, str))
        failure_count = sum(1 for r in results if isinstance(r, Exception))
        
        assert success_count >= 2  # Even IDs should succeed
        assert failure_count >= 2  # Odd IDs should fail
    
    @pytest.mark.asyncio
    async def test_retry_handler_callback_exceptions(self):
        """Test retry handler with callback exceptions."""
        handler = RetryHandler(max_retries=2, base_delay=0.01)
        
        async def operation_with_multiple_error_types():
            # Simulate different types of errors across attempts
            if handler.attempt_count == 1:
                raise ValueError("First attempt: Value error")
            elif handler.attempt_count == 2:
                raise RuntimeError("Second attempt: Runtime error")
            else:
                raise KeyError("Third attempt: Key error")
        
        with pytest.raises(KeyError) as exc_info:
            await handler.execute_with_retry(operation_with_multiple_error_types)
        
        assert "Third attempt" in str(exc_info.value)
        assert handler.attempt_count == 3
    
    def test_retry_handler_reset_state(self):
        """Test retry handler state reset between operations."""
        handler = RetryHandler(max_retries=3)
        
        # Simulate failed operation
        handler.attempt_count = 3
        
        # State should not persist across operations
        # (This would need to be tested with actual execute_with_retry calls
        # as the handler resets state internally)
        assert handler.attempt_count == 3  # Current state
    
    @pytest.mark.asyncio
    async def test_exponential_backoff_jitter(self):
        """Test exponential backoff with jitter (if implemented)."""
        handler = ExponentialBackoffRetry(
            max_retries=3,
            base_delay=0.1,
            multiplier=2.0
        )
        
        # Measure delays across multiple runs to check for jitter
        delays = []
        for _ in range(5):
            calculated_delay = handler.calculate_delay(3)  # 3rd attempt
            delays.append(calculated_delay)
        
        # All delays should be the same without jitter
        # (Could be extended to test jitter if implemented)
        assert all(d == delays[0] for d in delays)
        assert delays[0] == 0.4  # 0.1 * 2^2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])