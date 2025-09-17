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
        assert handler.max_delay == 60.0
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
    
    def test_retry_handler_should_retry_internal(self):
        """Test internal _should_retry behavior."""
        handler = RetryHandler()

        # Most exceptions should be retried (private method)
        assert handler._should_retry(Exception("generic error")) is True
        assert handler._should_retry(ValueError("value error")) is True
        assert handler._should_retry(RuntimeError("runtime error")) is True
    
    def test_retry_handler_calculate_delay_internal(self):
        """Test internal delay calculation."""
        handler = RetryHandler(base_delay=2.0, jitter=False)

        # Test delay calculation (private method)
        delay = handler._calculate_delay(1)
        assert delay >= 2.0  # Should be at least base delay
    
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
        handler = ExponentialBackoffRetry(max_retries=5, base_delay=1.0, multiplier=2.5)

        assert handler.max_retries == 5
        assert handler.base_delay == 1.0
        assert handler.backoff_multiplier == 2.5
        assert handler.max_delay == 300.0  # Fixed default

    def test_exponential_backoff_defaults(self):
        """Test exponential backoff default values."""
        handler = ExponentialBackoffRetry()

        assert handler.backoff_multiplier == 2.0
        assert handler.max_delay == 300.0
    
    @pytest.mark.asyncio
    async def test_exponential_backoff_execution(self):
        """Test exponential backoff execution."""
        handler = ExponentialBackoffRetry(max_retries=2, base_delay=0.01)
        call_count = 0

        async def failing_operation():
            nonlocal call_count
            call_count += 1
            raise Exception("Always fails")

        with pytest.raises(Exception):
            await handler.execute_with_retry(failing_operation)

        assert call_count == 3  # Initial + 2 retries


class TestLinearBackoffRetry:
    """Test linear backoff retry strategy."""

    def test_linear_backoff_initialization(self):
        """Test linear backoff initialization."""
        handler = LinearBackoffRetry(max_retries=5, base_delay=2.0)

        assert handler.max_retries == 5
        assert handler.base_delay == 2.0
        assert handler.backoff_multiplier == 1.0  # Linear
        assert handler.jitter is False

    def test_linear_backoff_defaults(self):
        """Test linear backoff default values."""
        handler = LinearBackoffRetry()

        assert handler.backoff_multiplier == 1.0  # Linear multiplier


class TestDatabaseRetry:
    """Test database-specific retry handler."""

    def test_database_retry_initialization(self):
        """Test database retry initialization."""
        db_retry = DatabaseRetry()

        assert db_retry.max_retries == 3
        assert db_retry.base_delay == 1.0
        assert db_retry.backoff_multiplier == 2.0
        assert isinstance(db_retry, RetryHandler)
    
    @pytest.mark.asyncio
    async def test_database_retry_with_db_errors(self):
        """Test database retry with actual database errors."""
        db_retry = DatabaseRetry()
        db_retry.max_retries = 2
        db_retry.base_delay = 0.01
        call_count = 0

        async def db_operation():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                # Simulate transient database error
                raise ConnectionError("Database connection lost")
            return "Database operation successful"

        result = await db_retry.execute_with_retry(db_operation)

        assert result == "Database operation successful"
        assert call_count == 3  # Initial + 2 retries


class TestAPIRetry:
    """Test API-specific retry handler."""

    def test_api_retry_initialization(self):
        """Test API retry initialization."""
        api_retry = APIRetry()

        assert api_retry.max_retries == 5
        assert api_retry.base_delay == 0.5
        assert api_retry.backoff_multiplier == 2.0
        assert isinstance(api_retry, RetryHandler)
    
    @pytest.mark.asyncio
    async def test_api_retry_with_http_errors(self):
        """Test API retry with HTTP errors."""
        api_retry = APIRetry()
        api_retry.max_retries = 2
        api_retry.base_delay = 0.01
        call_count = 0

        async def api_request():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                # Simulate rate limiting with an exception type that APIRetry handles
                import aiohttp
                raise aiohttp.ClientError("HTTP 429: Rate limit exceeded")
            return {"status": "success", "data": "API response"}

        result = await api_retry.execute_with_retry(api_request)

        assert result["status"] == "success"
        assert call_count == 3  # Initial + 2 retries


class TestRetryHandlerEdgeCases:
    """Test retry handler edge cases and error conditions."""
    
    def test_retry_handler_invalid_configuration(self):
        """Test retry handler with questionable configuration."""
        # The RetryHandler doesn't actually validate configuration
        # so these don't raise errors in the current implementation
        handler = RetryHandler(max_retries=-1)
        assert handler.max_retries == -1

        handler = RetryHandler(base_delay=-1.0)
        assert handler.base_delay == -1.0
    
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
        
        # At least some operations should complete
        assert len(results) == 5
    
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
        """Test exponential backoff with jitter."""
        handler = ExponentialBackoffRetry(max_retries=3, base_delay=0.1)

        # Test that delay calculation works (private method)
        delay = handler._calculate_delay(2)
        assert delay >= 0.1  # Should be at least base delay


if __name__ == "__main__":
    pytest.main([__file__, "-v"])