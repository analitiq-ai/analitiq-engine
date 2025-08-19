"""Unit tests for circuit breaker functionality."""

import asyncio
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch

from analitiq_stream.fault_tolerance.circuit_breaker import (
    CircuitBreaker, CircuitState, CircuitBreakerOpenError,
    DatabaseCircuitBreaker, APICircuitBreaker
)


class TestCircuitBreaker:
    """Test circuit breaker core functionality."""
    
    def test_circuit_breaker_initialization(self):
        """Test circuit breaker proper initialization."""
        cb = CircuitBreaker(
            failure_threshold=5,
            timeout_seconds=60,
            success_threshold=3
        )
        
        assert cb.failure_threshold == 5
        assert cb.timeout_seconds == 60
        assert cb.success_threshold == 3
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
        assert cb.success_count == 0
    
    def test_circuit_breaker_default_values(self):
        """Test circuit breaker default configuration."""
        cb = CircuitBreaker()
        
        assert cb.failure_threshold == 5
        assert cb.timeout_seconds == 60
        assert cb.success_threshold == 2
        assert cb.state == CircuitState.CLOSED
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_success_flow(self):
        """Test successful operations keep circuit closed."""
        cb = CircuitBreaker(failure_threshold=3)
        
        async def successful_operation():
            return "success"
        
        # Execute successful operations
        for i in range(5):
            result = await cb.call(successful_operation)
            assert result == "success"
            assert cb.state == CircuitState.CLOSED
            assert cb.failure_count == 0
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_failure_threshold(self):
        """Test circuit opens after failure threshold."""
        cb = CircuitBreaker(failure_threshold=3, timeout_seconds=1)
        
        async def failing_operation():
            raise Exception("Operation failed")
        
        # Execute failing operations up to threshold
        for i in range(2):
            with pytest.raises(Exception):
                await cb.call(failing_operation)
            assert cb.state == CircuitState.CLOSED
            assert cb.failure_count == i + 1
        
        # Third failure should open circuit
        with pytest.raises(Exception):
            await cb.call(failing_operation)
        
        assert cb.state == CircuitState.OPEN
        assert cb.failure_count == 3
        assert cb.last_failure_time is not None
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_open_state_blocks_calls(self):
        """Test open circuit blocks calls immediately."""
        cb = CircuitBreaker(failure_threshold=2, timeout_seconds=1)
        
        async def failing_operation():
            raise Exception("Operation failed")
        
        # Open the circuit
        for i in range(2):
            with pytest.raises(Exception):
                await cb.call(failing_operation)
        
        assert cb.state == CircuitState.OPEN
        
        # Subsequent calls should be blocked
        with pytest.raises(CircuitBreakerOpenError):
            await cb.call(failing_operation)
        
        # Circuit should still be open
        assert cb.state == CircuitState.OPEN
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_half_open_transition(self):
        """Test circuit transitions to half-open after timeout."""
        cb = CircuitBreaker(failure_threshold=2, timeout_seconds=0.1)  # Short timeout
        
        async def failing_operation():
            raise Exception("Operation failed")
        
        # Open the circuit
        for i in range(2):
            with pytest.raises(Exception):
                await cb.call(failing_operation)
        
        assert cb.state == CircuitState.OPEN
        
        # Wait for timeout
        await asyncio.sleep(0.15)
        
        # Next call should transition to half-open
        with pytest.raises(Exception):
            await cb.call(failing_operation)
        
        assert cb.state == CircuitState.OPEN  # Failed, so back to open
    
    @pytest.mark.asyncio 
    async def test_circuit_breaker_half_open_success_recovery(self):
        """Test circuit closes after successful operations in half-open."""
        cb = CircuitBreaker(
            failure_threshold=2, 
            timeout_seconds=0.1,
            success_threshold=2
        )
        
        async def failing_operation():
            raise Exception("Operation failed")
        
        async def successful_operation():
            return "success"
        
        # Open the circuit
        for i in range(2):
            with pytest.raises(Exception):
                await cb.call(failing_operation)
        
        assert cb.state == CircuitState.OPEN
        
        # Wait for timeout
        await asyncio.sleep(0.15)
        
        # Successful operations should close circuit
        result1 = await cb.call(successful_operation)
        assert result1 == "success"
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.success_count == 1
        
        result2 = await cb.call(successful_operation)
        assert result2 == "success"
        assert cb.state == CircuitState.CLOSED
        assert cb.success_count == 0  # Reset after closing
        assert cb.failure_count == 0  # Reset after closing
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_half_open_failure_reopens(self):
        """Test circuit reopens on failure in half-open state."""
        cb = CircuitBreaker(failure_threshold=2, timeout_seconds=0.1)
        
        async def failing_operation():
            raise Exception("Operation failed")
        
        async def successful_operation():
            return "success"
        
        # Open the circuit
        for i in range(2):
            with pytest.raises(Exception):
                await cb.call(failing_operation)
        
        # Wait for timeout
        await asyncio.sleep(0.15)
        
        # One success to get to half-open
        await cb.call(successful_operation)
        assert cb.state == CircuitState.HALF_OPEN
        
        # Failure should reopen circuit
        with pytest.raises(Exception):
            await cb.call(failing_operation)
        
        assert cb.state == CircuitState.OPEN
        assert cb.failure_count >= 1
    
    def test_circuit_breaker_is_call_allowed(self):
        """Test call permission logic."""
        cb = CircuitBreaker(failure_threshold=2, timeout_seconds=1)
        
        # Closed state allows calls
        assert cb._is_call_allowed() is True
        
        # Set to open state
        cb.state = CircuitState.OPEN
        cb.last_failure_time = datetime.now(timezone.utc)
        assert cb._is_call_allowed() is False
        
        # Set to half-open state  
        cb.state = CircuitState.HALF_OPEN
        assert cb._is_call_allowed() is True
        
        # Open state after timeout should allow (transition to half-open)
        cb.state = CircuitState.OPEN
        cb.last_failure_time = datetime.now(timezone.utc) - timedelta(seconds=2)
        assert cb._is_call_allowed() is True  # Should transition to half-open


class TestDatabaseCircuitBreaker:
    """Test database-specific circuit breaker."""
    
    def test_database_circuit_breaker_initialization(self):
        """Test database circuit breaker initialization."""
        db_cb = DatabaseCircuitBreaker()
        
        assert db_cb.failure_threshold == 3  # Database-specific default
        assert db_cb.timeout_seconds == 30   # Shorter timeout for DB
        assert db_cb.success_threshold == 2
    
    def test_database_circuit_breaker_custom_config(self):
        """Test database circuit breaker with custom config."""
        db_cb = DatabaseCircuitBreaker(
            failure_threshold=5,
            timeout_seconds=60
        )
        
        assert db_cb.failure_threshold == 5
        assert db_cb.timeout_seconds == 60
    
    @pytest.mark.asyncio
    async def test_database_circuit_breaker_db_specific_errors(self):
        """Test database circuit breaker handles DB errors."""
        db_cb = DatabaseCircuitBreaker(failure_threshold=2)
        
        async def db_connection_error():
            import psycopg2
            raise psycopg2.OperationalError("Connection failed")
        
        # Should count DB errors toward threshold
        for i in range(2):
            with pytest.raises(Exception):
                await db_cb.call(db_connection_error)
        
        assert db_cb.state == CircuitState.OPEN


class TestAPICircuitBreaker:
    """Test API-specific circuit breaker."""
    
    def test_api_circuit_breaker_initialization(self):
        """Test API circuit breaker initialization."""
        api_cb = APICircuitBreaker()
        
        assert api_cb.failure_threshold == 5  # API-specific default
        assert api_cb.timeout_seconds == 60   # Standard timeout for API
        assert api_cb.success_threshold == 3  # More successes needed for API
    
    def test_api_circuit_breaker_custom_config(self):
        """Test API circuit breaker with custom config."""
        api_cb = APICircuitBreaker(
            failure_threshold=10,
            timeout_seconds=120
        )
        
        assert api_cb.failure_threshold == 10
        assert api_cb.timeout_seconds == 120
    
    @pytest.mark.asyncio
    async def test_api_circuit_breaker_http_errors(self):
        """Test API circuit breaker handles HTTP errors."""
        api_cb = APICircuitBreaker(failure_threshold=3)
        
        async def api_timeout_error():
            import aiohttp
            raise aiohttp.ClientTimeout("Request timeout")
        
        async def api_connection_error():
            import aiohttp
            raise aiohttp.ClientConnectionError("Connection failed")
        
        # Should count different HTTP errors
        with pytest.raises(Exception):
            await api_cb.call(api_timeout_error)
        
        with pytest.raises(Exception):
            await api_cb.call(api_connection_error)
        
        assert api_cb.failure_count == 2
        assert api_cb.state == CircuitState.CLOSED  # Not at threshold yet
        
        # Third error should open
        with pytest.raises(Exception):
            await api_cb.call(api_timeout_error)
        
        assert api_cb.state == CircuitState.OPEN


class TestCircuitBreakerEdgeCases:
    """Test circuit breaker edge cases and error conditions."""
    
    def test_circuit_breaker_invalid_configuration(self):
        """Test circuit breaker with invalid configuration."""
        # Negative values should be handled
        with pytest.raises(ValueError):
            CircuitBreaker(failure_threshold=-1)
        
        with pytest.raises(ValueError):
            CircuitBreaker(timeout_seconds=-1)
        
        with pytest.raises(ValueError):
            CircuitBreaker(success_threshold=0)
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_concurrent_calls(self):
        """Test circuit breaker with concurrent operations."""
        cb = CircuitBreaker(failure_threshold=3, timeout_seconds=1)
        call_count = 0
        
        async def slow_operation():
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return f"result-{call_count}"
        
        # Execute concurrent operations
        tasks = [cb.call(slow_operation) for _ in range(5)]
        results = await asyncio.gather(*tasks)
        
        assert len(results) == 5
        assert all("result-" in str(r) for r in results)
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_mixed_success_failure(self):
        """Test circuit breaker with mixed success/failure patterns."""
        cb = CircuitBreaker(failure_threshold=3, timeout_seconds=0.1)
        
        async def conditional_operation(should_fail=False):
            if should_fail:
                raise Exception("Operation failed")
            return "success"
        
        # Mix of successes and failures
        await cb.call(conditional_operation, should_fail=False)  # Success
        assert cb.state == CircuitState.CLOSED
        
        with pytest.raises(Exception):
            await cb.call(conditional_operation, should_fail=True)  # Failure
        assert cb.failure_count == 1
        
        await cb.call(conditional_operation, should_fail=False)  # Success
        assert cb.failure_count == 1  # Should not reset on success in closed state
        
        # Two more failures to open
        for _ in range(2):
            with pytest.raises(Exception):
                await cb.call(conditional_operation, should_fail=True)
        
        assert cb.state == CircuitState.OPEN
    
    def test_circuit_breaker_state_transitions(self):
        """Test all possible state transitions."""
        cb = CircuitBreaker()
        
        # Initial state
        assert cb.state == CircuitState.CLOSED
        
        # Closed -> Open (via failures)
        cb._record_failure()
        cb._record_failure()
        cb._record_failure()  # Assuming default threshold of 5
        cb._record_failure() 
        cb._record_failure()  # This should open
        assert cb.state == CircuitState.OPEN
        
        # Open -> Half-Open (via timeout)
        cb.last_failure_time = datetime.now(timezone.utc) - timedelta(seconds=61)
        cb.state = CircuitState.HALF_OPEN  # Simulate transition
        assert cb.state == CircuitState.HALF_OPEN
        
        # Half-Open -> Closed (via successes)
        cb._record_success()
        cb._record_success()  # Assuming default success threshold of 2
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0  # Should reset
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_exception_handling(self):
        """Test circuit breaker handles various exception types."""
        cb = CircuitBreaker(failure_threshold=2)
        
        async def operation_with_value_error():
            raise ValueError("Invalid value")
        
        async def operation_with_runtime_error():
            raise RuntimeError("Runtime error")
        
        async def operation_with_custom_error():
            class CustomError(Exception):
                pass
            raise CustomError("Custom error")
        
        # All exception types should be counted
        with pytest.raises(ValueError):
            await cb.call(operation_with_value_error)
        
        with pytest.raises(RuntimeError):
            await cb.call(operation_with_runtime_error)
        
        assert cb.state == CircuitState.OPEN
        assert cb.failure_count == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])