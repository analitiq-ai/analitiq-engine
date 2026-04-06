"""Circuit breaker pattern implementation for fault tolerance."""

import asyncio
import logging
import time
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing fast
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """
    Circuit breaker implementation to prevent cascading failures.

    Features:
    - Three states: CLOSED, OPEN, HALF_OPEN
    - Configurable failure threshold and timeout
    - Automatic recovery attempts
    - Metrics collection
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        success_threshold: int = 2,
        expected_exception: type = Exception,
        name: Optional[str] = None,
    ):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time in seconds before attempting recovery
            expected_exception: Exception type that triggers circuit opening
            name: Optional name for the circuit breaker
        """
        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be positive")
        if recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be positive")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.expected_exception = expected_exception
        self.name = name or "CircuitBreaker"

        # State tracking
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.success_count = 0

        # Metrics
        self.total_requests = 0
        self.total_failures = 0
        self.total_successes = 0
        self.state_transitions = {
            CircuitState.CLOSED: 0,
            CircuitState.OPEN: 0,
            CircuitState.HALF_OPEN: 0,
        }

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function through the circuit breaker.

        Args:
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Result of the function execution

        Raises:
            CircuitBreakerOpenError: When circuit is open
            Original exception: When function fails
        """
        self.total_requests += 1

        # Check circuit state
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._transition_to_half_open()
            else:
                raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is open")

        try:
            # Execute function
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            # Success - handle state transitions
            self._on_success()
            return result

        except self.expected_exception as e:
            # Expected failure - handle state transitions
            self._on_failure()
            raise
        except BaseException as e:
            # Handle exceptions that don't inherit from Exception (like aiohttp.ClientTimeout)
            # For circuit breaker purposes, treat all failures the same
            self._on_failure()
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True

        # Handle both datetime objects and timestamps
        if hasattr(self.last_failure_time, 'timestamp'):
            last_time = self.last_failure_time.timestamp()
        else:
            last_time = self.last_failure_time

        return time.time() - last_time >= self.recovery_timeout

    def _transition_to_half_open(self):
        """Transition circuit to half-open state."""
        logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
        self.state = CircuitState.HALF_OPEN
        self.state_transitions[CircuitState.HALF_OPEN] += 1
        self.success_count = 0

    def _on_success(self):
        """Handle successful function execution."""
        self.total_successes += 1

        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            # After reaching success threshold in half-open, close the circuit
            if self.success_count >= self.success_threshold:
                self._close_circuit()
        elif self.state == CircuitState.CLOSED:
            # In closed state, don't reset failure count on success
            # Only reset on explicit reset or after opening/closing cycle
            pass

    def _on_failure(self):
        """Handle failed function execution."""
        self.total_failures += 1
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.state == CircuitState.HALF_OPEN:
            # Failure in half-open state - go back to open
            self._open_circuit()
        elif self.state == CircuitState.CLOSED:
            # Check if we should open the circuit
            if self.failure_count >= self.failure_threshold:
                self._open_circuit()

    def _open_circuit(self):
        """Open the circuit breaker."""
        logger.warning(
            f"Circuit breaker {self.name} opening due to {self.failure_count} failures"
        )
        self.state = CircuitState.OPEN
        self.state_transitions[CircuitState.OPEN] += 1

    def _close_circuit(self):
        """Close the circuit breaker."""
        logger.info(f"Circuit breaker {self.name} closing - service recovered")
        self.state = CircuitState.CLOSED
        self.state_transitions[CircuitState.CLOSED] += 1
        self.failure_count = 0
        self.success_count = 0

    def get_state(self) -> CircuitState:
        """Get current circuit state."""
        return self.state

    def get_metrics(self) -> dict:
        """Get circuit breaker metrics."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "total_requests": self.total_requests,
            "total_failures": self.total_failures,
            "total_successes": self.total_successes,
            "failure_rate": self.total_failures / max(self.total_requests, 1),
            "state_transitions": {
                state.value: count for state, count in self.state_transitions.items()
            },
        }

    def reset(self):
        """Reset circuit breaker to initial state."""
        logger.info(f"Resetting circuit breaker {self.name}")
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None

        # Reset metrics
        self.total_requests = 0
        self.total_failures = 0
        self.total_successes = 0
        self.state_transitions = {
            CircuitState.CLOSED: 0,
            CircuitState.OPEN: 0,
            CircuitState.HALF_OPEN: 0,
        }

    def force_open(self):
        """Force circuit breaker to open state."""
        logger.warning(f"Forcing circuit breaker {self.name} to open state")
        self.state = CircuitState.OPEN
        self.last_failure_time = time.time()

    def force_close(self):
        """Force circuit breaker to closed state."""
        logger.info(f"Forcing circuit breaker {self.name} to closed state")
        self.state = CircuitState.CLOSED
        self.failure_count = 0

    def _record_failure(self):
        """Record a failure for testing purposes."""
        self._on_failure()

    def _record_success(self):
        """Record a success for testing purposes."""
        self._on_success()

    def _is_call_allowed(self) -> bool:
        """Check if a call is allowed based on current circuit state."""
        if self.state == CircuitState.CLOSED:
            return True
        elif self.state == CircuitState.OPEN:
            return self._should_attempt_reset()
        elif self.state == CircuitState.HALF_OPEN:
            return True
        return False


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open."""

    pass


class DatabaseCircuitBreaker(CircuitBreaker):
    """Specialized circuit breaker for database operations."""

    def __init__(self, name: str = "DatabaseCircuitBreaker"):
        super().__init__(
            failure_threshold=5,  # Database-specific default
            recovery_timeout=30,
            expected_exception=Exception,  # Keep generic for now
            name=name,
        )


class APICircuitBreaker(CircuitBreaker):
    """Specialized circuit breaker for API operations."""

    def __init__(self, name: str = "APICircuitBreaker"):
        try:
            import aiohttp
            # Include specific aiohttp exceptions that inherit from Exception or BaseException
            api_exceptions = (aiohttp.ClientError, ConnectionError, TimeoutError, OSError, Exception)
        except ImportError:
            api_exceptions = (ConnectionError, TimeoutError, OSError, Exception)

        super().__init__(
            failure_threshold=3,
            recovery_timeout=60,
            expected_exception=api_exceptions,
            name=name,
        )
