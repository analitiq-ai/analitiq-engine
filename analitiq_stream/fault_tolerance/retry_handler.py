"""Retry mechanisms with exponential backoff and jitter."""

import asyncio
import logging
import random
from typing import Any, Callable, Optional, Tuple, Type

logger = logging.getLogger(__name__)


class RetryHandler:
    """
    Handles retry logic with configurable backoff strategies.

    Features:
    - Exponential backoff with jitter
    - Configurable retry conditions
    - Different strategies for different exception types
    - Metrics collection for retry attempts
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_multiplier: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    ):
        """
        Initialize retry handler.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
            backoff_multiplier: Multiplier for exponential backoff
            jitter: Whether to add random jitter to delays
            retryable_exceptions: Tuple of exception types that should trigger retries
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_multiplier = backoff_multiplier
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions or (Exception,)

        # Metrics
        self.retry_counts = {}
        self.total_retries = 0
        self.attempt_count = 0  # Track current attempt count

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt with exponential backoff and jitter."""
        delay = self.base_delay * (self.backoff_multiplier**attempt)
        delay = min(delay, self.max_delay)

        if self.jitter:
            # Add decorrelated jitter (recommended for distributed systems)
            delay = random.uniform(self.base_delay, delay)

        return delay

    def _should_retry(self, exception: Exception) -> bool:
        """Determine if an exception should trigger a retry."""
        return isinstance(exception, self.retryable_exceptions)

    async def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function with retry logic.

        Args:
            func: Function to execute (can be sync or async)
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Result of the function execution

        Raises:
            The last exception encountered after all retries are exhausted
        """
        func_name = getattr(func, "__name__", str(func))
        last_exception = None
        self.attempt_count = 0

        for attempt in range(self.max_retries + 1):
            self.attempt_count = attempt + 1
            try:
                # Execute function (handle both sync and async)
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)

                # Success - log if this was a retry
                if attempt > 0:
                    logger.info(
                        f"Function {func_name} succeeded after {attempt} retries"
                    )

                return result

            except Exception as e:
                last_exception = e

                # Check if this exception should trigger a retry
                if not self._should_retry(e):
                    logger.warning(f"Non-retryable exception in {func_name}: {str(e)}")
                    raise

                # Check if we've exhausted retries
                if attempt == self.max_retries:
                    logger.error(
                        f"All {self.max_retries} retries exhausted for {func_name}"
                    )
                    self._update_metrics(func_name, attempt + 1, False)
                    raise

                # Calculate delay and wait
                delay = self._calculate_delay(attempt)
                logger.warning(
                    f"Attempt {attempt + 1}/{self.max_retries + 1} failed for {func_name}: {str(e)}. Retrying in {delay:.2f}s"
                )

                await asyncio.sleep(delay)

        # This should never be reached, but just in case
        if last_exception:
            raise last_exception

    def _update_metrics(self, func_name: str, attempts: int, success: bool):
        """Update retry metrics."""
        if func_name not in self.retry_counts:
            self.retry_counts[func_name] = {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
            }

        self.retry_counts[func_name]["attempts"] += attempts
        self.total_retries += attempts - 1  # Subtract 1 for the initial attempt

        if success:
            self.retry_counts[func_name]["successes"] += 1
        else:
            self.retry_counts[func_name]["failures"] += 1

    def get_metrics(self) -> dict:
        """Get retry metrics."""
        return {
            "total_retries": self.total_retries,
            "function_metrics": self.retry_counts.copy(),
        }

    def reset_metrics(self):
        """Reset retry metrics."""
        self.retry_counts = {}
        self.total_retries = 0
        self.attempt_count = 0


class ExponentialBackoffRetry(RetryHandler):
    """Specialized retry handler with exponential backoff."""

    def __init__(self, max_retries: int = 5, base_delay: float = 0.5, multiplier: float = 2.0):
        super().__init__(
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=300.0,  # 5 minutes max
            backoff_multiplier=multiplier,
            jitter=True,
        )


class LinearBackoffRetry(RetryHandler):
    """Specialized retry handler with linear backoff."""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        super().__init__(
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=30.0,
            backoff_multiplier=1.0,  # Linear backoff
            jitter=False,
        )


class DatabaseRetry(RetryHandler):
    """Specialized retry handler for database operations."""

    def __init__(self):
        import asyncio

        # Define database-specific retryable exceptions
        retryable_exceptions = (
            ConnectionError,
            TimeoutError,
            asyncio.TimeoutError,
            # Add database-specific exceptions here
            # asyncpg.exceptions.ConnectionDoesNotExistError,
            # asyncpg.exceptions.InterfaceError,
        )

        super().__init__(
            max_retries=3,
            base_delay=1.0,
            max_delay=30.0,
            backoff_multiplier=2.0,
            jitter=True,
            retryable_exceptions=retryable_exceptions,
        )


class APIRetry(RetryHandler):
    """Specialized retry handler for API operations."""

    def __init__(self):
        import aiohttp

        # Define API-specific retryable exceptions
        retryable_exceptions = (
            aiohttp.ClientError,
            aiohttp.ServerTimeoutError,
            aiohttp.ClientConnectionError,
            ConnectionError,
            TimeoutError,
        )

        super().__init__(
            max_retries=5,
            base_delay=0.5,
            max_delay=60.0,
            backoff_multiplier=2.0,
            jitter=True,
            retryable_exceptions=retryable_exceptions,
        )
