"""Shared rate limiter for API requests.

This rate limiter uses a token bucket algorithm and is used by both
source API connectors and destination API handlers.
"""

import asyncio
from datetime import datetime
from typing import List


class RateLimiter:
    """Rate limiter for API requests using token bucket algorithm.

    This class provides rate limiting functionality to prevent
    overwhelming API endpoints with too many requests.

    Attributes:
        max_requests: Maximum number of requests allowed in time window
        time_window: Time window in seconds
    """

    def __init__(self, max_requests: int, time_window: int):
        """Initialize rate limiter.

        Args:
            max_requests: Maximum requests allowed in time window
            time_window: Time window in seconds

        Raises:
            ValueError: If max_requests or time_window is not positive
        """
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")
        if time_window <= 0:
            raise ValueError("time_window must be positive")

        self.max_requests = max_requests
        self.time_window = time_window
        self.requests: List[float] = []
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Acquire permission to make a request.

        Blocks if rate limited until a request slot becomes available.
        """
        while True:
            async with self.lock:
                now = datetime.now().timestamp()

                # Clean up old requests outside the time window
                self.requests = [
                    req_time
                    for req_time in self.requests
                    if now - req_time < self.time_window
                ]

                # If we're under the limit, we can proceed
                if len(self.requests) < self.max_requests:
                    self.requests.append(now)
                    return

                # We're at the limit, calculate wait time
                oldest_request = min(self.requests)
                wait_time = self.time_window - (now - oldest_request)

            # Sleep OUTSIDE the lock to avoid blocking other requests
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            else:
                # Small yield to prevent tight loop
                await asyncio.sleep(0)

    def reset(self) -> None:
        """Reset the rate limiter, clearing all recorded requests."""
        self.requests = []
