"""Unit tests for RateLimiter class."""

import asyncio
import time
from unittest.mock import patch

import pytest

from cdk.rate_limiter import RateLimiter


class TestRateLimiter:
    """Test suite for RateLimiter functionality."""

    def test_initialization_valid_params(self):
        """Test valid initialization parameters."""
        limiter = RateLimiter(max_requests=10, time_window=60)
        assert limiter.max_requests == 10
        assert limiter.time_window == 60
        assert limiter.requests == []
        assert limiter.lock is not None

    def test_initialization_invalid_max_requests(self):
        """Test error handling for invalid max_requests."""
        with pytest.raises(ValueError, match="max_requests must be positive"):
            RateLimiter(max_requests=0, time_window=60)

        with pytest.raises(ValueError, match="max_requests must be positive"):
            RateLimiter(max_requests=-1, time_window=60)

    def test_initialization_invalid_time_window(self):
        """Test error handling for invalid time_window."""
        with pytest.raises(ValueError, match="time_window must be positive"):
            RateLimiter(max_requests=10, time_window=0)

        with pytest.raises(ValueError, match="time_window must be positive"):
            RateLimiter(max_requests=10, time_window=-1)

    @pytest.mark.asyncio
    async def test_acquire_under_limit(self):
        """Test acquiring tokens when under rate limit."""
        limiter = RateLimiter(max_requests=5, time_window=60)

        # Should be able to acquire up to max_requests without delay
        start_time = time.time()
        for _i in range(5):
            await limiter.acquire()
        end_time = time.time()

        # Should complete quickly (no rate limiting delay)
        assert end_time - start_time < 0.1
        assert len(limiter.requests) == 5

    @pytest.mark.asyncio
    async def test_acquire_at_limit_with_delay(self):
        """Test acquiring tokens when at rate limit requires waiting."""
        limiter = RateLimiter(
            max_requests=2, time_window=1
        )  # Very short window for testing

        # Fill up the rate limit
        await limiter.acquire()
        await limiter.acquire()
        assert len(limiter.requests) == 2

        # Next request should wait
        start_time = time.time()
        await limiter.acquire()
        end_time = time.time()

        # Should have waited close to the time window
        elapsed = end_time - start_time
        assert elapsed >= 0.5  # Should wait at least half the window
        assert (
            len(limiter.requests) == 1
        )  # Old requests cleaned up, only current one remains

    @pytest.mark.asyncio
    async def test_acquire_multiple_concurrent_requests(self):
        """Test multiple concurrent acquire calls."""
        limiter = RateLimiter(max_requests=3, time_window=2)

        async def make_request(delay=0):
            if delay:
                await asyncio.sleep(delay)
            await limiter.acquire()

        start_time = time.time()

        # Launch 6 concurrent requests (double the limit)
        tasks = [asyncio.create_task(make_request()) for _ in range(6)]

        await asyncio.gather(*tasks)
        end_time = time.time()

        # Should have taken at least one time window due to rate limiting
        elapsed = end_time - start_time
        assert elapsed >= 1.5  # Should wait for rate limit reset

        # Final state should have at most max_requests recent entries
        assert len(limiter.requests) <= 3

    @pytest.mark.asyncio
    async def test_acquire_exact_timing_boundary(self):
        """Test behavior at exact time window boundaries."""
        limiter = RateLimiter(max_requests=1, time_window=1)

        # Make first request
        await limiter.acquire()
        first_time = time.time()

        # Wait exactly the time window
        await asyncio.sleep(1.0)

        # Should be able to make another request immediately
        await limiter.acquire()
        second_time = time.time()

        # Verify timing is as expected
        assert second_time - first_time >= 1.0
        assert len(limiter.requests) == 1  # Only recent request should remain

    @pytest.mark.asyncio
    async def test_zero_wait_time_edge_case(self):
        """Test edge case where calculated wait time is zero or negative."""
        limiter = RateLimiter(max_requests=1, time_window=1)

        # Mock time to create edge case
        with patch("cdk.rate_limiter.datetime") as mock_datetime:
            # First call returns current time
            mock_datetime.now.return_value.timestamp.return_value = 1000.0
            await limiter.acquire()

            # Second call - simulate time has passed exactly the window
            mock_datetime.now.return_value.timestamp.return_value = 1001.0

            # Should not wait (old request is exactly at window boundary)
            start_time = time.time()
            await limiter.acquire()
            end_time = time.time()

            # Should complete quickly
            assert end_time - start_time < 0.1

    @pytest.mark.asyncio
    async def test_high_frequency_requests(self):
        """Test behavior with high frequency request patterns."""
        limiter = RateLimiter(max_requests=10, time_window=1)

        request_times = []

        # Make requests as fast as possible
        for _i in range(20):
            start = time.time()
            await limiter.acquire()
            request_times.append(time.time() - start)

        # First 10 should be very fast
        assert all(t < 0.1 for t in request_times[:10])

        # Later requests should show rate limiting delays
        later_requests = request_times[10:]
        assert any(t > 0.5 for t in later_requests)

    @pytest.mark.asyncio
    async def test_acquire_cancellation(self):
        """Test that acquire can be cancelled while waiting."""
        limiter = RateLimiter(max_requests=1, time_window=2)

        # Fill the rate limit
        await limiter.acquire()

        # Start a request that will need to wait
        acquire_task = asyncio.create_task(limiter.acquire())

        # Give it a moment to start waiting
        await asyncio.sleep(0.1)

        # Cancel the waiting request
        acquire_task.cancel()

        # Should raise CancelledError
        with pytest.raises(asyncio.CancelledError):
            await acquire_task
