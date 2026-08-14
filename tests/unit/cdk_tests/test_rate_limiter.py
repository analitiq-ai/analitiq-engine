"""The CDK's request rate limiter.

Bounds and book-keeping only -- nothing here touches a connector, which is
why these live beside the module they exercise rather than beside a
connector's tests.
"""

from __future__ import annotations

import pytest

from cdk.rate_limiter import RateLimiter

pytestmark = pytest.mark.unit


class TestConstruction:
    def test_it_carries_the_declared_bounds(self) -> None:
        limiter = RateLimiter(max_requests=10, time_window=60)
        assert (limiter.max_requests, limiter.time_window) == (10, 60)
        assert limiter.requests == []

    def test_a_non_positive_bound_is_refused(self) -> None:
        with pytest.raises(ValueError, match="max_requests must be positive"):
            RateLimiter(max_requests=0, time_window=60)
        with pytest.raises(ValueError, match="time_window must be positive"):
            RateLimiter(max_requests=10, time_window=0)


@pytest.mark.asyncio
class TestAcquire:
    async def test_requests_under_the_limit_do_not_wait(self) -> None:
        limiter = RateLimiter(max_requests=5, time_window=60)
        for _ in range(3):
            await limiter.acquire()
        assert len(limiter.requests) == 3
