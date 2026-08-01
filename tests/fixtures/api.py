"""API fixtures for integration tests."""

import pytest
from aioresponses import aioresponses


@pytest.fixture
def sample_api_config():
    """Sample API configuration for testing."""
    return {
        "host": "https://api.example.com",
        "parameters": {
            "headers": {
                "Content-Type": "application/json",
                "Authorization": "Bearer test-token",
            },
            "rate_limit": {
                "max_requests": 100,
                "time_window": 60,
            },
        },
    }


@pytest.fixture
def mock_http_responses():
    """Mock HTTP responses using aioresponses."""
    with aioresponses() as m:
        yield m


@pytest.fixture
def sample_api_response():
    """Sample API response data for testing."""
    return {
        "data": [
            {
                "id": 123,
                "name": "Test Record",
                "created_at": "2025-08-19T10:30:00Z",
                "status": "active",
            }
        ],
        "pagination": {"page": 1, "per_page": 100, "total": 1},
    }
