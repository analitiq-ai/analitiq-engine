"""Shared fixtures and utilities for E2E tests."""

import asyncio
import json
import sys
import tempfile
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

# Ensure the repository root is available on sys.path before importing the package
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analitiq_stream.connectors.base import BaseConnector
from analitiq_stream.config import DIRECTORIES


class InMemorySourceConnector(BaseConnector):
    """Simple in-memory source connector used for end-to-end tests."""

    def __init__(self):
        super().__init__(name="InMemorySourceConnector")

    async def connect(self, config: Dict[str, Any]):
        self.is_connected = True
        # Ensure shared tracking structures exist so tests can inspect them
        config.setdefault("records_by_endpoint", {})
        config.setdefault("read_log", {})

    async def disconnect(self):
        self.is_connected = False

    async def read_batches(
        self,
        config: Dict[str, Any],
        *,
        state_manager,
        stream_name: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        pipeline_cfg = config.get("pipeline_config", {})
        stream_id = pipeline_cfg.get("stream_id") if isinstance(pipeline_cfg, dict) else None
        endpoint_id = config.get("endpoint_id", stream_name)
        records_map: Dict[str, List[Dict[str, Any]]] = config.get("records_by_endpoint", {})

        if stream_id and stream_id in records_map:
            records = list(records_map[stream_id])
        elif endpoint_id in records_map:
            records = list(records_map[endpoint_id])
        else:
            records = list(config.get("records", []))

        # Track which batches were delivered for assertions
        read_log = config.setdefault("read_log", {}).setdefault(endpoint_id, [])

        idx = 0
        while idx < len(records):
            batch = records[idx : idx + batch_size]
            idx += batch_size

            if not batch:
                break

            self.metrics["records_read"] += len(batch)
            self.metrics["batches_read"] += 1
            read_log.append({"size": len(batch)})

            yield batch

    async def write_batch(self, batch: List[Dict[str, Any]], config: Dict[str, Any]):
        raise NotImplementedError("Source connector does not support write operations")


class InMemoryDestinationConnector(BaseConnector):
    """Destination connector that persists data in memory for assertions."""

    def __init__(self):
        super().__init__(name="InMemoryDestinationConnector")
        self._attempt_counters = defaultdict(int)

    async def connect(self, config: Dict[str, Any]):
        self.is_connected = True
        config.setdefault("storage_by_endpoint", {})
        config.setdefault("attempt_log", {})
        config.setdefault("failure_scenarios", {})

    async def disconnect(self):
        self.is_connected = False

    async def read_batches(
        self,
        config: Dict[str, Any],
        *,
        state_manager,
        stream_name: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        # Destination connectors are not used as sources, but BaseConnector
        # defines this method as abstract. Implement a no-op generator so the
        # interface remains compatible should it be accidentally invoked.
        if False:
            yield []
        return

    async def write_batch(self, batch: List[Dict[str, Any]], config: Dict[str, Any]):
        endpoint_id = config.get("endpoint_id", "default")
        storage = config.setdefault("storage_by_endpoint", {})
        failure_scenarios = config.setdefault("failure_scenarios", {})
        attempt_log = config.setdefault("attempt_log", {})

        attempt_entries: List[Dict[str, Any]] = attempt_log.setdefault(endpoint_id, [])
        current_attempt = len(attempt_entries)
        attempt_entries.append({"attempt": current_attempt, "batch_size": len(batch)})

        scenario = failure_scenarios.get(endpoint_id, {})
        fail_on_attempts = set(scenario.get("fail_on_attempts", []))
        reject_if_missing = scenario.get("reject_if_missing", [])
        fail_on_match = scenario.get("fail_when_field_equals")

        # Apply simple data quality rule checks before persisting
        if reject_if_missing:
            for record in batch:
                for field in reject_if_missing:
                    if record.get(field) in (None, ""):
                        raise ValueError(f"Missing required field '{field}' for endpoint {endpoint_id}")

        if fail_on_match and isinstance(fail_on_match, dict):
            field = fail_on_match.get("field")
            value = fail_on_match.get("value")
            if field and any(record.get(field) == value for record in batch):
                message = scenario.get("message", "simulated failure")
                raise RuntimeError(message)

        if current_attempt in fail_on_attempts:
            message = scenario.get("message", "simulated destination failure")
            raise RuntimeError(message)

        stored_records = storage.setdefault(endpoint_id, [])
        stored_records.extend(batch)

        self.metrics["records_written"] += len(batch)
        self.metrics["batches_written"] += 1


@pytest.fixture
def temp_dirs(monkeypatch):
    """Create temporary directories for pipeline artifacts and patch globals."""
    with tempfile.TemporaryDirectory() as base_dir:
        base_path = Path(base_dir)
        dirs = {
            "state": base_path / "state",
            "logs": base_path / "logs",
            "deadletter": base_path / "deadletter",
            "config": base_path / "config",
        }

        for dir_path in dirs.values():
            dir_path.mkdir(parents=True, exist_ok=True)

        monkeypatch.setitem(DIRECTORIES, "state", dirs["state"])
        monkeypatch.setitem(DIRECTORIES, "deadletter", dirs["deadletter"])
        monkeypatch.setitem(DIRECTORIES, "logs", dirs["logs"])

        yield dirs


@pytest.fixture
def mock_pipeline_id():
    """Generate unique pipeline ID for test isolation."""
    return f"test-pipeline-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def base_engine_config():
    """Base engine configuration for tests."""
    return {
        "batch_size": 10,
        "max_concurrent_batches": 2,
        "buffer_size": 100,
        "checkpoint_interval": 5,
        "backpressure_threshold": 80
    }


@pytest.fixture
def fault_tolerance_config():
    """Fault tolerance configuration for stress tests."""
    return {
        "retry_config": {
            "max_retries": 3,
            "base_delay": 0.1,
            "max_delay": 1.0,
            "exponential_base": 2.0
        },
        "circuit_breaker": {
            "failure_threshold": 5,
            "recovery_timeout": 2.0,
            "expected_success_count": 3
        },
        "dead_letter_queue": {
            "max_retries": 2,
            "retention_hours": 24
        }
    }


class DataGenerator:
    """Generate various types of test data for edge case testing."""

    @staticmethod
    def generate_valid_records(count: int, start_id: int = 1) -> List[Dict[str, Any]]:
        """Generate valid, well-formed records."""
        records = []
        base_time = datetime(2023, 12, 1, 10, 0, 0)

        for i in range(count):
            records.append({
                "id": start_id + i,
                "name": f"User {start_id + i}",
                "email": f"user{start_id + i}@test.com",
                "created_at": base_time + timedelta(minutes=i),
                "status": "active" if i % 2 == 0 else "inactive",
                "age": 25 + (i % 40),
                "metadata": {
                    "score": 100 + i,
                    "tags": ["tag1", "tag2"] if i % 3 == 0 else ["tag3"]
                }
            })
        return records

    @staticmethod
    def generate_malformed_records(count: int) -> List[Dict[str, Any]]:
        """Generate records with various data quality issues."""
        records = []
        base_time = datetime(2023, 12, 1, 10, 0, 0)

        for i in range(count):
            if i % 5 == 0:
                # Missing required fields
                records.append({"id": i, "incomplete": True})
            elif i % 5 == 1:
                # Invalid data types
                records.append({
                    "id": f"invalid_id_{i}",  # String instead of int
                    "name": 12345,  # Number instead of string
                    "email": None,
                    "created_at": "invalid-date-format",
                    "age": "not_a_number"
                })
            elif i % 5 == 2:
                # Null values in required fields
                records.append({
                    "id": i,
                    "name": None,
                    "email": None,
                    "created_at": None,
                    "status": None
                })
            elif i % 5 == 3:
                # Extremely long values
                records.append({
                    "id": i,
                    "name": "x" * 1000,
                    "email": f"{'very' * 50}long@{'domain' * 20}.com",
                    "created_at": base_time + timedelta(minutes=i),
                    "description": "a" * 10000
                })
            else:
                # Special characters and edge cases
                records.append({
                    "id": i,
                    "name": "Test\x00\n\r\t\"'User",
                    "email": "test+special@domain.com",
                    "created_at": base_time + timedelta(minutes=i),
                    "unicode_field": "🚀🔥💯"
                })
        return records

    @staticmethod
    def generate_large_dataset(count: int) -> List[Dict[str, Any]]:
        """Generate a large dataset for performance testing."""
        records = []
        base_time = datetime(2023, 12, 1, 10, 0, 0)

        for i in range(count):
            records.append({
                "id": i,
                "name": f"User {i}",
                "email": f"user{i}@example.com",
                "created_at": base_time + timedelta(seconds=i),
                "status": "active",
                "large_text": "Lorem ipsum " * 100,  # ~1KB per record
                "nested_data": {
                    "level1": {
                        "level2": {
                            "level3": f"deep_value_{i}",
                            "array": list(range(10))
                        }
                    }
                }
            })
        return records


class MockConnectorFactory:
    """Factory for creating various types of mock connectors for edge case testing."""

    @staticmethod
    def create_failing_db_connection(failure_pattern: str = "intermittent"):
        """Create a database connection that fails according to patterns."""
        class FailingDBConnection:
            def __init__(self):
                self.call_count = 0
                self.is_closed = False
                self.executed_queries = []

            async def execute(self, query, *args):
                self.call_count += 1
                self.executed_queries.append(("execute", query, args))

                if failure_pattern == "immediate":
                    raise ConnectionError("Database connection failed")
                elif failure_pattern == "intermittent":
                    if self.call_count % 3 == 0:
                        raise ConnectionError("Intermittent database failure")
                elif failure_pattern == "timeout":
                    await asyncio.sleep(30)  # Simulate timeout

                return "EXECUTE"

            async def fetch(self, query, *args):
                self.call_count += 1
                self.executed_queries.append(("fetch", query, args))

                if failure_pattern == "data_corruption":
                    return [{"corrupted": b"\x00\x01\x02"}]
                elif failure_pattern == "partial_failure":
                    if self.call_count > 2:
                        raise ConnectionError("Connection lost during fetch")
                    return []

                return []

            async def close(self):
                self.is_closed = True

        return FailingDBConnection()

    @staticmethod
    def create_rate_limited_api_response(quota_limit: int = 5, reset_time: int = 60):
        """Create API responses that simulate rate limiting."""
        class RateLimitedResponse:
            def __init__(self):
                self.request_count = 0
                self.reset_time = datetime.now() + timedelta(seconds=reset_time)

            def __call__(self, *args, **kwargs):
                self.request_count += 1

                if self.request_count > quota_limit:
                    if datetime.now() < self.reset_time:
                        from unittest.mock import MagicMock
                        response = MagicMock()
                        response.status = 429
                        response.json = AsyncMock(return_value={
                            "error": "Rate limit exceeded",
                            "retry_after": int((self.reset_time - datetime.now()).total_seconds())
                        })
                        return response
                    else:
                        # Reset counter after reset time
                        self.request_count = 1
                        self.reset_time = datetime.now() + timedelta(seconds=reset_time)

                response = MagicMock()
                response.status = 200
                response.json = AsyncMock(return_value={"data": [{"id": self.request_count}]})
                return response

        return RateLimitedResponse()


@pytest.fixture
def data_generator():
    """Data generator fixture."""
    return DataGenerator()


@pytest.fixture
def mock_connector_factory():
    """Mock connector factory fixture."""
    return MockConnectorFactory()


@pytest.fixture
def sample_state_data(mock_pipeline_id):
    """Sample state data for checkpoint recovery tests."""
    return {
        "pipeline_id": mock_pipeline_id,
        "version": "1.0",
        "streams": {
            "test_stream": {
                "bookmarks": [
                    {
                        "partition": {},
                        "cursor": "2023-12-01T10:30:00Z",
                        "aux": {"last_id": 1500}
                    }
                ],
                "run": {
                    "run_id": f"2023-12-01T10:00:00Z-{uuid.uuid4().hex[:4]}",
                    "currently_syncing_partition": {}
                }
            }
        }
    }


@pytest.fixture
def corrupt_state_data(mock_pipeline_id):
    """Corrupt state data for error recovery tests."""
    return {
        "pipeline_id": mock_pipeline_id,
        "version": "invalid_version",
        "streams": {
            "test_stream": {
                "bookmarks": [
                    {
                        "partition": "invalid_partition_format",
                        "cursor": "invalid_cursor_format",
                        "aux": "not_a_dict"
                    }
                ],
                "run": {
                    "run_id": None,
                    "currently_syncing_partition": "invalid_format"
                }
            }
        }
    }


@pytest.fixture
def source_config():
    """Source configuration backed by predefined in-memory records."""
    return {
        "type": "in_memory_source",
        "endpoint_id": "test-src-endpoint",
        "host_id": "test-src-host",
        "records": [],
        "records_by_endpoint": {},
        "read_log": {},
    }


@pytest.fixture
def destination_config():
    """Destination configuration backed by in-memory storage."""
    return {
        "type": "in_memory_destination",
        "endpoint_id": "test-dst-endpoint",
        "host_id": "test-dst-host",
        "storage_by_endpoint": {},
        "failure_scenarios": {},
        "attempt_log": {},
    }


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def patch_streaming_engine_connectors(monkeypatch):
    """Use the lightweight in-memory connectors for end-to-end tests."""
    from analitiq_stream.core.engine import StreamingEngine

    original_get_connector = StreamingEngine._get_connector

    def _get_connector(self, config: Dict[str, Any]) -> BaseConnector:
        connector_type = (config or {}).get("type")
        if connector_type == "in_memory_source":
            return InMemorySourceConnector()
        if connector_type == "in_memory_destination":
            return InMemoryDestinationConnector()
        return original_get_connector(self, config)

    monkeypatch.setattr(StreamingEngine, "_get_connector", _get_connector)
    yield
    monkeypatch.setattr(StreamingEngine, "_get_connector", original_get_connector)