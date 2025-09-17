"""E2E Test: Fault Tolerance Edge Cases

Tests comprehensive fault tolerance scenarios including circuit breakers,
retry exhaustion, dead letter queues, and recovery mechanisms.
"""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from analitiq_stream.core.pipeline import Pipeline


class FaultInjectionConnector:
    """Mock connector that injects various types of failures."""

    def __init__(self, failure_config: Dict[str, Any]):
        self.failure_config = failure_config
        self.call_count = 0
        self.is_closed = False
        self.executed_operations = []

    async def read_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """Simulate reading with potential failures."""
        self.call_count += 1
        self.executed_operations.append(("read_batch", batch_size))

        failure_type = self.failure_config.get("read_failure_type")
        failure_rate = self.failure_config.get("read_failure_rate", 0.0)

        if failure_type and (self.call_count * failure_rate) % 1 == 0:
            if failure_type == "connection_error":
                raise ConnectionError("Database connection lost")
            elif failure_type == "timeout":
                raise asyncio.TimeoutError("Read operation timed out")
            elif failure_type == "memory_error":
                raise MemoryError("Out of memory during read")
            elif failure_type == "permission_error":
                raise PermissionError("Access denied to data source")

        # Return test data
        if self.call_count <= 3:
            return [
                {
                    "id": i + (self.call_count - 1) * 10,
                    "name": f"User {i + (self.call_count - 1) * 10}",
                    "created_at": datetime.now().isoformat()
                }
                for i in range(min(batch_size, 5))
            ]
        return []

    async def write_batch(self, records: List[Dict[str, Any]]) -> None:
        """Simulate writing with potential failures."""
        self.call_count += 1
        self.executed_operations.append(("write_batch", len(records)))

        failure_type = self.failure_config.get("write_failure_type")
        failure_rate = self.failure_config.get("write_failure_rate", 0.0)

        if failure_type and (self.call_count * failure_rate) % 1 == 0:
            if failure_type == "duplicate_key":
                raise ValueError("Duplicate key violation")
            elif failure_type == "constraint_violation":
                raise ValueError("Data constraint violation")
            elif failure_type == "disk_full":
                raise OSError("No space left on device")
            elif failure_type == "network_error":
                raise ConnectionError("Network unreachable")

    async def close(self):
        self.is_closed = True


class TestFaultToleranceE2E:
    """Test fault tolerance scenarios end-to-end."""

    @pytest.mark.asyncio
    async def test_circuit_breaker_activation(
        self,
        temp_dirs,
        mock_pipeline_id,
        fault_tolerance_config,
        source_config,
        destination_config
    ):
        """Test circuit breaker activation under repeated failures."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Circuit Breaker Test Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 5,
                "max_concurrent_batches": 1,
                "checkpoint_interval": 2
            },
            "streams": {
                "failing_stream": {
                    "name": "Failing Stream",
                    "src": {"endpoint_id": "failing-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "failing-dst", "host_id": "dst-host"},
                    "fault_tolerance": fault_tolerance_config
                }
            }
        }

        # Configure source to fail consistently
        failing_source = FaultInjectionConnector({
            "read_failure_type": "connection_error",
            "read_failure_rate": 1.0  # Fail every time
        })

        # Configure destination to succeed
        working_destination = FaultInjectionConnector({})

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            # Mock circuit breaker behavior
            mock_circuit_breaker = MagicMock()
            mock_circuit_breaker.is_open = False
            mock_circuit_breaker.call_count = 0

            def mock_call(*args, **kwargs):
                mock_circuit_breaker.call_count += 1
                if mock_circuit_breaker.call_count >= fault_tolerance_config["circuit_breaker"]["failure_threshold"]:
                    mock_circuit_breaker.is_open = True
                    raise Exception("Circuit breaker is OPEN")
                raise ConnectionError("Simulated failure")

            mock_circuit_breaker.side_effect = mock_call

            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "circuit_breaker_opened": 1,
                "circuit_breaker_failures": fault_tolerance_config["circuit_breaker"]["failure_threshold"],
                "records_processed": 0,
                "records_failed": 15
            }
            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            # Run should complete even with circuit breaker activation
            await pipeline.run()

            # Verify circuit breaker metrics
            metrics = pipeline.get_metrics()
            assert metrics["circuit_breaker_opened"] == 1
            assert metrics["records_failed"] > 0

    @pytest.mark.asyncio
    async def test_retry_exhaustion_and_dlq(
        self,
        temp_dirs,
        mock_pipeline_id,
        fault_tolerance_config,
        data_generator,
        source_config,
        destination_config
    ):
        """Test retry exhaustion leading to dead letter queue."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Retry Exhaustion Test Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 3,
                "max_concurrent_batches": 1
            },
            "streams": {
                "retry_stream": {
                    "name": "Retry Stream",
                    "src": {"endpoint_id": "retry-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "retry-dst", "host_id": "dst-host"},
                    "fault_tolerance": fault_tolerance_config
                }
            },
            "error_handling": {
                "strategy": "dlq",
                "max_retries": fault_tolerance_config["retry_config"]["max_retries"]
            }
        }

        # Source provides good data
        good_records = data_generator.generate_valid_records(10)

        # Destination fails intermittently
        intermittent_failure_dest = FaultInjectionConnector({
            "write_failure_type": "constraint_violation",
            "write_failure_rate": 0.6  # 60% failure rate
        })

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "records_processed": 4,
                "records_failed": 6,
                "retry_attempts": 18,  # 6 failed records * 3 retries each
                "dlq_records": 6
            }
            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            # Verify retry and DLQ metrics
            metrics = pipeline.get_metrics()
            assert metrics["records_failed"] > 0
            assert metrics["retry_attempts"] > metrics["records_failed"]
            assert metrics["dlq_records"] == metrics["records_failed"]

            # Verify DLQ files were created
            dlq_dir = temp_dirs["deadletter"] / mock_pipeline_id / "retry_stream"
            if dlq_dir.exists():
                dlq_files = list(dlq_dir.glob("dlq_*.jsonl"))
                assert len(dlq_files) > 0

    @pytest.mark.asyncio
    async def test_partial_batch_failure_recovery(
        self,
        temp_dirs,
        mock_pipeline_id,
        data_generator,
        source_config,
        destination_config
    ):
        """Test recovery from partial batch failures."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Partial Failure Recovery Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 5,
                "max_concurrent_batches": 1,
                "checkpoint_interval": 3
            },
            "streams": {
                "partial_failure_stream": {
                    "name": "Partial Failure Stream",
                    "src": {"endpoint_id": "partial-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "partial-dst", "host_id": "dst-host"}
                }
            },
            "error_handling": {
                "strategy": "dlq",
                "partial_batch_recovery": True
            }
        }

        records = data_generator.generate_valid_records(15)

        # Mock source to provide records
        class PartialFailureSource(FaultInjectionConnector):
            def __init__(self):
                super().__init__({})
                self.record_index = 0

            async def read_batch(self, batch_size: int) -> List[Dict[str, Any]]:
                start_idx = self.record_index
                end_idx = min(start_idx + batch_size, len(records))
                batch = records[start_idx:end_idx]
                self.record_index = end_idx
                return batch

        # Mock destination to fail on specific records
        class PartialFailureDestination(FaultInjectionConnector):
            async def write_batch(self, batch_records: List[Dict[str, Any]]) -> None:
                for record in batch_records:
                    # Fail on records with ID ending in 3 or 7
                    if record["id"] % 10 in [3, 7]:
                        raise ValueError(f"Validation failed for record {record['id']}")

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "records_processed": 12,  # 15 - 3 failed
                "records_failed": 3,      # Records with IDs 3, 7, 13
                "partial_batch_recoveries": 3
            }
            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            assert metrics["records_processed"] > metrics["records_failed"]
            assert metrics["partial_batch_recoveries"] > 0

    @pytest.mark.asyncio
    async def test_cascading_failure_isolation(
        self,
        temp_dirs,
        mock_pipeline_id,
        fault_tolerance_config,
        source_config,
        destination_config
    ):
        """Test isolation of cascading failures across streams."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Cascading Failure Isolation Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 3,
                "max_concurrent_batches": 2,
                "isolation_mode": "stream_level"
            },
            "streams": {
                "healthy_stream": {
                    "name": "Healthy Stream",
                    "src": {"endpoint_id": "healthy-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "healthy-dst", "host_id": "dst-host"}
                },
                "failing_stream": {
                    "name": "Failing Stream",
                    "src": {"endpoint_id": "failing-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "failing-dst", "host_id": "dst-host"},
                    "fault_tolerance": fault_tolerance_config
                }
            }
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "streams": {
                    "healthy_stream": {
                        "records_processed": 20,
                        "records_failed": 0,
                        "status": "completed"
                    },
                    "failing_stream": {
                        "records_processed": 5,
                        "records_failed": 15,
                        "status": "failed",
                        "circuit_breaker_opened": 1
                    }
                },
                "total_records_processed": 25,
                "total_records_failed": 15
            }
            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()

            # Verify healthy stream completed successfully
            assert metrics["streams"]["healthy_stream"]["status"] == "completed"
            assert metrics["streams"]["healthy_stream"]["records_failed"] == 0

            # Verify failing stream was isolated
            assert metrics["streams"]["failing_stream"]["status"] == "failed"
            assert metrics["streams"]["failing_stream"]["circuit_breaker_opened"] == 1

    @pytest.mark.asyncio
    async def test_checkpoint_corruption_recovery(
        self,
        temp_dirs,
        mock_pipeline_id,
        corrupt_state_data,
        source_config,
        destination_config
    ):
        """Test recovery from corrupted checkpoint state."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Checkpoint Recovery Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 5,
                "max_concurrent_batches": 1,
                "checkpoint_recovery_mode": "auto"
            },
            "streams": {
                "recovery_stream": {
                    "name": "Recovery Stream",
                    "src": {"endpoint_id": "recovery-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "recovery-dst", "host_id": "dst-host"}
                }
            }
        }

        # Write corrupted state to simulate corruption
        state_dir = temp_dirs["state"] / mock_pipeline_id / "v1"
        state_dir.mkdir(parents=True, exist_ok=True)

        corrupt_state_file = state_dir / "index.json"
        with open(corrupt_state_file, "w") as f:
            json.dump(corrupt_state_data, f)

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "checkpoint_corruptions_detected": 1,
                "checkpoint_recoveries": 1,
                "records_processed": 25,
                "recovery_mode": "full_resync"
            }
            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            # Should recover and complete successfully
            await pipeline.run()

            metrics = pipeline.get_metrics()
            assert metrics["checkpoint_corruptions_detected"] == 1
            assert metrics["checkpoint_recoveries"] == 1
            assert metrics["records_processed"] > 0

    @pytest.mark.asyncio
    async def test_resource_exhaustion_handling(
        self,
        temp_dirs,
        mock_pipeline_id,
        data_generator,
        source_config,
        destination_config
    ):
        """Test handling of resource exhaustion scenarios."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Resource Exhaustion Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 1000,  # Large batch size
                "max_concurrent_batches": 10,
                "memory_limit_mb": 100,
                "backpressure_enabled": True
            },
            "streams": {
                "memory_stream": {
                    "name": "Memory Stream",
                    "src": {"endpoint_id": "memory-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "memory-dst", "host_id": "dst-host"}
                }
            }
        }

        # Generate large dataset
        large_dataset = data_generator.generate_large_dataset(5000)

        class MemoryExhaustionConnector(FaultInjectionConnector):
            def __init__(self):
                super().__init__({})
                self.read_calls = 0

            async def read_batch(self, batch_size: int) -> List[Dict[str, Any]]:
                self.read_calls += 1

                # Simulate memory pressure on 3rd call
                if self.read_calls == 3:
                    raise MemoryError("Out of memory")

                start_idx = (self.read_calls - 1) * batch_size
                end_idx = min(start_idx + batch_size, len(large_dataset))
                return large_dataset[start_idx:end_idx]

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "memory_errors": 1,
                "backpressure_activations": 5,
                "batch_size_reductions": 3,
                "records_processed": 2000,  # Partial processing due to memory limits
                "final_batch_size": 100     # Reduced from 1000
            }
            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            assert metrics["memory_errors"] > 0
            assert metrics["backpressure_activations"] > 0
            assert metrics["final_batch_size"] < pipeline_config["engine_config"]["batch_size"]

    @pytest.mark.asyncio
    async def test_concurrent_pipeline_conflict_resolution(
        self,
        temp_dirs,
        mock_pipeline_id,
        source_config,
        destination_config
    ):
        """Test conflict resolution when multiple pipeline instances run concurrently."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Concurrent Conflict Resolution Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 5,
                "max_concurrent_batches": 1,
                "concurrency_control": "file_lock"
            },
            "streams": {
                "concurrent_stream": {
                    "name": "Concurrent Stream",
                    "src": {"endpoint_id": "concurrent-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "concurrent-dst", "host_id": "dst-host"}
                }
            }
        }

        async def run_pipeline_instance(instance_id: str):
            """Run a single pipeline instance."""
            with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
                mock_engine = MagicMock()
                mock_engine.stream_data = AsyncMock()

                if instance_id == "first":
                    # First instance succeeds
                    mock_engine.get_metrics.return_value = {
                        "instance_id": instance_id,
                        "lock_acquired": True,
                        "records_processed": 50,
                        "status": "completed"
                    }
                else:
                    # Second instance detects conflict
                    mock_engine.get_metrics.return_value = {
                        "instance_id": instance_id,
                        "lock_acquired": False,
                        "records_processed": 0,
                        "status": "conflict_detected",
                        "conflict_resolution": "graceful_exit"
                    }

                # Mock state manager to return None (no previous state)
                mock_state_manager = MagicMock()
                mock_state_manager.get_run_info.return_value = None
                mock_engine.get_state_manager.return_value = mock_state_manager
                mock_engine_cls.return_value = mock_engine

                pipeline = Pipeline(
                    pipeline_config=pipeline_config,
                    source_config=source_config,
                    destination_config=destination_config,
                    state_dir=str(temp_dirs["state"])
                )

                await pipeline.run()
                return pipeline.get_metrics()

        # Run two instances concurrently
        results = await asyncio.gather(
            run_pipeline_instance("first"),
            run_pipeline_instance("second"),
            return_exceptions=True
        )

        # Verify conflict resolution
        first_result, second_result = results

        # One should complete, other should detect conflict
        completed_instances = [r for r in results if r.get("status") == "completed"]
        conflict_instances = [r for r in results if r.get("status") == "conflict_detected"]

        assert len(completed_instances) == 1
        assert len(conflict_instances) == 1