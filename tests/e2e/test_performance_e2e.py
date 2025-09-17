"""E2E Test: Performance and Scalability Edge Cases

Tests performance scenarios including backpressure handling, memory constraints,
large datasets, concurrent processing, and resource optimization.
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from analitiq_stream.core.pipeline import Pipeline


class PerformanceTestConnector:
    """Mock connector for performance testing scenarios."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.call_count = 0
        self.total_records_read = 0
        self.total_records_written = 0
        self.read_latencies = []
        self.write_latencies = []
        self.memory_usage = []
        self.is_closed = False

    async def read_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """Simulate reading with performance characteristics."""
        start_time = time.time()
        self.call_count += 1

        # Simulate different performance patterns
        pattern = self.config.get("performance_pattern", "normal")

        if pattern == "slow_start":
            # Simulate slow initial connections
            if self.call_count <= 3:
                await asyncio.sleep(0.5)
        elif pattern == "degrading":
            # Simulate degrading performance over time
            await asyncio.sleep(0.1 * self.call_count * 0.1)
        elif pattern == "memory_leak":
            # Simulate memory leak by accumulating data
            self.memory_usage.append(self.call_count * 1024 * 1024)  # 1MB per call
        elif pattern == "burst_then_slow":
            # Fast for first few calls, then slow
            if self.call_count > 5:
                await asyncio.sleep(0.3)

        # Generate records based on dataset size
        dataset_size = self.config.get("dataset_size", 100)
        max_records = min(batch_size, dataset_size - self.total_records_read)

        if max_records <= 0:
            return []

        records = []
        for i in range(max_records):
            record_id = self.total_records_read + i + 1
            record = {
                "id": record_id,
                "name": f"User {record_id}",
                "email": f"user{record_id}@example.com",
                "created_at": datetime.now().isoformat(),
                "large_field": "x" * self.config.get("record_size", 100)  # Variable record size
            }

            # Add nested data for complexity testing
            if self.config.get("complex_nested", False):
                record["nested"] = {
                    "level1": {
                        "level2": {
                            "level3": {
                                "data": list(range(100)),
                                "metadata": {"key" + str(j): f"value{j}" for j in range(50)}
                            }
                        }
                    }
                }

            records.append(record)

        self.total_records_read += len(records)
        end_time = time.time()
        self.read_latencies.append(end_time - start_time)

        return records

    async def write_batch(self, records: List[Dict[str, Any]]) -> None:
        """Simulate writing with performance characteristics."""
        start_time = time.time()

        # Simulate different write patterns
        pattern = self.config.get("write_performance_pattern", "normal")

        if pattern == "batch_optimization":
            # Faster for larger batches
            base_time = 0.1
            batch_efficiency = min(len(records) / 100.0, 1.0)
            await asyncio.sleep(base_time * (1 - batch_efficiency))
        elif pattern == "contention":
            # Simulate database contention
            await asyncio.sleep(0.05 * len(records))
        elif pattern == "deadlock_prone":
            # Simulate occasional deadlocks
            if self.call_count % 10 == 0:
                await asyncio.sleep(0.5)  # Deadlock resolution time

        self.total_records_written += len(records)
        end_time = time.time()
        self.write_latencies.append(end_time - start_time)

    async def close(self):
        self.is_closed = True


class TestPerformanceE2E:
    """Test performance and scalability scenarios."""

    @pytest.mark.asyncio
    async def test_large_dataset_processing(
        self,
        temp_dirs,
        mock_pipeline_id,
        source_config,
        destination_config
    ):
        """Test processing of large datasets with memory constraints."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Large Dataset Processing Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 1000,
                "max_concurrent_batches": 5,
                "buffer_size": 5000,
                "memory_limit_mb": 500,
                "backpressure_threshold": 80
            },
            "streams": {
                "large_dataset_stream": {
                    "name": "Large Dataset Stream",
                    "src": {"endpoint_id": "large-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "large-dst", "host_id": "dst-host"},
                    "performance_config": {
                        "parallel_processing": True,
                        "memory_monitoring": True,
                        "batch_optimization": True
                    }
                }
            }
        }

        # Configure for large dataset
        large_dataset_config = {
            "dataset_size": 100000,  # 100K records
            "record_size": 1024,     # 1KB per record
            "performance_pattern": "normal"
        }

        source_connector = PerformanceTestConnector(large_dataset_config)
        dest_connector = PerformanceTestConnector({
            "write_performance_pattern": "batch_optimization"
        })

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()

            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager

            # Simulate processing metrics for large dataset
            mock_engine.get_metrics.return_value = {
                "records_processed": 100000,
                "processing_time_seconds": 45.5,
                "throughput_records_per_second": 2197,
                "memory_usage": {
                    "peak_mb": 485,
                    "average_mb": 340,
                    "memory_efficiency": 0.85
                },
                "batch_processing": {
                    "total_batches": 100,
                    "average_batch_time_ms": 455,
                    "batch_size_adjustments": 5,
                    "optimal_batch_size": 1200
                },
                "backpressure_activations": 12,
                "parallel_streams": 5,
                "performance_score": 0.92
            }
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            start_time = time.time()
            await pipeline.run()
            end_time = time.time()

            metrics = pipeline.get_metrics()
            processing_time = end_time - start_time

            # Verify large dataset processing metrics
            assert metrics["records_processed"] == 100000
            assert metrics["throughput_records_per_second"] > 1000
            assert metrics["memory_usage"]["peak_mb"] < 500  # Within limit
            assert metrics["backpressure_activations"] > 0
            assert metrics["performance_score"] > 0.8

    @pytest.mark.asyncio
    async def test_backpressure_handling(
        self,
        temp_dirs,
        mock_pipeline_id,
        source_config,
        destination_config
    ):
        """Test backpressure handling when destination is slower than source."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Backpressure Handling Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 100,
                "max_concurrent_batches": 3,
                "buffer_size": 500,
                "backpressure_threshold": 70,
                "backpressure_strategy": "adaptive_batching"
            },
            "streams": {
                "backpressure_stream": {
                    "name": "Backpressure Stream",
                    "src": {"endpoint_id": "fast-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "slow-dst", "host_id": "dst-host"}
                }
            }
        }

        # Fast source, slow destination
        fast_source_config = {
            "dataset_size": 10000,
            "performance_pattern": "normal"  # Fast reads
        }

        slow_dest_config = {
            "write_performance_pattern": "contention"  # Slow writes
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()

            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager

            mock_engine.get_metrics.return_value = {
                "records_processed": 10000,
                "backpressure_events": 25,
                "backpressure_activations": 8,
                "adaptive_batching": {
                    "batch_size_reductions": 12,
                    "batch_size_increases": 4,
                    "min_batch_size_reached": 25,
                    "max_batch_size_reached": 150,
                    "final_batch_size": 75
                },
                "buffer_usage": {
                    "peak_utilization": 0.85,
                    "average_utilization": 0.72,
                    "buffer_overflows": 0
                },
                "flow_control": {
                    "source_throttling_events": 15,
                    "destination_speedup_requests": 8
                },
                "processing_time_seconds": 125.5,
                "effective_throughput": 79.6  # Records per second after backpressure
            }
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            assert metrics["backpressure_events"] > 0
            assert metrics["adaptive_batching"]["batch_size_reductions"] > 0
            assert metrics["buffer_usage"]["peak_utilization"] < 1.0  # No overflow
            assert metrics["flow_control"]["source_throttling_events"] > 0

    @pytest.mark.asyncio
    async def test_concurrent_stream_processing(
        self,
        temp_dirs,
        mock_pipeline_id,
        source_config,
        destination_config
    ):
        """Test concurrent processing of multiple streams."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Concurrent Stream Processing Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 50,
                "max_concurrent_batches": 10,
                "stream_isolation": True,
                "resource_sharing": "optimized"
            },
            "streams": {
                "stream_1": {
                    "name": "High Priority Stream",
                    "src": {"endpoint_id": "hp-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "hp-dst", "host_id": "dst-host"},
                    "priority": "high",
                    "concurrency_limit": 4
                },
                "stream_2": {
                    "name": "Medium Priority Stream",
                    "src": {"endpoint_id": "mp-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "mp-dst", "host_id": "dst-host"},
                    "priority": "medium",
                    "concurrency_limit": 3
                },
                "stream_3": {
                    "name": "Low Priority Stream",
                    "src": {"endpoint_id": "lp-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "lp-dst", "host_id": "dst-host"},
                    "priority": "low",
                    "concurrency_limit": 2
                }
            }
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()

            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager

            mock_engine.get_metrics.return_value = {
                "streams": {
                    "stream_1": {
                        "records_processed": 5000,
                        "processing_time_seconds": 45.2,
                        "average_latency_ms": 9.04,
                        "concurrent_batches_peak": 4,
                        "resource_contention_events": 2
                    },
                    "stream_2": {
                        "records_processed": 3000,
                        "processing_time_seconds": 52.1,
                        "average_latency_ms": 17.37,
                        "concurrent_batches_peak": 3,
                        "resource_contention_events": 5
                    },
                    "stream_3": {
                        "records_processed": 2000,
                        "processing_time_seconds": 65.5,
                        "average_latency_ms": 32.75,
                        "concurrent_batches_peak": 2,
                        "resource_contention_events": 8
                    }
                },
                "concurrency_metrics": {
                    "total_concurrent_streams": 3,
                    "peak_concurrent_batches": 9,
                    "resource_utilization": 0.87,
                    "priority_enforcement_events": 12,
                    "load_balancing_adjustments": 6
                },
                "total_records_processed": 10000,
                "overall_processing_time": 65.5,  # Limited by slowest stream
                "concurrency_efficiency": 0.92
            }
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()

            # Verify priority-based processing
            stream_metrics = metrics["streams"]
            assert stream_metrics["stream_1"]["average_latency_ms"] < stream_metrics["stream_2"]["average_latency_ms"]
            assert stream_metrics["stream_2"]["average_latency_ms"] < stream_metrics["stream_3"]["average_latency_ms"]

            # Verify concurrency metrics
            assert metrics["concurrency_metrics"]["peak_concurrent_batches"] <= 10
            assert metrics["concurrency_metrics"]["priority_enforcement_events"] > 0
            assert metrics["concurrency_efficiency"] > 0.8

    @pytest.mark.asyncio
    async def test_memory_pressure_adaptation(
        self,
        temp_dirs,
        mock_pipeline_id,
        source_config,
        destination_config
    ):
        """Test adaptation to memory pressure situations."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Memory Pressure Adaptation Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 500,
                "max_concurrent_batches": 8,
                "memory_limit_mb": 256,
                "memory_pressure_threshold": 80,
                "memory_adaptation_strategy": "aggressive"
            },
            "streams": {
                "memory_intensive_stream": {
                    "name": "Memory Intensive Stream",
                    "src": {"endpoint_id": "memory-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "memory-dst", "host_id": "dst-host"},
                    "memory_config": {
                        "enable_memory_monitoring": True,
                        "gc_optimization": True,
                        "buffer_compression": True
                    }
                }
            }
        }

        # Configure for memory-intensive processing
        memory_intensive_config = {
            "dataset_size": 20000,
            "record_size": 5120,  # 5KB per record
            "complex_nested": True,
            "performance_pattern": "memory_leak"
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()

            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager

            mock_engine.get_metrics.return_value = {
                "records_processed": 20000,
                "memory_management": {
                    "peak_memory_mb": 248,
                    "memory_pressure_events": 15,
                    "gc_triggers": 25,
                    "memory_optimizations_applied": 12,
                    "buffer_compressions": 8,
                    "memory_efficiency_score": 0.78
                },
                "adaptive_responses": {
                    "batch_size_reductions": 18,
                    "concurrency_reductions": 6,
                    "buffer_size_reductions": 4,
                    "min_batch_size_reached": 50,
                    "final_batch_size": 75,
                    "final_concurrency": 3
                },
                "performance_impact": {
                    "throughput_reduction": 0.35,  # 35% reduction due to memory constraints
                    "latency_increase": 0.45,      # 45% increase in processing latency
                    "stability_maintained": True
                },
                "processing_time_seconds": 185.5
            }
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            memory_metrics = metrics["memory_management"]
            adaptive_metrics = metrics["adaptive_responses"]

            # Verify memory management
            assert memory_metrics["peak_memory_mb"] < 256  # Under limit
            assert memory_metrics["memory_pressure_events"] > 0
            assert memory_metrics["gc_triggers"] > 0

            # Verify adaptive responses
            assert adaptive_metrics["batch_size_reductions"] > 0
            assert adaptive_metrics["concurrency_reductions"] > 0
            assert adaptive_metrics["final_batch_size"] < 500  # Reduced from initial

            # Verify stability maintained despite pressure
            assert metrics["performance_impact"]["stability_maintained"] is True

    @pytest.mark.asyncio
    async def test_throughput_optimization(
        self,
        temp_dirs,
        mock_pipeline_id,
        source_config,
        destination_config
    ):
        """Test throughput optimization under various conditions."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Throughput Optimization Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 200,
                "max_concurrent_batches": 6,
                "optimization_mode": "throughput",
                "auto_tuning": True
            },
            "streams": {
                "throughput_stream": {
                    "name": "Throughput Optimization Stream",
                    "src": {"endpoint_id": "throughput-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "throughput-dst", "host_id": "dst-host"},
                    "optimization": {
                        "enable_batch_optimization": True,
                        "enable_connection_pooling": True,
                        "enable_parallel_processing": True,
                        "enable_predictive_prefetching": True
                    }
                }
            }
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()

            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager

            mock_engine.get_metrics.return_value = {
                "records_processed": 50000,
                "processing_time_seconds": 42.8,
                "throughput_records_per_second": 1168,
                "optimization_metrics": {
                    "auto_tuning_adjustments": 8,
                    "optimal_batch_size_found": 350,
                    "optimal_concurrency_found": 8,
                    "connection_pool_efficiency": 0.94,
                    "prefetch_hit_rate": 0.87,
                    "parallelization_efficiency": 0.91
                },
                "performance_improvements": {
                    "baseline_throughput": 650,      # Records/sec before optimization
                    "optimized_throughput": 1168,    # Records/sec after optimization
                    "improvement_percentage": 79.7,
                    "latency_reduction_percentage": 45.2
                },
                "resource_utilization": {
                    "cpu_utilization": 0.85,
                    "memory_utilization": 0.72,
                    "network_utilization": 0.68,
                    "io_utilization": 0.79
                }
            }
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            optimization_metrics = metrics["optimization_metrics"]
            improvements = metrics["performance_improvements"]

            # Verify optimization occurred
            assert optimization_metrics["auto_tuning_adjustments"] > 0
            assert optimization_metrics["optimal_batch_size_found"] != 200  # Changed from initial
            assert optimization_metrics["connection_pool_efficiency"] > 0.8

            # Verify performance improvements
            assert improvements["optimized_throughput"] > improvements["baseline_throughput"]
            assert improvements["improvement_percentage"] > 50
            assert improvements["latency_reduction_percentage"] > 30

            # Verify efficient resource utilization
            resource_util = metrics["resource_utilization"]
            assert all(0.6 < util < 0.9 for util in resource_util.values())

    @pytest.mark.asyncio
    async def test_degraded_performance_handling(
        self,
        temp_dirs,
        mock_pipeline_id,
        source_config,
        destination_config
    ):
        """Test handling of degraded performance conditions."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Degraded Performance Handling Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 100,
                "max_concurrent_batches": 4,
                "performance_monitoring": True,
                "degradation_response": "adaptive"
            },
            "streams": {
                "degrading_stream": {
                    "name": "Performance Degrading Stream",
                    "src": {"endpoint_id": "degrading-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "degrading-dst", "host_id": "dst-host"},
                    "performance_thresholds": {
                        "min_throughput": 100,      # Records per second
                        "max_latency_ms": 1000,     # Maximum acceptable latency
                        "degradation_threshold": 0.5  # 50% performance drop threshold
                    }
                }
            }
        }

        # Configure for degrading performance
        degrading_config = {
            "dataset_size": 10000,
            "performance_pattern": "degrading"
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()

            # Mock state manager to return None (no previous state)
            mock_state_manager = MagicMock()
            mock_state_manager.get_run_info.return_value = None
            mock_engine.get_state_manager.return_value = mock_state_manager

            mock_engine.get_metrics.return_value = {
                "records_processed": 10000,
                "performance_degradation": {
                    "degradation_detected": True,
                    "initial_throughput": 250,
                    "final_throughput": 95,
                    "degradation_percentage": 62,
                    "performance_alerts": 8,
                    "threshold_violations": 12
                },
                "adaptive_responses": {
                    "batch_size_adjustments": 15,
                    "concurrency_adjustments": 6,
                    "timeout_adjustments": 4,
                    "circuit_breaker_activations": 2,
                    "fallback_strategies_activated": 3
                },
                "recovery_attempts": {
                    "automatic_retries": 18,
                    "connection_resets": 5,
                    "resource_reallocation": 3,
                    "successful_recoveries": 12,
                    "failed_recoveries": 6
                },
                "final_status": "completed_with_degradation",
                "processing_time_seconds": 245.5
            }
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                source_config=source_config,
                destination_config=destination_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            degradation_metrics = metrics["performance_degradation"]
            adaptive_metrics = metrics["adaptive_responses"]
            recovery_metrics = metrics["recovery_attempts"]

            # Verify degradation was detected
            assert degradation_metrics["degradation_detected"] is True
            assert degradation_metrics["degradation_percentage"] > 50

            # Verify adaptive responses were triggered
            assert adaptive_metrics["batch_size_adjustments"] > 0
            assert adaptive_metrics["fallback_strategies_activated"] > 0

            # Verify recovery attempts were made
            assert recovery_metrics["automatic_retries"] > 0
            assert recovery_metrics["successful_recoveries"] > recovery_metrics["failed_recoveries"]

            # Verify pipeline completed despite degradation
            assert metrics["final_status"] == "completed_with_degradation"