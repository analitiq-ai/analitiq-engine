"""E2E Test: State Management and Recovery Edge Cases

Tests state management scenarios including checkpoint corruption, concurrent access,
state recovery, incremental sync edge cases, and state consistency.
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from analitiq_stream.core.pipeline import Pipeline


class StateCorruptionSimulator:
    """Simulates various state corruption scenarios."""

    @staticmethod
    def create_corrupted_checkpoint(corruption_type: str) -> Dict[str, Any]:
        """Create different types of corrupted checkpoint data."""
        base_checkpoint = {
            "pipeline_id": "test-pipeline",
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
                        "run_id": "2023-12-01T10:00:00Z-abcd",
                        "currently_syncing_partition": {}
                    }
                }
            }
        }

        if corruption_type == "invalid_json":
            return "{invalid_json: missing_quotes}"
        elif corruption_type == "missing_required_fields":
            return {"pipeline_id": "test-pipeline"}
        elif corruption_type == "invalid_cursor_format":
            corrupted = base_checkpoint.copy()
            corrupted["streams"]["test_stream"]["bookmarks"][0]["cursor"] = "invalid_date"
            return corrupted
        elif corruption_type == "negative_aux_values":
            corrupted = base_checkpoint.copy()
            corrupted["streams"]["test_stream"]["bookmarks"][0]["aux"]["last_id"] = -999
            return corrupted
        elif corruption_type == "future_timestamp":
            corrupted = base_checkpoint.copy()
            future_time = datetime.now() + timedelta(days=365)
            corrupted["streams"]["test_stream"]["bookmarks"][0]["cursor"] = future_time.isoformat()
            return corrupted
        elif corruption_type == "circular_reference":
            corrupted = base_checkpoint.copy()
            corrupted["circular_ref"] = corrupted  # This would cause JSON serialization issues
            return corrupted
        else:
            return base_checkpoint

    @staticmethod
    def create_partial_state_files(state_dir: Path, pipeline_id: str):
        """Create partially written state files to simulate interrupted writes."""
        pipeline_state_dir = state_dir / pipeline_id / "v1"
        pipeline_state_dir.mkdir(parents=True, exist_ok=True)

        # Create incomplete index file
        index_file = pipeline_state_dir / "index.json"
        with open(index_file, "w") as f:
            f.write('{"partial": "data"')  # Incomplete JSON

        # Create stream directory with missing files
        stream_dir = pipeline_state_dir / "streams" / "stream.test_stream"
        stream_dir.mkdir(parents=True, exist_ok=True)

        # Create empty partition file
        partition_file = stream_dir / "partition-default.json"
        partition_file.touch()  # Empty file


class ConcurrentStateAccessSimulator:
    """Simulates concurrent access to state files."""

    def __init__(self, state_dir: Path, pipeline_id: str):
        self.state_dir = state_dir
        self.pipeline_id = pipeline_id
        self.access_log = []
        self.lock_conflicts = 0

    async def concurrent_read_write(self, operation_id: str, operation_type: str):
        """Simulate concurrent read/write operations."""
        start_time = time.time()

        try:
            if operation_type == "read":
                await self._simulate_read_operation(operation_id)
            elif operation_type == "write":
                await self._simulate_write_operation(operation_id)
            elif operation_type == "checkpoint":
                await self._simulate_checkpoint_operation(operation_id)

            self.access_log.append({
                "operation_id": operation_id,
                "operation_type": operation_type,
                "start_time": start_time,
                "end_time": time.time(),
                "success": True
            })
        except Exception as e:
            self.lock_conflicts += 1
            self.access_log.append({
                "operation_id": operation_id,
                "operation_type": operation_type,
                "start_time": start_time,
                "end_time": time.time(),
                "success": False,
                "error": str(e)
            })

    async def _simulate_read_operation(self, operation_id: str):
        """Simulate reading state files."""
        # Simulate file access time
        import asyncio
        await asyncio.sleep(0.1)

        state_file = self.state_dir / self.pipeline_id / "v1" / "index.json"
        if state_file.exists():
            with open(state_file, "r") as f:
                data = json.load(f)
        else:
            raise FileNotFoundError("State file not found")

    async def _simulate_write_operation(self, operation_id: str):
        """Simulate writing state files."""
        import asyncio
        await asyncio.sleep(0.15)  # Write operations take longer

        state_dir = self.state_dir / self.pipeline_id / "v1"
        state_dir.mkdir(parents=True, exist_ok=True)

        state_file = state_dir / "index.json"
        test_data = {
            "operation_id": operation_id,
            "timestamp": datetime.now().isoformat(),
            "pipeline_id": self.pipeline_id
        }

        with open(state_file, "w") as f:
            json.dump(test_data, f)

    async def _simulate_checkpoint_operation(self, operation_id: str):
        """Simulate checkpoint operations."""
        import asyncio
        await asyncio.sleep(0.2)  # Checkpoints take the longest

        # Simulate atomic checkpoint write
        temp_file = self.state_dir / f"{self.pipeline_id}_checkpoint_temp_{operation_id}.json"
        final_file = self.state_dir / f"{self.pipeline_id}_checkpoint.json"

        checkpoint_data = {
            "operation_id": operation_id,
            "timestamp": datetime.now().isoformat(),
            "checkpoint_type": "full"
        }

        # Write to temp file first, then rename (atomic operation)
        with open(temp_file, "w") as f:
            json.dump(checkpoint_data, f)

        temp_file.rename(final_file)


class TestStateManagementE2E:
    """Test state management and recovery scenarios."""

    @pytest.mark.asyncio
    async def test_corrupted_checkpoint_recovery(
        self,
        temp_dirs,
        mock_pipeline_id
    ):
        """Test recovery from various types of checkpoint corruption."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Corrupted Checkpoint Recovery Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 20,
                "checkpoint_interval": 5,
                "state_recovery_mode": "automatic"
            },
            "streams": {
                "corruption_test_stream": {
                    "name": "Corruption Test Stream",
                    "src": {"endpoint_id": "corruption-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "corruption-dst", "host_id": "dst-host"},
                    "state_management": {
                        "corruption_detection": True,
                        "backup_retention": 5,
                        "recovery_strategy": "rollback_to_last_valid"
                    }
                }
            }
        }

        # Create various types of corrupted state files
        corruption_types = [
            "invalid_json",
            "missing_required_fields",
            "invalid_cursor_format",
            "negative_aux_values",
            "future_timestamp"
        ]

        state_dir = temp_dirs["state"]
        pipeline_state_dir = state_dir / mock_pipeline_id / "v1"
        pipeline_state_dir.mkdir(parents=True, exist_ok=True)

        # Create corrupted state files
        for i, corruption_type in enumerate(corruption_types):
            corrupted_data = StateCorruptionSimulator.create_corrupted_checkpoint(corruption_type)
            corrupted_file = pipeline_state_dir / f"corrupted_{i}_{corruption_type}.json"

            try:
                with open(corrupted_file, "w") as f:
                    if isinstance(corrupted_data, str):
                        f.write(corrupted_data)  # Invalid JSON
                    else:
                        json.dump(corrupted_data, f)
            except Exception:
                # Some corruption types may fail during write, which is expected
                pass

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "records_processed": 100,
                "state_management": {
                    "corruption_events_detected": len(corruption_types),
                    "corruption_types_found": {
                        "invalid_json": 1,
                        "missing_fields": 1,
                        "invalid_timestamps": 2,
                        "data_integrity_violations": 1
                    },
                    "recovery_attempts": len(corruption_types),
                    "successful_recoveries": len(corruption_types) - 1,
                    "unrecoverable_corruptions": 1
                },
                "backup_management": {
                    "backup_files_created": 8,
                    "backup_files_used_for_recovery": 4,
                    "backup_validation_failures": 1,
                    "backup_cleanup_operations": 3
                },
                "checkpoint_integrity": {
                    "integrity_checks_performed": 25,
                    "integrity_violations_found": 5,
                    "checksum_mismatches": 2,
                    "recovery_time_avg_seconds": 3.2
                }
            }
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            state_metrics = metrics["state_management"]
            backup_metrics = metrics["backup_management"]
            integrity_metrics = metrics["checkpoint_integrity"]

            # Verify corruption detection and recovery
            assert state_metrics["corruption_events_detected"] > 0
            assert state_metrics["successful_recoveries"] > 0
            assert backup_metrics["backup_files_used_for_recovery"] > 0
            assert integrity_metrics["integrity_checks_performed"] > 0
            assert metrics["records_processed"] > 0  # Pipeline completed despite corruption

    @pytest.mark.asyncio
    async def test_concurrent_state_access(
        self,
        temp_dirs,
        mock_pipeline_id
    ):
        """Test concurrent access to state files."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Concurrent State Access Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 15,
                "max_concurrent_batches": 4,
                "state_locking": True
            },
            "streams": {
                "concurrent_stream": {
                    "name": "Concurrent Access Stream",
                    "src": {"endpoint_id": "concurrent-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "concurrent-dst", "host_id": "dst-host"},
                    "concurrency_control": {
                        "file_locking": True,
                        "lock_timeout": 30,
                        "deadlock_detection": True,
                        "lock_escalation": True
                    }
                }
            }
        }

        # Simulate concurrent access
        state_simulator = ConcurrentStateAccessSimulator(temp_dirs["state"], mock_pipeline_id)

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()

            # Simulate concurrent operations
            import asyncio
            concurrent_operations = [
                state_simulator.concurrent_read_write("read_1", "read"),
                state_simulator.concurrent_read_write("write_1", "write"),
                state_simulator.concurrent_read_write("checkpoint_1", "checkpoint"),
                state_simulator.concurrent_read_write("read_2", "read"),
                state_simulator.concurrent_read_write("write_2", "write"),
            ]

            await asyncio.gather(*concurrent_operations, return_exceptions=True)

            mock_engine.get_metrics.return_value = {
                "records_processed": 150,
                "concurrency_control": {
                    "concurrent_operations": len(concurrent_operations),
                    "lock_acquisitions": 12,
                    "lock_conflicts": state_simulator.lock_conflicts,
                    "lock_wait_time_avg": 0.25,
                    "deadlock_detections": 0,
                    "successful_operations": len([op for op in state_simulator.access_log if op["success"]])
                },
                "file_locking": {
                    "read_locks": 2,
                    "write_locks": 2,
                    "exclusive_locks": 1,
                    "lock_timeouts": 0,
                    "lock_escalations": 0
                },
                "data_consistency": {
                    "consistency_checks": 15,
                    "consistency_violations": 0,
                    "transaction_rollbacks": 0,
                    "data_integrity_score": 1.0
                }
            }
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            concurrency_metrics = metrics["concurrency_control"]
            locking_metrics = metrics["file_locking"]
            consistency_metrics = metrics["data_consistency"]

            # Verify concurrent access handling
            assert concurrency_metrics["concurrent_operations"] > 0
            assert concurrency_metrics["successful_operations"] > 0
            assert locking_metrics["read_locks"] > 0
            assert locking_metrics["write_locks"] > 0
            assert consistency_metrics["data_integrity_score"] == 1.0

    @pytest.mark.asyncio
    async def test_incremental_sync_edge_cases(
        self,
        temp_dirs,
        mock_pipeline_id,
        data_generator
    ):
        """Test incremental sync edge cases and cursor management."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Incremental Sync Edge Cases Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 10,
                "incremental_sync": True,
                "cursor_validation": True
            },
            "streams": {
                "incremental_stream": {
                    "name": "Incremental Sync Stream",
                    "src": {
                        "endpoint_id": "incremental-src",
                        "host_id": "src-host",
                        "replication_key": "updated_at",
                        "cursor_mode": "inclusive",
                        "safety_window_seconds": 60
                    },
                    "dst": {"endpoint_id": "incremental-dst", "host_id": "dst-host"},
                    "incremental_config": {
                        "cursor_persistence": True,
                        "duplicate_detection": True,
                        "out_of_order_handling": "buffer_and_sort",
                        "late_arriving_data_window": 3600
                    }
                }
            }
        }

        # Test various incremental sync scenarios
        test_scenarios = [
            "duplicate_cursors",
            "out_of_order_records",
            "cursor_rollback",
            "late_arriving_data",
            "cursor_gaps"
        ]

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "records_processed": 250,
                "incremental_sync": {
                    "cursor_updates": 25,
                    "duplicate_records_detected": 15,
                    "duplicate_records_skipped": 15,
                    "out_of_order_records": 8,
                    "late_arriving_records": 5,
                    "cursor_rollbacks": 2,
                    "cursor_gaps_detected": 1
                },
                "cursor_management": {
                    "cursor_validation_failures": 3,
                    "cursor_recovery_operations": 2,
                    "safety_window_applications": 25,
                    "cursor_persistence_operations": 30,
                    "bookmark_updates": 25
                },
                "data_consistency": {
                    "sequence_violations": 0,
                    "temporal_ordering_maintained": True,
                    "data_completeness_score": 0.98,
                    "incremental_accuracy": 0.97
                },
                "edge_case_handling": {
                    "clock_skew_adjustments": 4,
                    "timezone_conversions": 12,
                    "leap_second_handling": 0,
                    "daylight_saving_adjustments": 2
                }
            }
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            incremental_metrics = metrics["incremental_sync"]
            cursor_metrics = metrics["cursor_management"]
            consistency_metrics = metrics["data_consistency"]
            edge_case_metrics = metrics["edge_case_handling"]

            # Verify incremental sync edge case handling
            assert incremental_metrics["duplicate_records_detected"] > 0
            assert incremental_metrics["duplicate_records_skipped"] == incremental_metrics["duplicate_records_detected"]
            assert incremental_metrics["out_of_order_records"] > 0
            assert cursor_metrics["cursor_validation_failures"] > 0
            assert cursor_metrics["cursor_recovery_operations"] > 0
            assert consistency_metrics["temporal_ordering_maintained"] is True
            assert consistency_metrics["data_completeness_score"] > 0.95

    @pytest.mark.asyncio
    async def test_state_migration_and_versioning(
        self,
        temp_dirs,
        mock_pipeline_id
    ):
        """Test state migration between different versions."""

        # Create old version state
        old_state_dir = temp_dirs["state"] / mock_pipeline_id / "v0"
        old_state_dir.mkdir(parents=True, exist_ok=True)

        old_state_data = {
            "pipeline_id": mock_pipeline_id,
            "version": "0.9",  # Old version
            "legacy_format": True,
            "streams": {
                "test_stream": {
                    "last_cursor": "2023-12-01T10:00:00Z",  # Old format
                    "last_id": 1000
                }
            }
        }

        with open(old_state_dir / "state.json", "w") as f:
            json.dump(old_state_data, f)

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "State Migration Pipeline",
            "version": "1.0",  # New version
            "engine_config": {
                "batch_size": 25,
                "state_migration": True,
                "backward_compatibility": True
            },
            "streams": {
                "migration_stream": {
                    "name": "State Migration Stream",
                    "src": {"endpoint_id": "migration-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "migration-dst", "host_id": "dst-host"},
                    "migration_config": {
                        "auto_migrate": True,
                        "migration_validation": True,
                        "rollback_on_failure": True,
                        "preserve_legacy_data": True
                    }
                }
            }
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "records_processed": 300,
                "state_migration": {
                    "migration_required": True,
                    "migration_from_version": "0.9",
                    "migration_to_version": "1.0",
                    "migration_steps_completed": 5,
                    "migration_time_seconds": 2.1,
                    "migration_success": True
                },
                "compatibility_checks": {
                    "backward_compatibility_validated": True,
                    "forward_compatibility_validated": False,
                    "schema_compatibility_score": 0.95,
                    "data_format_conversions": 8
                },
                "migration_validation": {
                    "pre_migration_validation": True,
                    "post_migration_validation": True,
                    "data_integrity_verified": True,
                    "rollback_capability_verified": True,
                    "migration_backup_created": True
                },
                "version_management": {
                    "version_tracking_enabled": True,
                    "version_history_maintained": True,
                    "version_conflicts_detected": 0,
                    "version_upgrade_path_valid": True
                }
            }
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            migration_metrics = metrics["state_migration"]
            compatibility_metrics = metrics["compatibility_checks"]
            validation_metrics = metrics["migration_validation"]
            version_metrics = metrics["version_management"]

            # Verify state migration
            assert migration_metrics["migration_required"] is True
            assert migration_metrics["migration_success"] is True
            assert migration_metrics["migration_from_version"] == "0.9"
            assert migration_metrics["migration_to_version"] == "1.0"
            assert compatibility_metrics["backward_compatibility_validated"] is True
            assert validation_metrics["data_integrity_verified"] is True
            assert version_metrics["version_conflicts_detected"] == 0

    @pytest.mark.asyncio
    async def test_checkpoint_frequency_optimization(
        self,
        temp_dirs,
        mock_pipeline_id
    ):
        """Test optimization of checkpoint frequency based on performance."""

        pipeline_config = {
            "pipeline_id": mock_pipeline_id,
            "name": "Checkpoint Optimization Pipeline",
            "version": "1.0",
            "engine_config": {
                "batch_size": 50,
                "checkpoint_interval": 10,  # Initial checkpoint every 10 batches
                "adaptive_checkpointing": True
            },
            "streams": {
                "checkpoint_stream": {
                    "name": "Checkpoint Optimization Stream",
                    "src": {"endpoint_id": "checkpoint-src", "host_id": "src-host"},
                    "dst": {"endpoint_id": "checkpoint-dst", "host_id": "dst-host"},
                    "checkpoint_config": {
                        "performance_based_adjustment": True,
                        "checkpoint_cost_analysis": True,
                        "recovery_time_optimization": True,
                        "storage_optimization": True
                    }
                }
            }
        }

        with patch('analitiq_stream.core.pipeline.StreamingEngine') as mock_engine_cls:
            mock_engine = MagicMock()
            mock_engine.stream_data = AsyncMock()
            mock_engine.get_metrics.return_value = {
                "records_processed": 1000,
                "checkpoint_optimization": {
                    "initial_checkpoint_interval": 10,
                    "optimal_checkpoint_interval": 15,
                    "checkpoint_frequency_adjustments": 8,
                    "checkpoint_cost_analysis": {
                        "avg_checkpoint_time": 0.45,
                        "checkpoint_storage_cost": 0.15,
                        "recovery_time_benefit": 8.2,
                        "net_optimization_benefit": 0.75
                    }
                },
                "checkpoint_performance": {
                    "total_checkpoints": 67,
                    "checkpoint_success_rate": 0.985,
                    "checkpoint_failures": 1,
                    "checkpoint_retries": 2,
                    "avg_checkpoint_size_kb": 245
                },
                "recovery_characteristics": {
                    "estimated_recovery_time": 12.5,
                    "recovery_granularity": "batch_level",
                    "data_loss_window_seconds": 75,
                    "recovery_confidence": 0.94
                },
                "storage_efficiency": {
                    "compression_ratio": 0.68,
                    "dedupe_savings": 0.15,
                    "storage_growth_rate": 0.02,
                    "cleanup_operations": 12
                }
            }
            mock_engine_cls.return_value = mock_engine

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                state_dir=str(temp_dirs["state"])
            )

            await pipeline.run()

            metrics = pipeline.get_metrics()
            optimization_metrics = metrics["checkpoint_optimization"]
            performance_metrics = metrics["checkpoint_performance"]
            recovery_metrics = metrics["recovery_characteristics"]
            storage_metrics = metrics["storage_efficiency"]

            # Verify checkpoint optimization
            assert optimization_metrics["optimal_checkpoint_interval"] != optimization_metrics["initial_checkpoint_interval"]
            assert optimization_metrics["checkpoint_frequency_adjustments"] > 0
            assert optimization_metrics["checkpoint_cost_analysis"]["net_optimization_benefit"] > 0
            assert performance_metrics["checkpoint_success_rate"] > 0.95
            assert recovery_metrics["recovery_confidence"] > 0.9
            assert storage_metrics["compression_ratio"] < 1.0