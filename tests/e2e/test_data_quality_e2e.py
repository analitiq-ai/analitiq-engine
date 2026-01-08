"""E2E tests exercising the real streaming engine with data quality scenarios."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Dict, List

from src.core.pipeline import Pipeline
from src.fault_tolerance.dead_letter_queue import DeadLetterQueue


def _configure_pipeline_directories(pipeline: Pipeline, temp_dirs: Dict[str, Path]) -> Path:
    """Ensure the pipeline writes DLQ files into the ephemeral test directory."""
    dlq_dir = temp_dirs["deadletter"] / pipeline.config["pipeline_id"]
    dlq_dir.mkdir(parents=True, exist_ok=True)
    pipeline.dlq_dir = str(dlq_dir)
    pipeline.engine.dlq = DeadLetterQueue(str(dlq_dir))
    return dlq_dir


def test_invalid_records_routed_to_dlq(
    temp_dirs,
    source_config,
    destination_config,
    e2e_data_quality_pipeline_config,
):
    """Ensure invalid records are rejected and written to the DLQ while valid ones persist."""

    pipeline_config, stream_id, source_endpoint_id, dest_endpoint_id = e2e_data_quality_pipeline_config

    source_config["endpoint_id"] = source_endpoint_id
    source_config["records_by_endpoint"][source_endpoint_id] = [
        {"id": 1, "email": "valid@example.com", "name": "Valid One"},
        {"id": 2, "email": None, "name": "Invalid"},
        {"id": 3, "email": "valid2@example.com", "name": "Valid Two"},
    ]
    source_config["records_by_endpoint"][stream_id] = source_config["records_by_endpoint"][source_endpoint_id]

    destination_config["endpoint_id"] = dest_endpoint_id
    destination_config["failure_scenarios"][dest_endpoint_id] = {
        "reject_if_missing": ["email_address"],  # Updated to match field mapping in fixture
    }

    pipeline = Pipeline(
        pipeline_config=pipeline_config,
        source_config=source_config,
        destination_config=destination_config,
        state_dir=str(temp_dirs["state"]),
    )

    dlq_dir = _configure_pipeline_directories(pipeline, temp_dirs)

    asyncio.run(pipeline.run())

    metrics = pipeline.get_metrics()
    assert metrics.records_processed == 3
    assert metrics.records_failed == 1
    assert metrics.batches_processed == 3
    assert metrics.batches_failed == 1

    stored_records = destination_config["storage_by_endpoint"][dest_endpoint_id]
    assert [record["user_id"] for record in stored_records] == [1, 3]  # Updated to match field mapping

    attempt_log = destination_config["attempt_log"][dest_endpoint_id]
    assert len(attempt_log) == 3  # one attempt per batch/record

    dlq_files = sorted(dlq_dir.rglob("dlq_*.jsonl"))
    assert dlq_files, "Expected DLQ entries for invalid records"

    dlq_payloads: List[Dict[str, object]] = []
    for file_path in dlq_files:
        dlq_payloads.extend(
            json.loads(line)
            for line in file_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )

    assert any(payload["original_record"]["id"] == 2 for payload in dlq_payloads)


def test_field_transformations_apply_to_valid_records(
    temp_dirs,
    source_config,
    destination_config,
    e2e_pipeline_config_base,
):
    """Validate that configured field mappings and transformations run end to end."""

    import uuid
    stream_id = str(uuid.uuid4())
    source_endpoint_id = str(uuid.uuid4())
    dest_endpoint_id = str(uuid.uuid4())

    # Create custom config for transformation testing
    pipeline_config = e2e_pipeline_config_base.copy()
    pipeline_config["name"] = "Transformation Pipeline"
    pipeline_config["engine_config"]["batch_size"] = 2
    pipeline_config["streams"] = {
        stream_id: {
            "name": "transformation-stream",
            "description": "Stream for testing field transformations",
            "source": {
                "endpoint_id": source_endpoint_id,
                "replication_method": "full_refresh",
                "primary_key": ["id"]
            },
            "destination": {
                "endpoint_id": dest_endpoint_id,
                "refresh_mode": "insert",
                "batch_support": True,
                "batch_size": 2
            },
            "mapping": {
                "field_mappings": {
                    "name": {"target": "full_name", "transformations": ["uppercase"]},
                    "amount": {"target": "amount", "transformations": ["to_float"]},
                },
                "computed_fields": {
                    "status": {
                        "expression": "processed"
                    },
                },
            },
        }
    }

    source_config["endpoint_id"] = source_endpoint_id
    source_config["records_by_endpoint"][source_endpoint_id] = [
        {"id": 10, "name": "alice", "amount": "5.5"},
        {"id": 11, "name": "Bob", "amount": "7"},
    ]
    source_config["records_by_endpoint"][stream_id] = source_config["records_by_endpoint"][source_endpoint_id]

    destination_config["endpoint_id"] = dest_endpoint_id

    pipeline = Pipeline(
        pipeline_config=pipeline_config,
        source_config=source_config,
        destination_config=destination_config,
        state_dir=str(temp_dirs["state"]),
    )

    _configure_pipeline_directories(pipeline, temp_dirs)

    asyncio.run(pipeline.run())

    metrics = pipeline.get_metrics()
    assert metrics.records_processed == 2
    assert metrics.records_failed == 0
    assert metrics.batches_processed == 1

    stored_records = destination_config["storage_by_endpoint"][dest_endpoint_id]
    assert stored_records == [
        {"full_name": "ALICE", "amount": 5.5, "status": "processed"},
        {"full_name": "BOB", "amount": 7.0, "status": "processed"},
    ]
