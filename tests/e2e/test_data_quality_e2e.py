"""E2E tests exercising the real streaming engine with data quality scenarios."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Dict, List

from analitiq_stream.core.pipeline import Pipeline
from analitiq_stream.fault_tolerance.dead_letter_queue import DeadLetterQueue


def _configure_pipeline_directories(pipeline: Pipeline, temp_dirs: Dict[str, Path]) -> Path:
    """Ensure the pipeline writes DLQ files into the ephemeral test directory."""
    dlq_dir = temp_dirs["deadletter"] / pipeline.config["pipeline_id"]
    dlq_dir.mkdir(parents=True, exist_ok=True)
    pipeline.dlq_dir = str(dlq_dir)
    pipeline.engine.dlq = DeadLetterQueue(str(dlq_dir))
    return dlq_dir


def test_invalid_records_routed_to_dlq(
    temp_dirs,
    mock_pipeline_id,
    source_config,
    destination_config,
):
    """Ensure invalid records are rejected and written to the DLQ while valid ones persist."""

    stream_id = "quality-stream"
    source_endpoint = "quality-source"
    destination_endpoint = "quality-destination"

    pipeline_config = {
        "pipeline_id": mock_pipeline_id,
        "name": "Data Quality Pipeline",
        "version": "1.0",
        "engine_config": {"batch_size": 1, "max_concurrent_batches": 1},
        "streams": {
            stream_id: {
                "name": "Data quality checks",
                "src": {"endpoint_id": source_endpoint},
                "dst": {"endpoint_id": destination_endpoint},
            }
        },
    }

    source_config["endpoint_id"] = source_endpoint
    source_config["records_by_endpoint"][source_endpoint] = [
        {"id": 1, "email": "valid@example.com", "name": "Valid One"},
        {"id": 2, "email": None, "name": "Invalid"},
        {"id": 3, "email": "valid2@example.com", "name": "Valid Two"},
    ]
    source_config["records_by_endpoint"][stream_id] = source_config["records_by_endpoint"][source_endpoint]

    destination_config["endpoint_id"] = destination_endpoint
    destination_config["failure_scenarios"][destination_endpoint] = {
        "reject_if_missing": ["email"],
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

    stored_records = destination_config["storage_by_endpoint"][destination_endpoint]
    assert [record["id"] for record in stored_records] == [1, 3]

    attempt_log = destination_config["attempt_log"][destination_endpoint]
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
    mock_pipeline_id,
    source_config,
    destination_config,
):
    """Validate that configured field mappings and transformations run end to end."""

    stream_id = "transformation-stream"
    source_endpoint = "transform-source"
    destination_endpoint = "transform-destination"

    pipeline_config = {
        "pipeline_id": mock_pipeline_id,
        "name": "Transformation Pipeline",
        "version": "1.0",
        "engine_config": {"batch_size": 2, "max_concurrent_batches": 1},
        "streams": {
            stream_id: {
                "name": "Transformation stream",
                "src": {"endpoint_id": source_endpoint},
                "dst": {"endpoint_id": destination_endpoint},
                "mapping": {
                    "field_mappings": {
                        "name": {"target": "full_name", "transformations": ["uppercase"]},
                        "amount": {"target": "amount", "transformations": ["to_float"]},
                    },
                    "computed_fields": {
                        "status": "processed",
                    },
                },
            }
        },
    }

    source_config["endpoint_id"] = source_endpoint
    source_config["records_by_endpoint"][source_endpoint] = [
        {"id": 10, "name": "alice", "amount": "5.5"},
        {"id": 11, "name": "Bob", "amount": "7"},
    ]
    source_config["records_by_endpoint"][stream_id] = source_config["records_by_endpoint"][source_endpoint]

    destination_config["endpoint_id"] = destination_endpoint

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

    stored_records = destination_config["storage_by_endpoint"][destination_endpoint]
    assert stored_records == [
        {"full_name": "ALICE", "amount": 5.5, "status": "processed"},
        {"full_name": "BOB", "amount": 7.0, "status": "processed"},
    ]
