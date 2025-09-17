"""E2E tests validating fault tolerance behaviour of the real streaming engine."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Dict

from analitiq_stream.core.pipeline import Pipeline
from analitiq_stream.fault_tolerance.dead_letter_queue import DeadLetterQueue


def _setup_pipeline_dlq(pipeline: Pipeline, temp_dirs: Dict[str, Path]) -> Path:
    dlq_dir = temp_dirs["deadletter"] / pipeline.config["pipeline_id"]
    dlq_dir.mkdir(parents=True, exist_ok=True)
    pipeline.dlq_dir = str(dlq_dir)
    pipeline.engine.dlq = DeadLetterQueue(str(dlq_dir))
    return dlq_dir


def test_destination_failures_are_captured_in_dlq(
    temp_dirs,
    mock_pipeline_id,
    source_config,
    destination_config,
):
    """A destination raising errors should not crash the pipeline but should populate the DLQ."""

    stream_id = "resilient-stream"
    source_endpoint = "faulty-source"
    destination_endpoint = "faulty-destination"

    pipeline_config = {
        "pipeline_id": mock_pipeline_id,
        "name": "Fault tolerance pipeline",
        "version": "1.0",
        "engine_config": {"batch_size": 1, "max_concurrent_batches": 1},
        "streams": {
            stream_id: {
                "name": "Failure stream",
                "src": {"endpoint_id": source_endpoint},
                "dst": {"endpoint_id": destination_endpoint},
            }
        },
    }

    source_config["endpoint_id"] = source_endpoint
    source_config["records_by_endpoint"][source_endpoint] = [
        {"id": 1, "value": "fail"},
        {"id": 2, "value": "fail"},
    ]

    destination_config["endpoint_id"] = destination_endpoint
    destination_config["failure_scenarios"][destination_endpoint] = {
        "fail_on_attempts": [0, 1],
        "message": "forced failure",
    }

    pipeline = Pipeline(
        pipeline_config=pipeline_config,
        source_config=source_config,
        destination_config=destination_config,
        state_dir=str(temp_dirs["state"]),
    )

    dlq_dir = _setup_pipeline_dlq(pipeline, temp_dirs)

    asyncio.run(pipeline.run())

    metrics = pipeline.get_metrics()
    assert metrics.records_processed == 2
    assert metrics.records_failed == 2
    assert metrics.batches_processed == 2
    assert metrics.batches_failed == 2

    assert destination_endpoint not in destination_config["storage_by_endpoint"]

    dlq_files = sorted(dlq_dir.rglob("dlq_*.jsonl"))
    assert dlq_files, "Failed records should be captured in DLQ files"

    dlq_records = [
        json.loads(line)
        for file_path in dlq_files
        for line in file_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert {record["original_record"]["id"] for record in dlq_records} == {1, 2}


def test_failing_stream_does_not_block_successful_stream(
    temp_dirs,
    mock_pipeline_id,
    source_config,
    destination_config,
):
    """The engine should continue processing healthy streams even if one experiences errors."""

    failing_stream = "failing-stream"
    success_stream = "success-stream"
    failing_source = "failing-source"
    success_source = "success-source"
    failing_destination = "failing-destination"
    success_destination = "success-destination"

    pipeline_config = {
        "pipeline_id": mock_pipeline_id,
        "name": "Mixed reliability pipeline",
        "version": "1.0",
        "engine_config": {"batch_size": 1, "max_concurrent_batches": 2},
        "streams": {
            failing_stream: {
                "name": "always failing",
                "src": {"endpoint_id": failing_source},
                "dst": {"endpoint_id": failing_destination},
            },
            success_stream: {
                "name": "always succeeds",
                "src": {"endpoint_id": success_source},
                "dst": {"endpoint_id": success_destination},
            },
        },
    }

    source_config["records_by_endpoint"] = {
        failing_stream: [{"id": 1, "should_fail": True}],
        success_stream: [
            {"id": 100, "should_fail": False},
            {"id": 101, "should_fail": False},
        ],
    }

    destination_config["endpoint_id"] = "shared-destination"
    destination_config["failure_scenarios"] = {
        "shared-destination": {
            "fail_when_field_equals": {"field": "should_fail", "value": True},
            "message": "expected failure",
        }
    }

    pipeline = Pipeline(
        pipeline_config=pipeline_config,
        source_config=source_config,
        destination_config=destination_config,
        state_dir=str(temp_dirs["state"]),
    )

    dlq_dir = _setup_pipeline_dlq(pipeline, temp_dirs)

    asyncio.run(pipeline.run())

    metrics = pipeline.get_metrics()
    assert metrics.records_processed == 3
    assert metrics.records_failed == 1
    assert metrics.batches_failed == 1

    stored_success = destination_config["storage_by_endpoint"]["shared-destination"]
    assert [record["id"] for record in stored_success] == [100, 101]

    dlq_files = list(dlq_dir.rglob("dlq_*.jsonl"))
    assert dlq_files, "At least one record should be diverted to the DLQ"
