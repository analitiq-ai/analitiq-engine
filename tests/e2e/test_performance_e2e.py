"""Performance oriented end-to-end tests using the live streaming engine."""

from __future__ import annotations

import asyncio
from pathlib import Path

from src.engine.pipeline import Pipeline
from src.state.dead_letter_queue import DeadLetterQueue


def _configure_pipeline(pipeline: Pipeline, temp_dirs) -> Path:
    dlq_dir = temp_dirs["deadletter"] / pipeline.config["pipeline_id"]
    dlq_dir.mkdir(parents=True, exist_ok=True)
    pipeline.dlq_dir = str(dlq_dir)
    pipeline.engine.dlq = DeadLetterQueue(str(dlq_dir))
    return dlq_dir


def test_large_batch_processing_generates_expected_metrics(
    temp_dirs,
    mock_pipeline_id,
    source_config,
    destination_config,
):
    """Process a moderately large dataset and verify metrics collected by the engine."""

    stream_id = "performance-stream"
    source_endpoint = "perf-source"
    destination_endpoint = "perf-destination"

    pipeline_config = {
        "pipeline_id": mock_pipeline_id,
        "name": "Performance Pipeline",
        "version": "1.0",
        "runtime": {"batch_size": 10, "max_concurrent_batches": 2},
        "streams": {
            stream_id: {
                "name": "Throughput test",
                "source": {"endpoint_id": source_endpoint},
                "destination": {"endpoint_id": destination_endpoint},
            }
        },
    }

    records = [{"id": i, "value": i} for i in range(50)]
    source_config["endpoint_id"] = source_endpoint
    source_config["records_by_endpoint"][source_endpoint] = records

    destination_config["endpoint_id"] = destination_endpoint

    pipeline = Pipeline(
        pipeline_config=pipeline_config,
        source_config=source_config,
        destination_config=destination_config,
        state_dir=str(temp_dirs["state"]),
    )

    _configure_pipeline(pipeline, temp_dirs)

    asyncio.run(pipeline.run())

    metrics = pipeline.get_metrics()
    assert metrics.records_processed == 50
    assert metrics.batches_processed == 5
    assert metrics.records_failed == 0
    assert metrics.batches_failed == 0

    stored_records = destination_config["storage_by_endpoint"][destination_endpoint]
    assert len(stored_records) == 50
    assert stored_records[0]["id"] == records[0]["id"]
    assert stored_records[-1]["id"] == records[-1]["id"]

    attempt_log = destination_config["attempt_log"][destination_endpoint]
    assert len(attempt_log) == 5
    assert all(entry["batch_size"] == 10 for entry in attempt_log)
