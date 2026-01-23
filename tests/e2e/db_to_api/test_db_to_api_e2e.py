"""Database to API style pipeline test using the in-memory connectors."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict

from src.engine.pipeline import Pipeline
from src.state.dead_letter_queue import DeadLetterQueue


def _configure(pipeline: Pipeline, temp_dirs: Dict[str, Path]) -> None:
    dlq_dir = temp_dirs["deadletter"] / pipeline.config["pipeline_id"]
    dlq_dir.mkdir(parents=True, exist_ok=True)
    pipeline.dlq_dir = str(dlq_dir)
    pipeline.engine.dlq = DeadLetterQueue(str(dlq_dir))


def test_db_to_api_pipeline_formats_payload_for_destination(
    temp_dirs,
    mock_pipeline_id,
    source_config,
    destination_config,
):
    """Database style records should be converted to API payloads."""

    stream_id = "db-api-stream"
    source_endpoint = "db-api-source"
    destination_endpoint = "db-api-destination"

    pipeline_config = {
        "pipeline_id": mock_pipeline_id,
        "name": "DB to API pipeline",
        "version": "1.0",
        "engine_config": {"batch_size": 1, "max_concurrent_batches": 1},
        "streams": {
            stream_id: {
                "name": "DB to API",
                "source": {"endpoint_id": source_endpoint},
                "destination": {"endpoint_id": destination_endpoint},
                "mapping": {
                    "field_mappings": {
                        "id": "id",
                        "email": {"target": "payload.email", "transformations": ["lowercase"]},
                    },
                    "computed_fields": {"payload.type": "user"},
                },
            }
        },
    }

    source_config["endpoint_id"] = source_endpoint
    source_config["records_by_endpoint"][source_endpoint] = [
        {"id": 1, "email": "USER@EXAMPLE.COM"},
        {"id": 2, "email": "Second@Example.com"},
    ]
    source_config["records_by_endpoint"][stream_id] = source_config["records_by_endpoint"][source_endpoint]

    destination_config["endpoint_id"] = destination_endpoint

    pipeline = Pipeline(
        pipeline_config=pipeline_config,
        source_config=source_config,
        destination_config=destination_config,
        state_dir=str(temp_dirs["state"]),
    )

    _configure(pipeline, temp_dirs)

    asyncio.run(pipeline.run())

    metrics = pipeline.get_metrics()
    assert metrics.records_processed == 2
    assert metrics.records_failed == 0

    stored = destination_config["storage_by_endpoint"][destination_endpoint]
    assert stored == [
        {"id": 1, "payload.email": "user@example.com", "payload.type": "user"},
        {"id": 2, "payload.email": "second@example.com", "payload.type": "user"},
    ]
