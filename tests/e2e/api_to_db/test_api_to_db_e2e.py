"""API to database style pipeline exercising the real streaming engine."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict

from src.engine.pipeline import Pipeline
from src.state.dead_letter_queue import DeadLetterQueue


def _wire_pipeline(pipeline: Pipeline, temp_dirs: Dict[str, Path]) -> None:
    dlq_dir = temp_dirs["deadletter"] / pipeline.config["pipeline_id"]
    dlq_dir.mkdir(parents=True, exist_ok=True)
    pipeline.dlq_dir = str(dlq_dir)
    pipeline.engine.dlq = DeadLetterQueue(str(dlq_dir))


def test_api_to_db_pipeline_persists_transformed_payloads(
    temp_dirs,
    mock_pipeline_id,
    source_config,
    destination_config,
):
    """Simulate API ingestion feeding a database style destination."""

    stream_id = "api-db-stream"
    source_endpoint = "api-db-source"
    destination_endpoint = "api-db-destination"

    pipeline_config = {
        "pipeline_id": mock_pipeline_id,
        "name": "API to DB pipeline",
        "version": "1.0",
        "runtime": {"batch_size": 2, "max_concurrent_batches": 1},
        "streams": {
            stream_id: {
                "name": "API to DB",
                "source": {"endpoint_id": source_endpoint},
                "destination": {"endpoint_id": destination_endpoint},
                "mapping": {
                    "field_mappings": {
                        "id": "external_id",
                        "payload.amount": {"target": "amount", "transformations": ["to_float"]},
                    },
                    "computed_fields": {
                        "record_type": "transaction",
                    },
                },
            }
        },
    }

    source_config["endpoint_id"] = source_endpoint
    source_config["records_by_endpoint"][source_endpoint] = [
        {"id": "A1", "payload": {"amount": "10.5"}},
        {"id": "B2", "payload": {"amount": "7.0"}},
    ]
    source_config["records_by_endpoint"][stream_id] = source_config["records_by_endpoint"][source_endpoint]

    destination_config["endpoint_id"] = destination_endpoint

    pipeline = Pipeline(
        pipeline_config=pipeline_config,
        source_config=source_config,
        destination_config=destination_config,
        state_dir=str(temp_dirs["state"]),
    )

    _wire_pipeline(pipeline, temp_dirs)

    asyncio.run(pipeline.run())

    metrics = pipeline.get_metrics()
    assert metrics.records_processed == 2
    assert metrics.records_failed == 0

    stored = destination_config["storage_by_endpoint"][destination_endpoint]
    assert stored == [
        {"external_id": "A1", "amount": 10.5, "record_type": "transaction"},
        {"external_id": "B2", "amount": 7.0, "record_type": "transaction"},
    ]
