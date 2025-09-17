"""API to API integration scenario executed with the real streaming engine."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict

from analitiq_stream.core.pipeline import Pipeline
from analitiq_stream.fault_tolerance.dead_letter_queue import DeadLetterQueue


def _prepare_pipeline(pipeline: Pipeline, temp_dirs: Dict[str, Path]) -> None:
    dlq_dir = temp_dirs["deadletter"] / pipeline.config["pipeline_id"]
    dlq_dir.mkdir(parents=True, exist_ok=True)
    pipeline.dlq_dir = str(dlq_dir)
    pipeline.engine.dlq = DeadLetterQueue(str(dlq_dir))


def test_api_to_api_stream_applies_nested_field_mapping(
    temp_dirs,
    mock_pipeline_id,
    source_config,
    destination_config,
):
    """Records should be transformed using API-style field mappings and routed to destination."""

    stream_id = "api-stream"
    source_endpoint = "api-source"
    destination_endpoint = "api-destination"

    pipeline_config = {
        "pipeline_id": mock_pipeline_id,
        "name": "API to API pipeline",
        "version": "1.0",
        "engine_config": {"batch_size": 2, "max_concurrent_batches": 1},
        "streams": {
            stream_id: {
                "name": "API stream",
                "src": {"endpoint_id": source_endpoint},
                "dst": {"endpoint_id": destination_endpoint},
                "mapping": {
                    "field_mappings": {
                        "created": {"target": "date", "transformations": ["iso_to_date"]},
                        "details.reference": "reference",
                    },
                    "computed_fields": {"source_system": "wise"},
                },
            }
        },
    }

    source_config["endpoint_id"] = source_endpoint
    source_config["records_by_endpoint"][source_endpoint] = [
        {
            "id": 1,
            "created": "2024-01-01T10:00:00Z",
            "details": {"reference": "Invoice-1"},
        },
        {
            "id": 2,
            "created": "2024-01-02T10:00:00Z",
            "details": {"reference": "Invoice-2"},
        },
    ]
    source_config["records_by_endpoint"][stream_id] = source_config["records_by_endpoint"][source_endpoint]

    destination_config["endpoint_id"] = destination_endpoint

    pipeline = Pipeline(
        pipeline_config=pipeline_config,
        source_config=source_config,
        destination_config=destination_config,
        state_dir=str(temp_dirs["state"]),
    )

    _prepare_pipeline(pipeline, temp_dirs)

    asyncio.run(pipeline.run())

    metrics = pipeline.get_metrics()
    assert metrics.records_processed == 2
    assert metrics.records_failed == 0

    stored_records = destination_config["storage_by_endpoint"][destination_endpoint]
    assert stored_records == [
        {"date": "2024-01-01", "reference": "Invoice-1", "source_system": "wise"},
        {"date": "2024-01-02", "reference": "Invoice-2", "source_system": "wise"},
    ]
