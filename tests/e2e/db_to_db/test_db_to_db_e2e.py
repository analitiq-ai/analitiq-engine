"""Database to database style pipeline using the live streaming engine."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict

from src.engine.pipeline import Pipeline
from src.state.dead_letter_queue import DeadLetterQueue


def _hook_pipeline(pipeline: Pipeline, temp_dirs: Dict[str, Path]) -> None:
    dlq_dir = temp_dirs["deadletter"] / pipeline.config["pipeline_id"]
    dlq_dir.mkdir(parents=True, exist_ok=True)
    pipeline.dlq_dir = str(dlq_dir)
    pipeline.engine.dlq = DeadLetterQueue(str(dlq_dir))


def test_db_to_db_pipeline_moves_multiple_tables(
    temp_dirs,
    mock_pipeline_id,
    source_config,
    destination_config,
):
    """Simulate two database tables syncing into separate destinations."""

    orders_stream = "orders-stream"
    users_stream = "users-stream"

    pipeline_config = {
        "pipeline_id": mock_pipeline_id,
        "name": "DB to DB pipeline",
        "version": "1.0",
        "runtime": {"buffer_size": 100, "batching": {"batch_size": 2, "max_concurrent_batches": 2}, "logging": {"log_level": "DEBUG", "metrics_enabled": False}, "error_handling": {"strategy": "dlq", "max_retries": 3, "retry_delay": 1}},
        "streams": {
            orders_stream: {
                "name": "orders",
                "source": {"endpoint_id": "orders-source"},
                "destination": {"endpoint_id": "orders-destination"},
            },
            users_stream: {
                "name": "users",
                "source": {"endpoint_id": "users-source"},
                "destination": {"endpoint_id": "users-destination"},
            },
        },
    }

    source_config["records_by_endpoint"] = {
        orders_stream: [
            {"order_id": 1, "total": 100},
            {"order_id": 2, "total": 50},
        ],
        users_stream: [
            {"user_id": 1, "email": "alpha@example.com"},
        ],
    }

    destination_config["endpoint_id"] = "db-destination"
    destination_config["storage_by_endpoint"] = {}

    pipeline = Pipeline(
        pipeline_config=pipeline_config,
        source_config=source_config,
        destination_config=destination_config,
        state_dir=str(temp_dirs["state"]),
    )

    _hook_pipeline(pipeline, temp_dirs)

    asyncio.run(pipeline.run())

    metrics = pipeline.get_metrics()
    assert metrics.records_processed == 3
    assert metrics.records_failed == 0
    assert metrics.streams_processed == 2

    storage = destination_config["storage_by_endpoint"]["db-destination"]
    assert len(storage) == 3
    expected_records = [
        {"order_id": 1, "total": 100},
        {"order_id": 2, "total": 50},
        {"user_id": 1, "email": "alpha@example.com"},
    ]
    assert {frozenset(record.items()) for record in storage} == {
        frozenset(record.items()) for record in expected_records
    }
