"""
Pipeline runner for executing Analitiq Stream pipelines.

Configuration paths are defined in manifest.json (single source of truth).
Requires PIPELINE_ID environment variable.
"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from src.engine.engine import StreamingEngine
from src.models.resolved import ResolvedPipeline, ResolvedStream, ResolvedSource, ResolvedDestination
from cdk.connection_runtime import ConnectionRuntime
from .engine.pipeline_config_prep import PipelineConfigPrep
from .shared.run_id import get_run_id
from .state.metrics_storage import save_pipeline_metrics


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Contract -> connector config translation
# ---------------------------------------------------------------------------


def _build_config_dict(
    pipeline_config: ResolvedPipeline,
    stream_configs: List[ResolvedStream],
) -> Dict[str, Any]:
    streams: Dict[str, Dict[str, Any]] = {}

    for stream in stream_configs:
        if not stream.destinations:
            raise ValueError(f"Stream {stream.stream_id!r} has no destinations")
        dest = stream.primary_destination()

        source_config = _translate_source_config(
            stream=stream,
            source=stream.source,
            endpoint=stream.source.endpoint_document,
            runtime=stream.source.runtime,
        )
        dest_config = _build_destination_config(dest)

        mapping = stream.mapping or {}
        mapping_config = {
            "assignments": [
                _translate_assignment(a)
                for a in (mapping.get("assignments") or [])
            ]
        }

        streams[stream.stream_id] = {
            "name": stream.stream_id,
            "source": source_config,
            "destination": dest_config,
            "mapping": mapping_config,
        }

    return {
        "pipeline_id": pipeline_config.pipeline_id,
        "name": pipeline_config.display_name or pipeline_config.pipeline_id,
        "streams": streams,
        "runtime": pipeline_config.runtime,
    }


def _translate_source_config(
    *,
    stream: ResolvedStream,
    source: ResolvedSource,
    endpoint: Dict[str, Any],
    runtime: ConnectionRuntime,
) -> Dict[str, Any]:
    _ = stream  # signature parity with the destination translator
    kind = runtime.connector_type
    base: Dict[str, Any] = {
        "connector_type": kind,
        "_resolved_source": source,
        "endpoint_ref": source.endpoint_ref.to_dict(),
        "connection_ref": source.connection_ref,
    }
    if kind == "database":
        base.update(_translate_database_source(source, endpoint))
    elif kind == "api":
        base.update(_translate_api_source(source, endpoint, runtime))
    else:
        raise ValueError(
            f"Unsupported source connector kind: {kind!r}; expected 'api' or 'database'"
        )
    return base


def _build_destination_config(destination: ResolvedDestination) -> Dict[str, Any]:
    return {"write_mode": destination.write.get("mode", "upsert")}


def _translate_database_source(
    source: ResolvedSource, endpoint: Dict[str, Any]
) -> Dict[str, Any]:
    return {
        "endpoint_document": endpoint,
        "stream_source": source.stream_source,
    }


def _translate_api_source(
    source: ResolvedSource,
    endpoint: Dict[str, Any],
    runtime: ConnectionRuntime,
) -> Dict[str, Any]:
    _ = runtime  # signature parity with the database path
    return {
        "endpoint_document": endpoint,
        "stream_source": source.stream_source,
        "stream_filters": list(source.stream_source.get("filters") or []),
    }


def _translate_assignment(assignment: Dict[str, Any]) -> Dict[str, Any]:
    """Translate a contract-shaped assignment to the transformer's shape.

    Contract shape:
        {"target": {"path": "id", "arrow_type": "Int64"}, "value": {"expression": {"op": "get", "path": "id"}}}

    Transformer shape:
        {"target": {"path": ["id"], "arrow_type": "Int64"}, "value": {"kind": "expr", "expr": {"op": "get", "path": ["id"]}}}
    """
    raw_target = assignment.get("target") or {}
    raw_value = assignment.get("value") or {}

    target_path_raw = raw_target.get("path", "")
    if isinstance(target_path_raw, str):
        target_path = [seg for seg in target_path_raw.split(".") if seg]
    elif isinstance(target_path_raw, list):
        target_path = list(target_path_raw)
    else:
        target_path = []

    target = dict(raw_target)
    target["path"] = target_path

    value: Dict[str, Any]
    if "expression" in raw_value:
        expression = dict(raw_value["expression"])
        expr_path = expression.get("path")
        if isinstance(expr_path, str):
            expression["path"] = [seg for seg in expr_path.split(".") if seg]
        value = {"kind": "expr", "expr": expression}
    elif "constant" in raw_value:
        value = {"kind": "const", "const": dict(raw_value["constant"] or {})}
    else:
        value = dict(raw_value)

    out: Dict[str, Any] = {"target": target, "value": value}
    if "validate" in assignment:
        out["validate"] = assignment["validate"]
    return out


# ---------------------------------------------------------------------------


class PipelineRunner:
    """Executes Analitiq Stream pipelines with proper error handling and metrics.

    Configuration paths are defined in manifest.json (single source of truth).
    Requires PIPELINE_ID environment variable to be set.
    """

    def __init__(self):
        self.pipeline_id = os.getenv("PIPELINE_ID")
        if not self.pipeline_id:
            raise ValueError("PIPELINE_ID environment variable is required")

    async def run(self) -> bool:
        """
        Execute the pipeline.

        Returns:
            True if successful, False if failed.
        """
        load_dotenv()

        run_id = get_run_id()
        start_time = datetime.now(timezone.utc)

        pipeline_config = None
        records_processed = 0
        records_failed = 0
        batches_processed = 0
        status = "failed"
        error_message = None

        try:
            logger.info("Initializing PipelineConfigPrep...")
            pipeline_config_prep = PipelineConfigPrep()
            pipeline_config, stream_configs, resolved_connections, resolved_endpoints, connectors = (
                pipeline_config_prep.create_config()
            )

            logger.info(f"Starting {pipeline_config.name} (ID: {pipeline_config.pipeline_id})")

            # Set up runtime directories
            project_root = Path(__file__).parent.parent
            dlq_dir = str(project_root / "deadletter" / pipeline_config.pipeline_id)
            Path(dlq_dir).mkdir(parents=True, exist_ok=True)

            # Extract runtime tuning parameters
            runtime = pipeline_config.runtime
            batching = runtime.get("batching") or {"batch_size": 1000, "max_concurrent_batches": 3}
            error_handling = runtime.get("error_handling") or {
                "max_retries": 3,
                "retry_delay_seconds": 5,
            }

            engine = StreamingEngine(
                pipeline_id=pipeline_config.pipeline_id,
                batch_size=batching.get("batch_size", 1000),
                max_concurrent_batches=batching.get("max_concurrent_batches", 3),
                buffer_size=runtime.get("buffer_size", 5000),
                dlq_path=dlq_dir,
                max_retries=error_handling.get("max_retries", 3),
                retry_delay=error_handling.get("retry_delay_seconds", 5),
            )

            config_dict = _build_config_dict(pipeline_config, stream_configs)

            logger.info("Starting pipeline execution...")
            await engine.stream_data(config_dict)

            end_time = datetime.now(timezone.utc)
            duration = end_time - start_time
            metrics = engine.get_metrics()

            logger.info("Pipeline execution completed successfully!")
            logger.info(f"Duration: {duration}")

            records_processed = getattr(metrics, "records_processed", 0)
            batches_processed = getattr(metrics, "batches_processed", 0)
            records_failed = getattr(metrics, "records_failed", 0)

            logger.info(f"Records processed: {records_processed}")
            logger.info(f"Batches processed: {batches_processed}")

            if records_failed > 0:
                logger.warning(f"Failed records: {records_failed} (check dead letter queue)")
                status = "partial"
            else:
                status = "success"

            return True

        except Exception as e:
            error_message = str(e)
            logger.error(f"Pipeline failed: {error_message}", exc_info=True)
            return False

        finally:
            end_time = datetime.now(timezone.utc)
            try:
                save_pipeline_metrics(
                    run_id=run_id,
                    pipeline_id=self.pipeline_id,
                    start_time=start_time,
                    end_time=end_time,
                    records_processed=records_processed,
                    records_failed=records_failed,
                    batches_processed=batches_processed,
                    status=status,
                    error_message=error_message,
                    pipeline_name=pipeline_config.name if pipeline_config else None,
                )
                logger.info("Emitted pipeline metrics to logs")
            except Exception as metrics_error:
                logger.error(f"Failed to emit pipeline metrics: {metrics_error}")
