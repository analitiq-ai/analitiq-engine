"""
Pipeline runner and contract translation for Analitiq Stream pipelines.

This module contains ``PipelineRunner``, which executes a pipeline end-to-end,
and the translation helpers that convert ``ResolvedPipeline``/``ResolvedStream``
contract documents into the flat config dicts consumed by ``StreamingEngine``
and its connectors. Configuration paths are defined in manifest.json.
Requires PIPELINE_ID environment variable.
"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from cdk.connection_runtime import ConnectionRuntime
from src.engine.engine import StreamingEngine
from src.models.resolved import (
    ResolvedDestination,
    ResolvedPipeline,
    ResolvedSource,
    ResolvedStream,
)

from .engine.pipeline_config_prep import PipelineConfigPrep
from .shared.run_id import get_or_generate_run_id
from .state.error_classification import (
    ErrorCode,
    FailureStage,
    classify_for_metrics,
    customer_message,
    detail_for_code,
    is_local_io_error,
    tag_failure,
)
from .state.metrics_storage import save_pipeline_metrics

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Contract -> connector config translation
# ---------------------------------------------------------------------------


def _build_config_dict(
    pipeline_config: ResolvedPipeline,
    stream_configs: list[ResolvedStream],
) -> dict[str, Any]:
    streams: dict[str, dict[str, Any]] = {}

    for stream in stream_configs:
        if not stream.destinations:
            raise ValueError(f"Stream {stream.stream_id!r} has no destinations")
        dest = stream.primary_destination()

        source_config = _translate_source_config(
            stream=stream,
            source=stream.source,
            runtime=stream.source.runtime,
        )
        dest_config = _build_destination_config(dest)

        mapping = stream.mapping or {}
        mapping_config = {
            "assignments": [
                _translate_assignment(a) for a in (mapping.get("assignments") or [])
            ]
        }

        streams[stream.stream_id] = {
            "name": stream.stream_id,
            "stream_version": stream.stream_version,
            "source": source_config,
            "destination": dest_config,
            "mapping": mapping_config,
        }

    return {
        "pipeline_id": pipeline_config.pipeline_id,
        "name": pipeline_config.display_name or pipeline_config.pipeline_id,
        "streams": streams,
    }


def _translate_source_config(
    *,
    stream: ResolvedStream,
    source: ResolvedSource,
    runtime: ConnectionRuntime,
) -> dict[str, Any]:
    """Attach the contract documents to the source-side runtime payload.

    The connectors (API + database) read replication, filters, columns,
    pagination, etc. directly off the contract ``endpoint_document`` and
    ``stream_source``. ``ResolvedSource.to_source_config`` is the one
    serializer of those documents (the endpoint document is dumped back
    to its authored JSON shape there); the translator only injects the
    runtime handle and the connector type discriminator so the engine
    knows which connector class to instantiate.

    All kinds — built-ins (``api``, ``database``, ``file``, ``stdout``)
    and non-built-in kinds — receive the same contract-document
    pass-through; the connectors read the stream's filters directly off
    ``stream_source``. The worker registry raises
    ``ConnectorNotRegisteredError`` at class-resolution time if no
    connector is registered for the given kind.
    """
    _ = stream  # received from _build_config_dict but not needed at this layer
    kind = runtime.connector_type
    base: dict[str, Any] = {
        "connector_type": kind,
        "_resolved_source": source,
        **source.to_source_config(),
    }
    if kind not in ("api", "database", "file", "stdout"):
        logger.warning(
            "Connector kind %r is not a recognised built-in kind; passing "
            "contract documents through as endpoint_document + stream_source. "
            "The worker registry will raise ConnectorNotRegisteredError if "
            "no connector is registered for this kind.",
            kind,
        )
    return base


def _build_destination_config(destination: ResolvedDestination) -> dict[str, Any]:
    """Engine-facing destination dict.

    The engine only needs the write mode (forwarded to the gRPC
    ``SchemaMessage``). Everything else about the destination — table name,
    columns, primary keys, conflict keys, batching — is consumed by the
    destination container, which loads the contract endpoint document via
    ``PipelineConfigPrep``.
    """
    return {"write_mode": destination.write.get("mode", "upsert")}


def _translate_assignment(assignment: dict[str, Any]) -> dict[str, Any]:
    """Translate a contract-shaped assignment to the transformer's shape.

    Contract shape (expression):
        {"target": {"path": "id", "arrow_type": "Int64"},
         "value": {"expression": {"op": "get", "path": "id"}}}

    Transformer shape (expression):
        {"target": {"path": ["id"], "arrow_type": "Int64"},
         "value": {"kind": "expr", "expr": {"op": "get", "path": ["id"]}}}

    Contract shape (constant):
        {"target": {"path": "x"},
         "value": {"constant": {"value": 42, "arrow_type": "Int32"}}}

    Transformer shape (constant):
        {"target": {"path": ["x"]},
         "value": {"kind": "const",
                   "const": {"value": 42, "arrow_type": "Int32"}}}

    Any other ``value`` shape is passed through unchanged.
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

    value: dict[str, Any]
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

    out: dict[str, Any] = {"target": target, "value": value}
    if "validate" in assignment:
        out["validate"] = assignment["validate"]
    return out


# ---------------------------------------------------------------------------


class PipelineRunner:
    """Executes Analitiq Stream pipelines with proper error handling and metrics.

    Configuration paths are defined in manifest.json. Requires PIPELINE_ID
    environment variable (may be supplied via a .env file).
    """

    def __init__(self) -> None:
        load_dotenv()  # must precede os.getenv so .env-based configs work
        pipeline_id = os.getenv("PIPELINE_ID")
        if not pipeline_id:
            raise ValueError("PIPELINE_ID environment variable is required")
        self.pipeline_id: str = pipeline_id
        # Terminal run status ("success" | "partial" | "failed"), set in run()'s
        # finally. Callers read it to decide ledger pruning: only a fully
        # "success" run prunes. A "partial" run (some streams failed or records
        # were dead-lettered) still returns True for exit-code purposes but must
        # keep the ledger so a resume with the same RUN_ID skips committed
        # batches.
        self.status: str = "failed"

    async def run(self) -> bool:
        """Execute the pipeline. Returns True on success, False on failure."""
        run_id = get_or_generate_run_id()
        start_time = datetime.now(timezone.utc)

        pipeline_config = None
        records_processed = 0
        records_failed = 0
        batches_processed = 0
        status = "failed"
        error_code: ErrorCode | None = None
        error_message = None
        error_detail = None
        config_ready = False

        try:
            logger.info("Initializing PipelineConfigPrep...")
            pipeline_config_prep = PipelineConfigPrep()
            (
                pipeline_config,
                stream_configs,
                _resolved_connections,
                _resolved_endpoints,
                _connectors,
            ) = pipeline_config_prep.create_config()

            # Translate the resolved contract into the engine config dict. This
            # still validates config (e.g. a stream with no destinations), so it
            # belongs in the config phase. Done immediately after create_config
            # so the flag below covers config load + translation only -- not the
            # directory/engine setup that follows, whose failures (a read-only
            # filesystem, etc.) are runtime, not config, errors.
            config_dict = _build_config_dict(pipeline_config, stream_configs)
            config_ready = True

            logger.info(
                f"Starting {pipeline_config.name} (ID: {pipeline_config.pipeline_id})"
            )

            # Set up runtime directories
            project_root = Path(__file__).parent.parent
            state_dir = project_root / "state"
            dlq_dir = project_root / "deadletter" / pipeline_config.pipeline_id
            state_dir.mkdir(parents=True, exist_ok=True)
            dlq_dir.mkdir(parents=True, exist_ok=True)

            # Runtime tuning is a single typed RuntimeConfig (defaults resolved
            # in the parser with pipeline-config > env > default precedence).
            engine = StreamingEngine(
                pipeline_id=pipeline_config.pipeline_id,
                runtime=pipeline_config.runtime,
                dlq_path=str(dlq_dir),
            )

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
            streams_failed = getattr(metrics, "streams_failed", 0)

            logger.info(f"Records processed: {records_processed}")
            logger.info(f"Batches processed: {batches_processed}")

            # stream_data only raises when ALL streams fail; a partial run (some
            # streams failed, or records were dead-lettered) returns normally, so
            # classify the dominant cause here rather than reporting success.
            stream_error = engine.get_dominant_stream_error()
            if stream_error is not None:
                # A stream raised (e.g. a source auth/config failure) while
                # others succeeded. Classify that exception.
                status = "partial"
                error_code, error_message, error_detail = classify_for_metrics(
                    stream_error
                )
                logger.warning(
                    f"Partial run: {streams_failed} stream(s) failed "
                    f"[{error_code.value}]"
                )
            elif records_failed > 0:
                status = "partial"
                # A destination failure completed without a raised exception.
                # The engine classified each exhausted batch where it broke
                # (declared category first, text fallback -- issue #351); the
                # dominant across partial streams names the run, so the
                # pipeline-level code matches what the fail strategy would
                # report for the same cause. error_detail carries only
                # allowlisted-safe fields; any DLQ failure_summary stays in
                # the dead-letter queue and logs.
                error_code = (
                    engine.get_partial_error_code()
                    or ErrorCode.DESTINATION_WRITE_FAILED
                )
                error_message = customer_message(error_code)
                # 'skip' drops exhausted batches without a DLQ entry, so those
                # records are NOT recoverable; do not point operators at the DLQ.
                if getattr(metrics, "records_skipped", 0) > 0:
                    logger.warning(
                        f"Skipped {records_failed} records (dropped, not dead-lettered)"
                    )
                    reason = "records skipped (dropped) after retries"
                else:
                    logger.warning(
                        f"Failed records: {records_failed} (check dead letter queue)"
                    )
                    reason = "records dead-lettered after retries"
                error_detail = detail_for_code(
                    error_code,
                    stage=FailureStage.DESTINATION_LOAD,
                    reason=reason,
                )
            else:
                status = "success"

            return True

        except Exception as e:
            # Classify the terminating exception into a stable, customer-safe
            # code here -- the runner is the catch site that sees the failure
            # whole. error_message is customer-safe; error_detail carries only
            # allowlisted-safe structured tokens (stage labels, error codes,
            # exception class names) via classify_for_metrics -- never raw
            # message text.
            if not config_ready and not is_local_io_error(e):
                # A failure before config_ready is config loading/parsing, which
                # surfaces as builtin types (FileNotFoundError, ValueError, ...)
                # the classifier cannot read -- the phase is the reliable signal.
                # Tag it CONFIG_INVALID so classification and the structured
                # detail are produced uniformly through classify_for_metrics. A
                # builtin local-IO error during config load (an unreadable config
                # file) is infra, not bad config, so it is left untagged and the
                # classifier keeps it INTERNAL.
                tag_failure(e, code=ErrorCode.CONFIG_INVALID, stage=FailureStage.CONFIG)
            error_code, error_message, error_detail = classify_for_metrics(e)
            logger.error(
                f"Pipeline failed [{error_code.value}]: {error_detail}", exc_info=True
            )
            return False

        finally:
            # Publish the terminal status for callers (e.g. ledger pruning keys
            # off "success" vs "partial"). On the exception path status stays
            # "failed"; on the normal path it is "success" or "partial".
            self.status = status
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
                    error_code=error_code,
                    error_message=error_message,
                    error_detail=error_detail,
                    pipeline_name=pipeline_config.name if pipeline_config else None,
                )
                logger.info("Emitted pipeline metrics to logs")
            except Exception as metrics_error:
                logger.error(f"Failed to emit pipeline metrics: {metrics_error}")
