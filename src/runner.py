"""Pipeline runner.

Loads the pipeline via :class:`PipelineConfigPrep` and hands it to a
:class:`StreamingEngine` for execution. Requires the ``PIPELINE_ID``
environment variable.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from .engine.engine import StreamingEngine
from .engine.pipeline_config_prep import PipelineConfigPrep
from .shared.run_id import get_run_id
from .state.metrics_storage import save_pipeline_metrics


logger = logging.getLogger(__name__)


class PipelineRunner:
    """Run a single pipeline end-to-end."""

    def __init__(self) -> None:
        self.pipeline_id = os.environ.get("PIPELINE_ID")
        if not self.pipeline_id:
            raise ValueError("PIPELINE_ID environment variable is required")

    async def run(self) -> bool:
        run_id = get_run_id()
        start_time = datetime.now(timezone.utc)

        pipeline_name = None
        records_processed = 0
        records_failed = 0
        batches_processed = 0
        status = "failed"
        error_message = None

        try:
            logger.info("Initializing PipelineConfigPrep")
            resolved, runtimes, raw_endpoints = PipelineConfigPrep().create_config()
            pipeline_name = resolved.display_name or resolved.pipeline_id

            logger.info("Starting %s (ID: %s)", pipeline_name, resolved.pipeline_id)
            engine = StreamingEngine.from_resolved(resolved)
            await engine.run(resolved, runtimes, raw_endpoints)

            metrics = engine.get_metrics()
            records_processed = getattr(metrics, "records_processed", 0)
            batches_processed = getattr(metrics, "batches_processed", 0)
            records_failed = getattr(metrics, "records_failed", 0)

            duration = datetime.now(timezone.utc) - start_time
            logger.info("Pipeline completed in %s", duration)
            logger.info("Records processed: %d", records_processed)
            logger.info("Batches processed: %d", batches_processed)

            if records_failed > 0:
                logger.warning("Failed records: %d (check dead letter queue)", records_failed)
                status = "partial"
            else:
                status = "success"
            return True

        except Exception as e:
            error_message = str(e)
            logger.error("Pipeline failed: %s", error_message, exc_info=True)
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
                    pipeline_name=pipeline_name,
                )
                logger.info("Emitted pipeline metrics to logs")
            except Exception as metrics_error:
                logger.error("Failed to emit pipeline metrics: %s", metrics_error)
