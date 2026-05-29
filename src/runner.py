"""Pipeline runner for executing Analitiq Stream pipelines."""

import logging
import os
from datetime import datetime, timezone

from src import Pipeline
from .engine.pipeline_config_prep import PipelineConfigPrep
from .shared.run_id import get_run_id
from .state.metrics_storage import save_pipeline_metrics


logger = logging.getLogger(__name__)


class PipelineRunner:
    """Executes Analitiq Stream pipelines with proper error handling and metrics."""

    def __init__(self):
        self.pipeline_id = os.getenv("PIPELINE_ID")
        if not self.pipeline_id:
            raise ValueError("PIPELINE_ID environment variable is required")

    async def run(self) -> bool:
        run_id = get_run_id()
        start_time = datetime.now(timezone.utc)

        resolved_pipeline = None
        records_processed = 0
        records_failed = 0
        batches_processed = 0
        status = "failed"
        error_message = None

        try:
            logger.info("Initializing PipelineConfigPrep...")
            config_prep = PipelineConfigPrep()
            resolved_pipeline = config_prep.create_config()

            logger.info(
                f"Starting {resolved_pipeline.display_name or resolved_pipeline.pipeline_id} "
                f"(ID: {resolved_pipeline.pipeline_id})"
            )

            pipeline = Pipeline(pipeline=resolved_pipeline)

            logger.info("Starting pipeline execution...")
            await pipeline.run()

            end_time = datetime.now(timezone.utc)
            duration = end_time - start_time
            metrics = pipeline.get_metrics()

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
                    pipeline_name=(
                        resolved_pipeline.display_name if resolved_pipeline else None
                    ),
                )
                logger.info("Emitted pipeline metrics to logs")
            except Exception as metrics_error:
                logger.error(f"Failed to emit pipeline metrics: {metrics_error}")
