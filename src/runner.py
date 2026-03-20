"""
Pipeline runner for executing Analitiq Stream pipelines.

Configuration paths are defined in analitiq.yaml (single source of truth).
Requires PIPELINE_ID environment variable.
"""

import logging
import os
from datetime import datetime, timezone

from src import Pipeline
from .engine.pipeline_config_prep import PipelineConfigPrep
from .shared.run_id import get_run_id
from .state.metrics_storage import save_pipeline_metrics


logger = logging.getLogger(__name__)


class PipelineRunner:
    """Executes Analitiq Stream pipelines with proper error handling and metrics.

    Configuration paths are defined in analitiq.yaml (single source of truth).
    Requires PIPELINE_ID environment variable to be set.
    """

    def __init__(self):
        """
        Initialize pipeline runner.

        Configuration paths are loaded from analitiq.yaml.
        Pipeline ID is read from PIPELINE_ID environment variable.
        """
        self.pipeline_id = os.getenv("PIPELINE_ID")
        if not self.pipeline_id:
            raise ValueError(
                "PIPELINE_ID environment variable is required"
            )

    async def run(self) -> bool:
        """
        Execute the pipeline.

        Returns:
            True if successful, False if failed.
        """
        # Get run_id from centralized source (already initialized at startup)
        run_id = get_run_id()
        start_time = datetime.now(timezone.utc)

        # Initialize variables for metrics
        pipeline_config = None
        records_processed = 0
        records_failed = 0
        batches_processed = 0
        status = "failed"
        error_message = None

        try:
            # Create pipeline config prep instance
            # Configuration paths are loaded from analitiq.yaml
            logger.info("Initializing PipelineConfigPrep...")

            pipeline_config_prep = PipelineConfigPrep()
            pipeline_config, stream_configs, resolved_connections, resolved_endpoints, connectors = pipeline_config_prep.create_config()

            # Create and run pipeline
            logger.info(f"Starting {pipeline_config.name} (ID: {pipeline_config.pipeline_id})")

            # Create pipeline instance
            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                stream_configs=stream_configs,
                resolved_connections=resolved_connections,
                resolved_endpoints=resolved_endpoints,
                connectors=connectors,
            )

            logger.info("Starting pipeline execution...")

            # Run the pipeline
            await pipeline.run()

            # Log results
            end_time = datetime.now(timezone.utc)
            duration = end_time - start_time
            metrics = pipeline.get_metrics()

            logger.info("Pipeline execution completed successfully!")
            logger.info(f"Duration: {duration}")

            # Safely access metrics attributes
            records_processed = getattr(metrics, 'records_processed', 0)
            batches_processed = getattr(metrics, 'batches_processed', 0)
            records_failed = getattr(metrics, 'records_failed', 0)

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
            # Always emit metrics, even on failure
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


