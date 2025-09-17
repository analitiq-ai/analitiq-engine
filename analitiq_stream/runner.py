"""
Pipeline runner for executing Analitiq Stream pipelines.

This module provides the core pipeline execution logic that can be used
both in containerized environments and for testing purposes.
"""

import asyncio
import logging
import os
from datetime import datetime
from typing import Optional, Dict, Any

from .core.pipeline_config_prep import PipelineConfigPrep


logger = logging.getLogger(__name__)


class PipelineRunner:
    """Executes Analitiq Stream pipelines with proper error handling and metrics."""

    def __init__(self, pipeline_id: Optional[str] = None):
        """
        Initialize pipeline runner.

        Args:
            pipeline_id: Pipeline UUID. If None, reads from PIPELINE_ID env var.
        """
        self.pipeline_id = pipeline_id or os.getenv("PIPELINE_ID")
        if not self.pipeline_id:
            raise ValueError("Pipeline ID must be provided or set via PIPELINE_ID env var")

    def validate_environment(self) -> Dict[str, Any]:
        """
        Validate environment configuration.

        Returns:
            Dictionary with environment information.
        """
        env = os.getenv("ENV", "local")

        env_info = {
            "env": env,
            "pipeline_id": self.pipeline_id
        }

        if env != "local":
            env_info.update({
                "aws_region": os.getenv("AWS_REGION", "eu-central-1"),
                "s3_bucket": os.getenv("S3_CONFIG_BUCKET", "analitiq-config")
            })
        else:
            env_info["local_mount"] = os.getenv("LOCAL_CONFIG_MOUNT", "/config")

        logger.info(f"Environment validated: {env_info}")
        return env_info

    async def run(self) -> bool:
        """
        Execute the pipeline.

        Returns:
            True if successful, False if failed.
        """
        try:
            # Validate environment
            self.validate_environment()

            # Create pipeline config prep instance
            logger.info("Initializing PipelineConfigPrep...")
            config_prep = PipelineConfigPrep()

            # Validate environment setup
            logger.info("Validating environment configuration...")
            config_prep.validate_environment()

            # Get pipeline info
            pipeline_info = config_prep.get_pipeline_info()
            logger.info(f"Pipeline Info: {pipeline_info}")

            # Create and run pipeline
            logger.info("Creating pipeline...")
            pipeline = config_prep.create_pipeline()

            logger.info("Starting pipeline execution...")
            start_time = datetime.now()

            # Run the pipeline
            await pipeline.run()

            # Log results
            duration = datetime.now() - start_time
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

            return True

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            return False


async def run_pipeline(pipeline_id: Optional[str] = None) -> bool:
    """
    Convenience function to run a pipeline.

    Args:
        pipeline_id: Pipeline UUID. If None, reads from PIPELINE_ID env var.

    Returns:
        True if successful, False if failed.
    """
    runner = PipelineRunner(pipeline_id)
    return await runner.run()