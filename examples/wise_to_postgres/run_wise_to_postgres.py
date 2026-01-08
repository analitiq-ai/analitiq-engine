#!/usr/bin/env python3
"""
Wise Transfers to PostgreSQL Pipeline

Example showing how to sync Wise transfers to a PostgreSQL database
using the Analitiq Stream framework with the new path-based connector configuration.

The pipeline uses:
- Shared connectors from: ../../connectors/
- Secrets from: ./secrets/

Requirements:
Set these environment variables in your .env file:
- ENV=local (for local testing)
- PIPELINE_ID=c1d2e3f4-a5b6-7890-cdef-123456789012

And populate secrets in secrets/:
- secrets/wise.json (Wise API token)
- secrets/postgres.json (PostgreSQL credentials)
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the parent directory so we can import src
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src import Pipeline
from src.core.pipeline_config_prep import PipelineConfigPrep, PipelineConfigPrepSettings


# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def create_pipeline() -> Pipeline:
    """Load configuration and create pipeline instance using PipelineConfigPrep."""

    # For local testing, override the config mount to point to this example directory
    example_dir = str(Path(__file__).parent)

    # Set environment variables for local mode
    os.environ.setdefault("ENV", "local")
    os.environ.setdefault("PIPELINE_ID", "c1d2e3f4-a5b6-7890-cdef-123456789012")
    os.environ.setdefault("LOCAL_CONFIG_MOUNT", example_dir)

    # Create settings for the config prep
    settings = PipelineConfigPrepSettings(
        env=os.getenv("ENV", "local"),
        pipeline_id=os.getenv("PIPELINE_ID"),
        local_config_mount=example_dir,
    )

    # Create the config prep instance
    config_prep = PipelineConfigPrep(settings)

    # Load pipeline and stream configurations
    pipeline_config, stream_configs, resolved_connections, resolved_endpoints = config_prep.create_config()

    logger.info(f"Loaded pipeline: {pipeline_config.name}")
    logger.info(f"Source connection: {pipeline_config.connections.source}")
    logger.info(f"Destination connections: {pipeline_config.connections.destinations}")
    logger.info(f"Streams: {len(stream_configs)}")
    for stream in stream_configs:
        logger.info(f"  - {stream.stream_id}: {stream.source.endpoint_id}")

    # Create and return the pipeline
    return Pipeline(
        pipeline_config=pipeline_config,
        stream_configs=stream_configs,
        resolved_connections=resolved_connections,
        resolved_endpoints=resolved_endpoints,
    )


async def run_pipeline():
    """Run the Wise to PostgreSQL sync pipeline."""

    try:
        # Create pipeline using common function
        pipeline = create_pipeline()

        logger.info("Starting pipeline execution...")
        start_time = datetime.now()

        # Run pipeline
        await pipeline.run()

        # Show results
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
        logger.error(f"Pipeline failed: {str(e)}")
        logger.info("Troubleshooting tips:")
        logger.info("  - Check your secrets/ directory has valid tokens")
        logger.info("  - Verify PostgreSQL is running on localhost:5432")
        logger.info("  - Check network connectivity to Wise API")
        logger.info("  - Verify connectors/ directory exists at project root")
        logger.info("  - Review logs above for specific error details")
        return False


async def main():
    """Main entry point."""

    try:
        success = await run_pipeline()
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("\nPipeline stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())