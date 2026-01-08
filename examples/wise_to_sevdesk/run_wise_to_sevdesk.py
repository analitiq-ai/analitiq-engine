#!/usr/bin/env python3
"""
Wise to SevDesk Multi-Stream Sync Pipeline

Example showing how to run multi-stream data sync between Wise and SevDesk
using the Analitiq Stream framework with ID-based connection configuration.

The pipeline uses:
- Connections from: ./connections/
- Secrets from: ./.secrets/
- Connectors from: ../../connectors/

Requirements:
Set these environment variables in your .env file:
- ENV=local (for local testing)
- PIPELINE_ID=22ab7b76-b4df-4c68-8b27-82c307436661

And populate secrets in .secrets/:
- .secrets/{connection_id}.json (API tokens)
"""

import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
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

    # Get the example directory and project root
    example_dir = Path(__file__).parent
    project_root = example_dir.parent.parent

    # Set environment variables for local mode
    os.environ.setdefault("ENV", "local")
    os.environ.setdefault("PIPELINE_ID", "22ab7b76-b4df-4c68-8b27-82c307436661")

    # Create mock config to override analitiq.yaml paths for this example
    mock_config = {
        "paths": {
            "connectors": str(project_root / "connectors"),
            "connections": str(example_dir / "connections"),
            "endpoints": str(example_dir / "endpoints"),
            "streams": str(example_dir / "streams"),
            "pipelines": str(example_dir / "pipelines"),
            "secrets": str(example_dir / ".secrets"),
        }
    }

    # Create settings for the config prep
    settings = PipelineConfigPrepSettings(
        env=os.getenv("ENV", "local"),
        pipeline_id=os.getenv("PIPELINE_ID"),
    )

    # Patch the config loader to use our example-specific paths
    with patch("src.core.pipeline_config_prep.load_analitiq_config", return_value=mock_config):
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
    """Run the multi-stream Wise to SevDesk sync pipeline."""

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
        logger.info("  - Verify connectors/ directory exists at project root")
        logger.info("  - Check network connectivity to APIs")
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
