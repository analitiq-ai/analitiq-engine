#!/usr/bin/env python3
"""
Simple example of using the Analitiq Stream framework with modular configuration.

This example demonstrates:
1. Loading modular configuration files
2. Creating and running a pipeline
3. Monitoring pipeline execution
4. Handling errors and metrics
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

# Add the parent directory to the path so we can import analitiq_stream
sys.path.insert(0, str(Path(__file__).parent.parent))

from analitiq_stream import Pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_json_config(file_path: Path) -> dict:
    """Load JSON configuration from file."""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {file_path}: {e}")
        raise


async def run_simple_pipeline():
    """Run a simple pipeline example using UUID-based configuration."""

    example_dir = Path(__file__).parent

    try:
        # Load UUID-based pipeline configuration
        logger.info("Loading UUID-based pipeline configuration...")

        pipeline_config = load_json_config(example_dir / "pipeline_config.json")

        logger.info(
            f"✓ Loaded pipeline config: {pipeline_config['name']} v{pipeline_config.get('version', '1.0')}"
        )
        logger.info(f"✓ Source endpoint: {pipeline_config['src']['endpoint_id']}")
        logger.info(f"✓ Destination endpoint: {pipeline_config['dst']['endpoint_id']}")
        
        # Check mapping configuration
        mapping_config = pipeline_config.get('mapping', {})
        transformations = mapping_config.get('transformations', [])
        if transformations:
            logger.info(f"✓ Loaded {len(transformations)} transformations")

        # Create pipeline with UUID-based configuration
        pipeline = Pipeline(
            pipeline_config=pipeline_config,
        )

        logger.info(
            f"✓ Pipeline '{pipeline_config['pipeline_id']}' initialized successfully"
        )

        # Run pipeline
        logger.info("Starting pipeline execution...")
        await pipeline.run()

        # Get metrics
        metrics = pipeline.get_metrics()
        logger.info("Pipeline completed successfully!")
        logger.info(f"Final metrics: {metrics}")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise


async def monitor_pipeline_execution():
    """Example of monitoring pipeline execution with custom metrics."""

    example_dir = Path(__file__).parent

    try:
        # Load UUID-based pipeline configuration
        pipeline_config = load_json_config(example_dir / "pipeline_config.json")

        # Create pipeline
        pipeline = Pipeline(
            pipeline_config=pipeline_config,
        )

        # Start pipeline execution in background
        pipeline_task = asyncio.create_task(pipeline.run())

        # Monitor execution
        while not pipeline_task.done():
            await asyncio.sleep(5)  # Check every 5 seconds

            metrics = pipeline.get_metrics()
            status = pipeline.get_status()

            logger.info(
                f"Pipeline {status['pipeline_id']} - Records processed: {metrics.records_processed}"
            )
            logger.info(
                f"Pipeline {status['pipeline_id']} - Batches processed: {metrics.batches_processed}"
            )

            # Check for errors
            if metrics.records_failed > 0:
                logger.warning(
                    f"Pipeline has {metrics.records_failed} failed records"
                )

        # Wait for completion
        await pipeline_task

        # Final metrics
        final_metrics = pipeline.get_metrics()
        logger.info(f"Pipeline completed. Final metrics: {final_metrics}")

    except Exception as e:
        logger.error(f"Pipeline monitoring failed: {str(e)}")
        raise


async def main():
    """Main function to run examples."""

    logger.info("Starting Analitiq Stream modular configuration examples...")

    # Set up environment variables (in real usage, these would be set externally)
    os.environ.setdefault("POSTGRES_PASSWORD", "password")
    os.environ.setdefault("API_TOKEN", "example-token")

    # Create necessary directories
    Path("deadletter").mkdir(exist_ok=True)
    Path("state").mkdir(exist_ok=True)

    try:
        # Run different examples
        logger.info("=" * 50)
        logger.info("Running simple pipeline example...")
        await run_simple_pipeline()

        logger.info("=" * 50)
        logger.info("Running monitored pipeline example...")
        await monitor_pipeline_execution()

        logger.info("=" * 50)
        logger.info("All examples completed successfully!")

    except Exception as e:
        logger.error(f"Example execution failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
