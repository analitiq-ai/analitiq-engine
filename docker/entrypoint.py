#!/usr/bin/env python3
"""
Docker entrypoint script for Analitiq Stream pipelines.

This script serves as the main entrypoint for containerized pipeline execution.
It uses the PipelineConfigPrep class to load configurations from either local
filesystem or AWS S3, depending on the ENV variable.

Environment Variables:
    ENV: Deployment environment (local, dev, prod)
    PIPELINE_ID: UUID of the pipeline to execute
    AWS_REGION: AWS region for S3 and other services (default: eu-central-1)
    S3_CONFIG_BUCKET: S3 bucket for configuration files (default: analitiq-config)
    LOCAL_CONFIG_MOUNT: Local mount point for configs (default: /config)
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, '/app')

from analitiq_stream.runner import PipelineRunner


# Set up logging for container environment
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def validate_required_env():
    """Validate that required environment variables are set."""
    env = os.getenv("ENV", "local")
    pipeline_id = os.getenv("PIPELINE_ID")

    if not pipeline_id:
        logger.error("PIPELINE_ID environment variable is required")
        sys.exit(1)

    logger.info(f"Environment: {env}")
    logger.info(f"Pipeline ID: {pipeline_id}")

    if env != "local":
        aws_region = os.getenv("AWS_REGION", "eu-central-1")
        s3_bucket = os.getenv("S3_CONFIG_BUCKET", "analitiq-config")
        logger.info(f"AWS Region: {aws_region}")
        logger.info(f"S3 Config Bucket: {s3_bucket}")
    else:
        local_mount = os.getenv("LOCAL_CONFIG_MOUNT", "/config")
        logger.info(f"Local Config Mount: {local_mount}")


async def run_pipeline():
    """Execute the pipeline using PipelineRunner."""
    try:
        # Validate environment
        validate_required_env()

        # Create and run pipeline
        runner = PipelineRunner()
        return await runner.run()

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        logger.info("Troubleshooting tips:")
        logger.info("  • Check environment variables are correctly set")
        logger.info("  • Verify configuration files exist and are accessible")
        logger.info("  • Check network connectivity to external services")
        logger.info("  • Review logs above for specific error details")
        return False


async def main():
    """Main entrypoint for containerized execution."""
    logger.info("=" * 60)
    logger.info("🚀 Analitiq Stream Container Starting")
    logger.info("=" * 60)

    try:
        success = await run_pipeline()
        exit_code = 0 if success else 1

        logger.info("=" * 60)
        if success:
            logger.info("✅ Pipeline completed successfully")
        else:
            logger.info("❌ Pipeline failed")
        logger.info("=" * 60)

        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.info("👋 Container stopped by signal")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())