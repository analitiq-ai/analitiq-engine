#!/usr/bin/env python3
"""
Docker entrypoint script for Analitiq Stream pipelines.

This script serves as the main entrypoint for containerized pipeline execution.

For cloud environments (dev, prod):
1. Runs config_fetcher.py to fetch configs from Lambda and S3
2. Configs are written to paths defined in analitiq.yaml (single source of truth)
3. Runs the pipeline using the fetched configs

For local environments:
- Runs the pipeline directly using configs from analitiq.yaml paths

Environment Variables:
    ENV: Deployment environment (local, dev, prod)
    PIPELINE_ID: UUID of the pipeline to execute
    CLIENT_ID: UUID of the client (required for cloud environments)
    AWS_REGION: AWS region for DynamoDB and other services (default: eu-central-1)
"""

import asyncio
import logging
import os
import subprocess
import sys
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, '/app')

from src.runner import PipelineRunner


# Set up logging for container environment
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def validate_required_env() -> str:
    """
    Validate that required environment variables are set.

    Returns:
        Environment name (local, dev, prod)
    """
    env = os.getenv("ENV", "local")
    pipeline_id = os.getenv("PIPELINE_ID")

    if not pipeline_id:
        logger.error("PIPELINE_ID environment variable is required")
        sys.exit(1)

    logger.info(f"Environment: {env}")
    logger.info(f"Pipeline ID: {pipeline_id}")

    if env != "local":
        aws_region = os.getenv("AWS_REGION", "eu-central-1")
        client_id = os.getenv("CLIENT_ID")

        if not client_id:
            logger.error("CLIENT_ID environment variable is required for cloud environments")
            sys.exit(1)

        logger.info(f"AWS Region: {aws_region}")
        logger.info(f"Client ID: {client_id}")
        logger.info(f"Invocation ID: {os.getenv('INVOCATION_ID')}")
    else:
        local_mount = os.getenv("LOCAL_CONFIG_MOUNT", "/config")
        logger.info(f"Local Config Mount: {local_mount}")

    return env


def fetch_cloud_configs() -> Path:
    """
    Fetch configurations from DynamoDB/S3 for cloud environments.

    Configs are written to paths defined in analitiq.yaml (single source of truth).

    Returns:
        Path to the pipeline config file

    Raises:
        RuntimeError: If config fetching fails
    """
    logger.info("Fetching configuration from Lambda and S3...")
    logger.info("Config paths determined by analitiq.yaml")

    try:
        result = subprocess.run(
            [
                sys.executable,
                "/app/docker/config_fetcher.py",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        pipeline_path = None
        # Log config fetcher output
        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                if line.startswith("PIPELINE_PATH="):
                    pipeline_path = Path(line.split("=", 1)[1])
                    logger.info(f"Pipeline config path: {pipeline_path}")
                else:
                    logger.info(f"[config_fetcher] {line}")

        logger.info("Configuration fetched successfully")
        return pipeline_path

    except subprocess.CalledProcessError as e:
        logger.error("Failed to fetch configuration:")
        if e.stdout:
            logger.error(f"stdout: {e.stdout}")
        if e.stderr:
            logger.error(f"stderr: {e.stderr}")
        raise RuntimeError(f"Config fetcher failed with exit code {e.returncode}")


async def run_pipeline(env: str):
    """
    Execute the pipeline using PipelineRunner.

    Args:
        env: Environment name (local, dev, prod)
    """
    try:
        # For cloud environments, fetch configs first
        # Configs are written to paths defined in analitiq.yaml
        if env != "local":
            pipeline_path = fetch_cloud_configs()
            logger.info(f"Configuration fetched. Pipeline: {pipeline_path}")

        # Create and run pipeline
        runner = PipelineRunner()
        return await runner.run()

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        logger.info("Troubleshooting tips:")
        logger.info("  - Check environment variables are correctly set")
        logger.info("  - Verify configuration files exist and are accessible")
        logger.info("  - Check network connectivity to external services")
        logger.info("  - Review logs above for specific error details")
        return False


async def main():
    """Main entrypoint for containerized execution."""
    logger.info("=" * 60)
    logger.info("Analitiq Stream Container Starting")
    logger.info("=" * 60)

    try:
        env = validate_required_env()
        success = await run_pipeline(env)
        exit_code = 0 if success else 1

        logger.info("=" * 60)
        if success:
            logger.info("Pipeline completed successfully")
        else:
            logger.info("Pipeline failed")
        logger.info("=" * 60)

        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.info("Container stopped by signal")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())