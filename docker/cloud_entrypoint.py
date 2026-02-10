#!/usr/bin/env python3
"""
Cloud entrypoint for Analitiq Stream.

This script runs ONLY in cloud environments (AWS Batch).
It fetches configuration from Lambda/S3, then runs src.main.

Local development should use: python -m src.main
"""

import asyncio
import logging
import os
import sys

log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_configs() -> None:
    """Fetch configurations from Lambda/S3."""
    logger.info("Fetching configuration from Lambda and S3...")

    from docker.config_fetcher import ConfigFetcher

    region = os.getenv("AWS_REGION", "eu-central-1")
    fetcher = ConfigFetcher(region=region)
    pipeline_path = fetcher.fetch_and_write_all()

    logger.info(f"Configuration fetched successfully: {pipeline_path}")


def main() -> None:
    """Cloud entrypoint - fetch configs then run main."""
    logger.info("=" * 60)
    logger.info("Analitiq Stream - Cloud Entrypoint")
    logger.info(f"INVOCATION_ID: {os.getenv('INVOCATION_ID')}")
    logger.info(f"AWS_BATCH_JOB_ID: {os.getenv('AWS_BATCH_JOB_ID')}")
    logger.info("=" * 60)

    env = os.getenv("ENV", "dev").lower()
    if env != "local":
        # Fetch configs from Lambda/S3 (this script only runs in cloud)
        fetch_configs()
    else:
        logger.info(
            "ENV=local; skipping config fetcher and reusing pipelines/*.json"
        )

    # Initialize run_id (uses AWS_BATCH_JOB_ID in cloud)
    from src.shared.run_id import initialize_run_id
    initialize_run_id()

    # Import and run src.main directly (single process, no subprocess overhead)
    logger.info("Running src.main...")
    from src.main import main as app_main
    exit_code = asyncio.run(app_main())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
