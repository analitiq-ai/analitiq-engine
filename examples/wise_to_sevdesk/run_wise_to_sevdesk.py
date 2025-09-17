#!/usr/bin/env python3
"""
Wise to SevDesk Multi-Stream Sync Pipeline

Simple example showing how to run multi-stream data sync between Wise and SevDesk
using the Analitiq Stream framework with the new PipelineConfigPrep architecture.

Requirements:
Set these environment variables in your .env file:
- ENV=local (for local testing)
- PIPELINE_ID=a1b2c3d4-e5f6-7890-abcd-ef1234567890 (or your pipeline ID)
- WISE_API_TOKEN=your_wise_token
- SEVDESK_API_TOKEN=your_sevdesk_token
- SEVDESK_BANK_ACCOUNT_ID=your_bank_account_id
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the parent directory so we can import analitiq_stream
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from analitiq_stream import Pipeline
from analitiq_stream.core.pipeline_config_prep import PipelineConfigPrep


# Simple logging setup for users
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def create_pipeline() -> Pipeline:
    """Load configuration and create pipeline instance using PipelineConfigPrep."""

    # For local testing, override the config mount to point to this example directory
    if os.getenv("ENV", "local") == "local":
        os.environ["LOCAL_CONFIG_MOUNT"] = str(Path(__file__).parent)

    # Create the config prep instance (will read ENV and PIPELINE_ID from environment)
    config_prep = PipelineConfigPrep()

    # Validate environment setup
    config_prep.validate_environment()

    # Create and return the pipeline
    return config_prep.create_pipeline()


async def run_pipeline():
    """Run the multi-stream Wise to SevDesk sync pipeline."""
    
    try:
        # Create pipeline using common function
        pipeline = create_pipeline()
        
        logger.info("▶️  Starting pipeline execution...")
        start_time = datetime.now()
        
        # Run pipeline (monitoring is controlled by pipeline_config.json)
        await pipeline.run()
        
        # Show results
        duration = datetime.now() - start_time
        metrics = pipeline.get_metrics()
        
        logger.info("✅ Pipeline execution completed successfully!")
        logger.info(f"⏱️  Duration: {duration}")
        
        # Safely access metrics attributes
        records_processed = getattr(metrics, 'records_processed', 0)
        batches_processed = getattr(metrics, 'batches_processed', 0) 
        records_failed = getattr(metrics, 'records_failed', 0)
        
        logger.info(f"📊 Records processed: {records_processed}")
        logger.info(f"📦 Batches processed: {batches_processed}")
        
        if records_failed > 0:
            logger.warning(f"⚠️  Failed records: {records_failed} (check dead letter queue)")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        logger.info("💡 Troubleshooting tips:")
        logger.info("  • Check your .env file has all required API tokens")
        logger.info("  • Verify hosts/*.json and endpoints/*.json files exist")
        logger.info("  • Check network connectivity to APIs")
        logger.info("  • Review logs above for specific error details")
        return False


async def main():
    """Main entry point - keep it simple for users."""
    
    try:
        success = await run_pipeline()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("\n👋 Pipeline stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())