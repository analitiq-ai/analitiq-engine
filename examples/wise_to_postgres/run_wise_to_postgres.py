#!/usr/bin/env python3
"""
Wise to PostgreSQL Multi-Stream Sync Pipeline

Simple example showing how to run multi-stream data sync between Wise and PostgreSQL
using the Analitiq Stream framework. This example handles loading and merging 
configuration files before passing them to the Pipeline.

Requirements:
Set these environment variables in your .env file:
- WISE_API_TOKEN=your_wise_token
- DB_HOST=localhost
- DB_PORT=5432
- DB_NAME=wise_data
- DB_USER=your_db_user
- DB_PASSWORD=your_db_password
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the parent directory so we can import analitiq_stream
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from analitiq_stream import Pipeline
from analitiq_stream.core.credentials import CredentialsManager


# Simple logging setup for users
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def load_json_config(file_path: Path, expand_env_vars: bool = False) -> Dict[str, Any]:
    """Load and parse a JSON configuration file.
    
    Args:
        file_path: Path to the JSON file
        expand_env_vars: Whether to expand environment variables like ${VAR_NAME}
    
    Returns:
        Parsed configuration dictionary
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    
    try:
        with open(file_path, "r") as f:
            config = json.load(f)
            
        # Expand environment variables if requested
        if expand_env_vars:
            credentials_manager = CredentialsManager()
            config = credentials_manager._expand_environment_variables(config)
            
        return config
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in file {file_path}: {e}")


def merge_config(pipeline_config: Dict[str, Any], config_type: str) -> Dict[str, Any]:
    """Load and merge endpoint schema with host credentials.
    
    Args:
        pipeline_config: The main pipeline configuration
        config_type: Either 'src' or 'dst' to specify source or destination
    
    Returns:
        Merged configuration dictionary
    """
    # Get pipeline-level host_id
    host_id = pipeline_config.get(config_type, {}).get("host_id")
    if not host_id:
        raise ValueError(f"pipeline_config must contain '{config_type}.host_id'")
    
    # Get endpoint_id from the first stream (assuming all streams use same endpoint)
    streams = pipeline_config.get("streams", {})
    if not streams:
        raise ValueError("No streams configured")
    
    first_stream = next(iter(streams.values()))
    endpoint_id = first_stream.get(config_type, {}).get("endpoint_id")
    if not endpoint_id:
        raise ValueError(f"First stream must contain '{config_type}.endpoint_id'")
    
    # Load endpoint schema and host credentials
    endpoint_config = load_json_config(Path(__file__).parent / f"ep_{endpoint_id}.json")
    host_config = load_json_config(Path(__file__).parent / f"hst_{host_id}.json", expand_env_vars=True)
    
    # Merge configurations (host credentials take precedence)
    merged_config = endpoint_config.copy()
    merged_config.update(host_config)
    
    config_name = "source" if config_type == "src" else "destination"
    logger.debug(f"Merged {config_name} config from endpoint {endpoint_id} and host {host_id}")
    return merged_config


def create_pipeline() -> Pipeline:
    """Load configuration and create pipeline instance."""
    # Load pipeline configuration
    pipeline_config = load_json_config(Path(__file__).parent / "pipeline_config.json")
    
    # Load and merge configurations
    source_config = merge_config(pipeline_config, "src")
    destination_config = merge_config(pipeline_config, "dst")
    
    # Log pipeline information
    pipeline_id = pipeline_config["pipeline_id"]
    pipeline_name = pipeline_config["name"]
    streams = pipeline_config.get("streams", {})
    
    logger.info(f"🚀 Starting {pipeline_name} (ID: {pipeline_id})")
    logger.info(f"📊 Configured streams: {len(streams)}")
    logger.info(f"📡 Source: {source_config.get('base_url', 'Wise API')}")
    logger.info(f"📤 Destination: PostgreSQL ({destination_config.get('host', 'localhost')}:{destination_config.get('port', 5432)})")
    
    # Create and return pipeline
    return Pipeline(
        pipeline_config=pipeline_config,
        source_config=source_config,
        destination_config=destination_config
    )


async def run_pipeline():
    """Run the multi-stream Wise to PostgreSQL sync pipeline."""
    
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
        logger.info("  • Verify hst_*.json and ep_*.json files exist")
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