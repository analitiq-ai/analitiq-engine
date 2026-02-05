"""
Pipeline runner for executing Analitiq Stream pipelines.

This module provides the core pipeline execution logic that can be used
both in containerized environments and for testing purposes.

Configuration paths are defined in analitiq.yaml (single source of truth).

Usage:
    # Via environment variables
    export PIPELINE_ID=b0c2f9d0-3b2a-4a7e-8c86-1b9c6c2d7b15
    python -m src.runner

    # Via command line
    python -m src.runner --pipeline ./pipelines/my_pipeline.json
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src import Pipeline
from .engine.pipeline_config_prep import PipelineConfigPrep, PipelineConfigPrepSettings
from .shared.run_id import get_run_id
from .state.metrics_storage import save_pipeline_metrics


logger = logging.getLogger(__name__)


class PipelineRunner:
    """Executes Analitiq Stream pipelines with proper error handling and metrics.

    Configuration paths are defined in analitiq.yaml (single source of truth).
    """

    def __init__(
        self,
        pipeline_id: Optional[str] = None,
        *,
        pipeline_path: Optional[str] = None,
    ):
        """
        Initialize pipeline runner.

        Configuration paths are loaded from analitiq.yaml.

        Args:
            pipeline_id: Pipeline UUID. If None, reads from PIPELINE_ID env var.
            pipeline_path: Path to pipeline JSON file (alternative to pipeline_id).
        """
        self.pipeline_path = pipeline_path

        if pipeline_path:
            # Extract pipeline_id from path if not provided
            path = Path(pipeline_path)
            self.pipeline_id = pipeline_id or path.stem
        else:
            self.pipeline_id = pipeline_id or os.getenv("PIPELINE_ID")

        if not self.pipeline_id and not self.pipeline_path:
            raise ValueError(
                "Pipeline ID or path must be provided. Use --pipeline flag or set PIPELINE_ID env var"
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

            # Create settings - paths are loaded from analitiq.yaml
            settings = PipelineConfigPrepSettings(
                env=os.getenv("ENV", "local"),
                pipeline_id=self.pipeline_id,
                client_id=os.getenv("CLIENT_ID"),
            )

            pipeline_config_prep = PipelineConfigPrep(settings)
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
                client_id = os.getenv("CLIENT_ID") or (pipeline_config.client_id if pipeline_config else self.pipeline_id)

                save_pipeline_metrics(
                    run_id=run_id,
                    pipeline_id=self.pipeline_id,
                    client_id=client_id,
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


async def run_pipeline(
    pipeline_id: Optional[str] = None,
    *,
    pipeline_path: Optional[str] = None,
) -> bool:
    """
    Convenience function to run a pipeline.

    Configuration paths are loaded from analitiq.yaml.

    Args:
        pipeline_id: Pipeline UUID. If None, reads from PIPELINE_ID env var.
        pipeline_path: Path to pipeline JSON file (alternative to pipeline_id).

    Returns:
        True if successful, False if failed.
    """
    runner = PipelineRunner(
        pipeline_id,
        pipeline_path=pipeline_path,
    )
    return await runner.run()


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run an Analitiq Stream pipeline. Config paths are from analitiq.yaml.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using environment variables
  export PIPELINE_ID=b0c2f9d0-3b2a-4a7e-8c86-1b9c6c2d7b15
  python -m src.runner

  # Using command line arguments
  python -m src.runner --pipeline ./pipelines/my_pipeline.json

  # Using pipeline ID directly
  python -m src.runner --id b0c2f9d0-3b2a-4a7e-8c86-1b9c6c2d7b15
""",
    )

    parser.add_argument(
        "--pipeline", "-p",
        type=str,
        help="Path to pipeline JSON file",
    )
    parser.add_argument(
        "--id",
        type=str,
        help="Pipeline ID (UUID)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    return parser.parse_args()


async def main() -> int:
    """Main entry point for CLI execution."""
    args = parse_args()

    # Set up logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        success = await run_pipeline(
            pipeline_id=args.id,
            pipeline_path=args.pipeline,
        )
        return 0 if success else 1

    except ValueError as e:
        logger.error(str(e))
        return 1
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))