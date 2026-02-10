#!/usr/bin/env python3
"""
Main entrypoint for Analitiq Stream with dual-mode support.

This module serves as the unified entrypoint for both:
- Engine mode (RUN_MODE=engine): Runs the data pipeline engine
- Destination mode (RUN_MODE=destination): Runs the gRPC destination server

The mode is determined by the RUN_MODE environment variable.

Usage:
    # As engine (default)
    RUN_MODE=engine python -m src.main

    # As destination server
    RUN_MODE=destination python -m src.main

Environment Variables:
    RUN_MODE: "engine" (default) or "destination"

    Common (both modes):
        ENV: Deployment environment (local, dev, prod)
        PIPELINE_ID: UUID of the pipeline to execute
        CLIENT_ID: UUID of the client (required for cloud environments)

    Engine Mode:
        DESTINATION_GRPC_HOST: Hostname of destination gRPC server
        DESTINATION_GRPC_PORT: Port of destination gRPC server (default: 50051)

    Destination Mode:
        GRPC_PORT: Port to listen on (default: 50051)
        DESTINATION_INDEX: Index of destination in pipeline config (default: 0)
"""

import asyncio
import logging
import os
import sys

# Set up logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


async def run_engine_mode() -> bool:
    """
    Run as pipeline engine.

    This mode:
    1. Loads pipeline configuration
    2. Connects to sources and gRPC destination
    3. Executes extract -> transform -> stream flow
    4. Sends shutdown signal to destination on completion
    """
    logger.info("Starting in ENGINE mode")

    # Import here to avoid circular imports
    from src.runner import PipelineRunner

    success = False
    try:
        runner = PipelineRunner()
        success = await runner.run()
    except Exception as e:
        logger.error(f"Engine failed: {e}", exc_info=True)
    finally:
        # Always send shutdown to destination after pipeline completes
        await _send_shutdown_to_destination()

    return success


async def _send_shutdown_to_destination() -> None:
    """
    Send shutdown signal to destination container.

    This signals the destination gRPC server to shut down gracefully,
    allowing both engine and destination containers to exit cleanly.
    """
    grpc_host = os.getenv("DESTINATION_GRPC_HOST")
    if not grpc_host:
        logger.debug("No DESTINATION_GRPC_HOST set, skipping shutdown signal")
        return

    grpc_port = int(os.getenv("DESTINATION_GRPC_PORT", "50051"))

    # Import here to avoid circular imports
    from src.grpc.client import DestinationGRPCClient

    logger.info(f"Sending shutdown signal to destination at {grpc_host}:{grpc_port}")

    client = DestinationGRPCClient(host=grpc_host, port=grpc_port)
    try:
        connected = await client.connect(max_connect_retries=1, retry_delay_seconds=1.0)
        if connected:
            await client.send_shutdown("pipeline_completed")
        else:
            logger.warning("Could not connect to destination to send shutdown signal")
    except Exception as e:
        logger.warning(f"Failed to send shutdown signal: {e}")
    finally:
        await client.disconnect()


async def run_destination_mode() -> None:
    """
    Run as gRPC destination server.

    This mode:
    1. Loads pipeline configuration using PipelineConfigPrep (same as engine)
    2. Extracts destination connection from pipeline config
    3. Initializes the appropriate handler (PostgreSQL, MySQL, etc.)
    4. Starts gRPC server to receive streamed data

    Uses DESTINATION_INDEX to select which destination from the pipeline's
    destinations list (default: 0, the first destination).
    """
    logger.info("Starting in DESTINATION mode")

    # Import here to avoid circular imports
    from src.destination.server import DestinationGRPCServer
    from src.destination.connectors import get_handler
    from src.engine.pipeline_config_prep import PipelineConfigPrep

    grpc_port = int(os.getenv("GRPC_PORT", "50051"))
    destination_index = int(os.getenv("DESTINATION_INDEX", "0"))

    # Load configuration using PipelineConfigPrep (same as engine)
    # This uses PIPELINE_ID and CLIENT_ID env vars
    logger.info("Loading pipeline configuration via PipelineConfigPrep")
    config_prep = PipelineConfigPrep()
    pipeline_config, stream_configs, resolved_connections, resolved_endpoints, _connectors = config_prep.create_config()

    # Get destination connection from pipeline config
    destinations = pipeline_config.connections.destinations
    if not destinations:
        logger.error("Pipeline has no destinations configured")
        sys.exit(1)

    if destination_index >= len(destinations):
        logger.error(
            f"DESTINATION_INDEX={destination_index} is out of range. "
            f"Pipeline has {len(destinations)} destination(s)."
        )
        sys.exit(1)

    # Get the connection ID for the selected destination
    # destinations is a list of dicts like [{"conn_2": "uuid"}, ...]
    dest_dict = destinations[destination_index]
    dest_alias = list(dest_dict.keys())[0]
    connection_id = dest_dict[dest_alias]

    logger.info(f"Using destination index {destination_index}: alias={dest_alias}, connection_id={connection_id}")

    # Get resolved connection config (with secrets already expanded)
    if connection_id not in resolved_connections:
        logger.error(f"Connection {connection_id} not found in resolved connections")
        sys.exit(1)

    resolved_conn = resolved_connections[connection_id]

    # For late-binding connections, resolve now; otherwise use already-resolved config
    if resolved_conn.is_late_binding:
        connection_config = await resolved_conn.resolve_config()
    else:
        connection_config = resolved_conn.config

    # Look up connector_type from connectors array using connector_id
    connector_type = config_prep.get_connector_type(connection_config, connection_id)

    logger.info(f"Connector type: {connector_type}")
    logger.info(f"gRPC port: {grpc_port}")

    # Create handler and start server
    handler = get_handler(connector_type)
    await handler.connect(connection_config)

    server = DestinationGRPCServer(handler, port=grpc_port)

    try:
        await server.start()
        logger.info("Destination server ready to receive streams")
        await server.wait_for_termination()
    finally:
        await handler.disconnect()
        await server.stop()


async def main() -> int:
    """
    Main entrypoint - dispatch based on RUN_MODE.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Initialize run_id if not already set (cloud_entrypoint sets it first)
    from src.shared.run_id import initialize_run_id
    initialize_run_id()

    logger.info("=" * 60)
    logger.info("Analitiq Stream Starting")
    logger.info("=" * 60)

    run_mode = os.getenv("RUN_MODE", "engine").lower()
    logger.info(f"Run mode: {run_mode}")

    try:
        if run_mode == "engine":
            success = await run_engine_mode()
            return 0 if success else 1

        elif run_mode == "destination":
            await run_destination_mode()
            return 0  # Server was terminated gracefully

        else:
            logger.error(f"Unknown RUN_MODE: {run_mode}")
            logger.error("Valid modes: engine, destination")
            return 1

    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
        return 0

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
