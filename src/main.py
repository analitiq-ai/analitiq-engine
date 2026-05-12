#!/usr/bin/env python3
"""
Main entrypoint for Analitiq Stream with dual-mode support.

This module serves as the unified entrypoint for both:
- Source mode (RUN_MODE=source): Runs the data pipeline engine
- Destination mode (RUN_MODE=destination): Runs the gRPC destination server

The mode is determined by the RUN_MODE environment variable.

Usage:
    # As engine (default)
    RUN_MODE=source python -m src.main

    # As destination server
    RUN_MODE=destination python -m src.main

Environment Variables:
    RUN_MODE: "source" (default) or "destination"

    Common (both modes):
        PIPELINE_ID: Pipeline alias to execute (matches `pipeline_id` in
            `pipelines/manifest.json`).

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
from typing import Any, Dict

from src.models.stream import WriteConfig, WriteMode

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
        logger.warning(
            "Failed to send shutdown signal: %s: %s",
            type(e).__name__, e, exc_info=True,
        )
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

    if not os.getenv("PIPELINE_ID"):
        logger.warning(
            "PIPELINE_ID not set; destination has nothing to configure. "
            "Exiting gracefully. Set PIPELINE_ID and restart to run."
        )
        return

    # Import here to avoid circular imports
    from src.destination.server import DestinationGRPCServer
    from src.destination.connectors import get_handler
    from src.engine.pipeline_config_prep import PipelineConfigPrep

    grpc_port = int(os.getenv("GRPC_PORT", "50051"))
    destination_index = int(os.getenv("DESTINATION_INDEX", "0"))

    # Load configuration using PipelineConfigPrep (same as engine)
    logger.info("Loading pipeline configuration via PipelineConfigPrep")
    config_prep = PipelineConfigPrep()
    pipeline_config, stream_configs, resolved_connections, resolved_endpoints, _connectors = config_prep.create_config()

    # Get destination connection from pipeline config
    destinations = pipeline_config["connections"]["destinations"]
    if not destinations:
        logger.error("Pipeline has no destinations configured")
        sys.exit(1)

    if destination_index >= len(destinations):
        logger.error(
            f"DESTINATION_INDEX={destination_index} is out of range. "
            f"Pipeline has {len(destinations)} destination(s)."
        )
        sys.exit(1)

    # Get the connection alias for the selected destination
    dest_alias = destinations[destination_index]

    logger.info(f"Using destination index {destination_index}: alias={dest_alias}")

    # Get ConnectionRuntime for selected destination
    if dest_alias not in resolved_connections:
        logger.error(f"Connection '{dest_alias}' not found in resolved connections")
        sys.exit(1)

    runtime = resolved_connections[dest_alias]

    logger.info(f"Connector type: {runtime.connector_type}")
    logger.info(f"gRPC port: {grpc_port}")

    # Build per-stream context for streams that target this destination:
    #   - endpoint_refs: stream_id -> dict-shape EndpointRef payload, used
    #     by the handler to pick the right TypeMapper (connector-scoped
    #     vs connection-scoped endpoints) for each SchemaMessage.
    #   - stream_endpoints: stream_id -> resolved contract endpoint
    #     document. Engine and destination both load these via
    #     PipelineConfigPrep, so handlers read schema details from this
    #     map instead of unpacking them off the wire.
    endpoint_refs: Dict[str, Dict[str, Any]] = {}
    stream_endpoints: Dict[str, Dict[str, Any]] = {}
    for stream in stream_configs:
        for dest in stream.get("destinations", []):
            if dest.get("connection_ref") != dest_alias:
                continue
            stream_id = stream["stream_id"]
            endpoint_refs[stream_id] = dest["endpoint_ref"]
            endpoint_doc = dest.get("_endpoint")
            if endpoint_doc is None:
                logger.error(
                    "Destination for stream %s has no resolved endpoint document; "
                    "PipelineConfigPrep should have populated _endpoint",
                    stream_id,
                )
                sys.exit(1)
            # Resolve effective conflict keys via WriteConfig. The
            # database handler uses a single flat list, so we flatten
            # the first composite returned by the helper. Errors
            # (unknown mode, UPSERT without keys) propagate so a
            # misconfigured pipeline fails at startup instead of
            # silently downgrading UPSERT to INSERT.
            write_block = dest.get("write") or {}
            mode_value = write_block.get("mode") or "upsert"
            primary_keys = list(endpoint_doc.get("primary_keys") or [])
            try:
                write_mode = WriteMode(mode_value)
            except ValueError as e:
                raise ValueError(
                    f"Stream {stream_id!r} destination has unknown write.mode "
                    f"{mode_value!r}; expected one of {[m.value for m in WriteMode]}"
                ) from e
            wc = WriteConfig(
                mode=write_mode,
                conflict_keys=write_block.get("conflict_keys"),
            )
            composites = wc.effective_conflict_keys(primary_keys) or []
            conflict_keys: list[str] = list(composites[0]) if composites else []
            enriched_endpoint = dict(endpoint_doc)
            enriched_endpoint["_write_conflict_keys"] = conflict_keys
            stream_endpoints[stream_id] = enriched_endpoint
            break
    logger.info(
        "Registered %d stream(s) targeting %s",
        len(endpoint_refs),
        dest_alias,
    )

    # Create handler and start server. ``set_endpoint_refs`` and
    # ``set_stream_endpoints`` are defined on ``BaseDestinationHandler``
    # as no-op defaults, so these calls are safe for every handler type
    # and fail loudly if an override is renamed.
    handler = get_handler(runtime.connector_type)
    handler.set_endpoint_refs(endpoint_refs)
    handler.set_stream_endpoints(stream_endpoints)
    await handler.connect(runtime)

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

    run_mode = os.getenv("RUN_MODE", "source").lower()
    logger.info(f"Run mode: {run_mode}")

    try:
        if run_mode == "source":
            success = await run_engine_mode()
            return 0 if success else 1

        elif run_mode == "destination":
            await run_destination_mode()
            return 0  # Server was terminated gracefully

        else:
            logger.error(f"Unknown RUN_MODE: {run_mode}")
            logger.error("Valid modes: source, destination")
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
