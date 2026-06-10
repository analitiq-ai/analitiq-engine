"""Worker entry point: ``python -m src.worker``.

Reads the one-shot bootstrap from stdin, resolves the connector class
through the registry (installed connector packages win over the generic
kind defaults), and serves the role contract on the bootstrap's Unix
domain socket until the shell shuts it down.

The process is launched by a shell (engine source side or destination
service) with a clean environment, its own session, and resource limits —
see ``src.worker.spawn``. Everything secret arrived resolved in the
bootstrap; this process never reads config volumes or the secret store.
"""

from __future__ import annotations

import asyncio
import logging
import sys

from src.worker import build_worker_registries
from src.worker.bootstrap import WorkerBootstrap, read_bootstrap_from_stdin

logger = logging.getLogger("src.worker")


async def _run_destination(bootstrap: WorkerBootstrap) -> int:
    from src.destination.server import DestinationGRPCServer

    _, destination_registry = build_worker_registries()
    handler_cls = destination_registry.resolve(
        bootstrap.kind, bootstrap.connector_id
    )
    handler = handler_cls()
    logger.info(
        "destination worker: %s serves (%s, %s)",
        handler_cls.__name__,
        bootstrap.kind,
        bootstrap.connector_id,
    )
    handler.set_endpoint_refs(bootstrap.endpoint_refs)
    handler.set_stream_endpoints(bootstrap.stream_endpoints)

    runtime = bootstrap.build_runtime()
    await handler.connect(runtime)
    server = DestinationGRPCServer(handler, address=f"unix:{bootstrap.uds_path}")
    try:
        await server.start()
        await server.wait_for_termination()
    finally:
        await handler.disconnect()
    return 0


async def _run_source(bootstrap: WorkerBootstrap) -> int:
    import grpc
    from src.grpc import DEFAULT_MAX_MESSAGE_SIZE
    from src.grpc.generated.analitiq.v1.source_service_pb2_grpc import (
        add_SourceServiceServicer_to_server,
    )
    from src.worker.source_service import SourceWorkerServicer

    source_registry, _ = build_worker_registries()
    readable_cls = source_registry.resolve(bootstrap.kind, bootstrap.connector_id)
    readable = readable_cls()
    logger.info(
        "source worker: %s serves (%s, %s)",
        readable_cls.__name__,
        bootstrap.kind,
        bootstrap.connector_id,
    )

    runtime = bootstrap.build_runtime()
    servicer = SourceWorkerServicer(readable, runtime, bootstrap.source_config)

    server = grpc.aio.server(
        options=[
            ("grpc.max_send_message_length", DEFAULT_MAX_MESSAGE_SIZE),
            ("grpc.max_receive_message_length", DEFAULT_MAX_MESSAGE_SIZE),
        ]
    )
    add_SourceServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"unix:{bootstrap.uds_path}")
    await server.start()
    logger.info("source worker serving on unix:%s", bootstrap.uds_path)
    # The shell terminates the worker (SIGTERM to the process group) when
    # the read finishes or the run is torn down; serve until then.
    await server.wait_for_termination()
    return 0


def main() -> int:
    # Worker logs go to stderr; the shell pipes and redacts them. The
    # bootstrap itself is never logged.
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[worker] %(asctime)s %(name)s %(levelname)s %(message)s",
    )
    bootstrap = read_bootstrap_from_stdin()
    if bootstrap.role == "destination":
        return asyncio.run(_run_destination(bootstrap))
    return asyncio.run(_run_source(bootstrap))


if __name__ == "__main__":
    sys.exit(main())
