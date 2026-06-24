"""Connector worker: the isolated process that runs connector code.

The shell (engine source side or destination service) spawns this package
as a subprocess (``python -m src.worker``), feeds it a one-shot JSON
bootstrap on stdin, and talks to it over a Unix domain socket. The worker
owns the connector class, the driver, and every external connection; it
never sees the secret store, the config volume, or the engine's state —
everything it may use arrived resolved in the bootstrap.

Registry seeding lives here (not in the engine) because the worker is
where connector classes execute. Built-ins are the generic kind defaults;
installed connector packages add themselves via entry points.
"""

from __future__ import annotations

from typing import Tuple

from cdk.registry import ConnectorRegistry, build_registries


def build_worker_registries() -> Tuple[ConnectorRegistry, ConnectorRegistry]:
    """Seed the kind defaults and discover installed connector packages."""
    # Imports are local so importing src.worker stays cheap for the shells
    # (they only need the spawn helpers, not the handler graph).
    from cdk.sql.generic import GenericSQLConnector
    from src.source.connectors.api import APIConnector
    from src.source.connectors.mongodb import MongoDbSourceConnector
    from src.destination.connectors import (
        ApiDestinationHandler,
        FileDestinationHandler,
        MongoDbDestinationHandler,
        StreamDestinationHandler,
    )

    return build_registries(
        source_builtins={
            "database": GenericSQLConnector,
            "api": APIConnector,
            "nosql": MongoDbSourceConnector,
        },
        destination_builtins={
            "database": GenericSQLConnector,
            "api": ApiDestinationHandler,
            "stdout": StreamDestinationHandler,
            "file": FileDestinationHandler,
            "s3": FileDestinationHandler,
            "nosql": MongoDbDestinationHandler,
        },
        discover=True,
    )
