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

from cdk.registry import ConnectorRegistry, build_registries


def build_worker_registries() -> tuple[ConnectorRegistry, ConnectorRegistry]:
    """Seed the kind defaults and discover installed connector packages."""
    # Imports are local so importing src.worker stays cheap for the shells
    # (they only need the spawn helpers, not the handler graph).
    from cdk.api import GenericAPIConnector
    from cdk.sql.generic import GenericSQLConnector
    from src.destination.connectors import (
        FileDestinationHandler,
        StreamDestinationHandler,
    )

    # ``database`` and ``api`` seed the same class in both registries: one
    # connector serves read and write for those kinds, so a role-specific
    # answer here would be a place for the two directions to drift.
    return build_registries(
        source_builtins={
            "database": GenericSQLConnector,
            "api": GenericAPIConnector,
        },
        destination_builtins={
            "database": GenericSQLConnector,
            "api": GenericAPIConnector,
            "stdout": StreamDestinationHandler,
            "file": FileDestinationHandler,
            "s3": FileDestinationHandler,
        },
        discover=True,
    )
