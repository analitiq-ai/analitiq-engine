"""Destination handler implementations.

The destination service resolves a connector in two steps through the shared
CDK :class:`~cdk.registry.ConnectorRegistry` (ADR §7): the connection's
``connector_id`` (``postgres``, ``mysql``, ``xero``) selects the connector
package's own class when one is installed; otherwise the generic handler for
the connector ``kind`` (``database`` / ``api`` / ``file`` / ``s3`` /
``stdout``) serves it — the thin path. The registry itself is built where
connector classes execute: in the worker, by
:func:`src.worker.build_worker_registries`.
"""

from cdk.sql.generic import GenericSQLConnector
from .stream import StreamDestinationHandler
from .file import FileDestinationHandler
from .api import ApiDestinationHandler
from .mongodb import MongoDbDestinationHandler


__all__ = [
    "GenericSQLConnector",
    "StreamDestinationHandler",
    "FileDestinationHandler",
    "ApiDestinationHandler",
    "MongoDbDestinationHandler",
]
