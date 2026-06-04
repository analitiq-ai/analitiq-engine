"""Destination handler implementations and registry.

The destination service resolves a connector in two steps through the shared
CDK :class:`~cdk.registry.ConnectorRegistry` (ADR §7): the connection's
``connector_id`` (``postgres``, ``mysql``, ``xero``) selects the connector
package's own class when one is installed; otherwise the generic handler for
the connector ``kind`` (``database`` / ``api`` / ``file`` / ``s3`` /
``stdout``) serves it — the thin path. Built-in kind defaults are always
available; installed connector packages add themselves via the
``analitiq.destination_connectors`` entry-point group (entry name =
connector_id).
"""

from cdk.base_handler import BaseDestinationHandler
from cdk.registry import build_registries
from cdk.sql.generic import GenericSQLConnector
from .stream import StreamDestinationHandler
from .file import FileDestinationHandler
from .api import ApiDestinationHandler


__all__ = [
    "GenericSQLConnector",
    "StreamDestinationHandler",
    "FileDestinationHandler",
    "ApiDestinationHandler",
    "destination_registry",
    "get_handler",
]


# Built-in kind defaults. The generic SQL connector serves any SQL database
# whose connector package ships no class of its own; file and s3 share the
# file handler.
_DESTINATION_BUILTINS = {
    "database": GenericSQLConnector,
    "api": ApiDestinationHandler,
    "stdout": StreamDestinationHandler,
    "file": FileDestinationHandler,
    "s3": FileDestinationHandler,
}

_, destination_registry = build_registries(
    destination_builtins=_DESTINATION_BUILTINS,
    discover=True,
)


def get_handler(kind: str, connector_id: str) -> BaseDestinationHandler:
    """Instantiate the destination handler for (*kind*, *connector_id*).

    Two-step resolution: the connector package's own class wins when
    installed; otherwise the generic handler for the kind serves the
    connector (the thin path). Raises
    :class:`~cdk.registry.ConnectorNotRegisteredError` when both miss.
    """
    return destination_registry.create(kind, connector_id)
