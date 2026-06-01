"""Destination handler implementations and registry.

The destination service resolves a connector ``kind`` (``database`` / ``api`` /
``file`` / ``s3`` / ``stdout``) to a handler class through the shared CDK
:class:`~cdk.registry.ConnectorRegistry` (ADR §7). Built-ins are always
available; externally installed connector packages add themselves via the
``analitiq.destination_connectors`` entry-point group.
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


# Built-in destination handlers, keyed by connector kind. The unified SQL
# connector serves every SQL dialect (SQLAlchemy or ADBC, selected from the
# connection's transport); file and s3 share the file handler.
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


def get_handler(connector_type: str) -> BaseDestinationHandler:
    """Instantiate the destination handler for *connector_type* (its kind).

    Raises :class:`~cdk.registry.ConnectorNotRegisteredError` if no handler is
    registered for the kind.
    """
    return destination_registry.create(connector_type)
