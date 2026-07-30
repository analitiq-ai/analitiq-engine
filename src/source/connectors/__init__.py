"""Source connector implementations.

Connectors handle reading data from various sources.
"""

from .api import APIConnector
from .base import BaseConnector, ConnectorConnectionError, ReadError

__all__ = [
    "BaseConnector",
    "ConnectorConnectionError",
    "ReadError",
    "APIConnector",
]
