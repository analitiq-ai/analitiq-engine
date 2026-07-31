"""Errors the API connector family raises for its own failures.

The read-error family (:class:`cdk.exceptions.ReadError` /
:class:`cdk.exceptions.TransientReadError`) deliberately does NOT live
here: SQL and API reads raise it for the same intent, so it sits at the
CDK root where both reach it without either family importing the other.
What is left is what only an HTTP connector can fail at.
"""

from __future__ import annotations

__all__ = ["ApiConnectorError", "ConnectorConnectionError"]


class ApiConnectorError(Exception):
    """Base class for failures the API connector family raises."""


class ConnectorConnectionError(ApiConnectorError):
    """The connection to the API could not be established.

    Deliberately not named ``ConnectionError``: that shadows the
    ``OSError`` builtin, and a handler catching the builtin by that name
    would silently stop catching this one.
    """
