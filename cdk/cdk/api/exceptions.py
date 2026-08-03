"""Errors the API connector family raises for its own failures.

The read-error family (:class:`cdk.exceptions.ReadError` /
:class:`cdk.exceptions.TransientReadError`) deliberately does NOT live
here: SQL and API reads raise it for the same intent, so it sits at the
CDK root where both reach it without either family importing the other.
What is left is what only an HTTP connector can fail at.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from ..exceptions import TransportSpecError, UnresolvedValueError

__all__ = [
    "ApiConnectorError",
    "ConnectorConnectionError",
    "RequestSpecError",
    "request_spec_errors",
]

#: Every way resolving one declared expression can fail. ``Resolver``
#: answers a malformed node with ``TransportSpecError``, a scope it does
#: not know with a plain ``KeyError`` and absent data with
#: ``UnresolvedValueError``; the registered derived functions answer a
#: missing field with ``ValueError`` and one of the wrong type with
#: ``TypeError``; the ``from_param`` binder answers a malformed binding
#: node with ``ValueError``.
#:
#: Listed once, here, because this is the only place that has to know it.
#: Every caller downstream catches :class:`RequestSpecError` alone, so the
#: set can never be half-copied into a catch site that then lets one member
#: through as a raw traceback.
_AUTHORING_DEFECTS = (
    TransportSpecError,
    UnresolvedValueError,
    KeyError,
    TypeError,
    ValueError,
)


class ApiConnectorError(Exception):
    """Base class for failures the API connector family raises."""


class ConnectorConnectionError(ApiConnectorError):
    """The connection to the API could not be established.

    Deliberately not named ``ConnectionError``: that shadows the
    ``OSError`` builtin, and a handler catching the builtin by that name
    would silently stop catching this one.
    """


class RequestSpecError(TransportSpecError):
    """A declared request block cannot be turned into a request to send.

    The one error the request build raises, whatever went wrong inside it:
    an unknown scope, a conflicting pair of expression markers, a derived
    function handed the wrong type, a path placeholder nothing binds. Every
    one of them is a defect in the endpoint document, so every one of them
    is the same thing to the caller -- the read fails the stream, the write
    refuses the schema, the conformance kit reports a finding -- and a
    caller that had to enumerate them would eventually enumerate all but
    one.

    A :class:`~cdk.exceptions.TransportSpecError` because that is exactly
    what it is: a deterministic connector-definition defect. The worker
    already classifies that family as non-retryable, so one escaping a path
    that forgot to catch it still fails loudly rather than being retried
    forever.
    """


@contextmanager
def request_spec_errors(context: str) -> Iterator[None]:
    """Raise :class:`RequestSpecError` for any authoring defect raised inside.

    *context* names what was being built -- the block, the key and the
    endpoint -- because the underlying exception rarely does: a bare
    ``KeyError('connection')`` says nothing about which header asked for it.

    An error already carrying its own context passes through unchanged; a
    second wrap would only repeat what the inner message already says.
    """
    try:
        yield
    except RequestSpecError:
        raise
    except _AUTHORING_DEFECTS as err:
        raise RequestSpecError(f"{context}: {err}") from err
