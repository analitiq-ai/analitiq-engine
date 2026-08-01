"""CDK-level exceptions for connector and transport spec validation failures."""


class ReadError(Exception):
    """A source read could not be set up or executed.

    One class for every transport, at the CDK root: SQL and API reads raise
    it for the same intent -- a caller-actionable read failure the worker
    classifies as deterministic and fails the stream on. Two parallel
    classes existed for that one intent, and every consumer had to list
    both or silently classify one of them as retryable.

    ``declared_category`` carries the connector-declared ``error_map``
    category when the raise site matched one (issue #401), so the declared
    verdict survives to the worker boundary instead of being re-derived
    from the message text.
    """

    def __init__(self, message: str, *, declared_category: str | None = None) -> None:
        super().__init__(message)
        self.declared_category = declared_category


class TransientReadError(Exception):
    """A read failure retrying can heal (rate limit, upstream outage).

    Deliberately NOT a :class:`ReadError` subclass: the worker classifies
    ``ReadError`` as deterministic and fails the stream on it, so a
    hierarchy here would make every rate-limited read fatal.
    """

    def __init__(self, message: str, *, declared_category: str | None = None) -> None:
        super().__init__(message)
        self.declared_category = declared_category


class TransportSpecError(Exception):
    """Raised for deterministic connector / transport-spec validation failures.

    Covers malformed transport definitions, resolution-grammar violations in
    connector templates, missing required fields, and unsupported transport
    kinds — anything that indicates a defective connector or connection
    definition rather than a transient infrastructure failure.

    Listed in ``DETERMINISTIC_CONNECT_ERRORS`` so the connect/read catch-alls
    in ``GenericSQLConnector`` re-raise it unchanged, letting operators
    distinguish "bad connector definition" from "network unreachable".
    """


class UnresolvedValueError(KeyError):
    """A referenced value is absent at resolution time.

    Raised when a ``ref`` path, template placeholder, or lookup input walks
    into a value that simply is not there — the well-formed-expression,
    missing-data case. This is the only failure the per-request resolution
    policy (:meth:`cdk.resolver.Resolver.resolve_for_request`) converts into
    an omitted field; authoring defects (:class:`TransportSpecError`, plain
    ``KeyError`` from elsewhere) propagate in every mode.

    Subclasses ``KeyError`` so strict-mode callers that classify resolution
    failures by ``KeyError`` keep working unchanged.
    """
