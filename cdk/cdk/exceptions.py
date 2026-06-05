"""CDK-level exceptions for connector and transport spec validation failures."""


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
