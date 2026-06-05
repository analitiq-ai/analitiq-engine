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
