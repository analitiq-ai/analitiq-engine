"""Errors raised by the CDK SQL control-plane helpers (discovery + DDL).

These are CDK-owned, transport-neutral errors. The engine / control-plane that
drives ``list_schemas`` / ``list_tables`` / ``list_columns`` / ``create_table``
catches them and maps them onto its own surface (a gRPC status, an HTTP code).
"""

from __future__ import annotations


class SqlIntrospectionError(Exception):
    """Base class for SQL control-plane (discovery + create_table) failures."""


class UnsupportedDialectOperationError(SqlIntrospectionError):
    """The active dialect has no implementation for a requested operation.

    The CDK ships only the vendor-neutral ANSI ``SqlDialect`` base; every
    per-system dialect (quoting rules, upsert SQL, ADBC DDL type names,
    stage-table syntax) lives in that system's connector package. Hitting
    this error means the operation needs the connector's own dialect — the
    connector package is either not installed or does not implement the
    operation. Deterministic: retrying cannot succeed.
    """

    def __init__(self, operation: str, *, dialect: str) -> None:
        self.operation = operation
        self.dialect = dialect
        super().__init__(
            f"dialect {dialect!r} does not implement {operation}; this "
            f"operation requires the system's connector package (its "
            f"connector.py ships the dialect that implements it)"
        )


class DiscoveryError(SqlIntrospectionError):
    """A discovery query (schemas / tables / columns) failed to run or parse."""


class CreateTableError(SqlIntrospectionError):
    """Standalone ``create_table`` failed to build or execute its DDL."""


class SchemaConfigurationError(Exception):
    """``configure_schema`` was given input it cannot act on.

    Raised for caller-actionable configuration failures: an unsupported
    proto write mode, an endpoint column missing its ``name`` or
    ``arrow_type``, or a stream state with no endpoint document to build
    DDL from. Deterministic: the same input always fails, so the gRPC
    layer surfaces it in the SchemaAck instead of retrying. Replaces the
    bare ``ValueError`` these paths used to raise (issue #153) so an
    intentional config-error signal is distinguishable from a defect.
    """


class ReadError(Exception):
    """A source read (``read_batches``) could not be set up or executed.

    Raised for caller-actionable read failures: an endpoint document
    missing its target table, an empty column projection, or a transport
    returning named parameters on the qmark-only ADBC path. Distinct from
    the control-plane :class:`SqlIntrospectionError` family — a read is an
    engine source operation, not a control-plane introspection — so the
    engine's extract stage can tell the two apart.
    """
