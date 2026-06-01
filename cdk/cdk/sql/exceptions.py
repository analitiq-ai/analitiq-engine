"""Errors raised by the CDK SQL control-plane helpers (discovery + DDL).

These are CDK-owned, transport-neutral errors. The engine / control-plane that
drives ``list_schemas`` / ``list_tables`` / ``list_columns`` / ``create_table``
catches them and maps them onto its own surface (a gRPC status, an HTTP code).
"""

from __future__ import annotations


class SqlIntrospectionError(Exception):
    """Base class for SQL control-plane (discovery + create_table) failures."""


class UnsupportedDialectError(SqlIntrospectionError):
    """No dialect strategy is registered for a runtime's driver.

    The CDK ships dialect strategies for the SQL databases the engine's
    transports support today (postgresql, mysql, snowflake, bigquery; redshift
    rides the postgres strategy). A runtime whose ``driver`` is outside that set
    cannot be introspected or have tables created until a strategy is added.
    """

    def __init__(self, driver: str | None, *, supported: tuple[str, ...]) -> None:
        self.driver = driver
        self.supported = supported
        super().__init__(
            f"no SQL dialect strategy for driver {driver!r}; "
            f"supported: {', '.join(supported)}"
        )


class DiscoveryError(SqlIntrospectionError):
    """A discovery query (schemas / tables / columns) failed to run or parse."""


class CreateTableError(SqlIntrospectionError):
    """Standalone ``create_table`` failed to build or execute its DDL."""
