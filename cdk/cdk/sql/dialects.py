"""Per-dialect SQL strategy for control-plane discovery + DDL.

This module is the **one** place vendor-specific SQL lives in the CDK. The
generic SQL base (ADR §8) stays vendor-neutral by delegating every dialect
quirk here: identifier quoting, schema-name normalization, the PRIMARY KEY
clause form, and the ``INFORMATION_SCHEMA`` query shapes. Adding a database
("would this change when you add Clickhouse?") is a new ``SqlDialect`` subclass
plus a registry row — never an edit to the generic base.

Discovery queries are emitted with ``?`` (qmark) placeholders and a positional
parameter list. The transport-agnostic executor (:mod:`cdk.sql.execution`)
binds them: qmark straight through on the ADBC path, rewritten to SQLAlchemy
named binds on the SQLAlchemy path. Identifiers that are structural (BigQuery's
dataset sits in the ``FROM`` path, not in a bind) are quoted here instead.
"""

from __future__ import annotations

from typing import List, Tuple

from ..database_utils import normalize_adbc_schema
from .exceptions import UnsupportedDialectError

# A discovery query: SQL text with ``?`` placeholders + its positional params.
Query = Tuple[str, List[object]]


class SqlDialect:
    """ANSI ``INFORMATION_SCHEMA`` strategy (postgres / mysql / snowflake share
    the flat-schema shape; BigQuery overrides it wholesale).

    Subclasses set class attributes for the vendor quirks and, where the query
    shape itself differs, override the ``*_query`` builders.
    """

    name: str = ""
    #: Identifier quote character. ANSI double-quote by default; backtick for
    #: MySQL/MariaDB and BigQuery (where ``"..."`` is a string literal).
    quote_char: str = '"'
    #: BigQuery does not enforce PK constraints and its parser requires the
    #: ``NOT ENFORCED`` qualifier; every other dialect accepts the bare clause.
    pk_not_enforced: bool = False
    #: Schemas hidden from ``list_schemas`` (catalog/internal schemas).
    system_schemas: Tuple[str, ...] = ()

    # ---- identifiers -------------------------------------------------------
    def quote_ident(self, name: str) -> str:
        """Quote a single identifier for this dialect."""
        if self.quote_char == "`":
            if "`" in name:
                raise ValueError(
                    f"{self.name} identifier {name!r} contains a backtick; "
                    "backticks cannot be escaped in identifier names"
                )
            return f"`{name}`"
        return '"' + name.replace('"', '""') + '"'

    def normalize_schema(self, schema: str) -> str:
        """Normalize a schema name before it is quoted (no-op by default)."""
        return schema

    def quote_qualified(self, schema: str, table: str) -> str:
        """Quote a ``schema.table`` (or bare ``table`` when schema is empty)."""
        if schema:
            return (
                f"{self.quote_ident(self.normalize_schema(schema))}"
                f".{self.quote_ident(table)}"
            )
        return self.quote_ident(table)

    def pk_clause(self, columns: List[str]) -> str:
        """Render the table-level ``PRIMARY KEY (...)`` clause."""
        cols = ", ".join(self.quote_ident(c) for c in columns)
        clause = f"PRIMARY KEY ({cols})"
        return f"{clause} NOT ENFORCED" if self.pk_not_enforced else clause

    # ---- discovery queries (qmark placeholders + positional params) --------
    def schemas_query(self) -> Query:
        sql = "SELECT schema_name FROM information_schema.schemata"
        params: List[object] = []
        if self.system_schemas:
            placeholders = ", ".join("?" for _ in self.system_schemas)
            sql += f" WHERE schema_name NOT IN ({placeholders})"
            params.extend(self.system_schemas)
        sql += " ORDER BY schema_name"
        return sql, params

    def tables_query(self, schema: str) -> Query:
        return (
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? ORDER BY table_name",
            [self.normalize_schema(schema)],
        )

    def columns_query(self, schema: str, table: str) -> Query:
        return (
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [self.normalize_schema(schema), table],
        )

    def primary_keys_query(self, schema: str, table: str) -> Query:
        return (
            "SELECT kcu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            " AND tc.table_schema = kcu.table_schema "
            " AND tc.table_name = kcu.table_name "
            "WHERE tc.constraint_type = 'PRIMARY KEY' "
            "  AND tc.table_schema = ? AND tc.table_name = ? "
            "ORDER BY kcu.ordinal_position",
            [self.normalize_schema(schema), table],
        )


class PostgresDialect(SqlDialect):
    """PostgreSQL — also serves Redshift (libpq-compatible, ADBC)."""

    name = "postgresql"
    system_schemas = ("information_schema", "pg_catalog", "pg_toast")

    def schemas_query(self) -> Query:
        # Exclude the catalog schemas plus the per-session temp schemas
        # (``pg_temp_N`` / ``pg_toast_temp_N``) that NOT IN cannot enumerate.
        placeholders = ", ".join("?" for _ in self.system_schemas)
        sql = (
            "SELECT schema_name FROM information_schema.schemata "
            f"WHERE schema_name NOT IN ({placeholders}) "
            "AND schema_name NOT LIKE 'pg_temp_%' "
            "AND schema_name NOT LIKE 'pg_toast_temp_%' "
            "ORDER BY schema_name"
        )
        return sql, list(self.system_schemas)


class MySQLDialect(SqlDialect):
    """MySQL / MariaDB — a schema is a database; identifiers use backticks."""

    name = "mysql"
    quote_char = "`"
    system_schemas = ("information_schema", "mysql", "performance_schema", "sys")


class SnowflakeDialect(SqlDialect):
    """Snowflake — flat ``INFORMATION_SCHEMA`` within the current database."""

    name = "snowflake"
    system_schemas = ("INFORMATION_SCHEMA",)

    def normalize_schema(self, schema: str) -> str:
        # Match the destination/source convention so read and write resolve the
        # same physical schema (unquoted ``public`` folds to ``PUBLIC``).
        return normalize_adbc_schema(schema, "snowflake")


class BigQueryDialect(SqlDialect):
    """BigQuery — ``INFORMATION_SCHEMA`` is dataset-qualified, not flat.

    The dataset is a structural identifier in the ``FROM`` path (backtick
    quoted here), while the table name stays a bind parameter. ``list_schemas``
    reads ``INFORMATION_SCHEMA.SCHEMATA`` resolved against the connection's
    default project; a cross-project/region listing needs an explicit
    ``region-…`` qualifier the connector would supply.
    """

    name = "bigquery"
    quote_char = "`"
    pk_not_enforced = True

    def schemas_query(self) -> Query:
        return (
            "SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA "
            "ORDER BY schema_name",
            [],
        )

    def tables_query(self, schema: str) -> Query:
        dataset = self.quote_ident(schema)
        return (
            f"SELECT table_name FROM {dataset}.INFORMATION_SCHEMA.TABLES "
            "ORDER BY table_name",
            [],
        )

    def columns_query(self, schema: str, table: str) -> Query:
        dataset = self.quote_ident(schema)
        return (
            "SELECT column_name, data_type, is_nullable "
            f"FROM {dataset}.INFORMATION_SCHEMA.COLUMNS "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table],
        )

    def primary_keys_query(self, schema: str, table: str) -> Query:
        dataset = self.quote_ident(schema)
        return (
            "SELECT kcu.column_name "
            f"FROM {dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
            f"JOIN {dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            "WHERE tc.constraint_type = 'PRIMARY KEY' "
            "  AND tc.table_name = ? "
            "ORDER BY kcu.ordinal_position",
            [table],
        )


# Registry: connector ``driver`` string -> dialect strategy. Mirrors the set of
# SQL transports the engine supports today; Redshift rides the postgres
# strategy (libpq-compatible), MariaDB rides the MySQL strategy.
_DIALECTS = {
    "postgresql": PostgresDialect,
    "postgres": PostgresDialect,
    "redshift": PostgresDialect,
    "mysql": MySQLDialect,
    "mariadb": MySQLDialect,
    "snowflake": SnowflakeDialect,
    "bigquery": BigQueryDialect,
}

SUPPORTED_DIALECTS: Tuple[str, ...] = tuple(_DIALECTS)


def get_dialect(driver: str | None) -> SqlDialect:
    """Return the dialect strategy for a runtime's ``driver`` string.

    Raises :class:`UnsupportedDialectError` for a driver with no strategy.
    """
    strategy = _DIALECTS.get((driver or "").lower())
    if strategy is None:
        raise UnsupportedDialectError(driver, supported=SUPPORTED_DIALECTS)
    return strategy()
