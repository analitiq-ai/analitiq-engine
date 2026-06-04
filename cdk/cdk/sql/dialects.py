"""The vendor-neutral SQL dialect base — per-system dialects live in packages.

``SqlDialect`` is the **complete extension surface** for everything
vendor-specific in the SQL path: identifier quoting, schema-name
normalization, the PRIMARY KEY clause form, the ``INFORMATION_SCHEMA``
discovery query shapes, the SQLAlchemy upsert statement, the pre-DDL
statements, and the ADBC-only write machinery (native DDL type names and
stage-table syntax).

The CDK ships **only this ANSI-neutral base**. Each connector package ships
its own subclass next to its connector class (``connector.py``), overriding
exactly the quirks its system has — postgres' ``ON CONFLICT`` and ``CREATE
SCHEMA``, BigQuery's backtick quoting and ``NOT ENFORCED`` primary keys,
Snowflake's ``PUBLIC`` folding, and so on. The generic SQL connector never
selects behavior by driver or connector_id; it delegates to the dialect
instance its class carries.

Base behavior is deliberately conservative:

* ANSI machinery (quoting, PK clause, INFORMATION_SCHEMA queries) works
  out of the box, so a thin connector with no package class still reads
  and plain-INSERTs through SQLAlchemy.
* Operations that have no portable form — upsert SQL, ADBC DDL type
  names, stage-table creation — raise
  :class:`~cdk.sql.exceptions.UnsupportedDialectOperationError` so the
  failure is loud, deterministic, and names the missing connector package
  instead of silently degrading.

Discovery queries are emitted with ``?`` (qmark) placeholders and a positional
parameter list. The transport-agnostic executor (:mod:`cdk.sql.execution`)
binds them: qmark straight through on the ADBC path, rewritten to SQLAlchemy
named binds on the SQLAlchemy path.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from .exceptions import UnsupportedDialectOperationError

# A discovery query: SQL text with ``?`` placeholders + its positional params.
Query = Tuple[str, List[object]]


class SqlDialect:
    """ANSI-neutral SQL strategy; per-system subclasses live in connector
    packages and override exactly the quirks their system has."""

    #: Dialect identifier (the connector package sets its own).
    name: str = "ansi"
    #: Identifier quote character. ANSI double-quote by default; backtick for
    #: MySQL/MariaDB and BigQuery (where ``"..."`` is a string literal).
    quote_char: str = '"'
    #: BigQuery does not enforce PK constraints and its parser requires the
    #: ``NOT ENFORCED`` qualifier; every other dialect accepts the bare clause.
    pk_not_enforced: bool = False
    #: Schemas hidden from ``list_schemas`` (catalog/internal schemas).
    system_schemas: Tuple[str, ...] = ()
    #: Whether the dialect implements ``build_sqlalchemy_upsert``.
    supports_upsert_sqlalchemy: bool = False
    #: Whether the dialect supports the ADBC stage-table + MERGE upsert path
    #: (requires ``adbc_stage_table_sql`` and the ADBC DDL type hooks).
    supports_upsert_adbc: bool = False

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
        """Normalize a schema name before it is quoted (no-op by default).

        Shared by the source reader and the destination handler so read and
        write resolve the same physical schema (e.g. Snowflake folds
        unquoted identifiers upper-case, so its dialect maps the
        conventional lowercase ``public`` to the real ``PUBLIC``).
        """
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

    # ---- SQLAlchemy write path ---------------------------------------------
    def build_sqlalchemy_upsert(
        self,
        table: Any,
        records: List[Dict[str, Any]],
        conflict_keys: List[str],
    ) -> Any:
        """Build the dialect's INSERT-or-UPDATE statement for *records*.

        No portable ANSI form exists (postgres ``ON CONFLICT``, MySQL
        ``ON DUPLICATE KEY UPDATE``); the connector package's dialect
        implements it. The base raises so an upsert against a dialect
        without one fails loudly instead of silently degrading to INSERT.
        """
        raise UnsupportedDialectOperationError(
            "build_sqlalchemy_upsert", dialect=self.name
        )

    def build_tls_connect_arg(self, mode: str, ca_pem: str | None) -> Any:
        """Turn the connection's stored TLS mode + CA bundle into the
        driver's connect argument (``connect_args["ssl"]``).

        Each driver speaks its own SSL vocabulary (libpq modes, MySQL
        modes); the connector package's dialect implements it, typically
        via :func:`cdk.transport_factory.ca_ssl_context` for the verifying
        cases. The base raises so a declared TLS mode never silently
        downgrades to no TLS.
        """
        raise UnsupportedDialectOperationError(
            "build_tls_connect_arg", dialect=self.name
        )

    def sqlalchemy_pre_ddl(self, schema_name: str) -> List[str]:
        """Statements to run before ``MetaData.create_all`` (e.g. postgres'
        ``CREATE SCHEMA IF NOT EXISTS`` for a non-default schema). None by
        default."""
        return []

    # ---- schema semantics ----------------------------------------------------
    def schema_is_implicit_default(self, schema_name: str) -> bool:
        """True when *schema_name* is the dialect's implicit default namespace.

        Used to skip ``CREATE SCHEMA`` for namespaces that always exist
        (e.g. postgres/snowflake ``public``). The neutral base only treats
        the empty name as implicit.
        """
        return not schema_name

    # ---- ADBC-only write path (native DDL) -----------------------------------
    def adbc_column_type(self, native_type: str, type_mapper: Any) -> str:
        """Render *native_type* to the dialect's DDL type string via the
        connector's type-map. ADBC-only DDL has no portable form; the
        connector package's dialect implements it."""
        raise UnsupportedDialectOperationError(
            "adbc_column_type", dialect=self.name
        )

    def adbc_synced_at_type(self) -> str:
        """Native timestamp type for the ``_synced_at`` audit column."""
        raise UnsupportedDialectOperationError(
            "adbc_synced_at_type", dialect=self.name
        )

    def adbc_binary_type(self) -> str:
        """Native binary type for ``_batch_commits.committed_cursor``."""
        raise UnsupportedDialectOperationError(
            "adbc_binary_type", dialect=self.name
        )

    def adbc_commit_timestamp_type(self) -> str:
        """Native timestamp type for ``_batch_commits.committed_at``."""
        raise UnsupportedDialectOperationError(
            "adbc_commit_timestamp_type", dialect=self.name
        )

    def adbc_text_type(self) -> str:
        """Native string type for the ``_batch_commits`` text columns."""
        raise UnsupportedDialectOperationError(
            "adbc_text_type", dialect=self.name
        )

    def adbc_stage_table_sql(
        self, stage_qualified: str, target_qualified: str
    ) -> str:
        """SQL creating an empty staging table shaped like the target (for
        the ADBC upsert's ingest-to-stage + MERGE). Column-copy syntax is
        vendor-specific; the connector package's dialect implements it."""
        raise UnsupportedDialectOperationError(
            "adbc_stage_table_sql", dialect=self.name
        )

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
