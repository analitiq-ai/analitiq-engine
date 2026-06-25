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

from typing import Any, Dict, List, Optional, Tuple

from .exceptions import UnsupportedDialectOperationError

# A discovery query: SQL text with ``?`` placeholders + its positional params.
Query = Tuple[str, List[object]]


class SqlDialect:
    """ANSI-neutral SQL strategy; per-system subclasses live in connector
    packages and override exactly the quirks their system has."""

    #: Dialect identifier (the connector package sets its own).
    name: str = "ansi"
    #: SQLAlchemy registry name used to compile read-path SQL, when it differs
    #: from ``name`` because the system needs a specific driver *flavour*
    #: (e.g. Redshift's ``redshift.redshift_connector`` instead of the
    #: psycopg2-shaped default). ``None`` means "use ``name``" — the common
    #: case for systems whose registered SA dialect name equals ``name``.
    #: This keeps the driver-flavour quirk in the connector, not the engine.
    #: Ignored for built-in dialects (postgresql/mysql/mssql/sqlite), which
    #: the QueryBuilder resolves by ``name`` before consulting this field.
    sqlalchemy_registry_name: Optional[str] = None
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

    # ---- paged reads -------------------------------------------------------
    def paging_order_fallback(self) -> str | None:
        """Raw SQL ordering expression for paged reads that declare no
        ordering (no cursor field, no ``order_by``).

        ANSI SQL accepts ``OFFSET`` without ``ORDER BY``, so the base
        returns ``None`` and no ordering is injected. Systems that refuse
        ``OFFSET`` without ``ORDER BY`` (T-SQL) override this in their
        connector package — returning a no-op expression such as
        ``(SELECT NULL)``, or raising to demand an explicit ordering.

        ``QueryBuilder`` consults the hook lazily, only when a paged query
        actually lacks an ordering, so a raising override never affects
        cursor-ordered reads.
        """
        return None

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

    def build_tls_connect_args(self, mode: str, ca_pem: str | None) -> Dict[str, Any]:
        """Full ``connect_args`` mapping for the driver's TLS configuration.

        This is the hook the transport factory calls. Most drivers take
        their entire TLS configuration as a single ``ssl`` argument, so
        the default places :meth:`build_tls_connect_arg`'s value under
        that key (omitting it when the value is ``None``, meaning "no ssl
        argument at all"). A dialect whose driver spreads TLS over several
        connect parameters (``redshift_connector``: ``ssl: bool`` +
        ``sslmode: str``) overrides this method instead of the singular.
        """
        value = self.build_tls_connect_arg(mode, ca_pem)
        return {} if value is None else {"ssl": value}

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

    # ---- column type rendering (one write surface: type-map-write.json) -----
    def render_column_type(
        self,
        canonical: str,
        type_mapper: Any,
        *,
        params: Any = None,
    ) -> str:
        """Render a canonical Arrow type to this system's native DDL type.

        The default is fully declarative: the connector's
        ``type-map-write.json`` (via ``TypeMapper.to_native_type``) is the
        single write-direction surface for every transport — SQLAlchemy,
        ADBC, and the standalone control-plane ``create_table``. A dialect
        overrides this ONLY when rules cannot express the logic (e.g.
        BigQuery's NUMERIC/BIGNUMERIC precision-range arithmetic); the
        override should handle its special case and delegate the rest here.
        """
        return type_mapper.to_native_type(canonical, params=params)

    def current_timestamp_default(self) -> str:
        """SQL DEFAULT expression for server-stamped timestamp columns.

        ANSI ``CURRENT_TIMESTAMP`` by default. MySQL/MariaDB require the
        expression's fractional-seconds precision to match the column's
        (``CURRENT_TIMESTAMP(6)`` for ``DATETIME(6)``) and reject the bare
        form with error 1067.
        """
        return "CURRENT_TIMESTAMP"

    def batch_commits_key_type(self, type_mapper: Any) -> str:
        """Native type for the ``_batch_commits`` primary-key text columns.

        Defaults to the write map's ``Utf8``. Systems whose unbounded text
        type cannot be a primary key (MySQL/MariaDB: TEXT keys need a
        prefix length) override with a bounded type.
        """
        return self.render_column_type("Utf8", type_mapper)

    def adbc_stage_table_sql(
        self, stage_qualified: str, target_qualified: str
    ) -> str:
        """SQL creating an empty staging table shaped like the target (for
        the ADBC upsert's ingest-to-stage + MERGE). Column-copy syntax is
        vendor-specific; the connector package's dialect implements it."""
        raise UnsupportedDialectOperationError(
            "adbc_stage_table_sql", dialect=self.name
        )

    def adbc_ingest_schema_kwargs(self, schema_name: str) -> Dict[str, Any]:
        """Schema-targeting kwargs for ``cursor.adbc_ingest``.

        ADBC exposes per-statement ingest targeting through the
        ``adbc.ingest.target_db_schema`` option (the ``db_schema_name``
        kwarg). The postgres driver and most others implement it, so the
        base targets the normalized schema explicitly — read and write then
        resolve the same physical schema.

        Drivers that do not implement per-statement ingest targeting
        (Snowflake rejects both ``target_db_schema`` and ``target_catalog``)
        override this to return no kwargs; ingest follows the connection's
        session schema instead, where the stage and target tables already
        live.
        """
        if schema_name:
            return {"db_schema_name": self.normalize_schema(schema_name)}
        return {}

    def adbc_binary_bind(self, value: bytes) -> Tuple[str, Any]:
        """Placeholder SQL + bind value for a binary value in an ADBC
        parameterized statement (the ``_batch_commits.committed_cursor``).

        Default: a plain ``?`` placeholder bound to the raw bytes, which the
        postgres driver and most others accept. Drivers that cannot bind a
        binary parameter (Snowflake rejects it with "Unsupported bind param
        type binary") override this to bind a hex string and convert it back
        in SQL (``TO_BINARY(?, 'HEX')``); the column stays BINARY, so reads
        round-trip to the original bytes either way.
        """
        return "?", value

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
