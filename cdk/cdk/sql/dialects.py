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

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from .exceptions import UnsupportedDialectOperationError

if TYPE_CHECKING:
    from cdk.type_map.mapper import TypeMapper

# A discovery query: SQL text with ``?`` placeholders + its positional params.
Query = tuple[str, list[object]]


class SqlDialect:
    """ANSI-neutral SQL strategy.

    Per-system subclasses live in connector packages and override exactly the
    quirks their system has.
    """

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
    sqlalchemy_registry_name: str | None = None
    #: Identifier quote character. ANSI double-quote by default; backtick for
    #: MySQL/MariaDB and BigQuery (where ``"..."`` is a string literal).
    quote_char: str = '"'
    #: BigQuery does not enforce PK constraints and its parser requires the
    #: ``NOT ENFORCED`` qualifier; every other dialect accepts the bare clause.
    pk_not_enforced: bool = False
    #: Schemas hidden from ``list_schemas`` (catalog/internal schemas).
    system_schemas: tuple[str, ...] = ()
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

    def quote_qualified(self, schema: str, table: str, *, catalog: str = "") -> str:
        """Quote a qualified table name.

        When *catalog* is provided, emits ``catalog.schema.table`` (or
        ``catalog.table`` when *schema* is empty).  Without *catalog*, falls
        back to the two-part ``schema.table`` or bare ``table`` form.
        """
        if schema:
            qualified = (
                f"{self.quote_ident(self.normalize_schema(schema))}"
                f".{self.quote_ident(table)}"
            )
        else:
            qualified = self.quote_ident(table)
        if catalog:
            return f"{self.quote_ident(self.normalize_schema(catalog))}.{qualified}"
        return qualified

    def pk_clause(self, columns: list[str]) -> str:
        """Render the table-level ``PRIMARY KEY (...)`` clause."""
        cols = ", ".join(self.quote_ident(c) for c in columns)
        clause = f"PRIMARY KEY ({cols})"
        return f"{clause} NOT ENFORCED" if self.pk_not_enforced else clause

    # ---- paged reads -------------------------------------------------------
    def paging_order_fallback(self) -> str | None:
        """Return the ordering expression for paged reads that declare none.

        Applies when a paged read has no cursor field and no ``order_by``.
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
        records: list[dict[str, Any]],
        conflict_keys: list[str],
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
        """Turn the stored TLS mode and CA bundle into the connect argument.

        The result is the driver's ``connect_args["ssl"]`` value.
        Each driver speaks its own SSL vocabulary (libpq modes, MySQL
        modes); the connector package's dialect implements it, typically
        via :func:`cdk.transport_factory.ca_ssl_context` for the verifying
        cases. The base raises so a declared TLS mode never silently
        downgrades to no TLS.
        """
        raise UnsupportedDialectOperationError(
            "build_tls_connect_arg", dialect=self.name
        )

    def build_tls_connect_args(self, mode: str, ca_pem: str | None) -> dict[str, Any]:
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

    def sqlalchemy_pre_ddl(
        self, schema_name: str, *, catalog_name: str = ""
    ) -> list[str]:
        """Return statements to run before ``MetaData.create_all``.

        For example, postgres' ``CREATE SCHEMA IF NOT EXISTS`` for a
        non-default schema. When *catalog_name* is set, an override creates
        the schema in that catalog (database / project) so a subsequent
        catalog-qualified ``CREATE TABLE`` resolves it. None by default.
        """
        return []

    # ---- schema semantics ----------------------------------------------------
    def schema_is_implicit_default(self, schema_name: str) -> bool:
        """Return True when *schema_name* is the dialect's implicit default.

        Used to skip ``CREATE SCHEMA`` for namespaces that always exist
        (e.g. postgres/snowflake ``public``). The neutral base only treats
        the empty name as implicit.
        """
        return not schema_name

    # ---- column type rendering (one write surface: type-map-write.json) -----
    def render_column_type(
        self,
        canonical: str,
        type_mapper: TypeMapper,
        *,
        params: Mapping[str, Any] | None = None,
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
        """Return the SQL DEFAULT expression for server-stamped timestamps.

        ANSI ``CURRENT_TIMESTAMP`` by default. MySQL/MariaDB require the
        expression's fractional-seconds precision to match the column's
        (``CURRENT_TIMESTAMP(6)`` for ``DATETIME(6)``) and reject the bare
        form with error 1067.
        """
        return "CURRENT_TIMESTAMP"

    def adbc_stage_table_sql(self, stage_qualified: str, target_qualified: str) -> str:
        """Return SQL creating an empty staging table shaped like the target.

        Used for the ADBC upsert's ingest-to-stage + MERGE. Column-copy syntax
        is vendor-specific; the connector package's dialect implements it.
        """
        raise UnsupportedDialectOperationError(
            "adbc_stage_table_sql", dialect=self.name
        )

    def adbc_ingest_schema_kwargs(
        self, schema_name: str, *, catalog_name: str = ""
    ) -> dict[str, Any]:
        """Return schema- and catalog-targeting kwargs for ``cursor.adbc_ingest``.

        ADBC exposes per-statement ingest targeting through the
        ``adbc.ingest.target_db_schema`` option (the ``db_schema_name``
        kwarg) and ``adbc.ingest.target_catalog`` (the ``catalog_name``
        kwarg).  The postgres driver and most others implement schema
        targeting, so the base targets the normalized schema explicitly —
        read and write then resolve the same physical schema.  When
        *catalog_name* is provided, the base also passes ``catalog_name``
        for drivers that support cross-catalog ingest.

        Drivers that do not implement per-statement ingest targeting
        (Snowflake rejects both ``target_db_schema`` and ``target_catalog``)
        override this to return no kwargs; ingest follows the connection's
        session schema instead, where the stage and target tables already
        live.
        """
        kwargs: dict[str, Any] = {}
        if schema_name:
            kwargs["db_schema_name"] = self.normalize_schema(schema_name)
        if catalog_name:
            kwargs["catalog_name"] = catalog_name
        return kwargs

    # ---- discovery queries (qmark placeholders + positional params) --------
    def schemas_query(self) -> Query:
        sql = "SELECT schema_name FROM information_schema.schemata"
        params: list[object] = []
        if self.system_schemas:
            placeholders = ", ".join("?" for _ in self.system_schemas)
            sql += f" WHERE schema_name NOT IN ({placeholders})"
            params.extend(self.system_schemas)
        sql += " ORDER BY schema_name"
        return sql, params

    def tables_query(self, schema: str, *, catalog: str = "") -> Query:
        """Return ``(sql, params)`` listing tables in *schema*.

        When *catalog* is provided, an additional ``table_catalog = ?`` filter
        restricts discovery to that catalog (database / project).
        """
        sql = (
            "SELECT table_name FROM information_schema.tables " "WHERE table_schema = ?"
        )
        params: list[object] = [self.normalize_schema(schema)]
        if catalog:
            sql += " AND table_catalog = ?"
            params.append(self.normalize_schema(catalog))
        sql += " ORDER BY table_name"
        return sql, params

    def columns_query(self, schema: str, table: str, *, catalog: str = "") -> Query:
        """Return ``(sql, params)`` describing columns of *schema.table*.

        When *catalog* is provided, an additional ``table_catalog = ?`` filter
        restricts discovery to that catalog.
        """
        sql = (
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?"
        )
        params: list[object] = [self.normalize_schema(schema), table]
        if catalog:
            sql += " AND table_catalog = ?"
            params.append(self.normalize_schema(catalog))
        sql += " ORDER BY ordinal_position"
        return sql, params

    def primary_keys_query(
        self, schema: str, table: str, *, catalog: str = ""
    ) -> Query:
        """Return ``(sql, params)`` listing PK columns of *schema.table*.

        When *catalog* is provided, an additional ``tc.table_catalog = ?``
        filter restricts discovery to that catalog.
        """
        sql = (
            "SELECT kcu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_catalog = kcu.constraint_catalog "
            " AND tc.constraint_schema = kcu.constraint_schema "
            " AND tc.constraint_name = kcu.constraint_name "
            " AND tc.table_schema = kcu.table_schema "
            " AND tc.table_name = kcu.table_name "
            " AND tc.table_catalog = kcu.table_catalog "
            "WHERE tc.constraint_type = 'PRIMARY KEY' "
            "  AND tc.table_schema = ? AND tc.table_name = ?"
        )
        params: list[object] = [self.normalize_schema(schema), table]
        if catalog:
            sql += " AND tc.table_catalog = ?"
            params.append(self.normalize_schema(catalog))
        sql += " ORDER BY kcu.ordinal_position"
        return sql, params
