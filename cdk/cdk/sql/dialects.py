"""The vendor-neutral SQL dialect base — per-system dialects live in packages.

``SqlDialect`` is the **complete extension surface** for everything
vendor-specific in the SQL path: identifier quoting and normalization, the
``catalog.schema.table`` address composer (:class:`TableAddress`), the
PRIMARY KEY clause form, the ``INFORMATION_SCHEMA`` discovery query shapes,
the stage-then-merge write hooks (stage DDL, the merge-form upsert
statement, the target-emptying statement, the optional native bulk land),
the pre-DDL statements, the per-connection session-init statements, and the
ADBC-only write machinery (native DDL type names and stage-table syntax,
until #389 aligns that transport on the same hooks).

Table addressing follows the engine's bind-once/sink-many rule: the intent
(``catalog``/``schema``/``table``) is resolved exactly once into a
:class:`TableAddress` via :meth:`SqlDialect.table_address` — normalization
and the catalog capability check happen there — and every surface consumes
the same address through a sink (:meth:`SqlDialect.quote_table` for SQL
text, :meth:`SqlDialect.adbc_ingest_kwargs` for ADBC ingest, the
``QueryBuilder`` schema argument for compiled reads). A catalog on a
dialect that cannot address one fails loud at address construction, never
by compiling SQL the system will misread.

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

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .capabilities import SqlCapabilities, undeclared_capability_error
from .exceptions import CatalogAddressingError, UnsupportedDialectOperationError

if TYPE_CHECKING:
    from cdk.type_map.mapper import TypeMapper

# A discovery query: SQL text with ``?`` placeholders + its positional params.
Query = tuple[str, list[object]]


@dataclass(frozen=True)
class TableAddress:
    """A resolved ``(catalog.)schema.table`` address, normalized once.

    Built via :meth:`SqlDialect.table_address`, which applies
    :meth:`SqlDialect.normalize_ident` to every component and enforces the
    declared catalog capability — so DDL, DML, ingest kwargs, query
    building, and logs all work from the same components and none can
    diverge. Empty string means "absent" (no catalog / no schema).
    Construct directly only with already-normalized components (tests on
    the identity-normalizing base, engine-generated names via
    ``dataclasses.replace``); everything else goes through the factory.

    Frozen so an address can live on shared per-stream state and cross
    worker-thread boundaries without defensive copying.
    """

    table: str
    schema: str = ""
    catalog: str = ""

    def __post_init__(self) -> None:
        """Reject the one ill-formed shape no dialect can render safely.

        Catalog-implies-schema is dialect-independent, so it lives on the
        type itself: a two-part ``catalog.table`` would be misread as
        ``schema.table`` by every SQL sink. Runs on direct construction
        and ``dataclasses.replace`` alike, so no bypass of the factory can
        produce the shape.
        """
        if self.catalog and not self.schema:
            raise CatalogAddressingError(
                f"catalog {self.catalog!r} requires an explicit schema; a "
                f"two-part catalog.table address has no portable meaning"
            )

    def __str__(self) -> str:
        """Dotted display form for logs and error messages."""
        return ".".join(p for p in (self.catalog, self.schema, self.table) if p)


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
    #: Identifier byte budget the engine composes generated names within
    #: (stage tables). 63 is the tightest budget among supported systems
    #: (Postgres/Redshift NAMEDATALEN - 1); a dialect whose system allows
    #: less overrides, one that allows more may raise it to keep longer
    #: readable name tails.
    max_identifier_length: int = 63
    #: The connector's declared ``sql_capabilities`` (issue #390), attached
    #: by the facade when a runtime binds (``GenericSQLConnector``). The
    #: dialect class renders SQL; whether the system HAS a shape — catalog
    #: addressability, merge form, session-targeting regime, bulk mechanism,
    #: stage shape — is declared data in ``connector.json``, never a class
    #: boolean. ``None`` means undeclared: any gate that needs a fact
    #: refuses loudly instead of guessing.
    capabilities: SqlCapabilities | None = None

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

    def normalize_ident(self, name: str) -> str:
        """Normalize one identifier component before quoting (no-op by default).

        Applied uniformly to catalog, schema, and table at
        :meth:`table_address` construction and inside the discovery
        queries, so read, write, DDL, and discovery all resolve the same
        physical object (e.g. a case-folding system's dialect maps the
        conventional lowercase ``public`` to the stored ``PUBLIC``).
        """
        return name

    def _check_catalog(self, catalog: str) -> None:
        """Reject a catalog the system cannot address (single choke point).

        Shared by :meth:`table_address` (read / write / DDL) and
        :meth:`information_schema_ref` (discovery) — the only two doors a
        catalog enters SQL composition through. The authority is the
        connector's declared ``sql_capabilities.catalog``: ``read`` or
        ``full`` passes this door (write/DDL sites additionally require
        ``full`` where the address is a write target); ``none`` refuses;
        undeclared refuses naming the missing declaration — never a guess.
        """
        if not catalog:
            return
        caps = self.capabilities
        if caps is None:
            raise CatalogAddressingError(
                str(
                    undeclared_capability_error(
                        "catalog",
                        need=f"this operation targets catalog {catalog!r}",
                    )
                )
            )
        if caps.catalog == "none":
            raise CatalogAddressingError(
                f"connector declares sql_capabilities.catalog 'none': "
                f"{self.name!r} cannot address a catalog per statement; "
                f"cannot target catalog {catalog!r}. Use a connection whose "
                f"default catalog is {catalog!r}."
            )

    def table_address(
        self, table: str, *, schema: str = "", catalog: str = ""
    ) -> TableAddress:
        """Build the normalized :class:`TableAddress` every SQL surface consumes.

        The one entry point for table-addressing intent: normalization
        happens here (once), and a catalog the dialect cannot address
        fails loud here — before any SQL is composed — instead of
        compiling into a statement the system would misread. The
        catalog-implies-schema rule is dialect-independent and enforced by
        :class:`TableAddress` itself. Normalization runs before the
        capability gate so both entry doors (here and the discovery
        queries) report the same catalog spelling.
        """
        catalog = self.normalize_ident(catalog) if catalog else ""
        self._check_catalog(catalog)
        return TableAddress(
            table=self.normalize_ident(table),
            schema=self.normalize_ident(schema) if schema else "",
            catalog=catalog,
        )

    def quote_table(self, address: TableAddress) -> str:
        """Quote *address* for SQL text (up to ``catalog.schema.table``)."""
        qualifiers = [p for p in (address.catalog, address.schema) if p]
        return ".".join(self.quote_ident(p) for p in (*qualifiers, address.table))

    def quote_schema(self, address: TableAddress) -> str:
        """Quote *address*'s schema path for SQL text (``catalog.schema``).

        Callers guard that ``address.schema`` is set (an implicit-default
        schema is never CREATEd, so this is only reached for explicit ones).
        """
        qualifiers = [p for p in (address.catalog, address.schema) if p]
        return ".".join(self.quote_ident(p) for p in qualifiers)

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

    # ---- stage-then-merge write path (ADR sql-write-path-v2) ---------------
    def stage_table_sql(
        self, stage: TableAddress, target: TableAddress, *, temp: bool
    ) -> str:
        """``CREATE [TEMPORARY] TABLE`` *stage* shaped like *target*.

        Every SQL write lands in a stage table first; this renders the
        stage's DDL. ``temp`` comes from the connector's declared
        ``sql_capabilities.stage.scope`` — ``True`` means a session-scoped
        temporary table (a temp-scope *stage* carries no schema: it lives
        in the session's namespace), ``False`` an ordinary table placed by
        the declared schema rule. Column-copy syntax is vendor-specific
        (postgres ``(LIKE target INCLUDING DEFAULTS)``, MySQL
        ``LIKE target``); the connector package's dialect implements it.
        The base raises so a declared stage shape a dialect cannot render
        fails loudly instead of guessing DDL the system may misread.
        """
        raise UnsupportedDialectOperationError("stage_table_sql", dialect=self.name)

    def merge_statement_sql(
        self,
        stage: TableAddress,
        target: TableAddress,
        conflict_keys: Sequence[str],
        columns: Sequence[str],
    ) -> str:
        """Render the upsert statement from *stage* to *target*.

        Rendered in the system's declared merge form (``MERGE INTO``,
        ``INSERT … ON CONFLICT DO UPDATE``, ``INSERT … ON DUPLICATE KEY
        UPDATE``); no portable ANSI form exists, so the connector
        package's dialect implements it. Serves upsert only — insert uses
        the ANSI anti-join the CDK renders itself.

        The statement's semantics are part of the write contract:

        * ``columns`` are the landed columns, in stage order; the update
          set is ``columns`` minus ``conflict_keys``. Columns the target
          has but the batch did not land (the engine-managed
          ``_synced_at``, physical columns added out-of-band) keep their
          stored value on matched rows and their DEFAULT on inserted ones.
        * When every landed column is a conflict key there is nothing to
          update: render the dialect's insert-only degradation (postgres
          ``ON CONFLICT DO NOTHING``, a MERGE without ``WHEN MATCHED``) —
          matched rows stay untouched, never an error.

        The base raises so an upsert against a dialect without a merge
        rendering fails loudly instead of silently degrading to INSERT.
        """
        raise UnsupportedDialectOperationError("merge_statement_sql", dialect=self.name)

    def empty_table_sql(self, target: TableAddress) -> str:
        """Render the statement that empties *target* for a full refresh.

        ANSI ``DELETE FROM`` — deliberately never ``TRUNCATE``, which
        implicitly commits on several systems and would break the
        stage cycle's single-transaction shape where one is declared.
        Overridable for systems whose DELETE grammar deviates (BigQuery
        requires a ``WHERE`` clause).
        """
        # Composed of dialect-quoted identifiers; no user values.
        return f"DELETE FROM {self.quote_table(target)}"  # nosec B608

    def bulk_land(  # skipcq: PYL-R0201
        self,
        conn: Any,
        stage: TableAddress,
        batch: Any,
        *,
        runtime: Any,
    ) -> bool:
        """Land *batch* (a ``pyarrow.RecordBatch``) into *stage* natively.

        A pure speed slot: stage contents must be byte-identical to the
        executemany default, and the downstream mode statement is the same
        statement either way. Return ``True`` when landed; ``False``
        declines and the backend falls back to executemany (logged INFO —
        a speed downgrade is visible, never silent). Called only when the
        connector declares a bulk mechanism (``sql_capabilities.bulk_load``).

        ``conn`` is the backend's native connection object (the sync
        SQLAlchemy ``Connection`` on the SQLAlchemy transport). ``runtime``
        is the resolved ``ConnectionRuntime`` the backend connected with:
        a dialect whose mechanism runs through the system's own client
        rather than the transport connection builds that client from
        *runtime* inside the call and discards it on return — no client is
        cached on the dialect or the backend.
        """
        return False

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

    def verify_tls_state(self, dbapi_connection: Any, mode: str) -> None:
        """Verify the established session satisfies the declared TLS *mode*.

        Connect arguments only request TLS; they cannot guarantee it. A
        driver may silently skip the TLS handshake when the server does
        not advertise the capability (aiomysql does, for every mode), so
        an active MITM can strip the server's TLS capability and downgrade
        the strictest mode to plaintext unless the established session is
        checked. The transport factory calls this hook on every new DBAPI
        connection the engine's pool creates — the build-time probe
        included — whenever the transport declares a TLS mode.

        ``dbapi_connection`` is the driver's connection (for async
        drivers, SQLAlchemy's asyncio adapter, which exposes the same
        DBAPI cursor surface). The connector package's dialect implements
        the system-specific probe (MySQL: ``SHOW STATUS LIKE
        'Ssl_cipher'`` — empty value means unencrypted) and raises
        :class:`~cdk.sql.exceptions.TlsVerificationError` when a mode
        that promises encryption finds an unencrypted session; modes that
        do not promise encryption pass without checking. The base is a
        no-op so existing connectors are unaffected — a dialect opts in
        when its driver can silently downgrade.
        """
        return None

    def session_init_sql(self) -> list[str]:
        """Statements the CDK runs on every new database connection.

        Executed before the connection is used — the build-time probe
        included — on both SQL transports: every new pooled DBAPI
        connection of a SQLAlchemy engine (async and sync, after
        :meth:`verify_tls_state`) and every connection an ADBC transport
        opens. The statements run through one cursor and are committed,
        so session state survives a later rollback on systems where it
        is transactional (Postgres ``SET``).

        The base declares no statements, so existing connectors are
        unaffected. A dialect opts in when its system has session state
        that must be pinned for type-mapping correctness — e.g. MySQL's
        ``SET time_zone = '+00:00'``, without which retrieved TIMESTAMP
        values are converted through whatever ``time_zone`` the server
        happens to run and the tz-aware canonical types carry the wrong
        instants.
        """
        return []

    def sqlalchemy_pre_ddl(self, schema_name: str) -> list[str]:
        """Return statements to run before ``MetaData.create_all``.

        For example, postgres' ``CREATE SCHEMA IF NOT EXISTS`` for a
        non-default schema. None by default.
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

    def adbc_ingest_kwargs(self, address: TableAddress) -> dict[str, Any]:
        """Targeting kwargs for ``cursor.adbc_ingest``.

        ADBC exposes per-statement ingest targeting through the
        ``adbc.ingest.target_db_schema`` option (the ``db_schema_name``
        kwarg) and ``adbc.ingest.target_catalog`` (``catalog_name``).
        Components come off the already-normalized *address*, so ingest
        resolves the same physical objects the DDL created. The catalog
        kwarg is only reachable on connectors whose declared
        ``sql_capabilities.catalog`` is ``full`` — :meth:`table_address`
        rejects a catalog on an undeclared or ``none`` declaration before
        an address exists, and the write path requires ``full``.

        Drivers that do not implement per-statement ingest targeting
        override this to return no kwargs; ingest then follows the
        connection's session defaults, where the target tables already
        live. That opt-out shifts the schema-correctness burden onto
        the session, so the engine guards it: before a bare-name ingest
        it checks :meth:`adbc_session_schema_sql` against the address's
        schema and refuses on mismatch (issue #377).
        """
        kwargs: dict[str, Any] = {}
        if address.schema:
            kwargs["db_schema_name"] = address.schema
        if address.catalog:
            kwargs["catalog_name"] = address.catalog
        return kwargs

    def adbc_session_schema_sql(self) -> str:
        """Return SQL yielding the connection's current session schema.

        Companion guard to overriding :meth:`adbc_ingest_kwargs` to
        return no targeting kwargs: when ingest follows the connection's
        session defaults, the engine verifies — probing once per
        connection — that the session schema equals the address's schema
        before handing ``adbc_ingest`` a bare table name (issue #377).
        The statement must return one row whose first column is the
        session schema name, NULL when the session has none selected.

        ``SELECT CURRENT_SCHEMA()`` is the spelling of the one
        session-default system shipped today (Snowflake) and is equally
        valid Postgres SQL; a dialect whose system spells the probe
        differently overrides. Dialects that keep the default
        :meth:`adbc_ingest_kwargs` targeting never reach this probe.
        """
        return "SELECT CURRENT_SCHEMA()"

    # ---- discovery queries (qmark placeholders + positional params) --------
    def information_schema_ref(
        self, view: str, *, catalog: str = "", schema: str = ""
    ) -> str:
        """FROM-clause reference for an ``INFORMATION_SCHEMA`` view.

        The single hook governing how the metadata path is composed. The
        base emits the session-local ``information_schema.<view>``,
        prefixing the quoted catalog when one is requested (the shape
        catalog-addressing systems such as DuckDB use). *schema* is part
        of the signature so a dialect whose metadata views are
        schema-scoped (BigQuery's ``project.dataset.INFORMATION_SCHEMA``)
        can compose the full path in its override; the base ignores it —
        ANSI scopes rows through ``table_schema`` filters instead. Both
        arguments arrive pre-normalized (the query builders below
        normalize once at entry).
        """
        self._check_catalog(catalog)
        if catalog:
            return f"{self.quote_ident(catalog)}.information_schema.{view}"
        return f"information_schema.{view}"

    def schemas_query(self, catalog: str = "") -> Query:
        """List non-system schemas, scoped to *catalog* when one is given."""
        catalog = self.normalize_ident(catalog) if catalog else ""
        ref = self.information_schema_ref("schemata", catalog=catalog)
        # The interpolated FROM path is composed of dialect-quoted
        # identifiers (information_schema_ref); filter values bind via
        # qmark placeholders. Same rationale for the three queries below.
        sql = f"SELECT schema_name FROM {ref}"  # nosec B608
        conditions: list[str] = []
        params: list[object] = []
        if self.system_schemas:
            placeholders = ", ".join("?" for _ in self.system_schemas)
            conditions.append(f"schema_name NOT IN ({placeholders})")
            params.extend(self.system_schemas)
        if catalog:
            conditions.append("catalog_name = ?")
            params.append(catalog)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY schema_name"
        return sql, params

    def tables_query(self, schema: str, catalog: str = "") -> Query:
        """List tables in *schema*, scoped to *catalog* when one is given."""
        catalog = self.normalize_ident(catalog) if catalog else ""
        schema = self.normalize_ident(schema)
        ref = self.information_schema_ref("tables", catalog=catalog, schema=schema)
        sql = f"SELECT table_name FROM {ref} WHERE table_schema = ?"  # nosec B608
        params: list[object] = [schema]
        if catalog:
            sql += " AND table_catalog = ?"
            params.append(catalog)
        sql += " ORDER BY table_name"
        return sql, params

    def columns_query(self, schema: str, table: str, catalog: str = "") -> Query:
        """Describe columns of *schema.table*, catalog-scoped when given."""
        catalog = self.normalize_ident(catalog) if catalog else ""
        schema = self.normalize_ident(schema)
        ref = self.information_schema_ref("columns", catalog=catalog, schema=schema)
        sql = (
            "SELECT column_name, data_type, is_nullable "  # nosec B608
            f"FROM {ref} "
            "WHERE table_schema = ? AND table_name = ?"
        )
        params: list[object] = [schema, self.normalize_ident(table)]
        if catalog:
            sql += " AND table_catalog = ?"
            params.append(catalog)
        sql += " ORDER BY ordinal_position"
        return sql, params

    def primary_keys_query(self, schema: str, table: str, catalog: str = "") -> Query:
        """List PK columns of *schema.table*, catalog-scoped when given."""
        catalog = self.normalize_ident(catalog) if catalog else ""
        schema = self.normalize_ident(schema)
        tc_ref = self.information_schema_ref(
            "table_constraints", catalog=catalog, schema=schema
        )
        kcu_ref = self.information_schema_ref(
            "key_column_usage", catalog=catalog, schema=schema
        )
        sql = (
            "SELECT kcu.column_name "  # nosec B608
            f"FROM {tc_ref} tc "
            f"JOIN {kcu_ref} kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            " AND tc.table_schema = kcu.table_schema "
            " AND tc.table_name = kcu.table_name "
            "WHERE tc.constraint_type = 'PRIMARY KEY' "
            "  AND tc.table_schema = ? AND tc.table_name = ?"
        )
        params: list[object] = [schema, self.normalize_ident(table)]
        if catalog:
            sql += " AND tc.table_catalog = ?"
            params.append(catalog)
        sql += " ORDER BY kcu.ordinal_position"
        return sql, params
