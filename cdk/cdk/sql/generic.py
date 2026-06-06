"""Generic SQL connector — one class serving every SQL role.

``GenericSQLConnector`` implements the four CDK capability Protocols
(:mod:`cdk.contract`) for SQL databases over SQLAlchemy or ADBC:

* **Readable** — ``read_batches`` streams a source table as Arrow batches,
  paging via ``QueryBuilder`` with the incremental cursor as a filter.
* **Writable** — ``connect`` / ``configure_schema`` / ``write_batch`` /
  ``disconnect`` / ``health_check`` load batches with idempotency tracking.
* **Discoverable** / **TableCreator** — control-plane introspection and
  standalone DDL, delegated to :mod:`cdk.sql` (``list_*`` / ``create_table``).

The active transport is selected by the connector definition and set on the
``ConnectionRuntime``: ``transport_type: "sqlalchemy"`` (async SQLAlchemy
engine) or ``transport_type: "adbc"`` (direct ADBC DBAPI). Type casting is
handled by the Arrow-based ``SchemaContract``.

This base is vendor-neutral: every per-system quirk (quoting, upsert SQL,
pre-DDL, ADBC DDL type names, stage-table syntax, discovery queries) is
delegated to the :class:`~cdk.sql.dialects.SqlDialect` carried by
``dialect_class`` — which each connector package overrides with its own
dialect next to its connector class. The base never branches on a driver
or connector_id; operations with no portable form raise
``UnsupportedDialectOperationError`` naming the missing connector package.
"""

import asyncio
import hashlib
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Literal, Mapping, Optional, Tuple

import pyarrow as pa
from sqlalchemy import MetaData, Table, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult
from cdk.schema_contract import SchemaContract
from cdk.type_map import (
    InvalidTypeMapError,
    TypeMapper,
    UnmappedTypeError,
)
from cdk.types import (
    AckStatus,
    CheckpointStore,
    Cursor,
    EndpointScope,
    SchemaSpec,
)
from cdk.adbc_registry import AdbcConfigurationError
from cdk.connection_runtime import (
    ConnectionRuntime,
    DETERMINISTIC_CONNECT_ERRORS,
    materialize_runtime,
)
from cdk.database_utils import acquire_connection
from cdk.query_builder import Filter, QueryBuilder, QueryConfig
from .adbc_reader import open_adbc_reader
from .dialects import SqlDialect
from .discovery import list_columns as _sql_list_columns
from .discovery import list_schemas as _sql_list_schemas
from .discovery import list_tables as _sql_list_tables
from .ddl import build_create_table_sql
from .ddl import create_table as _sql_create_table
from .exceptions import (
    ReadError,
    SchemaConfigurationError,
    UnsupportedDialectOperationError,
)
from ..contract import ColumnDef


logger = logging.getLogger(__name__)


# Tracks (table, column) pairs already warned about ORDER BY fallback so a
# long-lived source connector warns once per stream, not once per page.
_order_by_fallback_logged: set = set()


def _note_order_by_fallback(table_name: str, column_name: str) -> None:
    """Warn once per (table, column) that ORDER BY fell back to a column.

    The ADBC-only read path pages with OFFSET, which needs a stable ORDER
    BY or rows silently skip/duplicate across pages on PG/Snowflake/
    BigQuery. When a stream has no cursor or order_by_field we order by the
    first selected column. A WARNING (not INFO) because the operator may
    need to act: a JSON / STRUCT / VARIANT first column fails at query time
    with an opaque "ORDER BY does not support this type" error. Fix by
    setting ``cursor_field`` (incremental) or
    ``database_pagination.order_by_field`` (full-refresh) on the stream.
    """
    key = (table_name, column_name)
    if key in _order_by_fallback_logged:
        return
    _order_by_fallback_logged.add(key)
    logger.warning(
        "ADBC reader: no cursor_field for table %r; defaulting ORDER BY to "
        "first selected column %r. Set cursor_field on the stream if this "
        "column is a non-orderable type (JSON / STRUCT / VARIANT) -- the "
        "warehouse will otherwise reject the query.",
        table_name, column_name,
    )


# PEP-249 exception class names that indicate the failure cannot heal
# between retries against an identical request: bad SQL, missing
# objects, permission denials, type mismatches, unsupported operations.
# Driver modules re-export these names per PEP-249; we match on the
# class name so the check works without importing the optional driver.
_FATAL_ADBC_ERROR_NAMES = frozenset({
    "ProgrammingError",
    "NotSupportedError",
    "IntegrityError",
    "DataError",
})


def _is_fatal_adbc_error(exc: BaseException) -> bool:
    """``True`` when *exc* is a class of failure retries cannot heal."""
    for cls in type(exc).__mro__:
        if cls.__name__ in _FATAL_ADBC_ERROR_NAMES:
            return True
    return False


def _reclassify_as_fatal(exc: BaseException) -> AdbcConfigurationError:
    """Wrap a fatal PEP-249 exception in ``AdbcConfigurationError``.

    The wrapped message preserves the original class name so operators
    triaging an opaque ``str(exception)`` in the engine's failure
    summary can still distinguish ProgrammingError (syntax / missing
    object / permission denial) from IntegrityError (PK collision)
    from DataError (type / value mismatch).
    """
    inner_name = type(exc).__name__
    wrapped = AdbcConfigurationError(f"{inner_name}: {exc}")
    wrapped.__cause__ = exc
    return wrapped


WriteMode = Literal["insert", "upsert", "truncate_insert"]


class AdbcCommitRecordError(RuntimeError):
    """ADBC ingest succeeded, but ``_batch_commits`` recording failed.

    The data is already in the destination table. Retry behavior
    depends on ``write_mode``:

    * ``insert`` — retry re-ingests, duplicating rows.
    * ``truncate_insert`` — retry truncates then re-ingests, so the
      net effect is idempotent (no duplication).
    * ``upsert`` — retry re-MERGEs on the conflict keys; idempotent
      provided the conflict-key set is actually unique.

    The exception carries the inner error so the engine's failure
    summary surfaces the divergence rather than a generic commit
    error.
    """

    _RETRY_SEMANTICS: Dict[str, str] = {
        "insert": "retry will duplicate rows",
        "truncate_insert": "retry is idempotent (truncate + re-ingest)",
        "upsert": "retry is idempotent under conflict keys (re-MERGE)",
    }

    def __init__(
        self,
        inner: BaseException,
        write_mode: WriteMode = "insert",
    ) -> None:
        if write_mode not in self._RETRY_SEMANTICS:
            # The Literal already constrains callers at type-check time;
            # this guards against runtime values that bypass typing (e.g.
            # constructed from arbitrary strings in tests). Loud here
            # beats a misleading "retry semantics unknown" in production.
            raise ValueError(
                f"AdbcCommitRecordError.write_mode must be one of "
                f"{sorted(self._RETRY_SEMANTICS)}; got {write_mode!r}"
            )
        super().__init__(
            f"adbc ingest committed; commit-record failed "
            f"({self._RETRY_SEMANTICS[write_mode]}): {inner}"
        )
        self.__cause__ = inner
        self.write_mode: WriteMode = write_mode


@dataclass
class _StreamState:
    """Per-stream destination state.

    The handler is a single instance shared across all streams writing to
    this destination, so every field that depends on a specific stream's
    endpoint document lives here, keyed by ``stream_id``. Sharing handler
    instance fields directly across streams would race when the engine
    fires schema/configure calls in parallel.
    """

    schema_name: str = "public"
    table_name: str = ""
    table: Optional[Table] = None
    batch_commits_table: Optional[Table] = None
    primary_keys: List[str] = field(default_factory=list)
    # Columns used as the ON CONFLICT target for upsert. Set at
    # configure_schema time from ``endpoint_doc["_write_conflict_keys"]``
    # — main.py computes that via WriteConfig.effective_conflict_keys,
    # which uses the stream's explicit ``write.conflict_keys`` when
    # set and falls back to ``primary_keys``. Empty here means INSERT
    # mode or no conflict target available.
    conflict_keys: List[str] = field(default_factory=list)
    write_mode: WriteMode = "upsert"
    endpoint_document: Dict[str, Any] = field(default_factory=dict)
    schema_contract: Optional[SchemaContract] = None


class GenericSQLConnector(BaseDestinationHandler):
    """Unified SQL connector implementing all four CDK capability Protocols.

    One class serves source reads (``Readable``), destination writes
    (``Writable``), and the control-plane operations (``Discoverable`` /
    ``TableCreator``). A given instance is driven in one role at a time:
    the engine constructs a source-role instance and calls
    ``read_batches``; the destination service constructs a write-role
    instance and calls ``connect`` / ``configure_schema`` / ``write_batch``.

    Supports two transports, selected by the connector definition and
    set on the runtime that ``connect()`` consumes:

    * ``transport_type: "sqlalchemy"`` — async SQLAlchemy engine
      (Postgres asyncpg, MySQL aiomysql). DDL via ``MetaData.create_all``,
      DML via the dialect's INSERT/INSERT-ON-CONFLICT/MERGE compilers.
    * ``transport_type: "adbc"`` — direct ADBC DBAPI 2.0 connection
      (Snowflake, BigQuery, Postgres-via-ADBC for Redshift). DDL via
      ``cursor.execute`` of per-driver native SQL, ingest via
      ``cursor.adbc_ingest``, upsert via stage-table + ``MERGE INTO``.

    Both modes share idempotency tracking (``_batch_commits`` table
    keyed on ``(run_id, stream_id, batch_seq)``), the schema-contract
    Arrow cast, and the per-stream state machine. Configuration is
    resolved through ``ConnectionRuntime`` rather than read off the
    raw connection JSON — the handler never inspects host/port/secret
    fields directly.

    Per-stream destination settings (schema, table, primary keys,
    columns) are read from the preloaded contract endpoint document at
    ``configure_schema`` time. The SchemaSpec off the wire only
    carries ``stream_id``, ``version``, and ``write_mode``.
    """

    BATCH_COMMITS_TABLE = "_batch_commits"

    # Server-managed column added to every database destination table.
    # Populated by the database via DEFAULT NOW() at INSERT time so the
    # engine never has to ship a per-record timestamp.
    SYNCED_AT_COLUMN = "_synced_at"

    # The dialect strategy carrying every vendor-specific piece of SQL:
    # quoting, upsert statements, pre-DDL, ADBC DDL type names, stage-table
    # syntax, discovery queries. The CDK base is ANSI-neutral; a connector
    # package's class overrides ``dialect_class`` with its own SqlDialect
    # subclass. This is the ONLY per-system extension point — the generic
    # connector never branches on driver or connector_id.
    dialect_class: type[SqlDialect] = SqlDialect

    def __init__(self) -> None:
        """Initialize the database handler."""
        self.dialect: SqlDialect = self.dialect_class()
        self._runtime: ConnectionRuntime | None = None
        self._engine: AsyncEngine | None = None
        self._config: Dict[str, Any] = {}
        self._connected: bool = False
        self._driver: str = ""
        # ADBC-only mode: the runtime exposes no SQLAlchemy engine and
        # every write/DDL/idempotency operation runs through the cached
        # ADBC DBAPI connection. Set in ``connect()`` from
        # ``runtime.is_adbc``. The SQLAlchemy transport (``self._engine``)
        # is the alternative; the two are selected by ``transport_type``
        # on the connector, never mixed.
        self._adbc_only: bool = False

        # Per-stream state derived from the contract endpoint document at
        # configure_schema() time. The SchemaSpec off the wire only
        # carries stream_id, version, and write_mode; everything else comes
        # from the preloaded ``stream_endpoints`` map. Keyed by stream_id
        # so concurrent streams sharing this handler instance do not race
        # on shared mutable fields.
        self._streams: Dict[str, _StreamState] = {}

        # Serializes CREATE TABLE statements across streams. Even when each
        # stream owns its own SQLAlchemy ``MetaData``, two concurrent
        # ``create_all`` calls can still race the database's catalog
        # writes (e.g. PostgreSQL's ``pg_type_typname_nsp_index``).
        self._ddl_lock: asyncio.Lock = asyncio.Lock()

        # Stream-id -> structured endpoint_ref dict so configure_schema()
        # can pick the type-mapper matching the endpoint's scope (connector
        # vs connection). Populated by set_endpoint_refs() at startup.
        self._endpoint_refs: Dict[str, Dict[str, Any]] = {}

        # Stream-id -> contract endpoint document (database_object,
        # columns, primary_keys, …). Populated by set_stream_endpoints()
        # at startup.
        self._stream_endpoints: Dict[str, Dict[str, Any]] = {}

        # Cached ADBC DBAPI connection for ADBC-only mode (Snowflake /
        # BigQuery / Postgres), opened eagerly in connect() via
        # runtime.open_adbc_connection(). Nulled on any failure under
        # _adbc_conn_lock so the next operation reopens instead of
        # reusing a poisoned handle.
        self._adbc_conn: Any = None
        # Guards mutations of ``self._adbc_conn`` from worker threads.
        # ``asyncio.to_thread`` dispatches each ADBC call to the default
        # thread pool; without this lock two concurrent failures could
        # double-close the same DBAPI handle (libpq segfault risk) and
        # two concurrent reopens could open and leak a second connection.
        # Sync (``threading.Lock``) because the protected sections run
        # off the event loop.
        self._adbc_conn_lock: threading.Lock = threading.Lock()
        # PEP-249 reports ``threadsafety = 1`` for every ADBC driver
        # we ship — "threads may share the module, but not connections".
        # ``asyncio.to_thread`` gives no guarantee that subsequent
        # awaited calls land on the same worker thread, so concurrent
        # batches against one cached connection can corrupt cursor /
        # transaction state. This lock serializes ALL cursor operations
        # on the cached connection. Concurrent batches against the same
        # destination handler queue here — acceptable given the
        # PEP-249 constraint; the alternative is opening a fresh
        # connection per batch, which is far more expensive.
        # ``RLock`` (reentrant) because some sync helpers compose
        # internally (e.g. ``_truncate_then_ingest_sync`` calls
        # ``_adbc_only_ingest_sync`` after the truncate); both run on
        # the same worker thread within one ``asyncio.to_thread`` call.
        self._adbc_op_lock: threading.RLock = threading.RLock()

        # Read-path (Readable role) counters. Not consumed by the engine's
        # pipeline metrics — kept for parity with the source connector's
        # logging and for tests asserting per-stream read volume.
        self.metrics: Dict[str, int] = {"records_read": 0, "batches_read": 0}

    def set_endpoint_refs(self, endpoint_refs: Mapping[str, Any]) -> None:
        """Register stream_id → endpoint_ref for each stream writing to this
        destination. Called once by ``src.main`` before the gRPC server starts;
        the handler consults the map per incoming ``SchemaSpec`` to decide
        which ``TypeMapper`` applies (public endpoint → connector's map,
        private endpoint → connection's map).

        Values are dict-shape ``EndpointRef`` payloads
        (``{"scope", "connection_id", "endpoint_id"}`` plus optional ``x-*``
        extension keys).
        """
        self._endpoint_refs = dict(endpoint_refs)

    def set_stream_endpoints(
        self, stream_endpoints: Mapping[str, Mapping[str, Any]]
    ) -> None:
        """Register stream_id → contract endpoint document for streams
        writing to this destination. The handler reads the database object
        (catalog/schema/name), columns, and primary keys from this map at
        ``configure_schema`` time rather than unpacking them off the wire.
        """
        self._stream_endpoints = {
            sid: dict(doc) for sid, doc in stream_endpoints.items()
        }

    def _type_mapper_for_stream(self, stream_id: str) -> TypeMapper:
        """Resolve the type-mapper appropriate for ``stream_id``'s endpoint."""
        if self._runtime is None:
            raise RuntimeError(
                "GenericSQLConnector._type_mapper_for_stream() called before connect()"
            )
        endpoint_ref = self._endpoint_refs.get(stream_id)
        if endpoint_ref is None:
            raise RuntimeError(
                f"GenericSQLConnector has no endpoint_ref registered for "
                f"stream_id={stream_id!r}; call set_endpoint_refs() before the "
                f"gRPC server starts"
            )
        # The CDK takes only the resolved scope string, never the engine's
        # EndpointRef model. ``EndpointScope(scope)`` raises ValueError on an
        # unknown scope, preserving the validation that lived in
        # ``EndpointRef.__post_init__`` engine-side.
        scope = endpoint_ref.get("scope") if isinstance(endpoint_ref, Mapping) else None
        if not scope:
            raise RuntimeError(
                f"endpoint_ref for stream_id={stream_id!r} has no 'scope'; "
                f"expected one of {[s.value for s in EndpointScope]}"
            )
        return self._runtime.type_mapper_for(scope=EndpointScope(scope))

    @property
    def connector_type(self) -> str:
        """Return the connector type identifier."""
        return "database"

    @property
    def supports_transactions(self) -> bool:
        """All SQL databases support transactions."""
        return True

    @property
    def supports_upsert(self) -> bool:
        """True when the active dialect has an upsert path.

        SA-mode dialects implement ``build_sqlalchemy_upsert``; ADBC-only
        mode uses stage-table + ``MERGE INTO`` (``adbc_stage_table_sql``).
        Both are declared by the connector package's dialect — the neutral
        base supports neither.
        """
        if self._adbc_only:
            return self.dialect.supports_upsert_adbc
        return self.dialect.supports_upsert_sqlalchemy

    @property
    def supports_bulk_load(self) -> bool:
        """Bulk-load capability is not advertised here.

        ADBC dialects use Arrow-native ingest (``adbc_ingest``) for every
        write regardless of this flag; SA dialects use parameterized
        INSERT batches. Returning False keeps the destination protocol
        unaware of the distinction — the engine always batches the same
        way.
        """
        return False

    async def connect(self, runtime: ConnectionRuntime) -> None:
        """
        Establish database connection using ConnectionRuntime.

        Args:
            runtime: ConnectionRuntime with enriched config
        """
        self._runtime = runtime
        try:
            await materialize_runtime(
                runtime, sql_dialect=self.dialect
            )
        except DETERMINISTIC_CONNECT_ERRORS:
            raise
        except Exception as e:
            logger.error("Database destination connection failed: %s", e)
            raise ConnectionError(f"Database connection failed: {e}") from e
        self._driver = runtime.driver or ""
        # Reset prior-connection state so a long-lived handler that
        # reconnects across runtimes (e.g. tests) doesn't carry the
        # previous mode forward.
        self._adbc_only = False
        self._engine = None
        if runtime.is_adbc:
            self._adbc_only = True
            # Open the ADBC connection eagerly so a bad credential
            # fails at connect() time, not on the first batch. Wrap
            # the driver-specific exception in ConnectionError to
            # match the materialize() failure shape.
            try:
                self._adbc_conn = await asyncio.to_thread(
                    runtime.open_adbc_connection
                )
            except Exception as e:
                logger.error(
                    "ADBC eager-open failed during connect: %s", e, exc_info=True
                )
                # materialize() already acquired the runtime; the caller does
                # not disconnect a handler whose connect() raised, so release
                # the ref here to keep the lifecycle balanced.
                await runtime.close()
                raise ConnectionError(f"ADBC connection failed: {e}") from e
            logger.info(
                "GenericSQLConnector connected via ADBC to %s",
                self._driver,
            )
        else:
            self._engine = runtime.engine
            logger.info(
                "GenericSQLConnector connected via SQLAlchemy to %s",
                self._driver,
            )
        self._connected = True

    async def disconnect(self) -> None:
        """Close database connection.

        Both the ADBC connection and the SQLAlchemy runtime are
        released even if the other side fails or the coroutine is
        cancelled mid-close. The ADBC release uses ``BaseException``
        so ``asyncio.CancelledError`` during shutdown still gives the
        SQLAlchemy runtime a chance to dispose its engine pool.
        ``CancelledError`` is re-raised after both releases so the
        caller's cancellation is honored.

        The ``_adbc_conn`` mutation goes through ``_adbc_conn_lock``
        so a worker thread mid-poison cannot race with disconnect on
        the same handle (libpq double-close risk).
        """
        cancelled: Optional[BaseException] = None
        with self._adbc_conn_lock:
            adbc_conn = self._adbc_conn
            self._adbc_conn = None
        if adbc_conn is not None:
            try:
                await asyncio.to_thread(adbc_conn.close)
            except asyncio.CancelledError as exc:
                logger.error(
                    "ADBC close cancelled during disconnect; "
                    "server-side resources may remain allocated"
                )
                cancelled = exc
            except Exception:
                logger.error(
                    "Failed to close ADBC connection during disconnect; "
                    "server-side resources may remain allocated",
                    exc_info=True,
                )
        if self._runtime:
            try:
                await self._runtime.close()
            except asyncio.CancelledError as exc:
                logger.error("SQLAlchemy runtime close cancelled during disconnect")
                cancelled = exc
            except Exception:
                logger.error(
                    "Failed to close SQLAlchemy runtime during disconnect",
                    exc_info=True,
                )
        self._connected = False
        logger.info("GenericSQLConnector disconnected")
        if cancelled is not None:
            raise cancelled

    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        """Configure the destination from the preloaded contract endpoint.

        The SchemaSpec only carries identification fields; this method
        looks up the contract database endpoint document by stream_id and
        reads its ``database_object``, ``columns``, and ``primary_keys``
        directly. Both engine and destination load the same artifacts via
        ``PipelineConfigPrep``, so no schema details cross the wire.
        """
        if not self._connected:
            logger.error("Cannot configure schema: not connected")
            return False

        stream_id = schema_spec.stream_id
        endpoint_doc = self._stream_endpoints.get(stream_id)
        if endpoint_doc is None:
            logger.error(
                "No preloaded endpoint document for stream_id=%r; "
                "call set_stream_endpoints() before the gRPC server starts",
                stream_id,
            )
            return False

        # Every exception propagates to the gRPC layer. Deterministic,
        # actionable errors (SchemaConfigurationError, type-map errors,
        # missing secret, malformed endpoint document, engine not
        # connected at DDL time via AdbcConfigurationError) are
        # translated into the SchemaAck there with their real type and
        # message; anything else — wiring-defect RuntimeErrors from
        # _type_mapper_for_stream, raw driver errors during DDL — is a
        # defect that must fail the RPC as-is rather than degrade into
        # a generic schema rejection.
        database_object = endpoint_doc.get("database_object") or {}
        table_name = database_object.get("name") or ""
        if not table_name:
            logger.error(
                "Endpoint document for stream %r has no database_object.name",
                stream_id,
            )
            return False

        primary_keys = list(endpoint_doc.get("primary_keys") or [])
        # ``_write_conflict_keys`` is populated at startup by main.py
        # from the stream's WriteConfig (already resolved against the
        # endpoint's primary keys). A missing key means INSERT mode
        # — main.py always sets the field for UPSERT streams. An
        # explicit empty list also means "no conflict target", so we
        # only fall back to primary_keys when the key is absent.
        if "_write_conflict_keys" in endpoint_doc:
            conflict_keys = list(endpoint_doc["_write_conflict_keys"] or [])
        else:
            conflict_keys = list(primary_keys)
        raw_schema = database_object.get("schema")
        if self._adbc_only:
            # Snowflake's default schema is account-/role-dependent;
            # BigQuery requires an explicit dataset. Falling back to
            # "public" silently writes into a Postgres-shaped
            # namespace that may not exist. Require the endpoint
            # document to declare the schema explicitly.
            if not raw_schema:
                logger.error(
                    "ADBC destination requires database_object.schema "
                    "for stream %r (no implicit default)",
                    stream_id,
                )
                return False
            schema_name = raw_schema
        else:
            schema_name = raw_schema or "public"
        state = _StreamState(
            schema_name=schema_name,
            table_name=table_name,
            endpoint_document=dict(endpoint_doc),
            write_mode=self._get_write_mode(schema_spec.write_mode),
            primary_keys=primary_keys,
            conflict_keys=conflict_keys,
        )

        # Resolve the type-mapper for this stream's endpoint once —
        # both DDL generation and the schema contract use it.
        type_mapper = self._type_mapper_for_stream(stream_id)

        await self._ensure_tables_exist(state, type_mapper)

        state.schema_contract = SchemaContract(state.endpoint_document)

        self._streams[stream_id] = state
        logger.info(
            "Schema configured for stream %r: %s.%s, mode=%s, pk=%s",
            stream_id,
            state.schema_name,
            state.table_name,
            state.write_mode,
            state.primary_keys,
        )
        return True

    def _get_write_mode(self, proto_write_mode: int) -> str:
        mode_map = {
            1: "insert",
            2: "upsert",
            3: "truncate_insert",
        }
        if proto_write_mode not in mode_map:
            raise SchemaConfigurationError(
                f"Unsupported proto write_mode={proto_write_mode}; expected one "
                f"of {sorted(mode_map)} (WRITE_MODE_INSERT/UPSERT/TRUNCATE_INSERT)"
            )
        return mode_map[proto_write_mode]

    def _build_column_defs(
        self, state: _StreamState, type_mapper: TypeMapper
    ) -> List[ColumnDef]:
        """Contract endpoint columns -> ColumnDefs for the shared DDL builder.

        Each column's native type goes through the READ map to its canonical
        Arrow string; the builder renders it back through the WRITE map for
        this destination — the two declarative surfaces are the entire type
        vocabulary, for every transport. ``_synced_at`` is appended as a
        server-defaulted audit column when the endpoint doesn't declare it.
        """
        columns: List[ColumnDef] = []
        declared: set[str] = set()
        for index, col_def in enumerate(state.endpoint_document.get("columns") or []):
            col_name = col_def.get("name")
            if not col_name:
                raise SchemaConfigurationError(
                    f"endpoint column at index {index} has no 'name' field"
                )
            native_type = col_def.get("native_type")
            if not native_type:
                raise SchemaConfigurationError(
                    f"column {col_name!r} has no 'native_type' field"
                )
            raw_default = col_def.get("default")
            columns.append(
                ColumnDef(
                    name=col_name,
                    canonical_type=type_mapper.to_arrow_type(native_type),
                    nullable=bool(col_def.get("nullable", True)),
                    primary_key=col_name in state.primary_keys,
                    default=(
                        raw_default
                        if isinstance(raw_default, str) and raw_default.strip()
                        else None
                    ),
                )
            )
            declared.add(col_name)
        if self.SYNCED_AT_COLUMN not in declared:
            columns.append(
                ColumnDef(
                    name=self.SYNCED_AT_COLUMN,
                    canonical_type="Timestamp(MICROSECOND, UTC)",
                    nullable=True,
                    default=self.dialect.current_timestamp_default(),
                )
            )
        return columns

    def _build_batch_commits_ddl(
        self, schema_name: str, type_mapper: TypeMapper
    ) -> str:
        """``CREATE TABLE IF NOT EXISTS _batch_commits`` for any transport.

        Types derive from the connector's write map through the dialect's
        ``render_column_type`` (key columns via ``batch_commits_key_type``,
        bounded where unbounded text cannot key); quoting and the PK clause
        come from the dialect.
        """
        quote = self.dialect.quote_ident
        render = self.dialect.render_column_type
        qualified = self.dialect.quote_qualified(schema_name, self.BATCH_COMMITS_TABLE)
        key_type = self.dialect.batch_commits_key_type(type_mapper)
        return (
            f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
            f"  {quote('run_id')} {key_type} NOT NULL,\n"
            f"  {quote('stream_id')} {key_type} NOT NULL,\n"
            f"  {quote('batch_seq')} {render('Int64', type_mapper)} NOT NULL,\n"
            f"  {quote('committed_cursor')} {render('Binary', type_mapper)},\n"
            f"  {quote('records_written')} {render('Int32', type_mapper)},\n"
            f"  {quote('committed_at')} "
            f"{render('Timestamp(MICROSECOND)', type_mapper)} "
            f"DEFAULT {self.dialect.current_timestamp_default()},\n"
            f"  {self.dialect.pk_clause(['run_id', 'stream_id', 'batch_seq'])}\n"
            f")"
        )

    async def _ensure_tables_exist(
        self, state: _StreamState, type_mapper: TypeMapper
    ) -> None:
        """Create the target and ``_batch_commits`` tables if absent.

        ONE DDL builder serves every transport: column types come from the
        connector's read+write maps via the dialect, quoting/PK from the
        dialect, and the rendered statements execute over SQLAlchemy or the
        ADBC cursor. On the SQLAlchemy path the tables are then REFLECTED so
        DML binding uses the real column types the database reports.
        """
        if not state.endpoint_document:
            raise SchemaConfigurationError(
                f"destination stream for {state.schema_name}.{state.table_name} "
                f"has no endpoint document; cannot build DDL"
            )
        target_ddl = build_create_table_sql(
            self.dialect,
            type_mapper,
            state.schema_name,
            state.table_name,
            self._build_column_defs(state, type_mapper),
            list(state.primary_keys),
            if_not_exists=True,
        )
        commits_ddl = self._build_batch_commits_ddl(state.schema_name, type_mapper)

        if self._adbc_only:
            await self._ensure_tables_via_adbc(state, [target_ddl, commits_ddl])
            return

        if self._engine is None:
            # Silently skipping DDL here would leave state.table None and the
            # write_batch readiness guard returning RETRYABLE_FAILURE forever.
            raise AdbcConfigurationError(
                f"SQLAlchemy engine is None during DDL for "
                f"{state.schema_name}.{state.table_name}; "
                "connect() must be called before configure_schema()"
            )

        # Serialize DDL across concurrent streams so they do not race the
        # database catalog (e.g. PostgreSQL's pg_type_typname_nsp_index).
        async with self._ddl_lock:
            async with self._engine.begin() as conn:
                # Dialect-declared preparation (e.g. postgres' CREATE SCHEMA
                # IF NOT EXISTS for a non-default schema). The neutral base
                # declares none.
                for stmt in self.dialect.sqlalchemy_pre_ddl(state.schema_name):
                    await conn.execute(text(stmt))
                await conn.execute(text(target_ddl))
                await conn.execute(text(commits_ddl))

                # Reflect both tables for DML binding: SQLAlchemy derives the
                # column types from what the database actually created, so
                # inserts/upserts bind correctly without a second hand-kept
                # type surface.
                def _reflect(sync_conn):
                    meta = MetaData()
                    table = Table(
                        state.table_name,
                        meta,
                        autoload_with=sync_conn,
                        schema=state.schema_name or None,
                    )
                    commits = Table(
                        self.BATCH_COMMITS_TABLE,
                        meta,
                        autoload_with=sync_conn,
                        schema=state.schema_name or None,
                    )
                    return table, commits

                state.table, state.batch_commits_table = await conn.run_sync(
                    lambda sync_conn: _reflect(sync_conn)
                )

        logger.debug(f"Ensured tables exist in schema {state.schema_name}")


    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: List[str],
        cursor: Cursor,
    ) -> BatchWriteResult:
        """Write an Arrow record batch to the database.

        The batch is realigned to the destination schema in Arrow space
        (``cast_arrow_batch``) and only materialized to dicts at the very
        last SQLAlchemy boundary.
        """
        if not self._connected:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Handler not connected",
            )
        if not self._adbc_only and self._engine is None:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Handler not connected",
            )

        state = self._streams.get(stream_id)
        if state is None or (
            not self._adbc_only
            and (state.table is None or state.batch_commits_table is None)
        ):
            # ADBC writes never build SQLAlchemy table objects; readiness
            # there is the configured state + schema contract.
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Schema not configured",
            )

        existing = await self._check_batch_committed(state, run_id, stream_id, batch_seq)
        if existing:
            logger.info(f"Batch already committed: {run_id}/{stream_id}/{batch_seq}")
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_ALREADY_COMMITTED,
                records_written=existing["records_written"],
                committed_cursor=Cursor(token=existing["committed_cursor"]),
            )

        record_count = record_batch.num_rows
        if record_count == 0:
            # Empty batch - still record for idempotency
            await self._record_batch_commit(state, run_id, stream_id, batch_seq, cursor.token, 0)
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=0,
                committed_cursor=cursor,
            )

        try:
            if self._adbc_only:
                await self._write_batch_adbc_only(
                    state, run_id, stream_id, batch_seq,
                    record_batch, cursor.token, record_count,
                )
            else:
                prepared = self._prepare_for_sqlalchemy(state, record_batch)

                async with self._engine.begin() as conn:
                    if state.write_mode == "truncate_insert":
                        await self._truncate_and_insert(conn, state, prepared)
                    elif state.write_mode == "upsert":
                        await self._upsert_records(conn, state, prepared)
                    else:
                        await self._insert_records(conn, state, prepared)

                    # Record batch commit
                    await self._record_batch_commit_in_txn(
                        conn, state, run_id, stream_id, batch_seq, cursor.token, record_count
                    )

            logger.info(f"Wrote batch {batch_seq}: {record_count} records")
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=record_count,
                committed_cursor=cursor,
            )

        except (UnmappedTypeError, InvalidTypeMapError) as e:
            # Type-map errors are deterministic — retrying cannot succeed.
            # Classify as a fatal failure so the engine stops burning
            # cycles on a batch that will never go through.
            logger.error(f"Type-map error writing batch: {e}", exc_info=True)
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"type-map: {e}",
            )
        except UnsupportedDialectOperationError as e:
            # The dialect lacks the requested operation (e.g. upsert with
            # no connector package installed). Deterministic — fail fast.
            logger.error("Dialect operation unsupported: %s", e, exc_info=True)
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"dialect: {e}",
            )
        except AdbcConfigurationError as e:
            # ADBC misconfiguration cannot heal between attempts; bail
            # fatally so the engine does not retry forever.
            logger.error("ADBC configuration error: %s", e, exc_info=True)
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"adbc: {e}",
            )
        except AdbcCommitRecordError as e:
            # ADBC ingest already committed; the _batch_commits record
            # write failed. The retry will re-ingest. Stay RETRYABLE so
            # the engine reconciles, but surface the divergence text in
            # failure_summary so DLQ / monitoring can route on it.
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=str(e),
            )
        except Exception as e:
            logger.error(f"Error writing batch: {e}", exc_info=True)
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=str(e),
            )

    async def _check_batch_committed(
        self,
        state: _StreamState,
        run_id: str,
        stream_id: str,
        batch_seq: int,
    ) -> Optional[Dict[str, Any]]:
        """Check if batch was already committed."""
        if self._adbc_only:
            return await self._check_batch_committed_via_adbc(
                state, run_id, stream_id, batch_seq,
            )
        if state.batch_commits_table is None:
            return None
        if self._engine is None:
            return None

        commits = state.batch_commits_table
        async with self._engine.connect() as conn:
            result = await conn.execute(
                commits.select().where(
                    (commits.c.run_id == run_id) &
                    (commits.c.stream_id == stream_id) &
                    (commits.c.batch_seq == batch_seq)
                )
            )
            row = result.fetchone()
            if row:
                return {
                    "records_written": row.records_written,
                    "committed_cursor": row.committed_cursor,
                }
        return None

    async def _record_batch_commit(
        self,
        state: _StreamState,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        cursor_bytes: bytes,
        records_written: int,
    ) -> None:
        """Record batch commit (outside transaction)."""
        if self._adbc_only:
            await self._record_batch_commit_via_adbc(
                state, run_id, stream_id, batch_seq, cursor_bytes, records_written,
            )
            return
        if state.batch_commits_table is None:
            return
        if self._engine is None:
            return

        async with self._engine.begin() as conn:
            await self._record_batch_commit_in_txn(
                conn, state, run_id, stream_id, batch_seq, cursor_bytes, records_written
            )

    async def _record_batch_commit_in_txn(
        self,
        conn: AsyncConnection,
        state: _StreamState,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        cursor_bytes: bytes,
        records_written: int,
    ) -> None:
        """Record batch commit within existing transaction."""
        if state.batch_commits_table is None:
            return

        await conn.execute(
            state.batch_commits_table.insert().values(
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
                committed_cursor=cursor_bytes,
                records_written=records_written,
                committed_at=datetime.utcnow(),
            )
        )

    async def _insert_records(
        self,
        conn: AsyncConnection,
        state: _StreamState,
        records: List[Dict[str, Any]],
    ) -> None:
        """Insert pre-cast records (plain INSERT)."""
        if state.table is None or not records:
            return
        await conn.execute(state.table.insert(), records)

    async def _upsert_records(
        self,
        conn: AsyncConnection,
        state: _StreamState,
        records: List[Dict[str, Any]],
    ) -> None:
        """Upsert pre-cast records via the dialect's INSERT-or-UPDATE form.

        The statement shape is vendor-specific (postgres ``ON CONFLICT``,
        MySQL ``ON DUPLICATE KEY UPDATE``) and comes from the connector
        package's dialect. A dialect without one raises
        ``UnsupportedDialectOperationError`` — loud and fatal, never a
        silent downgrade to INSERT. The engine should not have routed an
        upsert here in the first place: ``supports_upsert`` gates the
        advertised write modes.

        When no conflict keys resolve (neither ``conflict_keys`` nor
        ``primary_keys``), there is nothing to match conflicts on; the
        write falls back to plain INSERT with a WARNING, since duplicates
        are then possible.
        """
        conflict_keys = state.conflict_keys or state.primary_keys
        if state.table is None or not conflict_keys:
            logger.warning(
                "upsert requested for %s.%s but no conflict keys resolved; "
                "falling back to plain INSERT — duplicates are possible",
                state.schema_name, state.table_name,
            )
            await self._insert_records(conn, state, records)
            return
        if not records:
            return

        stmt = self.dialect.build_sqlalchemy_upsert(
            state.table, records, conflict_keys
        )
        await conn.execute(stmt)

    async def _truncate_and_insert(
        self,
        conn: AsyncConnection,
        state: _StreamState,
        records: List[Dict[str, Any]],
    ) -> None:
        """Truncate table and insert pre-cast records."""
        if state.table is None:
            return
        await conn.execute(state.table.delete())
        await self._insert_records(conn, state, records)

    def _prepare_for_sqlalchemy(
        self, state: _StreamState, record_batch: pa.RecordBatch
    ) -> List[Dict[str, Any]]:
        """Materialise a batch for SQLAlchemy via the schema contract.

        ``to_db_records`` aligns the batch to the destination schema
        and materialises once. JSON columns stay as wire-format
        strings, so they bind directly to TEXT or JSONB columns
        without per-row coercion.
        """
        if state.schema_contract is None:
            raise AdbcConfigurationError(
                f"SQLAlchemy write for {state.schema_name}.{state.table_name} "
                "requires a configured SchemaContract; schema alignment was skipped"
            )
        return state.schema_contract.to_db_records(record_batch)

    # ------------------------------------------------------------------
    # ADBC-only mode (DDL + idempotency + writes via ADBC cursor)
    # ------------------------------------------------------------------
    #
    # When the runtime exposes an ADBC transport instead of a SQLAlchemy
    # engine, every DDL/idempotency/write call goes through the cached
    # ADBC DBAPI connection. Snowflake and BigQuery are the dialects
    # that require this path today (no async SA driver). Postgres can
    # also opt in (e.g. for Redshift via the libpq-compatible driver),
    # but its SA path remains the primary route.



    async def _ensure_tables_via_adbc(
        self, state: _StreamState, rendered_ddl: List[str]
    ) -> None:
        statements: List[str] = []
        if not self.dialect.schema_is_implicit_default(state.schema_name):
            # BigQuery uses ``CREATE SCHEMA`` for datasets (Standard
            # SQL). Snowflake and Postgres both accept the same DDL.
            # Normalize before quoting so a Snowflake ``public`` matches
            # the warehouse's real ``PUBLIC`` schema instead of creating
            # a quoted-lowercase sibling.
            statements.append(
                f"CREATE SCHEMA IF NOT EXISTS "
                f"{self.dialect.quote_ident(self.dialect.normalize_schema(state.schema_name))}"
            )
        statements.extend(rendered_ddl)
        async with self._ddl_lock:
            await asyncio.to_thread(self._execute_adbc_ddl_sync, statements)
        logger.debug(
            "ADBC-only DDL applied for %s.%s", state.schema_name, state.table_name
        )

    def _execute_adbc_ddl_sync(self, statements: List[str]) -> None:
        """Run a list of DDL statements on the ADBC connection.

        ``_adbc_op_lock`` held for the duration so concurrent batches
        can't interleave cursor use against PEP-249 threadsafety=1.
        """
        with self._adbc_op_lock:
            conn = self._reopen_adbc_if_needed_sync()
            try:
                cursor = conn.cursor()
                try:
                    for stmt in statements:
                        cursor.execute(stmt)
                    conn.commit()
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        logger.debug("ADBC cursor close failed", exc_info=True)
            except Exception as exc:
                self._poison_adbc_connection()
                if _is_fatal_adbc_error(exc):
                    raise _reclassify_as_fatal(exc) from exc
                raise

    def _poison_adbc_connection(self) -> None:
        """Drop and close the cached ADBC connection after a failure.

        The next operation re-opens via ``runtime.open_adbc_connection``.
        Close runs outside the lock so a slow libpq close path doesn't
        block other threads waiting to reopen; ``_adbc_conn_lock``
        ensures only one thread runs the close, preventing double-free
        on libpq handles.
        """
        with self._adbc_conn_lock:
            conn = self._adbc_conn
            self._adbc_conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                # Promoted from DEBUG: a failing close on a Snowflake /
                # BigQuery / Postgres ADBC handle is a server-side
                # resource leak (warehouse session, libpq fd, gRPC
                # context) operators may need to act on.
                logger.warning(
                    "Discarded poisoned ADBC connection; close failed — "
                    "potential server-side resource leak",
                    exc_info=True,
                )

    def _reopen_adbc_if_needed_sync(self) -> Any:
        """Return the cached ADBC connection, opening on demand.

        In ADBC-only mode the connection is opened in ``connect()`` but
        may be poisoned by an earlier failure. This helper transparently
        re-opens via the runtime so each write is self-healing.
        The lock guards a check-then-act race: two threads could both
        observe ``_adbc_conn is None`` and each open a new connection,
        leaking one.
        """
        with self._adbc_conn_lock:
            if self._adbc_conn is not None:
                return self._adbc_conn
            if self._runtime is None:
                raise AdbcConfigurationError(
                    "Runtime not available for ADBC reconnect"
                )
            # open_adbc_connection is sync; safe to call inside the lock
            # because the lock is fast (no I/O) — only the connect() call
            # itself blocks, but that's the work this method is doing.
            self._adbc_conn = self._runtime.open_adbc_connection()
            return self._adbc_conn

    async def _check_batch_committed_via_adbc(
        self,
        state: _StreamState,
        run_id: str,
        stream_id: str,
        batch_seq: int,
    ) -> Optional[Dict[str, Any]]:
        qualified = self.dialect.quote_qualified(
            state.schema_name, self.BATCH_COMMITS_TABLE
        )
        sql = (
            f"SELECT {self.dialect.quote_ident('records_written')}, "
            f"{self.dialect.quote_ident('committed_cursor')} "
            f"FROM {qualified} "
            f"WHERE {self.dialect.quote_ident('run_id')} = ? "
            f"AND {self.dialect.quote_ident('stream_id')} = ? "
            f"AND {self.dialect.quote_ident('batch_seq')} = ?"
        )
        row = await asyncio.to_thread(
            self._fetch_one_adbc_sync, sql, (run_id, stream_id, batch_seq),
        )
        if row is None:
            return None
        records_written, committed_cursor = row
        return {
            "records_written": int(records_written) if records_written is not None else 0,
            "committed_cursor": bytes(committed_cursor) if committed_cursor else b"",
        }

    def _fetch_one_adbc_sync(
        self, sql: str, params: Tuple[Any, ...]
    ) -> Optional[Tuple[Any, ...]]:
        with self._adbc_op_lock:
            conn = self._reopen_adbc_if_needed_sync()
            try:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, params)
                    return cursor.fetchone()
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        logger.debug("ADBC cursor close failed", exc_info=True)
            except Exception as exc:
                self._poison_adbc_connection()
                if _is_fatal_adbc_error(exc):
                    raise _reclassify_as_fatal(exc) from exc
                raise

    async def _record_batch_commit_via_adbc(
        self,
        state: _StreamState,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        cursor_bytes: bytes,
        records_written: int,
    ) -> None:
        """Record the commit row over ADBC: plain INSERT against the
        enforced ``(run_id, stream_id, batch_seq)`` primary key.

        This base implementation relies on the destination *enforcing* that
        PK: a concurrent retry's duplicate INSERT raises IntegrityError,
        which is the collision signal. A system whose primary keys are not
        enforced (e.g. BigQuery) must override this method in its connector
        package with a collision-safe statement (BigQuery uses ``MERGE …
        WHEN NOT MATCHED THEN INSERT`` plus a rowcount check).
        """
        qualified = self.dialect.quote_qualified(
            state.schema_name, self.BATCH_COMMITS_TABLE
        )
        sql = (
            f"INSERT INTO {qualified} ("
            f"{self.dialect.quote_ident('run_id')}, "
            f"{self.dialect.quote_ident('stream_id')}, "
            f"{self.dialect.quote_ident('batch_seq')}, "
            f"{self.dialect.quote_ident('committed_cursor')}, "
            f"{self.dialect.quote_ident('records_written')}) "
            f"VALUES (?, ?, ?, ?, ?)"
        )
        try:
            await asyncio.to_thread(
                self._execute_adbc_dml_sync,
                sql,
                (run_id, stream_id, batch_seq, cursor_bytes, records_written),
            )
        except AdbcConfigurationError as exc:
            # An IntegrityError on the (run_id, stream_id, batch_seq)
            # PK means a concurrent retry already wrote the commit row.
            # For idempotent write modes (truncate_insert, upsert) the
            # destination's rows are unchanged by the retry's re-ingest,
            # so surfacing fatal would orphan ingested rows without a
            # _batch_commits entry → cold-start would re-ingest and
            # duplicate. Treat as success.
            #
            # For ``insert`` mode the re-ingest already duplicated rows
            # in the target table; reporting success would hide the
            # duplication. Surface as AdbcCommitRecordError so the
            # engine's failure summary carries the divergence and DLQ
            # rules can route on it.
            cause = exc.__cause__
            # MRO walk (mirroring _is_fatal_adbc_error) so a driver-side
            # subclass like ``MyDriverIntegrityError(IntegrityError)``
            # is still recognised. A bare ``type(cause).__name__`` check
            # would let the subclass slip past and be re-raised as fatal,
            # orphaning ingested rows on idempotent write modes.
            if cause is not None and any(
                cls.__name__ == "IntegrityError" for cls in type(cause).__mro__
            ):
                self._handle_commit_collision(
                    state, run_id, stream_id, batch_seq, cause,
                )
                return
            raise

    def _handle_commit_collision(
        self,
        state: _StreamState,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        cause: BaseException,
    ) -> None:
        """Centralise the per-write_mode handling of a concurrent commit
        collision (whichever driver-specific signal surfaced it — PG/Snow
        IntegrityError, or BigQuery MERGE rowcount=0).

        For ``insert`` mode the colliding retry already ingested rows
        and reporting success here would hide the duplication. Raise
        ``AdbcCommitRecordError`` so the engine's failure summary carries
        the divergence. For ``upsert`` / ``truncate_insert`` the re-ingest
        is idempotent, so we treat the collision as already-committed.
        """
        if state.write_mode == "insert":
            logger.error(
                "ADBC _batch_commits raced concurrent retry for %s/%s/%s "
                "in insert mode — rows likely duplicated by the loser's "
                "prior adbc_ingest",
                run_id, stream_id, batch_seq,
            )
            raise AdbcCommitRecordError(cause, state.write_mode) from cause
        logger.info(
            "ADBC _batch_commits raced concurrent retry for %s/%s/%s "
            "(%s mode); idempotent — treating as already-committed",
            run_id, stream_id, batch_seq, state.write_mode,
        )

    def _execute_adbc_dml_sync(self, sql: str, params: Tuple[Any, ...]) -> int:
        """Execute ``sql`` with ``params``; return DBAPI ``rowcount`` or -1.

        ``rowcount`` matters for BigQuery's MERGE-based commit-record
        path: a successful MERGE that NO-OPs (because the conflict row
        already exists) returns 0 rows-affected. Without a rowcount
        check, the BigQuery PK NOT-ENFORCED collision is indistinguishable
        from a fresh insert, and insert-mode duplication slips through
        silently — re-introducing exactly the bug the Snowflake/Postgres
        IntegrityError path catches.

        Some ADBC drivers return -1 when rowcount is unavailable; callers
        must treat -1 as "unknown" rather than "no rows affected".
        """
        with self._adbc_op_lock:
            conn = self._reopen_adbc_if_needed_sync()
            try:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, params)
                    rowcount = getattr(cursor, "rowcount", -1)
                    conn.commit()
                    return rowcount if isinstance(rowcount, int) else -1
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        logger.debug("ADBC cursor close failed", exc_info=True)
            except Exception as exc:
                self._poison_adbc_connection()
                if _is_fatal_adbc_error(exc):
                    raise _reclassify_as_fatal(exc) from exc
                raise

    async def _write_batch_adbc_only(
        self,
        state: _StreamState,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        cursor_bytes: bytes,
        record_count: int,
    ) -> None:
        """Full write path for ADBC-only mode.

        Insert: append via ``adbc_ingest``. Truncate-insert: TRUNCATE
        TABLE then append. Upsert: ingest into a session-scoped temp
        table, then ``MERGE INTO`` the target.

        Unlike the SA path's atomicity (write + commit-record in one
        transaction), the ADBC path commits the ingest separately from
        the ``_batch_commits`` INSERT. A crash between the two will
        cause the next retry to:

        * ``insert`` — re-ingest, duplicating rows.
        * ``truncate_insert`` — TRUNCATE then re-ingest; idempotent.
        * ``upsert`` — re-MERGE on conflict keys; idempotent.

        See :class:`AdbcCommitRecordError` for the exception that
        surfaces this divergence to the engine's failure summary.
        """
        if state.schema_contract is None:
            raise AdbcConfigurationError(
                f"ADBC-only write for {state.schema_name}.{state.table_name} "
                "requires a configured SchemaContract; schema alignment was skipped"
            )
        cast_batch = state.schema_contract.cast_arrow_batch(record_batch)
        conflict_keys = state.conflict_keys or state.primary_keys

        if state.write_mode == "truncate_insert":
            await asyncio.to_thread(
                self._truncate_then_ingest_sync,
                cast_batch, state.schema_name, state.table_name,
            )
        elif state.write_mode == "upsert" and conflict_keys:
            # Fingerprint via SHA-256 over (run_id, stream_id, batch_seq)
            # gives a fixed-width collision-resistant token that survives
            # any future identifier-length pressure. Critical here because
            # Postgres' NAMEDATALEN is 63 and a UUID-shaped stream_id would
            # otherwise force truncation that drops batch_seq, defeating
            # the per-batch uniqueness this token exists to provide. Hex
            # digest first 16 chars = 64 bits of entropy, plenty for
            # per-(stream, batch) uniqueness within a destination handler's
            # lifetime; "b" prefix keeps the token a valid identifier in
            # every supported dialect.
            stage_token = "b" + hashlib.sha256(
                f"{run_id}|{stream_id}|{batch_seq}".encode("utf-8")
            ).hexdigest()[:16]
            await asyncio.to_thread(
                self._merge_ingest_sync,
                cast_batch, state.schema_name, state.table_name,
                list(cast_batch.schema.names), conflict_keys,
                stage_token,
            )
        else:
            if state.write_mode == "upsert":
                # Same fallback semantics as the SQLAlchemy path's
                # _upsert_records: no conflict keys means nothing to match
                # conflicts on, so the write degrades loudly to plain ingest.
                logger.warning(
                    "upsert requested for %s.%s but no conflict keys resolved; "
                    "falling back to plain ingest — duplicates are possible",
                    state.schema_name, state.table_name,
                )
            await asyncio.to_thread(
                self._adbc_only_ingest_sync,
                cast_batch, state.schema_name, state.table_name,
            )

        try:
            await self._record_batch_commit_via_adbc(
                state, run_id, stream_id, batch_seq, cursor_bytes, record_count,
            )
        except AdbcConfigurationError:
            # Already classified as fatal by _record_batch_commit_via_adbc
            # (PEP-249 ProgrammingError, NotSupportedError, DataError).
            # Propagate as fatal — wrapping would re-classify as retryable.
            raise
        except AdbcCommitRecordError:
            # The insert-mode IntegrityError-on-PK collision branch in
            # _record_batch_commit_via_adbc raises this directly with
            # the right write_mode and "retry will duplicate" message.
            # Re-wrapping would double-prefix the failure_summary and
            # collapse the precise insert-race wording into a generic
            # "commit-record failed".
            raise
        except Exception as commit_exc:
            logger.error(
                "ADBC-only ingest committed but commit-record failed "
                "for %s/%s/%s — see AdbcCommitRecordError for retry semantics",
                run_id, stream_id, batch_seq,
                exc_info=True,
            )
            raise AdbcCommitRecordError(commit_exc, state.write_mode) from commit_exc

    def _adbc_only_ingest_sync(
        self,
        cast_batch: pa.RecordBatch,
        schema_name: str,
        table_name: str,
    ) -> None:
        """ADBC ingest for ADBC-only mode (poison-aware, fatal-reclassifying)."""
        with self._adbc_op_lock:
            conn = self._reopen_adbc_if_needed_sync()
            try:
                cursor = conn.cursor()
                try:
                    cursor.adbc_ingest(
                        table_name,
                        cast_batch,
                        mode="append",
                        db_schema_name=(
                            self.dialect.normalize_schema(schema_name)
                            if schema_name else None
                        ),
                    )
                    conn.commit()
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        logger.debug("ADBC cursor close failed", exc_info=True)
            except Exception as exc:
                self._poison_adbc_connection()
                if _is_fatal_adbc_error(exc):
                    raise _reclassify_as_fatal(exc) from exc
                raise

    def _truncate_then_ingest_sync(
        self,
        cast_batch: pa.RecordBatch,
        schema_name: str,
        table_name: str,
    ) -> None:
        qualified = self.dialect.quote_qualified(schema_name, table_name)
        with self._adbc_op_lock:
            conn = self._reopen_adbc_if_needed_sync()
            try:
                cursor = conn.cursor()
                try:
                    cursor.execute(f"TRUNCATE TABLE {qualified}")
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        logger.debug("ADBC cursor close failed", exc_info=True)
                conn.commit()
            except Exception as exc:
                self._poison_adbc_connection()
                if _is_fatal_adbc_error(exc):
                    raise _reclassify_as_fatal(exc) from exc
                raise
            # RLock is reentrant: this same-thread acquire inside
            # _adbc_only_ingest_sync is safe.
            self._adbc_only_ingest_sync(cast_batch, schema_name, table_name)

    def _merge_ingest_sync(
        self,
        cast_batch: pa.RecordBatch,
        schema_name: str,
        table_name: str,
        all_columns: List[str],
        conflict_keys: List[str],
        stage_token: str,
    ) -> None:
        """Upsert via ingest-to-stage + ``MERGE INTO`` target.

        Creates a stage table named ``_analitiq_stage_<target>_<token>``
        in the target schema, ingests the cast batch via
        ``adbc_ingest``, runs ``MERGE INTO target USING stage``, then
        explicitly DROPs the stage. ``stage_token`` is a fixed-width
        SHA-256 fingerprint of ``(run_id, stream_id, batch_seq)``
        computed at the call site, so the name is unique across
        concurrent streams writing to the same target, across batches
        of the same stream, and across retries overlapping the previous
        attempt's DROP — all within Postgres' 63-char NAMEDATALEN budget.

        When every column is a conflict key (composite-PK table with
        no non-key columns), MERGE's ``WHEN MATCHED THEN UPDATE`` is
        omitted and the operation degrades to insert-if-not-exists. A
        warning surfaces this so operators don't silently see "matched
        rows unchanged" without an explanation.
        """
        # Suffix with the per-write token so concurrent streams and
        # retries (which may overlap before the previous DROP completes)
        # do not collide on the stage table name.
        stage_name = f"_analitiq_stage_{table_name}_{stage_token}"
        target_qualified = self.dialect.quote_qualified(schema_name, table_name)
        stage_qualified = self.dialect.quote_qualified(schema_name, stage_name)
        update_cols = [c for c in all_columns if c not in conflict_keys]
        if not update_cols:
            logger.warning(
                "ADBC upsert into %s.%s has no non-key columns to update "
                "(all_columns == conflict_keys); MERGE will only INSERT "
                "new rows. Consider write_mode='insert' for clarity.",
                schema_name, table_name,
            )
        # _adbc_op_lock serializes the full DROP+CREATE+INGEST+MERGE+DROP
        # sequence so concurrent streams against the same handler don't
        # interleave cursor operations on the cached connection
        # (PEP-249 threadsafety=1). One acquire for the whole transaction
        # so a parallel ingest can't slip between CREATE and INGEST and
        # leave the stage table empty.
        with self._adbc_op_lock:
            self._merge_ingest_locked_sync(
                cast_batch, target_qualified, stage_qualified, stage_name,
                schema_name, all_columns, conflict_keys, update_cols,
            )

    def _merge_ingest_locked_sync(
        self,
        cast_batch: pa.RecordBatch,
        target_qualified: str,
        stage_qualified: str,
        stage_name: str,
        schema_name: str,
        all_columns: List[str],
        conflict_keys: List[str],
        update_cols: List[str],
    ) -> None:
        """Body of :meth:`_merge_ingest_sync`, called while
        ``_adbc_op_lock`` is held. Extracted so the lock acquisition
        site is small and obvious; the inner method assumes the lock
        and never reacquires."""
        conn = self._reopen_adbc_if_needed_sync()
        try:
            cursor = conn.cursor()
            try:
                # DROP-IF-EXISTS before CREATE so a retry of the same
                # (run_id, batch_seq) — typical when the previous
                # attempt crashed between adbc_ingest and the success-
                # path DROP — finds a clean slate. Without this, the
                # retry's CREATE TABLE hits "already exists" → PEP-249
                # ProgrammingError → fatal reclassification → engine
                # stops retrying a recoverable batch. The leftover
                # stage is opaque to the warehouse's planner, so the
                # extra DROP is one cheap statement per upsert.
                cursor.execute(f"DROP TABLE IF EXISTS {stage_qualified}")
                cursor.execute(
                    self.dialect.adbc_stage_table_sql(stage_qualified, target_qualified)
                )
                conn.commit()
                cursor.adbc_ingest(
                    stage_name,
                    cast_batch,
                    mode="append",
                    # Stage lives in the target schema; the driver needs
                    # the schema name to resolve the right table. Same
                    # normalization as the DDL path so Snowflake's real
                    # PUBLIC schema is matched when the connector
                    # declared lowercase ``public``.
                    db_schema_name=(
                        self.dialect.normalize_schema(schema_name)
                        if schema_name else None
                    ),
                )
                conn.commit()
                on_clause = " AND ".join(
                    f"t.{self.dialect.quote_ident(k)} = s.{self.dialect.quote_ident(k)}"
                    for k in conflict_keys
                )
                set_clause = ", ".join(
                    f"t.{self.dialect.quote_ident(c)} = s.{self.dialect.quote_ident(c)}"
                    for c in update_cols
                )
                insert_cols = ", ".join(self.dialect.quote_ident(c) for c in all_columns)
                insert_vals = ", ".join(
                    f"s.{self.dialect.quote_ident(c)}" for c in all_columns
                )
                merge_sql = (
                    f"MERGE INTO {target_qualified} t USING {stage_qualified} s "
                    f"ON {on_clause} "
                )
                if update_cols:
                    merge_sql += f"WHEN MATCHED THEN UPDATE SET {set_clause} "
                merge_sql += (
                    f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                    f"VALUES ({insert_vals})"
                )
                cursor.execute(merge_sql)
                conn.commit()
            finally:
                try:
                    cursor.close()
                except Exception:
                    logger.debug("ADBC cursor close failed", exc_info=True)
        except Exception as exc:
            # Best-effort stage cleanup using the local ``conn`` (not
            # ``self._adbc_conn``) so a concurrent poisoning by another
            # thread cannot turn this into a use-after-poison race or
            # commit() against a freshly-reopened connection that owns
            # no transaction state for the stage. If ``conn`` itself is
            # the one that failed, the DROP will also fail and the warn
            # below will fire — same observable outcome, no race.
            try:
                drop_cursor = conn.cursor()
                try:
                    drop_cursor.execute(
                        f"DROP TABLE IF EXISTS {stage_qualified}"
                    )
                    conn.commit()
                finally:
                    try:
                        drop_cursor.close()
                    except Exception:
                        logger.debug(
                            "ADBC cursor close failed", exc_info=True
                        )
            except Exception:
                # The next retry's pre-flight DROP-IF-EXISTS will clean
                # the orphan up; warn so an operator sees the leftover
                # in the meantime.
                logger.warning(
                    "ADBC stage table %s left behind after MERGE failure; "
                    "the next retry's pre-flight DROP-IF-EXISTS will clean it up",
                    stage_qualified, exc_info=True,
                )
            self._poison_adbc_connection()
            if _is_fatal_adbc_error(exc):
                raise _reclassify_as_fatal(exc) from exc
            raise
        # Successful path — DROP the stage so subsequent writes start
        # clean. If this DROP fails, the next retry of the same batch
        # cleans it up via the pre-flight DROP-IF-EXISTS at the top of
        # this method, so even a persistent DROP failure does not break
        # idempotency.
        try:
            drop_cursor = conn.cursor()
            try:
                drop_cursor.execute(f"DROP TABLE IF EXISTS {stage_qualified}")
                conn.commit()
            finally:
                try:
                    drop_cursor.close()
                except Exception:
                    logger.debug("ADBC cursor close failed", exc_info=True)
        except Exception:
            logger.warning(
                "ADBC stage table %s post-MERGE DROP failed; next retry of "
                "this batch will clean it up via pre-flight DROP-IF-EXISTS",
                stage_qualified, exc_info=True,
            )

    async def health_check(self) -> bool:
        """Check database health."""
        if not self._connected:
            return False

        if self._adbc_only:
            try:
                await asyncio.to_thread(self._health_check_adbc_sync)
                return True
            except Exception as e:
                logger.warning(f"Health check failed: {e}")
                return False

        if self._engine is None:
            return False
        try:
            async with self._engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False

    def _health_check_adbc_sync(self) -> None:
        """Health probe for ADBC-only mode.

        Self-heals a poisoned cached connection by reopening through
        the runtime. Without the reopen, a poisoned cache would make
        this probe fail until some other caller (next write_batch)
        repopulated the cache — i.e. liveness would lag the actual
        DB reachability by one batch interval. The reopen makes the
        probe self-sufficient. ``_adbc_op_lock`` held so the SELECT 1
        does not interleave with a concurrent ingest on the same
        connection.
        """
        with self._adbc_op_lock:
            conn = self._reopen_adbc_if_needed_sync()
            try:
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                finally:
                    try:
                        cursor.close()
                    except Exception:
                        logger.debug("ADBC cursor close failed", exc_info=True)
            except Exception:
                self._poison_adbc_connection()
                raise

    # ==================================================================
    # Readable role (source reads)
    # ==================================================================
    #
    # ``read_batches`` is self-contained: it materializes the runtime it is
    # handed, pages the table, and releases the runtime on exit. No prior
    # ``connect()`` is required — ``runtime`` is the only connection input,
    # matching the ``Readable`` Protocol. The write role's connection state
    # (``self._engine`` / ``self._adbc_conn``) is untouched; a source-role
    # instance and a write-role instance are distinct objects.

    async def read_batches(
        self,
        runtime: ConnectionRuntime,
        config: Dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Read upstream rows as Arrow batches typed via the endpoint contract."""
        endpoint_doc = config.get("endpoint_document")
        if not endpoint_doc:
            raise ReadError(
                "GenericSQLConnector: source config missing 'endpoint_document'"
            )
        database_object = endpoint_doc.get("database_object") or {}
        table_name = database_object.get("name")
        if not table_name:
            raise ReadError("endpoint document missing database_object.name")
        # No default schema: dialects without a schema concept (sqlite,
        # duckdb) would emit invalid ``public.<table>`` references if we
        # forced one. When the endpoint omits ``schema``, QueryBuilder emits
        # an unqualified table name and the driver uses the connection's
        # current schema/database.
        schema_name = database_object.get("schema")

        try:
            await materialize_runtime(
                runtime, sql_dialect=self.dialect
            )
        except DETERMINISTIC_CONNECT_ERRORS:
            raise
        except Exception as e:
            logger.error("Failed to connect to source database: %s", e)
            raise ReadError(f"Database connection failed: {e}") from e

        driver = runtime.driver or ""
        adbc_only = runtime.is_adbc
        engine = None if adbc_only else runtime.engine
        logger.info(
            "Reading source via %s (%s)",
            driver, "ADBC" if adbc_only else "SQLAlchemy",
        )

        try:
            stream_source = config.get("stream_source") or {}
            schema_contract = SchemaContract(endpoint_doc)
            column_names = self._select_columns(endpoint_doc, stream_source)
            filters = self._build_filters(stream_source.get("filters") or [])

            replication = stream_source.get("replication") or {}
            cursor_field = replication.get("cursor_field")
            if isinstance(cursor_field, list):
                cursor_field = cursor_field[0] if cursor_field else None
            replication_method = replication.get("method", "full_refresh")

            database_pagination = stream_source.get("database_pagination") or {}
            order_by_field = database_pagination.get("order_by_field") or None

            if replication_method == "incremental" and cursor_field:
                # The wildcard projection compiles to SELECT * (see
                # QueryBuilder.build_select_query), but the fetched batch
                # is cast through SchemaContract, which keeps only the
                # endpoint contract's columns — so the cursor column must
                # be declared there for its value to survive the cast.
                if column_names == ["*"]:
                    effective_columns = [
                        c["name"]
                        for c in (endpoint_doc.get("columns") or [])
                        if c.get("name")
                    ]
                else:
                    effective_columns = column_names
                if cursor_field not in effective_columns:
                    # An incremental stream whose projection drops the
                    # cursor column silently reverts to "full-scan +
                    # upsert" every run: no cursor value is observable, so
                    # no state advances. The stream is misconfigured —
                    # fail before any extraction work.
                    raise ReadError(
                        f"stream {stream_name!r}: incremental replication "
                        f"requires cursor_field {cursor_field!r} to be present "
                        f"in the projection. Effective columns: "
                        f"{effective_columns!r}"
                    )

            effective_order = (
                cursor_field if replication_method == "incremental" else order_by_field
            )
            if driver.lower() == "mssql" and effective_order is None:
                raise ReadError(
                    f"stream {stream_name!r}: MSSQL full-refresh paging requires "
                    f"database_pagination.order_by_field — T-SQL refuses OFFSET "
                    f"without ORDER BY and no cursor_field is available"
                )

            partition = partition or {}
            cursor_state = await checkpoint.get_cursor(stream_name, partition)
            stored_cursor = cursor_state.get("cursor") if cursor_state else None
            cursor_value = (
                stored_cursor if replication_method == "incremental" else None
            )

            if adbc_only:
                async for batch in self._read_via_adbc_only(
                    runtime=runtime,
                    driver=driver,
                    schema_contract=schema_contract,
                    schema_name=schema_name,
                    table_name=table_name,
                    columns=column_names,
                    filters=filters,
                    cursor_field=(
                        cursor_field if replication_method == "incremental" else None
                    ),
                    order_by_field=order_by_field,
                    cursor_value=cursor_value,
                    batch_size=batch_size,
                    checkpoint=checkpoint,
                    stream_name=stream_name,
                    partition=partition,
                ):
                    yield batch
                logger.debug("Source read (ADBC-only) completed")
                return

            builder = QueryBuilder(driver)

            def page_query(offset: int):
                """Build the per-page SELECT. Limit / offset are pushed into
                ``QueryConfig`` so SQLAlchemy compiles dialect-correct paging.
                ``params`` is a list for positional dialects and a dict for
                named ones."""
                sql, params = builder.build_select_query(
                    QueryConfig(
                        schema_name=schema_name,
                        table_name=table_name,
                        columns=column_names,
                        filters=filters,
                        cursor_field=(
                            cursor_field
                            if replication_method == "incremental"
                            else None
                        ),
                        cursor_value=cursor_value,
                        cursor_mode="inclusive",
                        order_by=(
                            cursor_field if replication_method == "incremental"
                            else order_by_field
                        ),
                        limit=batch_size,
                        offset=offset,
                    )
                )
                return sql, params

            last_cursor_value = cursor_value
            offset = 0

            async with acquire_connection(engine) as conn:
                while True:
                    paged_query, paged_params = page_query(offset)
                    if isinstance(paged_params, dict):
                        # Named-paramstyle dialects (Snowflake pyformat,
                        # BigQuery named): the driver binds by name and
                        # expects a dict, not a positional tuple.
                        if paged_params:
                            result = await conn.exec_driver_sql(
                                paged_query, paged_params
                            )
                        else:
                            result = await conn.exec_driver_sql(paged_query)
                    elif paged_params:
                        result = await conn.exec_driver_sql(
                            paged_query, tuple(paged_params)
                        )
                    else:
                        result = await conn.exec_driver_sql(paged_query)

                    rows = [dict(row._mapping) for row in result]
                    if not rows:
                        break

                    if cursor_field:
                        last_cursor_value = rows[-1].get(
                            cursor_field, last_cursor_value
                        )

                    self.metrics["records_read"] += len(rows)
                    self.metrics["batches_read"] += 1

                    yield schema_contract.from_pylist(rows)

                    if last_cursor_value is not None:
                        await checkpoint.save_cursor(
                            stream_name, partition, {"cursor": last_cursor_value}
                        )

                    offset += batch_size
                    if len(rows) < batch_size:
                        break

            logger.debug("Source read completed with cursor: %s", last_cursor_value)
        finally:
            # ``runtime`` is the only connection input; the source role owns
            # its lifecycle, so release it (disposes the SA engine pool /
            # closes the cached ADBC handle) whatever the read's outcome.
            await runtime.close()

    async def _read_via_adbc_only(
        self,
        *,
        runtime: ConnectionRuntime,
        driver: str,
        schema_contract: SchemaContract,
        schema_name: Optional[str],
        table_name: str,
        columns: List[str],
        filters: List[Filter],
        cursor_field: Optional[str],
        order_by_field: Optional[str],
        cursor_value: Any,
        batch_size: int,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: Dict[str, Any],
    ) -> AsyncIterator[pa.RecordBatch]:
        """Stream Arrow batches via the ADBC-only path.

        SQL is compiled by the shared :class:`QueryBuilder` in qmark mode
        (forced ``?`` placeholders, every identifier quoted, inlined
        LIMIT/OFFSET) so filters and the incremental cursor render through
        the same WHERE machinery as the SQLAlchemy transport. Holds one
        DBAPI connection for the lifetime of the read; each page goes
        ``cursor.execute -> fetch_arrow_table -> cast``.

        The WHERE clause is fixed at the read's initial cursor_value
        (matching the SA path); paging advances via OFFSET only. Mixing
        cursor advancement with OFFSET would skip rows on every page after
        the first.

        ORDER BY priority: ``cursor_field`` (incremental), then
        ``order_by_field`` (from ``database_pagination``), then
        ``columns[0]`` with a logged warning.
        """
        if not columns:
            # An empty projection compiles to ``SELECT`` with no columns —
            # an invalid statement regardless of ordering. Fail loudly.
            raise ReadError(
                "ADBC-only source requires a non-empty column projection"
            )

        # The ADBC path quotes every identifier, so normalize the schema
        # through the connector's dialect — the same rule the destination
        # handler applies (e.g. Snowflake folds lowercase ``public`` ->
        # ``PUBLIC``) — or the quoted name targets a different schema.
        effective_schema = (
            self.dialect.normalize_schema(schema_name) if schema_name else None
        )

        if cursor_field:
            order_by = cursor_field
        elif order_by_field:
            order_by = order_by_field
        else:
            order_by = columns[0]
            _note_order_by_fallback(table_name, order_by)

        builder = QueryBuilder(
            driver,
            paramstyle="qmark",
            quote_identifiers=True,
            inline_paging=True,
        )

        # Initial cursor value is fixed for the duration of the read;
        # last_cursor_value advances purely for checkpoint state.
        initial_cursor_value = cursor_value
        last_cursor_value: Any = cursor_value
        offset = 0
        cursor_missing_warned = False
        async with open_adbc_reader(driver, runtime) as reader:
            while True:
                sql, params = builder.build_select_query(
                    QueryConfig(
                        schema_name=effective_schema,
                        table_name=table_name,
                        columns=columns,
                        filters=filters,
                        cursor_field=cursor_field,
                        cursor_value=initial_cursor_value if cursor_field else None,
                        cursor_mode="inclusive",
                        order_by=order_by,
                        limit=batch_size,
                        offset=offset,
                    )
                )
                if isinstance(params, dict):
                    # QueryBuilder is built with paramstyle="qmark", so it
                    # must return positional params. A dict means the dialect
                    # ignored the forced paramstyle; the ADBC execute path
                    # (cursor.execute(sql, list(params))) would then bind
                    # parameter *names* instead of values. Fail loudly rather
                    # than corrupt the binds.
                    raise ReadError(
                        f"ADBC-only source for driver {driver!r}: expected "
                        f"positional qmark parameters but QueryBuilder produced "
                        f"named parameters; the ADBC execute path binds "
                        f"positionally"
                    )
                batches = await reader.fetch_page(sql, params)
                if not batches:
                    break

                page_rows = 0
                for batch in batches:
                    cast_batch = schema_contract.cast_arrow_batch(batch)
                    page_rows += cast_batch.num_rows
                    if cursor_field and cast_batch.num_rows > 0:
                        if cursor_field in cast_batch.schema.names:
                            last_cursor_value = cast_batch.column(cursor_field)[-1].as_py()
                        elif not cursor_missing_warned:
                            logger.warning(
                                "stream %r: cursor_field %r not present in "
                                "result batch; cursor will not advance",
                                stream_name, cursor_field,
                            )
                            cursor_missing_warned = True
                    self.metrics["records_read"] += cast_batch.num_rows
                    self.metrics["batches_read"] += 1
                    yield cast_batch

                if last_cursor_value is not None:
                    await checkpoint.save_cursor(
                        stream_name, partition, {"cursor": last_cursor_value}
                    )

                if page_rows < batch_size:
                    break
                offset += page_rows

    @staticmethod
    def _select_columns(
        endpoint_doc: Dict[str, Any], stream_source: Dict[str, Any]
    ) -> List[str]:
        selected = stream_source.get("selected_columns")
        if selected:
            return list(selected)
        columns = endpoint_doc.get("columns") or []
        return [c["name"] for c in columns if c.get("name")]

    @staticmethod
    def _build_filters(stream_filters: List[Dict[str, Any]]) -> List[Filter]:
        out: List[Filter] = []
        for f in stream_filters:
            field_name = f.get("field")
            if not field_name:
                # A declared filter that compiles away silently widens the
                # result set — a configuration defect retries cannot heal.
                raise ReadError(f"stream filter missing 'field': {f!r}")
            out.append(
                Filter(
                    field=field_name,
                    op=f.get("operator", "eq"),
                    value=f.get("value"),
                )
            )
        return out

    # ==================================================================
    # Discoverable + TableCreator roles (control-plane)
    # ==================================================================
    #
    # Thin delegators to the standalone ``cdk.sql`` helpers (ADR §6). They
    # take a materialized ``ConnectionRuntime`` directly and run no gRPC
    # server or engine orchestration — the control-plane calls them.

    async def list_schemas(self, runtime: ConnectionRuntime) -> List[str]:
        return await _sql_list_schemas(runtime, dialect=self.dialect)

    async def list_tables(
        self, runtime: ConnectionRuntime, schema: str
    ) -> List[str]:
        return await _sql_list_tables(runtime, schema, dialect=self.dialect)

    async def list_columns(
        self, runtime: ConnectionRuntime, schema: str, table: str
    ) -> Tuple[List[ColumnDef], List[str]]:
        return await _sql_list_columns(
            runtime, schema, table, dialect=self.dialect
        )

    async def create_table(
        self,
        runtime: ConnectionRuntime,
        schema: str,
        table: str,
        columns: List[ColumnDef],
        primary_keys: List[str],
    ) -> None:
        await _sql_create_table(
            runtime, schema, table, columns, primary_keys, dialect=self.dialect
        )
