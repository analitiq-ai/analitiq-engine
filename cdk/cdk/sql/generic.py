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
import json
import logging
import threading
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, AsyncExitStack, nullcontext
from dataclasses import dataclass, field
from typing import Any, Literal

import pyarrow as pa
from sqlalchemy import MetaData, Table, and_, bindparam, literal, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from cdk.adbc_registry import AdbcConfigurationError
from cdk.base_handler import BaseDestinationHandler, BatchWriteResult
from cdk.connection_runtime import (
    DETERMINISTIC_CONNECT_ERRORS,
    ConnectionRuntime,
    materialize_runtime,
)
from cdk.database_utils import acquire_connection
from cdk.query_builder import Filter, ParamsLike, QueryBuilder, QueryConfig
from cdk.schema_contract import SchemaContract
from cdk.type_map import InvalidTypeMapError, TypeMapper, UnmappedTypeError
from cdk.types import (
    AckStatus,
    CheckpointStore,
    Cursor,
    EndpointScope,
    RetrySemantics,
    RetryVerdict,
    SchemaSpec,
)

from ..contract import ColumnDef
from ._adbc_utils import _close_cursor_quietly
from .adbc_reader import open_adbc_reader
from .ddl import build_create_table_sql
from .ddl import create_table as _sql_create_table
from .dialects import SqlDialect
from .discovery import list_columns as _sql_list_columns
from .discovery import list_schemas as _sql_list_schemas
from .discovery import list_tables as _sql_list_tables
from .exceptions import (
    ReadError,
    SchemaConfigurationError,
    UnsupportedDialectOperationError,
)

logger = logging.getLogger(__name__)


# Tracks (table, column) pairs already warned about ORDER BY fallback so a
# long-lived source connector warns once per stream, not once per page.
_order_by_fallback_logged: set = set()


def _note_order_by_fallback(table_name: str, column_name: str) -> None:
    """Warn once per (table, column) that ORDER BY fell back to a column.

    The ADBC-only read path pages with OFFSET, which needs a stable ORDER
    BY or rows silently skip/duplicate across pages on PG/Snowflake/
    BigQuery. When a stream has no cursor we order by the first selected
    column. A WARNING (not INFO) because the operator may need to act: a
    JSON / STRUCT / VARIANT first column fails at query time with an opaque
    "ORDER BY does not support this type" error, and the fix is to set
    ``cursor_field`` on the stream -- not something a stack trace points
    at directly.
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
        table_name,
        column_name,
    )


# PEP-249 exception class names that indicate the failure cannot heal
# between retries against an identical request: bad SQL, missing
# objects, permission denials, type mismatches, unsupported operations.
# Driver modules re-export these names per PEP-249; we match on the
# class name so the check works without importing the optional driver.
_FATAL_ADBC_ERROR_NAMES = frozenset(
    {
        "ProgrammingError",
        "NotSupportedError",
        "IntegrityError",
        "DataError",
    }
)


def _is_fatal_adbc_error(exc: BaseException) -> bool:
    """Return ``True`` when *exc* is a failure class retries cannot heal."""
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
    table: Table | None = None
    primary_keys: list[str] = field(default_factory=list)
    # Columns used as the ON CONFLICT / MERGE target for upsert. Set at
    # configure_schema time from ``endpoint_doc["_write_conflict_keys"]``,
    # which main.py copies verbatim from the stream's Infra-validated
    # ``write.conflict_keys``. Empty here means INSERT mode — an upsert
    # always carries an explicit conflict target under the contract.
    conflict_keys: list[str] = field(default_factory=list)
    write_mode: WriteMode = "upsert"
    endpoint_document: dict[str, Any] = field(default_factory=dict)
    schema_contract: SchemaContract | None = None


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

    * ``transport_type: "sqlalchemy"`` — SQLAlchemy engine. Async drivers
      (Postgres asyncpg, MySQL aiomysql) run on an ``AsyncEngine``;
      sync-only drivers (Redshift ``redshift_connector``) run on a plain
      sync ``Engine`` whose operations are dispatched via
      ``asyncio.to_thread``. Both flavours share one set of
      sync-``Connection`` transaction bodies (the async path enters them
      through ``AsyncConnection.run_sync``), so DML/DDL semantics are
      identical. DDL via rendered ``CREATE TABLE`` + reflection, DML via
      the dialect's INSERT/INSERT-ON-CONFLICT/MERGE compilers.
    * ``transport_type: "adbc"`` — direct ADBC DBAPI 2.0 connection
      (Snowflake, BigQuery). DDL via ``cursor.execute`` of per-driver
      native SQL, ingest via ``cursor.adbc_ingest``, upsert via
      stage-table + ``MERGE INTO``.

    Both modes are idempotent on the write mode's own keys (a MERGE on
    ``conflict_keys`` for upsert, the synthetic ``_record_hash`` key for a
    keyless insert), not a side ledger; they share the schema-contract
    Arrow cast and the per-stream state machine. Configuration is
    resolved through ``ConnectionRuntime`` rather than read off the
    raw connection JSON — the handler never inspects host/port/secret
    fields directly.

    Per-stream destination settings (schema, table, primary keys,
    columns) are read from the preloaded contract endpoint document at
    ``configure_schema`` time. The SchemaSpec off the wire only
    carries ``stream_id``, ``version``, and ``write_mode``.
    """

    # Server-managed column added to every database destination table.
    # Populated by the database via DEFAULT NOW() at INSERT time so the
    # engine never has to ship a per-record timestamp.
    SYNCED_AT_COLUMN = "_synced_at"

    # Engine-managed dedup key for a keyless insert stream (one with no
    # contract primary key). Holds the content-derived record id and is
    # declared as the table's primary key, so the database enforces the
    # uniqueness the insert anti-join relies on -- a re-read row is never
    # written twice (issue #282). Absent on streams that carry a primary key
    # or use upsert/truncate_insert.
    RECORD_HASH_COLUMN = "_record_hash"

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
        # Sync SQLAlchemy engine for sync-only drivers (e.g. Redshift's
        # redshift_connector). Mutually exclusive with ``self._engine``
        # and ADBC-only mode; its operations run via asyncio.to_thread,
        # mirroring the ADBC sync-in-thread pattern.
        self._sync_engine: Engine | None = None
        self._config: dict[str, Any] = {}
        self._connected: bool = False
        self._driver: str = ""
        # Seconds to bound a destination SQL handler attempt, set by the
        # destination servicer via set_statement_timeout() on each schema
        # handshake from the sender-stamped gRPC ack budget. None (source-role
        # instances, or unset) means unbounded - asyncio.timeout(None) never
        # fires. See _statement_deadline.
        self._statement_timeout_seconds: float | None = None
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
        self._streams: dict[str, _StreamState] = {}

        # (run_id, stream_id) pairs whose full-refresh truncate already ran.
        # truncate_insert must truncate at most once per run and stream —
        # truncating per batch keeps only the final batch of a multi-batch
        # refresh (issue #307). Marked only after the truncating write
        # commits, so a failed first batch re-truncates on retry. In-memory
        # on purpose: a restarted worker re-truncates on its first batch,
        # which is correct because the engine restarts a truncate_insert
        # stream from scratch (it never resumes one from a cursor).
        self._refresh_truncated: set[tuple[str, str]] = set()

        # Serializes CREATE TABLE statements across streams. Even when each
        # stream owns its own SQLAlchemy ``MetaData``, two concurrent
        # ``create_all`` calls can still race the database's catalog
        # writes (e.g. PostgreSQL's ``pg_type_typname_nsp_index``).
        self._ddl_lock: asyncio.Lock = asyncio.Lock()

        # Stream-id -> structured endpoint_ref dict so configure_schema()
        # can pick the type-mapper matching the endpoint's scope (connector
        # vs connection). Populated by set_endpoint_refs() at startup.
        self._endpoint_refs: dict[str, dict[str, Any]] = {}

        # Stream-id -> contract endpoint document (database_object,
        # columns, primary_keys, …). Populated by set_stream_endpoints()
        # at startup.
        self._stream_endpoints: dict[str, dict[str, Any]] = {}

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
        self.metrics: dict[str, int] = {"records_read": 0, "batches_read": 0}

    def set_endpoint_refs(self, endpoint_refs: Mapping[str, Any]) -> None:
        """Register stream_id → endpoint_ref for each stream writing here.

        Called once by ``src.main`` before the gRPC server starts;
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
        """Register stream_id → contract endpoint document for each stream.

        Covers streams writing to this destination. The handler reads the
        database object (catalog/schema/name), columns, and primary keys from
        this map at ``configure_schema`` time rather than unpacking them off
        the wire.
        """
        self._stream_endpoints = {
            sid: dict(doc) for sid, doc in stream_endpoints.items()
        }

    def set_statement_timeout(self, seconds: float | None) -> None:
        """Bound every destination SQL statement to *seconds* (issue #231).

        Called by the destination servicer on each schema handshake, before
        ``configure_schema``, with a value derived from the ack budget the
        sender stamped into the schema message (issue #234). ``None`` leaves
        statements unbounded. Bounds the async-SQLAlchemy DDL and write
        attempts (with their idempotency statements) via
        :meth:`_statement_deadline`; the ADBC and sync-engine paths are
        unaffected (their statements run on worker threads that
        ``asyncio.timeout`` cannot cancel).
        """
        self._statement_timeout_seconds = seconds
        if seconds is not None and (self._adbc_only or self._sync_engine is not None):
            # The budget is accepted but cannot be enforced on this path;
            # say so once per handshake instead of letting the operator
            # believe the bound holds — a hung statement here reproduces
            # the bare gRPC ack timeout this budget exists to prevent.
            logger.warning(
                "statement timeout %gs requested but cannot be enforced on "
                "the %s path (statements run on worker threads "
                "asyncio.timeout cannot cancel); relying on the driver's "
                "own timeout",
                seconds,
                "ADBC" if self._adbc_only else "sync-engine",
            )

    def _statement_deadline(self) -> AbstractAsyncContextManager[Any]:
        """Return a statement-timeout deadline for one whole handler attempt.

        An attempt is a DDL handshake, or a write with its idempotency read
        and commit record - so the total database time stays under the
        sender-stamped gRPC ack budget rather than each phase getting its own
        full budget that can sum past the ack deadline (issue #231).
        Async-SQLAlchemy operations get an ``asyncio.timeout``; the ADBC and
        sync-engine paths get a null deadline because their operations run in
        a worker thread that ``asyncio.timeout`` cannot cancel - they rely on
        the driver's own timeout. With no timeout configured (source-role
        instances) the SQLAlchemy deadline is ``asyncio.timeout(None)``,
        which never fires.

        Callers guard ``self._engine is not None`` before entering the
        transaction inside the deadline.
        """
        if self._adbc_only or self._sync_engine is not None:
            return nullcontext()
        return asyncio.timeout(self._statement_timeout_seconds)

    def _require_async_engine(self) -> AsyncEngine:
        """Return the async SQLAlchemy engine, or fail loud if path is unset.

        ``connect()`` wires exactly one transport per role; the async
        engine is reached only on the async-SQLAlchemy path. A ``None``
        here means a method ran on the wrong transport path.
        """
        if self._engine is None:
            raise RuntimeError(
                "async SQLAlchemy engine not available; this handler is not "
                "on the async-SQLAlchemy transport path"
            )
        return self._engine

    def _require_sync_engine(self) -> Engine:
        """Return the sync SQLAlchemy engine, or fail loud if path is unset."""
        if self._sync_engine is None:
            raise RuntimeError(
                "sync SQLAlchemy engine not available; this handler is not "
                "on the sync-SQLAlchemy transport path"
            )
        return self._sync_engine

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

    @property
    def supports_auto_create(self) -> bool:
        """Report that auto-create is supported.

        SQL destinations create the target table via ``configure_schema`` DDL
        (rendered CREATE TABLE).
        """
        return True

    @property
    def supports_truncate(self) -> bool:
        """Report that the full-refresh write mode is supported.

        SQL destinations implement truncate-insert (TRUNCATE then ingest).
        """
        return True

    def retry_semantics(self, stream_id: str) -> RetryVerdict:
        """Retry-safety verdict per write mode, keys, and transport (#286).

        Upsert is idempotent on its conflict keys on every transport.
        Insert dedups by row identity only on the SQLAlchemy path
        (anti-join on the contract primary key or the synthetic
        ``_record_hash``); the ADBC path is plain append until the
        anti-join lands there (issue #282 follow-up). Truncate-insert
        truncates once per run (issue #307) and its append phase is a
        plain insert with no row-identity dedup — deliberately, since
        deduping a full refresh would collapse legitimate duplicate rows
        — so a replayed already-committed batch re-inserts its rows.
        """
        state = self._streams.get(stream_id)
        if state is None:
            return super().retry_semantics(stream_id)
        if state.write_mode == "upsert":
            return RetryVerdict(
                semantics=RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE,
                reason=(
                    f"upsert merges on conflict keys "
                    f"{state.conflict_keys}; a re-sent row updates in place"
                ),
            )
        if state.write_mode == "truncate_insert":
            return RetryVerdict(
                semantics=RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE,
                reason=(
                    "truncate-insert truncates once per run and appends "
                    "after that with no row-identity dedup; a replayed "
                    "already-committed batch re-inserts its rows"
                ),
            )
        if self._adbc_only:
            return RetryVerdict(
                semantics=RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE,
                reason=(
                    "plain insert on the ADBC transport has no row-level "
                    "dedup yet (issue #282 follow-up); a same-run restart "
                    "can re-ingest re-read rows"
                ),
            )
        return RetryVerdict(
            semantics=RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE,
            reason=(
                f"insert anti-joins on row identity "
                f"{self._identity_columns(state)}; a re-read row lands "
                f"only once"
            ),
        )

    async def connect(self, runtime: ConnectionRuntime) -> None:
        """
        Establish database connection using ConnectionRuntime.

        Args:
            runtime: ConnectionRuntime with enriched config
        """
        self._runtime = runtime
        try:
            await materialize_runtime(runtime, sql_dialect=self.dialect)
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
        self._sync_engine = None
        if runtime.is_adbc:
            self._adbc_only = True
            # Open the ADBC connection eagerly so a bad credential
            # fails at connect() time, not on the first batch. Wrap
            # the driver-specific exception in ConnectionError to
            # match the materialize() failure shape.
            try:
                self._adbc_conn = await asyncio.to_thread(runtime.open_adbc_connection)
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
        elif runtime.is_sync_sqlalchemy:
            self._sync_engine = runtime.sync_engine
            logger.info(
                "GenericSQLConnector connected via sync SQLAlchemy to %s",
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
        cancelled: BaseException | None = None
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
        # ``_write_conflict_keys`` is the stream's Infra-validated upsert
        # conflict target, copied verbatim by main.py. Absent or empty
        # means no conflict target (INSERT mode); the engine never derives
        # one from ``primary_keys``.
        conflict_keys = list(endpoint_doc.get("_write_conflict_keys") or [])
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

        try:
            await self._ensure_tables_exist(state, type_mapper)
        except TimeoutError as exc:
            if (
                self._adbc_only
                or self._sync_engine is not None
                or self._statement_timeout_seconds is None
            ):
                # Only the async-SQLAlchemy DDL is wrapped in asyncio.timeout;
                # ADBC and sync-engine DDL run in a worker thread. A
                # TimeoutError here is a driver timeout, not our cancellation
                # - let it propagate as the raw driver error rather than
                # mislabel it.
                raise
            # The bounded DDL transaction was cancelled. Re-raise as the
            # deterministic schema error the gRPC layer translates into the
            # SchemaAck (server.py), so the operator gets the cancelled
            # CREATE TABLE and reason instead of a bare ACK timeout.
            raise SchemaConfigurationError(
                f"CREATE TABLE for {state.schema_name}.{state.table_name} did "
                f"not complete within the {self._statement_timeout_seconds:g}s "
                f"destination statement timeout (likely blocked on a lock or a "
                f"slow catalog); the statement was cancelled"
            ) from exc

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

    def _get_write_mode(self, proto_write_mode: int) -> WriteMode:
        mode_map: dict[int, WriteMode] = {
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
    ) -> list[ColumnDef]:
        """Contract endpoint columns -> ColumnDefs for the shared DDL builder.

        Each column's native type goes through the READ map to its canonical
        Arrow string; the builder renders it back through the WRITE map for
        this destination — the two declarative surfaces are the entire type
        vocabulary, for every transport. ``_synced_at`` is appended as a
        server-defaulted audit column when the endpoint doesn't declare it; a
        keyless insert stream also gets ``_record_hash`` as its synthetic
        primary key (see :meth:`_needs_record_hash`).
        """
        columns: list[ColumnDef] = []
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
        if self._needs_record_hash(state):
            if self.RECORD_HASH_COLUMN in declared:
                raise SchemaConfigurationError(
                    f"keyless insert stream for "
                    f"{state.schema_name}.{state.table_name} declares a column "
                    f"named {self.RECORD_HASH_COLUMN!r}, which the engine reserves "
                    f"as its synthetic dedup primary key; rename the column"
                )
            # Keyless insert: the content-derived hash is the row's only
            # identity. Declared NOT NULL primary key (see _identity_columns)
            # so the database structurally enforces the dedup the insert
            # anti-join performs (issue #282).
            columns.append(
                ColumnDef(
                    name=self.RECORD_HASH_COLUMN,
                    canonical_type="Utf8",
                    nullable=False,
                    primary_key=True,
                )
            )
        return columns

    def _needs_record_hash(self, state: _StreamState) -> bool:
        """Whether this stream uses the synthetic ``_record_hash`` dedup key.

        Only a keyless ``insert`` stream on the SQLAlchemy transport: it has no
        contract primary key to dedup on, and the SQLAlchemy write path can
        populate the hash and anti-join on it. Upsert/truncate_insert dedup on
        their own keys; ADBC-only keyless insert stays plain append until the
        content-hash dedup lands there (issue #282 follow-up).
        """
        return (
            not self._adbc_only
            and state.write_mode == "insert"
            and not state.primary_keys
        )

    def _identity_columns(self, state: _StreamState) -> list[str]:
        """Columns the insert anti-join matches a re-read row on.

        The contract primary key when the stream has one; otherwise the
        synthetic ``_record_hash`` for a keyless insert. Empty for
        upsert/truncate_insert, which are idempotent on their own terms.
        """
        if state.primary_keys:
            return list(state.primary_keys)
        if self._needs_record_hash(state):
            return [self.RECORD_HASH_COLUMN]
        return []

    async def _ensure_tables_exist(
        self, state: _StreamState, type_mapper: TypeMapper
    ) -> None:
        """Create the target table if absent.

        ONE DDL builder serves every transport: column types come from the
        connector's read+write maps via the dialect, quoting/PK from the
        dialect, and the rendered statement executes over SQLAlchemy or the
        ADBC cursor. On the SQLAlchemy path the table is then REFLECTED so
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
            self._identity_columns(state),
            if_not_exists=True,
        )

        # Announce DDL before it runs (and before any lock wait) so a slow
        # CREATE TABLE is attributable to a schema.table instead of a silent
        # stall between "Received schema" and the SchemaAck. Paired with the
        # "Destination table ready" line below, the gap renders as elapsed
        # time between two INFO logs.
        logger.info(
            "Ensuring destination table exists for %s.%s (executing DDL)",
            state.schema_name,
            state.table_name,
        )

        if self._adbc_only:
            await self._ensure_tables_via_adbc(state, [target_ddl])
            return

        if self._engine is None and self._sync_engine is None:
            # Silently skipping DDL here would leave state.table None and the
            # write_batch readiness guard returning RETRYABLE_FAILURE forever.
            raise AdbcConfigurationError(
                f"SQLAlchemy engine is None during DDL for "
                f"{state.schema_name}.{state.table_name}; "
                "connect() must be called before configure_schema()"
            )

        # Serialize DDL across concurrent streams so they do not race the
        # database catalog (e.g. PostgreSQL's pg_type_typname_nsp_index).
        #
        # The statement deadline wraps the _ddl_lock acquisition, not just the
        # transaction: a stream queued behind another stream's slow DDL would
        # otherwise wait here outside any budget and then start its own
        # CREATE TABLE with a fresh timer, by which point the engine's ack for
        # this stream has long expired. Bounding the wait + statement together
        # keeps the whole handshake under the ack budget (issue #231).
        async with self._statement_deadline():
            async with self._ddl_lock:
                if self._sync_engine is not None:
                    state.table = await asyncio.to_thread(
                        self._ddl_and_reflect_on_sync_engine,
                        state,
                        target_ddl,
                    )
                else:
                    async with self._require_async_engine().begin() as conn:
                        state.table = await conn.run_sync(
                            self._run_ddl_and_reflect,
                            state,
                            target_ddl,
                        )

        if self._needs_record_hash(state) and (
            state.table is None or self.RECORD_HASH_COLUMN not in state.table.c
        ):
            # A keyless insert table created before issue #282 has no
            # _record_hash column; CREATE TABLE IF NOT EXISTS is a no-op and
            # reflection returns it without the column, so every write would
            # then fail indexing table.c[_record_hash]. Fail loud with a clear
            # message instead -- the engine adds no migration (the column is the
            # primary key, so it cannot be back-filled on existing rows).
            raise SchemaConfigurationError(
                f"keyless insert target {state.schema_name}.{state.table_name} "
                f"has no {self.RECORD_HASH_COLUMN!r} column; it predates the "
                f"content-hash dedup key (issue #282). Recreate the target so the "
                f"engine manages {self.RECORD_HASH_COLUMN} as its primary key."
            )

        logger.info(
            "Destination table ready for %s.%s",
            state.schema_name,
            state.table_name,
        )

    def _run_ddl_and_reflect(
        self,
        conn: Connection,
        state: _StreamState,
        target_ddl: str,
    ) -> Table:
        """Run the DDL + reflection transaction body on a sync connection.

        Written once against the sync ``Connection`` API. The async engine
        enters via ``AsyncConnection.run_sync``; the sync engine runs it
        directly on a worker thread — both transports execute the identical
        statements.
        Reflection matters for DML binding: SQLAlchemy derives the column
        types from what the database actually created, so inserts/upserts
        bind correctly without a second hand-kept type surface.
        """
        # Dialect-declared preparation (e.g. postgres' CREATE SCHEMA
        # IF NOT EXISTS for a non-default schema). The neutral base
        # declares none.
        for stmt in self.dialect.sqlalchemy_pre_ddl(state.schema_name):
            conn.execute(text(stmt))
        conn.execute(text(target_ddl))

        meta = MetaData()
        return Table(
            state.table_name,
            meta,
            autoload_with=conn,
            schema=state.schema_name or None,
        )

    def _ddl_and_reflect_on_sync_engine(
        self, state: _StreamState, target_ddl: str
    ) -> Table:
        """Run the shared DDL + reflection body on the sync engine.

        Runs on a worker thread.
        """
        engine = self._require_sync_engine()
        with engine.begin() as conn:
            return self._run_ddl_and_reflect(conn, state, target_ddl)

    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: list[str],
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
        if not self._adbc_only and self._engine is None and self._sync_engine is None:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Handler not connected",
            )

        state = self._streams.get(stream_id)
        if state is None or (not self._adbc_only and state.table is None):
            # ADBC writes never build SQLAlchemy table objects; readiness
            # there is the configured state + schema contract.
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Schema not configured",
            )

        try:
            # One deadline for the whole SQLAlchemy attempt so the write stays
            # under the ack budget. Bounding sub-phases separately would give
            # each its own full budget, which can sum past the ack deadline and
            # let the engine retry while the first write is still running
            # (issue #231).
            async with self._statement_deadline():
                record_count = record_batch.num_rows
                if record_count == 0:
                    # Empty batch: nothing to write. The cursor still advances
                    # (the watermark moved); idempotency lives in the write
                    # itself, so there is no separate marker to record.
                    return BatchWriteResult(
                        status=AckStatus.ACK_STATUS_SUCCESS,
                        records_written=0,
                        committed_cursor=cursor,
                    )

                # A full refresh truncates on its first committed write only
                # (issue #307); every later batch of the same run appends.
                truncate_now = (
                    state.write_mode == "truncate_insert"
                    and (run_id, stream_id) not in self._refresh_truncated
                )

                if self._adbc_only:
                    await self._write_batch_adbc_only(
                        state,
                        run_id,
                        stream_id,
                        batch_seq,
                        record_batch,
                        truncate_now=truncate_now,
                    )
                elif self._sync_engine is not None:
                    prepared = self._prepare_for_sqlalchemy(state, record_batch)
                    self._attach_record_hash(state, prepared)
                    await asyncio.to_thread(
                        self._write_batch_on_sync_engine,
                        state,
                        prepared,
                        truncate_now,
                    )
                else:
                    prepared = self._prepare_for_sqlalchemy(state, record_batch)
                    self._attach_record_hash(state, prepared)
                    async with self._require_async_engine().begin() as conn:
                        await conn.run_sync(
                            self._apply_write_in_txn, state, prepared, truncate_now
                        )

                # Mark only after the write landed: a failed truncating batch
                # must truncate again on its retry, not skip to appending
                # into a table that was never emptied.
                if truncate_now:
                    self._refresh_truncated.add((run_id, stream_id))

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
        except SchemaConfigurationError as e:
            # The stream is misconfigured for this write (e.g. upsert with
            # no conflict_keys). Deterministic — retrying cannot heal it, so
            # fail fatally instead of silently degrading or looping forever.
            logger.error("Write configuration error: %s", e, exc_info=True)
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"write-config: {e}",
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
        except TimeoutError as e:
            if (
                self._adbc_only
                or self._sync_engine is not None
                or self._statement_timeout_seconds is None
            ):
                # Only the async-SQLAlchemy path is wrapped in asyncio.timeout.
                # The ADBC and sync-engine paths run in a worker thread (and a
                # handler with no budget set is never bounded), so a
                # TimeoutError here is a driver/socket timeout, not our
                # cancellation - classify it generically rather than claiming
                # a statement was cancelled. A bare TimeoutError often
                # stringifies empty; never ack a reason-less failure.
                logger.error(f"Error writing batch: {e}", exc_info=True)
                return BatchWriteResult(
                    status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                    records_written=0,
                    failure_summary=str(e) or f"driver timeout ({type(e).__name__})",
                )
            # The bounded SQLAlchemy statement was cancelled (issue #231). A lock
            # or slow write may clear, so stay retryable; carry the reason so it
            # surfaces instead of an empty str(TimeoutError).
            summary = (
                f"destination write for {state.schema_name}.{state.table_name} "
                f"did not complete within the {self._statement_timeout_seconds:g}s "
                f"statement timeout (likely a lock or slow write); the statement "
                f"was cancelled"
            )
            logger.error("Timeout writing batch: %s", summary)
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=summary,
            )
        except Exception as e:
            logger.error(f"Error writing batch: {e}", exc_info=True)
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=str(e),
            )

    def _apply_write_in_txn(
        self,
        conn: Connection,
        state: _StreamState,
        records: list[dict[str, Any]],
        truncate_now: bool = False,
    ) -> None:
        """Dispatch one batch's DML on an open transaction.

        Written once against the sync ``Connection`` API so both
        SQLAlchemy engine flavours execute identical statements: the
        async engine enters via ``AsyncConnection.run_sync``, the sync
        engine directly from its worker thread. ``truncate_now`` is the
        once-per-run truncate decision made in ``write_batch`` (issue
        #307); only truncate_insert reads it.
        """
        if state.write_mode == "truncate_insert":
            self._truncate_and_insert(conn, state, records, truncate_now)
        elif state.write_mode == "upsert":
            self._upsert_records(conn, state, records)
        else:
            self._insert_records(conn, state, records)

    def _write_batch_on_sync_engine(
        self,
        state: _StreamState,
        records: list[dict[str, Any]],
        truncate_now: bool = False,
    ) -> None:
        """Run one write attempt on the sync engine (worker thread).

        Mirrors the async path: the DML runs in a single transaction.
        """
        with self._require_sync_engine().begin() as conn:
            self._apply_write_in_txn(conn, state, records, truncate_now)

    def _attach_record_hash(
        self,
        state: _StreamState,
        records: list[dict[str, Any]],
    ) -> None:
        """Populate the synthetic ``_record_hash`` for a keyless insert stream.

        A keyless stream has no key identity, so the hash is the row's *content*,
        computed here at the sink from the cast record -- deliberately not the
        engine's wire ``record_id``, which derives from the source's declared
        primary key when it has one. Deriving from content means two
        byte-identical rows share a hash and collapse to one, while a row whose
        content differs -- even if it repeats the source primary key -- gets a
        distinct hash and is kept (issue #282). The record carries only contract
        columns at this point (no ``_record_hash``/``_synced_at``), so the digest
        is a stable function of the data and matches across attempts. No-op for
        any stream that dedups on a real key.
        """
        if not self._needs_record_hash(state):
            return
        for record in records:
            canonical = json.dumps(record, sort_keys=True, default=str)
            digest = hashlib.sha256(canonical.encode()).hexdigest()
            record[self.RECORD_HASH_COLUMN] = digest

    def _insert_records(
        self,
        conn: Connection,
        state: _StreamState,
        records: list[dict[str, Any]],
    ) -> None:
        """Insert pre-cast records, skipping rows whose identity already exists.

        Insert mode is at-least-once at the wire: a same-run retry re-reads the
        inclusive cursor boundary, so a row can arrive twice. Instead of a
        positional ledger (issue #282), each row lands only when its identity is
        absent -- the contract primary key, or the synthetic ``_record_hash``
        for a keyless stream. The check is one ``INSERT ... SELECT ... WHERE NOT
        EXISTS`` per row built with SQLAlchemy core, so the engine emits no
        dialect-specific SQL; the identity column's PRIMARY KEY is the
        structural backstop. With no identity column (not expected for insert
        mode) it degrades to a plain INSERT.

        Coalescing is identity-only: a row whose identity already exists is
        skipped without comparing its other columns. For a keyed insert that
        means a same-key row with *different* content is dropped (first
        occurrence wins) -- the same tradeoff as two byte-identical keyless
        rows. Insert mode cannot tell a retry's re-read from a genuinely
        conflicting key without a per-row read-back, which would defeat the
        single-statement anti-join; a stream that must reconcile changed rows
        should use upsert.
        """
        if state.table is None or not records:
            return
        table = state.table
        # The anti-join is an insert-mode concern. truncate_insert reaches this
        # helper too (after the run's first batch emptied the table); deduping
        # there would silently drop a same-key row in a full-refresh batch
        # instead of surfacing the PK violation, so it gets a plain INSERT.
        identity = self._identity_columns(state) if state.write_mode == "insert" else []
        if not identity:
            conn.execute(table.insert(), records)
            return

        # Drop intra-batch duplicates first: two rows sharing an identity both
        # pass NOT EXISTS (neither is in the table yet) and would then collide
        # on the primary key. First occurrence wins -- the Fivetran _record_hash
        # tradeoff for byte-identical keyless rows. The identity columns are the
        # table's PRIMARY KEY (NOT NULL), so the Python ``None == None`` collapse
        # that a nullable key could cause here is unreachable in practice.
        seen: set[tuple[Any, ...]] = set()
        deduped: list[dict[str, Any]] = []
        for record in records:
            key = tuple(record.get(col) for col in identity)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(record)

        columns = list(deduped[0].keys())
        binds = {col: bindparam(col, type_=table.c[col].type) for col in columns}
        already_present = (
            select(literal(1))
            .where(and_(*(table.c[col] == binds[col] for col in identity)))
            .exists()
        )
        source = select(*(binds[col].label(col) for col in columns)).where(
            ~already_present
        )
        conn.execute(table.insert().from_select(columns, source), deduped)

    def _upsert_records(
        self,
        conn: Connection,
        state: _StreamState,
        records: list[dict[str, Any]],
    ) -> None:
        """Upsert pre-cast records via the dialect's INSERT-or-UPDATE form.

        The statement shape is vendor-specific (postgres ``ON CONFLICT``,
        MySQL ``ON DUPLICATE KEY UPDATE``) and comes from the connector
        package's dialect. A dialect without one raises
        ``UnsupportedDialectOperationError`` — loud and fatal, never a
        silent downgrade to INSERT. The engine should not have routed an
        upsert here in the first place: ``supports_upsert`` gates the
        advertised write modes.

        ``conflict_keys`` is the stream's Infra-validated upsert target;
        the contract guarantees it is non-empty for an upsert. If it is
        empty here the stream is misconfigured — fail loud rather than
        silently fall back to INSERT, which would duplicate rows.
        """
        if state.table is None:
            return
        if not state.conflict_keys:
            raise SchemaConfigurationError(
                f"upsert requested for {state.schema_name}.{state.table_name} "
                f"but the stream carries no conflict_keys; refusing to fall back "
                f"to plain INSERT (would silently duplicate rows)"
            )
        if not records:
            return

        stmt = self.dialect.build_sqlalchemy_upsert(
            state.table, records, state.conflict_keys
        )
        conn.execute(stmt)

    def _truncate_and_insert(
        self,
        conn: Connection,
        state: _StreamState,
        records: list[dict[str, Any]],
        truncate_now: bool = False,
    ) -> None:
        """Insert pre-cast records, emptying the table first when told to.

        The truncate runs only on the run's first write (``truncate_now``,
        decided in ``write_batch``): truncating per batch would keep only
        the final batch of a multi-batch refresh (issue #307). The delete
        shares the batch's transaction, so a failed first batch rolls the
        truncate back with it.
        """
        if state.table is None:
            return
        if truncate_now:
            conn.execute(state.table.delete())
        self._insert_records(conn, state, records)

    def _prepare_for_sqlalchemy(
        self, state: _StreamState, record_batch: pa.RecordBatch
    ) -> list[dict[str, Any]]:
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
        self, state: _StreamState, rendered_ddl: list[str]
    ) -> None:
        statements: list[str] = []
        if not self.dialect.schema_is_implicit_default(state.schema_name):
            # BigQuery uses ``CREATE SCHEMA`` for datasets (Standard
            # SQL). Snowflake and Postgres both accept the same DDL.
            # Normalize before quoting so a Snowflake ``public`` matches
            # the warehouse's real ``PUBLIC`` schema instead of creating
            # a quoted-lowercase sibling.
            quoted_schema = self.dialect.quote_ident(
                self.dialect.normalize_schema(state.schema_name)
            )
            statements.append(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}")
        statements.extend(rendered_ddl)
        async with self._ddl_lock:
            await asyncio.to_thread(self._execute_adbc_ddl_sync, statements)
        logger.info(
            "Destination tables ready for %s.%s",
            state.schema_name,
            state.table_name,
        )

    def _execute_adbc_ddl_sync(self, statements: list[str]) -> None:
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
                    _close_cursor_quietly(cursor)
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
                raise AdbcConfigurationError("Runtime not available for ADBC reconnect")
            # open_adbc_connection is sync; safe to call inside the lock
            # because the lock is fast (no I/O) — only the connect() call
            # itself blocks, but that's the work this method is doing.
            self._adbc_conn = self._runtime.open_adbc_connection()
            return self._adbc_conn

    def _execute_adbc_dml_sync(self, sql: str, params: tuple[Any, ...]) -> int:
        """Execute ``sql`` with ``params`` on the ADBC connection.

        Commits the statement and returns the DBAPI ``rowcount`` (or -1 when
        the driver does not report one) for callers that need it; most ADBC
        writes ignore it. Poison-aware: a failure poisons the cached connection
        and a fatal driver error is reclassified so the engine does not retry
        it forever.
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
                    _close_cursor_quietly(cursor)
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
        truncate_now: bool = False,
    ) -> None:
        """Full write path for ADBC-only mode.

        Insert: append via ``adbc_ingest``. Truncate-insert: TRUNCATE
        TABLE on the run's first write (``truncate_now``, issue #307),
        then append; later batches of the refresh append only. Upsert:
        ingest into a session-scoped temp table, then ``MERGE INTO`` the
        target.

        Upsert (MERGE on conflict keys) is idempotent under retry on its
        own terms. Plain ``insert`` — and truncate_insert's append phase —
        is at-least-once here: the ADBC path has no row-level dedup yet,
        so a same-run retry can re-ingest the inclusive cursor boundary.
        The SQLAlchemy path's ``_record_hash`` anti-join (issue #282) is
        the follow-up that closes this for ADBC.
        """
        if state.schema_contract is None:
            raise AdbcConfigurationError(
                f"ADBC-only write for {state.schema_name}.{state.table_name} "
                "requires a configured SchemaContract; schema alignment was skipped"
            )
        cast_batch = state.schema_contract.cast_arrow_batch(record_batch)

        if state.write_mode == "truncate_insert" and truncate_now:
            await asyncio.to_thread(
                self._truncate_then_ingest_sync,
                cast_batch,
                state.schema_name,
                state.table_name,
            )
        elif state.write_mode == "truncate_insert":
            await asyncio.to_thread(
                self._adbc_only_ingest_sync,
                cast_batch,
                state.schema_name,
                state.table_name,
            )
        elif state.write_mode == "upsert":
            # ``conflict_keys`` is the stream's Infra-validated upsert
            # target; the contract guarantees it is non-empty for an
            # upsert. If it is empty the stream is misconfigured — fail
            # loud rather than silently ingest, which would duplicate rows.
            if not state.conflict_keys:
                raise SchemaConfigurationError(
                    f"upsert requested for {state.schema_name}.{state.table_name} "
                    f"but the stream carries no conflict_keys; refusing to fall "
                    f"back to plain ingest (would silently duplicate rows)"
                )
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
            stage_token = (
                "b"
                + hashlib.sha256(
                    f"{run_id}|{stream_id}|{batch_seq}".encode()
                ).hexdigest()[:16]
            )
            await asyncio.to_thread(
                self._merge_ingest_sync,
                cast_batch,
                state.schema_name,
                state.table_name,
                list(cast_batch.schema.names),
                state.conflict_keys,
                stage_token,
            )
        else:
            await asyncio.to_thread(
                self._adbc_only_ingest_sync,
                cast_batch,
                state.schema_name,
                state.table_name,
            )

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
                        **self.dialect.adbc_ingest_schema_kwargs(schema_name),
                    )
                    conn.commit()
                finally:
                    _close_cursor_quietly(cursor)
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
                    _close_cursor_quietly(cursor)
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
        all_columns: list[str],
        conflict_keys: list[str],
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
                schema_name,
                table_name,
            )
        # _adbc_op_lock serializes the full DROP+CREATE+INGEST+MERGE+DROP
        # sequence so concurrent streams against the same handler don't
        # interleave cursor operations on the cached connection
        # (PEP-249 threadsafety=1). One acquire for the whole transaction
        # so a parallel ingest can't slip between CREATE and INGEST and
        # leave the stage table empty.
        with self._adbc_op_lock:
            self._merge_ingest_locked_sync(
                cast_batch,
                target_qualified,
                stage_qualified,
                stage_name,
                schema_name,
                all_columns,
                conflict_keys,
                update_cols,
            )

    def _merge_ingest_locked_sync(
        self,
        cast_batch: pa.RecordBatch,
        target_qualified: str,
        stage_qualified: str,
        stage_name: str,
        schema_name: str,
        all_columns: list[str],
        conflict_keys: list[str],
        update_cols: list[str],
    ) -> None:
        """Run the body of :meth:`_merge_ingest_sync` under the held lock.

        Called while ``_adbc_op_lock`` is held. Extracted so the lock
        acquisition site is small and obvious; the inner method assumes the
        lock and never reacquires.
        """
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
                # Stage lives in the target schema. Dialects that support
                # per-statement ingest targeting resolve it via
                # ``db_schema_name`` (same normalization as the DDL path, so
                # Snowflake's real PUBLIC matches a connector's lowercase
                # ``public``); dialects that don't (Snowflake) fall back to
                # the connection's session schema, where the stage was just
                # created.
                cursor.adbc_ingest(
                    stage_name,
                    cast_batch,
                    mode="append",
                    **self.dialect.adbc_ingest_schema_kwargs(schema_name),
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
                insert_cols = ", ".join(
                    self.dialect.quote_ident(c) for c in all_columns
                )
                insert_vals = ", ".join(
                    f"s.{self.dialect.quote_ident(c)}" for c in all_columns
                )
                merge_sql = (
                    f"MERGE INTO {target_qualified} t USING {stage_qualified} s "
                    f"ON {on_clause} "
                )
                if update_cols:
                    merge_sql += (
                        f"WHEN MATCHED THEN UPDATE SET {set_clause} "  # nosec B608
                    )
                merge_sql += (
                    f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                    f"VALUES ({insert_vals})"
                )
                cursor.execute(merge_sql)
                conn.commit()
            finally:
                _close_cursor_quietly(cursor)
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
                    drop_cursor.execute(f"DROP TABLE IF EXISTS {stage_qualified}")
                    conn.commit()
                finally:
                    _close_cursor_quietly(drop_cursor)
            except Exception:
                # The next retry's pre-flight DROP-IF-EXISTS will clean
                # the orphan up; warn so an operator sees the leftover
                # in the meantime.
                logger.warning(
                    "ADBC stage table %s left behind after MERGE failure; "
                    "the next retry's pre-flight DROP-IF-EXISTS will clean it up",
                    stage_qualified,
                    exc_info=True,
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
                _close_cursor_quietly(drop_cursor)
        except Exception:
            logger.warning(
                "ADBC stage table %s post-MERGE DROP failed; next retry of "
                "this batch will clean it up via pre-flight DROP-IF-EXISTS",
                stage_qualified,
                exc_info=True,
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

        if self._sync_engine is not None:
            try:
                await asyncio.to_thread(self._health_check_sync_engine)
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

    def _health_check_sync_engine(self) -> None:
        """Health probe for the sync engine (worker thread)."""
        with self._require_sync_engine().connect() as conn:
            conn.execute(text("SELECT 1"))

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
                    _close_cursor_quietly(cursor)
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
        config: dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any] | None = None,
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
            await materialize_runtime(runtime, sql_dialect=self.dialect)
        except DETERMINISTIC_CONNECT_ERRORS:
            raise
        except Exception as e:
            logger.error("Failed to connect to source database: %s", e)
            raise ReadError(f"Database connection failed: {e}") from e

        driver = runtime.driver or ""
        adbc_only = runtime.is_adbc
        sa_sync = not adbc_only and runtime.is_sync_sqlalchemy
        engine = None if (adbc_only or sa_sync) else runtime.engine
        logger.info(
            "Reading source via %s (%s)",
            driver,
            "ADBC" if adbc_only else "sync SQLAlchemy" if sa_sync else "SQLAlchemy",
        )

        try:
            stream_source = config.get("stream_source") or {}
            schema_contract = SchemaContract(endpoint_doc)
            column_names = self._select_columns(endpoint_doc, stream_source)
            filters = self._build_filters(stream_source.get("filters") or [])

            replication = stream_source.get("replication") or {}
            # cursor_field is a contract string|null (validated upstream), so no
            # list normalization is needed.
            cursor_field = replication.get("cursor_field")
            replication_method = replication.get("method", "full_refresh")

            # Stream-declared page ordering (the contract's
            # ``source.database_pagination.order_by_field``). Takes
            # precedence over the ADBC first-column fallback; a
            # full-refresh stream uses it to declare the ordering its
            # paged read needs.
            database_pagination = stream_source.get("database_pagination") or {}
            order_by_field = database_pagination.get("order_by_field")
            if (
                order_by_field
                and replication_method == "incremental"
                and cursor_field
                and order_by_field != cursor_field
            ):
                # Checkpoint advancement takes the cursor value of the
                # page's last row, which is the maximum only when pages
                # are ordered by the cursor. An ordering that diverges
                # from the cursor would save arbitrary cursor values and
                # silently skip rows on later runs — fail before any
                # extraction work.
                raise ReadError(
                    f"stream {stream_name!r}: database_pagination."
                    f"order_by_field {order_by_field!r} conflicts with "
                    f"incremental cursor_field {cursor_field!r}; cursor "
                    f"checkpointing requires pages ordered by the cursor. "
                    f"Drop order_by_field or make it the cursor field."
                )

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
                    cursor_value=cursor_value,
                    order_by_field=order_by_field,
                    batch_size=batch_size,
                    checkpoint=checkpoint,
                    stream_name=stream_name,
                    partition=partition,
                ):
                    yield batch
                logger.debug("Source read (ADBC-only) completed")
                return

            builder = QueryBuilder(
                driver,
                registry_name=self.dialect.sqlalchemy_registry_name,
                paging_order_fallback=self.dialect.paging_order_fallback,
            )

            def page_query(offset: int) -> tuple[str, ParamsLike]:
                """Build the per-page SELECT.

                Limit / offset are pushed into ``QueryConfig`` so SQLAlchemy
                compiles dialect-correct paging. ``params`` is a list for
                positional dialects and a dict for named ones.
                """
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
                        # Resume inclusively (>=), re-reading the boundary row.
                        # A non-unique cursor (e.g. a coarse timestamp with
                        # ties) can gain a new row at the last committed value
                        # between runs; an exclusive > would filter that row out
                        # at the source and lose it for good. Re-reading is safe
                        # under the default upsert write mode, which dedups the
                        # boundary row against its conflict_keys. Under insert
                        # mode a unique/primary key rejects the re-read duplicate
                        # loudly; a keyless insert stream has nothing to dedup
                        # against and would append a duplicate boundary row, so
                        # insert + an incremental cursor without a uniqueness
                        # key is an unsafe combination.
                        cursor_mode="inclusive",
                        order_by=order_by_field,
                        limit=batch_size,
                        offset=offset,
                    )
                )
                return sql, params

            last_cursor_value = cursor_value
            offset = 0

            # One connection for the whole read on either engine flavour.
            # The sync engine's connection is opened/used/closed on worker
            # threads (sequential use, same pattern as the ADBC reader);
            # the async engine pages via run_sync so both flavours execute
            # the identical page body (_fetch_page_rows).
            async with AsyncExitStack() as stack:
                if sa_sync:
                    sync_conn = await asyncio.to_thread(runtime.sync_engine.connect)
                    stack.push_async_callback(asyncio.to_thread, sync_conn.close)

                    async def fetch_page(sql: str, params: Any) -> list[dict[str, Any]]:
                        return await asyncio.to_thread(
                            self._fetch_page_rows, sync_conn, sql, params
                        )

                else:
                    if engine is None:
                        raise RuntimeError(
                            "async SQLAlchemy engine not available on the "
                            "async read path; this indicates a transport "
                            "dispatch error"
                        )
                    conn = await stack.enter_async_context(acquire_connection(engine))

                    async def fetch_page(sql: str, params: Any) -> list[dict[str, Any]]:
                        rows: list[dict[str, Any]] = await conn.run_sync(
                            self._fetch_page_rows, sql, params
                        )
                        return rows

                while True:
                    paged_query, paged_params = page_query(offset)
                    rows = await fetch_page(paged_query, paged_params)
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
        schema_name: str | None,
        table_name: str,
        columns: list[str],
        filters: list[Filter],
        cursor_field: str | None,
        cursor_value: Any,
        order_by_field: str | None,
        batch_size: int,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any],
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
        """
        if not columns:
            # The first selected column is the ORDER BY fallback and an empty
            # projection compiles to ``SELECT`` with no columns; fail loudly
            # rather than emit an invalid statement.
            raise ReadError("ADBC-only source requires a non-empty column projection")

        # The ADBC path quotes every identifier, so normalize the schema
        # through the connector's dialect — the same rule the destination
        # handler applies (e.g. Snowflake folds lowercase ``public`` ->
        # ``PUBLIC``) — or the quoted name targets a different schema.
        effective_schema = (
            self.dialect.normalize_schema(schema_name) if schema_name else None
        )

        # Page ordering: the stream's declared order_by_field wins (a
        # conflict with the incremental cursor is rejected in
        # read_batches), then the cursor, then the first projected column
        # (warned once — an undeclared order makes OFFSET paging
        # best-effort).
        if order_by_field:
            order_by = order_by_field
        elif cursor_field:
            order_by = cursor_field
        else:
            order_by = columns[0]
            _note_order_by_fallback(table_name, order_by)

        builder = QueryBuilder(
            driver,
            paramstyle="qmark",
            registry_name=self.dialect.sqlalchemy_registry_name,
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
                        # Inclusive resume bound: see the matching note on the
                        # SQLAlchemy read path. Re-reading the boundary row is
                        # what keeps a late row sharing the last cursor value
                        # from being lost; upsert dedups the re-read.
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
                            last_cursor_value = cast_batch.column(cursor_field)[
                                -1
                            ].as_py()
                        elif not cursor_missing_warned:
                            logger.warning(
                                "stream %r: cursor_field %r not present in "
                                "result batch; cursor will not advance",
                                stream_name,
                                cursor_field,
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
    def _fetch_page_rows(
        conn: Connection, sql: str, params: Any
    ) -> list[dict[str, Any]]:
        """Run one page SELECT on a sync ``Connection`` and return dict rows.

        Shared by both SQLAlchemy engine flavours (async via ``run_sync``,
        sync via ``asyncio.to_thread``). ``params`` is a list for
        positional dialects and a dict for named-paramstyle dialects
        (Snowflake pyformat, BigQuery named) — the driver binds by name
        and expects a dict, not a positional tuple.
        """
        if isinstance(params, dict):
            if params:
                result = conn.exec_driver_sql(sql, params)
            else:
                result = conn.exec_driver_sql(sql)
        elif params:
            result = conn.exec_driver_sql(sql, tuple(params))
        else:
            result = conn.exec_driver_sql(sql)
        return [dict(row._mapping) for row in result]

    @staticmethod
    def _select_columns(
        endpoint_doc: dict[str, Any], stream_source: dict[str, Any]
    ) -> list[str]:
        selected = stream_source.get("selected_columns")
        if selected:
            return list(selected)
        columns = endpoint_doc.get("columns") or []
        return [c["name"] for c in columns if c.get("name")]

    @staticmethod
    def _build_filters(stream_filters: list[dict[str, Any]]) -> list[Filter]:
        out: list[Filter] = []
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

    async def list_schemas(self, runtime: ConnectionRuntime) -> list[str]:
        return await _sql_list_schemas(runtime, dialect=self.dialect)

    async def list_tables(self, runtime: ConnectionRuntime, schema: str) -> list[str]:
        return await _sql_list_tables(runtime, schema, dialect=self.dialect)

    async def list_columns(
        self, runtime: ConnectionRuntime, schema: str, table: str
    ) -> tuple[list[ColumnDef], list[str]]:
        return await _sql_list_columns(runtime, schema, table, dialect=self.dialect)

    async def create_table(
        self,
        runtime: ConnectionRuntime,
        schema: str,
        table: str,
        columns: list[ColumnDef],
        primary_keys: list[str],
    ) -> None:
        await _sql_create_table(
            runtime, schema, table, columns, primary_keys, dialect=self.dialect
        )
