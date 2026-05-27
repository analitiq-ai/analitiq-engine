"""SQLAlchemy-based database destination handler.

This handler provides a unified interface for all SQL databases supported by SQLAlchemy.
The specific database is determined by the `driver` field in the connection config.

Type casting is handled by the Arrow-based SchemaContract, which provides
efficient columnar type conversion for batch operations.
"""

import asyncio
import hashlib
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal, Mapping, Optional, Set, Tuple

import pyarrow as pa
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection

from ..base_handler import BaseDestinationHandler, BatchWriteResult
from ..schema_contract import SchemaContract
from ..sql_types import (
    native_to_bigquery,
    native_to_postgres,
    native_to_snowflake,
    native_to_sqlalchemy,
)
from ...engine.type_map import (
    InvalidTypeMapError,
    TypeMapper,
    UnmappedTypeError,
)
from ...secrets.exceptions import PlaceholderExpansionError
from ...grpc.generated.analitiq.v1 import (
    AckStatus,
    Cursor,
    SchemaMessage,
)
from ...shared.adbc_registry import (
    _ADBC_IMPORT_FAILED,
    AdbcConfigurationError,
    adbc_flag_enabled as _adbc_flag_enabled,
    build_adbc_uri,
    load_adbc_module,
)
from ...shared.connection_runtime import ConnectionRuntime


logger = logging.getLogger(__name__)


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
    metadata: MetaData = field(default_factory=MetaData)


class DatabaseDestinationHandler(BaseDestinationHandler):
    """Unified database destination handler.

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
    ``configure_schema`` time. The SchemaMessage off the wire only
    carries ``stream_id``, ``version``, and ``write_mode``.
    """

    BATCH_COMMITS_TABLE = "_batch_commits"

    # Server-managed column added to every database destination table.
    # Populated by the database via DEFAULT NOW() at INSERT time so the
    # engine never has to ship a per-record timestamp.
    SYNCED_AT_COLUMN = "_synced_at"

    def __init__(self) -> None:
        """Initialize the database handler."""
        self._runtime: ConnectionRuntime | None = None
        self._engine: AsyncEngine | None = None
        self._config: Dict[str, Any] = {}
        self._connected: bool = False
        self._driver: str = ""
        # ADBC-only mode: the runtime exposes no SQLAlchemy engine and
        # every write/DDL/idempotency operation runs through the cached
        # ADBC DBAPI connection. Set in ``connect()`` from
        # ``runtime.is_adbc``. The fast-path code that was added for
        # Postgres (SA engine + opportunistic ADBC ingest) is unrelated:
        # that path still uses ``self._engine`` for everything except
        # the ingest itself.
        self._adbc_only: bool = False

        # Per-stream state derived from the contract endpoint document at
        # configure_schema() time. The SchemaMessage off the wire only
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

        # Lazily-imported ADBC dbapi module — used only by the Postgres
        # fast path (SA engine + opportunistic ADBC ingest). ADBC-only
        # mode does NOT use this field; its driver module is captured in
        # the closure returned by ``runtime.open_adbc_connection``.
        # ``None`` means the import has not been attempted;
        # ``_ADBC_IMPORT_FAILED`` means it was attempted and the package
        # is unavailable — we do not retry within the handler's lifetime.
        self._adbc_module: Any = None
        # Cached ADBC DBAPI connection. Shared by:
        #   * Fast path (Postgres, opened lazily via _open_adbc_connection,
        #     libpq transport).
        #   * ADBC-only mode (Snowflake / BigQuery / Postgres, opened
        #     eagerly in connect() via runtime.open_adbc_connection(),
        #     driver-specific transport).
        # Nulled on any failure under _adbc_conn_lock so the next
        # operation reopens instead of reusing a poisoned handle.
        self._adbc_conn: Any = None
        # Set of ``(stream_id, reason)`` pairs already logged when the
        # fast path was demoted to SQLAlchemy. Keyed on stream_id so
        # two streams writing to the same physical table each get their
        # own first-demotion log line.
        self._adbc_demotion_logged: Set[Tuple[str, str]] = set()
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

    def set_endpoint_refs(self, endpoint_refs: Mapping[str, Any]) -> None:
        """Register stream_id → endpoint_ref for each stream writing to this
        destination. Called once by ``src.main`` before the gRPC server starts;
        the handler consults the map per incoming ``SchemaMessage`` to decide
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
                "DatabaseDestinationHandler._type_mapper_for_stream() called before connect()"
            )
        endpoint_ref = self._endpoint_refs.get(stream_id)
        if endpoint_ref is None:
            raise RuntimeError(
                f"DatabaseDestinationHandler has no endpoint_ref registered for "
                f"stream_id={stream_id!r}; call set_endpoint_refs() before the "
                f"gRPC server starts"
            )
        return self._runtime.type_mapper_for(endpoint_ref)

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
        """True when the active driver has an upsert path.

        SA-mode dialects use INSERT ON CONFLICT / ON DUPLICATE KEY
        UPDATE; ADBC-only mode uses stage-table + ``MERGE INTO``.
        """
        if self._adbc_only:
            return self._driver in ("postgresql", "snowflake", "bigquery")
        return self._driver in ("postgresql", "postgres", "mysql", "mariadb")

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
        runtime.acquire()
        try:
            await runtime.materialize(require_port=False)
        except (
            InvalidTypeMapError,
            UnmappedTypeError,
            PlaceholderExpansionError,
            ValueError,
        ):
            # Deterministic configuration / secret-resolution errors: let
            # them propagate with their real type so the caller can
            # distinguish "your type-map is missing a rule" from "the DB
            # is unreachable".
            raise
        except Exception as e:
            logger.error(f"Database destination connection failed: {e}")
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
                raise ConnectionError(f"ADBC connection failed: {e}") from e
            logger.info(
                "DatabaseDestinationHandler connected via ADBC to %s",
                self._driver,
            )
        else:
            self._engine = runtime.engine
            logger.info(
                "DatabaseDestinationHandler connected via SQLAlchemy to %s",
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
        logger.info("DatabaseDestinationHandler disconnected")
        if cancelled is not None:
            raise cancelled

    async def configure_schema(self, schema_msg: SchemaMessage) -> bool:
        """Configure the destination from the preloaded contract endpoint.

        The SchemaMessage only carries identification fields; this method
        looks up the contract database endpoint document by stream_id and
        reads its ``database_object``, ``columns``, and ``primary_keys``
        directly. Both engine and destination load the same artifacts via
        ``PipelineConfigPrep``, so no schema details cross the wire.
        """
        if not self._connected:
            logger.error("Cannot configure schema: not connected")
            return False

        stream_id = schema_msg.stream_id
        endpoint_doc = self._stream_endpoints.get(stream_id)
        if endpoint_doc is None:
            logger.error(
                "No preloaded endpoint document for stream_id=%r; "
                "call set_stream_endpoints() before the gRPC server starts",
                stream_id,
            )
            return False

        try:
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
                write_mode=self._get_write_mode(schema_msg.write_mode),
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

        except (
            UnmappedTypeError,
            InvalidTypeMapError,
            PlaceholderExpansionError,
            KeyError,
            TypeError,
            ValueError,
        ):
            # Deterministic, actionable errors propagate with their real
            # type so the schema-ack carries the precise reason (unmapped
            # native type, malformed type-map, missing secret, bad endpoint
            # document) instead of a generic "configure failed".
            raise
        except Exception as e:
            logger.error("Failed to configure schema: %s", e, exc_info=True)
            return False

    def _get_write_mode(self, proto_write_mode: int) -> str:
        mode_map = {
            1: "insert",
            2: "upsert",
            3: "truncate_insert",
        }
        if proto_write_mode not in mode_map:
            raise ValueError(
                f"Unsupported proto write_mode={proto_write_mode}; expected one "
                f"of {sorted(mode_map)} (WRITE_MODE_INSERT/UPSERT/TRUNCATE_INSERT)"
            )
        return mode_map[proto_write_mode]

    async def _ensure_tables_exist(
        self, state: _StreamState, type_mapper: TypeMapper
    ) -> None:
        """Create the target table and batch commits table if they don't exist.

        ``state`` owns the SQLAlchemy ``MetaData`` for this stream so each
        ``create_all`` only emits DDL for this stream's tables. The DDL
        itself is serialized with ``self._ddl_lock`` so concurrent streams
        do not race the database catalog (e.g. PostgreSQL's
        ``pg_type_typname_nsp_index``).
        """
        state.batch_commits_table = Table(
            self.BATCH_COMMITS_TABLE,
            state.metadata,
            Column("run_id", String(255), primary_key=True),
            Column("stream_id", String(255), primary_key=True),
            Column("batch_seq", BigInteger, primary_key=True),
            Column("committed_cursor", LargeBinary),
            Column("records_written", Integer),
            Column("committed_at", DateTime, default=datetime.utcnow),
            schema=state.schema_name,
        )

        # Build the target table from the contract endpoint document.
        if state.endpoint_document:
            state.table = self._create_table_from_schema(
                state.table_name,
                state.endpoint_document,
                state.primary_keys,
                state.schema_name,
                state.metadata,
                type_mapper,
            )
        else:
            logger.warning(
                "No endpoint document available; table must already exist"
            )

        if self._adbc_only:
            await self._ensure_tables_via_adbc(state, type_mapper)
            return

        if self._engine is None:
            return

        # Serialize DDL across concurrent streams. ``create_all`` only
        # adds tables that don't yet exist; pre-existing tables are left
        # untouched. New tables get ``_synced_at`` because
        # ``_create_table_from_schema`` appends it when the endpoint
        # document doesn't declare it.
        async with self._ddl_lock:
            async with self._engine.begin() as conn:
                if self._driver in ("postgresql", "postgres") and state.schema_name != "public":
                    await conn.execute(
                        text(f"CREATE SCHEMA IF NOT EXISTS {state.schema_name}")
                    )
                await conn.run_sync(state.metadata.create_all)

        logger.debug(f"Ensured tables exist in schema {state.schema_name}")

    def _create_table_from_schema(
        self,
        table_name: str,
        endpoint_document: Dict[str, Any],
        primary_keys: List[str],
        schema_name: str,
        metadata: MetaData,
        type_mapper: TypeMapper,
    ) -> Table:
        """Build a SQLAlchemy :class:`Table` from a database-endpoint document.

        The document is the contract ``database-endpoint`` payload — a
        ``columns`` array where each entry declares ``name``, ``native_type``,
        ``nullable``, and (optionally) a SQL-expression ``default``. The
        connector's ``type-map.json`` resolves ``native_type`` to a
        SQLAlchemy type.
        """
        columns: List[Column] = []
        declared_names: set[str] = set()

        for index, col_def in enumerate(endpoint_document.get("columns") or []):
            col_name = col_def.get("name")
            if not col_name:
                raise ValueError(
                    f"endpoint column at index {index} has no 'name' field"
                )
            native_type = col_def.get("native_type")
            if not native_type:
                raise ValueError(
                    f"column {col_name!r} has no 'native_type' field"
                )
            sa_type = native_to_sqlalchemy(native_type, type_mapper)
            is_pk = col_name in primary_keys
            nullable = col_def.get("nullable", True) and not is_pk

            column_kwargs: Dict[str, Any] = {
                "primary_key": is_pk,
                "nullable": nullable,
            }
            # Honour the endpoint's declared ``default`` only when it is a
            # SQL expression like ``now()`` — passed through as
            # ``server_default=text(...)``. Non-string defaults are
            # connector-specific values that do not belong in DDL.
            raw_default = col_def.get("default")
            if isinstance(raw_default, str) and raw_default.strip():
                column_kwargs["server_default"] = text(raw_default)

            columns.append(Column(col_name, sa_type, **column_kwargs))
            declared_names.add(col_name)

        # Every database destination table carries a server-managed
        # ``_synced_at`` audit column. If the endpoint declares it, it is
        # already in ``columns``; otherwise append one with a NOW() default.
        if self.SYNCED_AT_COLUMN not in declared_names:
            columns.append(
                Column(
                    self.SYNCED_AT_COLUMN,
                    DateTime(timezone=True),
                    nullable=True,
                    server_default=func.now(),
                )
            )

        return Table(
            table_name,
            metadata,
            *columns,
            schema=schema_name,
        )

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
        if state is None or state.table is None or state.batch_commits_table is None:
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
            elif self._can_use_adbc(stream_id, state):
                await self._write_via_adbc(state, record_batch)
                try:
                    await self._record_batch_commit(
                        state, run_id, stream_id, batch_seq,
                        cursor.token, record_count,
                    )
                except AdbcConfigurationError:
                    # Fatal classification from the commit-record path
                    # (e.g. ProgrammingError on a missing table). Let
                    # it propagate as fatal — wrapping it as a
                    # commit-record error would re-classify a fatal
                    # failure as retryable and burn cycles forever.
                    raise
                except Exception as commit_exc:
                    # The ingest already committed on the ADBC
                    # connection; the SA ``_batch_commits`` write
                    # failed. A retry will pass the pre-flight check
                    # and re-ingest, duplicating rows. Surface the
                    # divergence both in the log and in the failure
                    # summary returned to the engine.
                    logger.error(
                        "ADBC ingest committed but commit-record failed "
                        "for %s/%s/%s — retry will duplicate %d row(s)",
                        run_id, stream_id, batch_seq, record_count,
                        exc_info=True,
                    )
                    raise AdbcCommitRecordError(commit_exc, state.write_mode) from commit_exc
            else:
                prepared = self._prepare_for_sqlalchemy(state, record_batch)

                async with self._engine.begin() as conn:
                    if state.write_mode == "truncate_insert":
                        await self._truncate_and_insert(conn, state, prepared)
                    elif state.write_mode == "upsert" and (
                        state.conflict_keys or state.primary_keys
                    ):
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
            # Ingest already committed; the SA _batch_commits write
            # failed. The retry will re-ingest. Stay RETRYABLE so the
            # engine reconciles, but surface the divergence text in
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
        if state.batch_commits_table is None:
            return None
        if self._adbc_only:
            return await self._check_batch_committed_via_adbc(
                state, run_id, stream_id, batch_seq,
            )
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
        if state.batch_commits_table is None:
            return
        if self._adbc_only:
            await self._record_batch_commit_via_adbc(
                state, run_id, stream_id, batch_seq, cursor_bytes, records_written,
            )
            return
        if self._engine is None:
            return

        async with self._engine.begin() as conn:
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
        """Upsert pre-cast records (INSERT ... ON CONFLICT)."""
        conflict_keys = state.conflict_keys or state.primary_keys
        if state.table is None or not conflict_keys:
            await self._insert_records(conn, state, records)
            return
        if not records:
            return

        if self._driver in ("postgresql", "postgres"):
            stmt = pg_insert(state.table).values(records)
            record_columns = set(records[0].keys())
            update_cols = {
                c.name: c for c in stmt.excluded
                if c.name not in conflict_keys and c.name in record_columns
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_keys,
                set_=update_cols,
            )
            await conn.execute(stmt)

        elif self._driver in ("mysql", "mariadb"):
            stmt = mysql_insert(state.table).values(records)
            record_columns = set(records[0].keys())
            update_cols = {
                c.name: c for c in stmt.inserted
                if c.name not in conflict_keys and c.name in record_columns
            }
            stmt = stmt.on_duplicate_key_update(**update_cols)
            await conn.execute(stmt)

        else:
            # Fallback to plain insert for other databases
            await self._insert_records(conn, state, records)

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

    # ------------------------------------------------------------------
    # ADBC fast path (append-only, opt-in)
    # ------------------------------------------------------------------

    def _load_adbc_module(self) -> Any:
        """Lazily import the ADBC dbapi module for the active dialect.

        Caches the result per handler instance so a failed import is
        not retried on every batch.
        """
        if self._adbc_module is not None:
            return self._adbc_module
        self._adbc_module = load_adbc_module(self._driver)
        return self._adbc_module

    def _build_adbc_uri(self) -> Optional[str]:
        """Render the per-dialect ADBC connect argument from the engine.

        Returns ``None`` when no URI builder is registered for the
        active dialect or the engine URL is missing required parts.
        TLS mode flows in from the runtime so the ADBC connection
        matches the SQLAlchemy engine's posture; ``verify-ca`` /
        ``verify-full`` always demote to the SA path because the URI
        cannot carry the CA file path (``sslrootcert=``) those modes
        need.
        """
        if self._engine is None:
            return None
        tls_mode = self._runtime.tls_mode if self._runtime else None
        tls_has_ca = (
            self._runtime.tls_ca_bundle_present if self._runtime else False
        )
        return build_adbc_uri(
            self._driver,
            self._engine,
            tls_mode=tls_mode,
            tls_ca_bundle_present=tls_has_ca,
        )

    def _note_adbc_demotion(
        self, stream_id: str, state: _StreamState, reason: str
    ) -> None:
        """Log once per ``(stream_id, reason)`` why the fast path was
        skipped. Silent when the flag is off — demotion is the default
        and only worth surfacing when the user actually opted in.
        """
        if not _adbc_flag_enabled():
            return
        key = (stream_id, reason)
        if key in self._adbc_demotion_logged:
            return
        self._adbc_demotion_logged.add(key)
        logger.info(
            "ADBC fast path disabled for stream=%s table=%s.%s (driver=%s): %s",
            stream_id,
            state.schema_name,
            state.table_name,
            self._driver,
            reason,
        )

    def _can_use_adbc(self, stream_id: str, state: _StreamState) -> bool:
        """Decide whether the current batch takes the ADBC fast path.

        Returns ``True`` iff all of the following hold:

        * Opt-in feature flag (``ADBC_FAST_PATH`` env var) is set.
        * Stream is append-only (``write_mode == "insert"``);
          ``adbc_ingest`` is INSERT/APPEND only.
        * The dialect is in ``_ADBC_MODULES`` (from
          ``src.shared.adbc_registry``) and the matching driver package
          is importable.
        * A connection URI can be built from the engine's URL.
        * The stream has a configured schema contract (needed for the
          vectorized cast before ingest).

        This is the SA-backed fast path only — ADBC-only mode (set by
        the runtime's ``transport_type: "adbc"``) bypasses this method
        and dispatches via :meth:`_write_batch_adbc_only`.
        """
        if not _adbc_flag_enabled():
            return False
        if state.write_mode != "insert":
            self._note_adbc_demotion(
                stream_id, state,
                f"write_mode={state.write_mode!r} requires SQLAlchemy",
            )
            return False
        if self._load_adbc_module() is _ADBC_IMPORT_FAILED:
            self._note_adbc_demotion(
                stream_id, state,
                f"no ADBC driver registered for dialect={self._driver!r}",
            )
            return False
        if self._build_adbc_uri() is None:
            self._note_adbc_demotion(
                stream_id, state,
                "could not build ADBC URI from engine.url",
            )
            return False
        if state.schema_contract is None:
            self._note_adbc_demotion(
                stream_id, state, "stream has no configured schema contract"
            )
            return False
        return True

    def _open_adbc_connection(self) -> Any:
        """Return the cached ADBC connection, opening it on first use.

        Raises :class:`AdbcConfigurationError` when the driver module
        or URI cannot be produced. The cached handle is dropped on any
        ingest error so a poisoned connection is not reused.

        Lock-guarded check-then-act: without the lock, two worker
        threads can both observe ``self._adbc_conn is None`` and each
        call ``module.connect(uri)``, leaking the loser's connection.
        Connection establishment runs inside the lock; the lock
        sections are short relative to the network RTT a real connect
        takes, which is acceptable because contention is rare (only
        first-use plus post-poison reopens).
        """
        with self._adbc_conn_lock:
            if self._adbc_conn is not None:
                return self._adbc_conn
            module = self._load_adbc_module()
            if module is _ADBC_IMPORT_FAILED:
                raise AdbcConfigurationError(
                    f"ADBC module for driver={self._driver!r} not available"
                )
            uri = self._build_adbc_uri()
            if uri is None:
                raise AdbcConfigurationError(
                    f"Could not build ADBC URI for driver={self._driver!r}"
                )
            self._adbc_conn = module.connect(uri)
            return self._adbc_conn

    def _adbc_ingest_sync(
        self,
        cast_batch: pa.RecordBatch,
        schema_name: str,
        table_name: str,
    ) -> None:
        """Synchronous ADBC ingest. Runs on a worker thread.

        Caller must ensure the target table exists.

        Only ingest / cursor failures invalidate ``self._adbc_conn`` —
        misconfiguration raised by ``_open_adbc_connection`` does not
        touch a connection (none was opened) and the
        ``AdbcConfigurationError`` propagates straight through.
        Poisoning goes through :meth:`_poison_adbc_connection` so the
        ``_adbc_conn_lock`` is honored (avoiding a libpq double-close
        when two concurrent ingest failures race). The outer
        ``_adbc_op_lock`` serializes cursor use against PEP-249
        ``threadsafety=1``.
        """
        with self._adbc_op_lock:
            conn = self._open_adbc_connection()
            try:
                cursor = conn.cursor()
                try:
                    cursor.adbc_ingest(
                        table_name,
                        cast_batch,
                        mode="append",
                        db_schema_name=(
                            self._normalize_adbc_schema(schema_name)
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

    async def _write_via_adbc(
        self,
        state: _StreamState,
        record_batch: pa.RecordBatch,
    ) -> None:
        """Append-only fast path. INSERT via ``adbc_ingest``.

        Reuses :meth:`SchemaContract.cast_arrow_batch` so the Arrow
        types match the destination table exactly. The cast batch is
        handed straight to ADBC — no ``to_pylist()`` round trip, no
        per-row SQLAlchemy parameter loop.

        Append-only: ingest commits before ``_batch_commits`` is
        recorded; see :class:`AdbcCommitRecordError` for the
        retry-duplication window.
        """
        # Defensive — _can_use_adbc gates on schema_contract being set,
        # so this only triggers if a caller invokes _write_via_adbc
        # outside of the dispatch in write_batch.
        if state.schema_contract is None:
            raise AdbcConfigurationError(
                "ADBC fast path requires a configured SchemaContract"
            )
        cast_batch = state.schema_contract.cast_arrow_batch(record_batch)
        await asyncio.to_thread(
            self._adbc_ingest_sync,
            cast_batch,
            state.schema_name,
            state.table_name,
        )

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
            return record_batch.to_pylist()
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

    def _adbc_quote_ident(self, name: str) -> str:
        """Quote a SQL identifier for the active ADBC driver.

        BigQuery GoogleSQL uses backticks for identifier quoting;
        double-quoted strings are STRING literals there. Snowflake and
        Postgres use ANSI double quotes. Per-driver dispatch keeps the
        engine's lower-case-by-convention names intact across dialects
        (Snowflake uppercases unquoted identifiers; BigQuery's name
        rules are case-sensitive; Postgres folds unquoted to lowercase).
        """
        if self._driver == "bigquery":
            # BigQuery does not accept embedded backticks in identifier
            # names — they are quote-only. Defensively raise so the
            # failure mode is loud rather than a confusing parse error
            # from the warehouse.
            if "`" in name:
                raise ValueError(
                    f"BigQuery identifier {name!r} contains a backtick; "
                    "BigQuery does not support escaped backticks in names"
                )
            return f"`{name}`"
        return '"' + name.replace('"', '""') + '"'

    def _adbc_quote_qualified(self, schema: str, name: str) -> str:
        if schema:
            return f"{self._adbc_quote_ident(self._normalize_adbc_schema(schema))}.{self._adbc_quote_ident(name)}"
        return self._adbc_quote_ident(name)

    def _normalize_adbc_schema(self, schema: str) -> str:
        """Normalize a schema name for the active ADBC driver.

        Snowflake folds unquoted identifiers to upper case; its built-in
        default schema is unquoted ``PUBLIC``. If a connector declares
        the common lowercase ``public``, quoting it as ``"public"``
        targets a different (usually non-existent) schema and DDL fails.
        Upcase ``public`` → ``PUBLIC`` so the quoted form matches the
        real schema; preserve any other case-sensitive name verbatim
        because the connector author may have intended a quoted-name
        namespace.

        BigQuery and Postgres are case-sensitive after quoting in the
        ways operators expect (BigQuery datasets are case-sensitive;
        Postgres unquoted folds to lowercase, so quoted ``"public"``
        matches the conventional default). No normalization there.
        """
        if self._driver == "snowflake" and schema.lower() == "public":
            return "PUBLIC"
        return schema

    def _adbc_native_renderer(self):
        """Return the native-type → DDL renderer for the active ADBC driver."""
        if self._driver == "snowflake":
            return native_to_snowflake
        if self._driver == "bigquery":
            return native_to_bigquery
        if self._driver in ("postgresql", "postgres"):
            return native_to_postgres
        raise AdbcConfigurationError(
            f"ADBC-only mode has no DDL renderer for driver={self._driver!r}; "
            f"supported: snowflake, bigquery, postgresql"
        )

    def _adbc_timestamp_default_type(self) -> str:
        """Per-driver native timestamp type for the _synced_at audit column."""
        if self._driver == "snowflake":
            return "TIMESTAMP_TZ"
        if self._driver == "bigquery":
            return "TIMESTAMP"
        # PG, Redshift
        return "TIMESTAMP WITH TIME ZONE"

    def _adbc_binary_type(self) -> str:
        """Per-driver native binary column type for committed_cursor."""
        if self._driver == "snowflake":
            return "BINARY"
        if self._driver == "bigquery":
            return "BYTES"
        return "BYTEA"

    def _adbc_commit_timestamp_type(self) -> str:
        """Per-driver type used for committed_at in _batch_commits."""
        if self._driver == "snowflake":
            return "TIMESTAMP_NTZ"
        if self._driver == "bigquery":
            return "DATETIME"
        return "TIMESTAMP"

    def _build_adbc_create_table_ddl(
        self,
        state: _StreamState,
        type_mapper: TypeMapper,
    ) -> str:
        """Build a ``CREATE TABLE IF NOT EXISTS`` for the active ADBC driver.

        Mirrors :meth:`_create_table_from_schema` but emits a DDL string
        instead of a SQLAlchemy ``Table``. Always appends ``_synced_at``
        as a server-defaulted timestamp when the endpoint document does
        not declare it.
        """
        renderer = self._adbc_native_renderer()
        column_defs: List[str] = []
        declared_names: set[str] = set()
        for index, col_def in enumerate(state.endpoint_document.get("columns") or []):
            col_name = col_def.get("name")
            if not col_name:
                raise ValueError(
                    f"endpoint column at index {index} has no 'name' field"
                )
            native_type = col_def.get("native_type")
            if not native_type:
                raise ValueError(
                    f"column {col_name!r} has no 'native_type' field"
                )
            sql_type = renderer(native_type, type_mapper)
            is_pk = col_name in state.primary_keys
            nullable = col_def.get("nullable", True) and not is_pk
            parts = [self._adbc_quote_ident(col_name), sql_type]
            if not nullable:
                parts.append("NOT NULL")
            raw_default = col_def.get("default")
            if isinstance(raw_default, str) and raw_default.strip():
                parts.append(f"DEFAULT {raw_default}")
            column_defs.append(" ".join(parts))
            declared_names.add(col_name)

        if self.SYNCED_AT_COLUMN not in declared_names:
            column_defs.append(
                f"{self._adbc_quote_ident(self.SYNCED_AT_COLUMN)} "
                f"{self._adbc_timestamp_default_type()} DEFAULT CURRENT_TIMESTAMP"
            )

        # PRIMARY KEY clause is per-driver: Snowflake/Postgres accept
        # bare ``PRIMARY KEY (...)``; BigQuery requires the
        # ``NOT ENFORCED`` suffix (it does not enforce PK constraints
        # and the parser rejects them without the qualifier). Omit the
        # clause entirely when no PKs are declared.
        if state.primary_keys:
            pk_cols = ", ".join(self._adbc_quote_ident(k) for k in state.primary_keys)
            column_defs.append(self._build_adbc_pk_clause(pk_cols))

        qualified = self._adbc_quote_qualified(state.schema_name, state.table_name)
        return (
            f"CREATE TABLE IF NOT EXISTS {qualified} (\n  "
            + ",\n  ".join(column_defs)
            + "\n)"
        )

    def _build_adbc_pk_clause(self, pk_cols: str) -> str:
        """Per-driver PRIMARY KEY clause for inclusion in a CREATE TABLE.

        BigQuery's parser rejects bare ``PRIMARY KEY (...)`` — it
        requires the ``NOT ENFORCED`` qualifier and does not actually
        enforce the constraint at runtime (it's a planner hint).
        Snowflake and Postgres accept and enforce the bare form.
        """
        if self._driver == "bigquery":
            return f"PRIMARY KEY ({pk_cols}) NOT ENFORCED"
        return f"PRIMARY KEY ({pk_cols})"

    def _adbc_text_type(self) -> str:
        """Per-driver native string type for the _batch_commits text columns.

        BigQuery's GoogleSQL has only ``STRING`` (with optional length);
        ``VARCHAR(n)`` is rejected at parse time. Snowflake and Postgres
        both accept ``VARCHAR(n)``.
        """
        if self._driver == "bigquery":
            return "STRING"
        return "VARCHAR(255)"

    def _build_adbc_batch_commits_ddl(self, schema_name: str) -> str:
        qualified = self._adbc_quote_qualified(schema_name, self.BATCH_COMMITS_TABLE)
        pk_cols = (
            f"{self._adbc_quote_ident('run_id')}, "
            f"{self._adbc_quote_ident('stream_id')}, "
            f"{self._adbc_quote_ident('batch_seq')}"
        )
        text_type = self._adbc_text_type()
        return (
            f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
            f"  {self._adbc_quote_ident('run_id')} {text_type} NOT NULL,\n"
            f"  {self._adbc_quote_ident('stream_id')} {text_type} NOT NULL,\n"
            f"  {self._adbc_quote_ident('batch_seq')} BIGINT NOT NULL,\n"
            f"  {self._adbc_quote_ident('committed_cursor')} "
            f"{self._adbc_binary_type()},\n"
            f"  {self._adbc_quote_ident('records_written')} INTEGER,\n"
            f"  {self._adbc_quote_ident('committed_at')} "
            f"{self._adbc_commit_timestamp_type()} DEFAULT CURRENT_TIMESTAMP,\n"
            f"  {self._build_adbc_pk_clause(pk_cols)}\n"
            f")"
        )

    def _schema_is_implicit_default(self, schema_name: str) -> bool:
        """True when the schema name is the dialect's implicit default.

        Avoids issuing ``CREATE SCHEMA "public"`` against Snowflake
        roles that can create tables in PUBLIC but lack CREATE SCHEMA
        on the database. The connector's case-sensitive declaration
        is preserved when the user intends a quoted-name namespace.
        """
        if not schema_name:
            return True
        normalized = schema_name.lower()
        if self._driver == "snowflake":
            return normalized == "public"
        if self._driver == "bigquery":
            # BigQuery has no implicit default dataset; any named dataset
            # must be created explicitly. Returning False for any non-
            # empty name causes _ensure_tables_via_adbc to always emit
            # CREATE SCHEMA IF NOT EXISTS for it (the empty case is
            # already short-circuited above).
            return False
        # Postgres / Redshift
        return normalized == "public"

    async def _ensure_tables_via_adbc(
        self, state: _StreamState, type_mapper: TypeMapper
    ) -> None:
        if not state.endpoint_document:
            raise AdbcConfigurationError(
                "ADBC-only mode requires an endpoint document for DDL"
            )
        create_table_sql = self._build_adbc_create_table_ddl(state, type_mapper)
        create_commits_sql = self._build_adbc_batch_commits_ddl(state.schema_name)
        statements: List[str] = []
        if not self._schema_is_implicit_default(state.schema_name):
            # BigQuery uses ``CREATE SCHEMA`` for datasets (Standard
            # SQL). Snowflake and Postgres both accept the same DDL.
            # Normalize before quoting so a Snowflake ``public`` matches
            # the warehouse's real ``PUBLIC`` schema instead of creating
            # a quoted-lowercase sibling.
            statements.append(
                f"CREATE SCHEMA IF NOT EXISTS "
                f"{self._adbc_quote_ident(self._normalize_adbc_schema(state.schema_name))}"
            )
        statements.extend([create_table_sql, create_commits_sql])
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
        qualified = self._adbc_quote_qualified(
            state.schema_name, self.BATCH_COMMITS_TABLE
        )
        sql = (
            f"SELECT {self._adbc_quote_ident('records_written')}, "
            f"{self._adbc_quote_ident('committed_cursor')} "
            f"FROM {qualified} "
            f"WHERE {self._adbc_quote_ident('run_id')} = ? "
            f"AND {self._adbc_quote_ident('stream_id')} = ? "
            f"AND {self._adbc_quote_ident('batch_seq')} = ?"
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
        qualified = self._adbc_quote_qualified(
            state.schema_name, self.BATCH_COMMITS_TABLE
        )
        if self._driver == "bigquery":
            # BigQuery PRIMARY KEY is NOT ENFORCED (parser allows it
            # only as a planner hint), so a plain INSERT racing a
            # concurrent retry would produce two commit rows instead
            # of one — no IntegrityError to detect. ``MERGE INTO ...
            # WHEN NOT MATCHED THEN INSERT`` is atomic per-statement
            # on BigQuery and gives the same idempotency the enforced
            # PK provides on Snowflake / Postgres.
            sql = (
                f"MERGE INTO {qualified} t USING (\n"
                f"  SELECT ? AS {self._adbc_quote_ident('run_id')}, "
                f"? AS {self._adbc_quote_ident('stream_id')}, "
                f"? AS {self._adbc_quote_ident('batch_seq')}, "
                f"? AS {self._adbc_quote_ident('committed_cursor')}, "
                f"? AS {self._adbc_quote_ident('records_written')}\n"
                f") s\n"
                f"ON t.{self._adbc_quote_ident('run_id')} = s.{self._adbc_quote_ident('run_id')} "
                f"AND t.{self._adbc_quote_ident('stream_id')} = s.{self._adbc_quote_ident('stream_id')} "
                f"AND t.{self._adbc_quote_ident('batch_seq')} = s.{self._adbc_quote_ident('batch_seq')}\n"
                f"WHEN NOT MATCHED THEN INSERT ("
                f"{self._adbc_quote_ident('run_id')}, "
                f"{self._adbc_quote_ident('stream_id')}, "
                f"{self._adbc_quote_ident('batch_seq')}, "
                f"{self._adbc_quote_ident('committed_cursor')}, "
                f"{self._adbc_quote_ident('records_written')}) "
                f"VALUES (s.{self._adbc_quote_ident('run_id')}, "
                f"s.{self._adbc_quote_ident('stream_id')}, "
                f"s.{self._adbc_quote_ident('batch_seq')}, "
                f"s.{self._adbc_quote_ident('committed_cursor')}, "
                f"s.{self._adbc_quote_ident('records_written')})"
            )
        else:
            sql = (
                f"INSERT INTO {qualified} ("
                f"{self._adbc_quote_ident('run_id')}, "
                f"{self._adbc_quote_ident('stream_id')}, "
                f"{self._adbc_quote_ident('batch_seq')}, "
                f"{self._adbc_quote_ident('committed_cursor')}, "
                f"{self._adbc_quote_ident('records_written')}) "
                f"VALUES (?, ?, ?, ?, ?)"
            )
        try:
            rowcount = await asyncio.to_thread(
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
        # BigQuery's MERGE-based commit-record path doesn't raise
        # IntegrityError on a (run_id, stream_id, batch_seq) collision —
        # MERGE matches the existing row and the WHEN NOT MATCHED INSERT
        # silently no-ops with rowcount=0. Without this check, insert-mode
        # duplication would slip through reported as success.
        #
        # This rowcount-as-collision invariant is tightly coupled to the
        # exact MERGE SQL shape constructed above: a single-row constant
        # source CTE plus a WHEN NOT MATCHED INSERT clause only. Adding
        # a WHEN MATCHED UPDATE clause, or replacing the source CTE
        # with a multi-row SELECT, breaks the invariant — rowcount could
        # then legitimately be 0 for non-collision reasons. Do not add
        # either without revisiting this branch.
        if self._driver == "bigquery":
            if rowcount == 0:
                collision_marker = RuntimeError(
                    "BigQuery _batch_commits MERGE no-op: a row for "
                    f"({run_id!r}, {stream_id!r}, {batch_seq}) already exists; "
                    "concurrent retry won"
                )
                self._handle_commit_collision(
                    state, run_id, stream_id, batch_seq, collision_marker,
                )
                return
            if rowcount < 0:
                # PEP-249 -1 means "unavailable". The BQ ADBC driver
                # documents non-negative counts on DML, so seeing -1
                # here is suspicious — we can't distinguish a successful
                # insert from a collision-no-op. Log loudly because
                # insert-mode duplication would silently slip through.
                logger.warning(
                    "BigQuery _batch_commits MERGE returned rowcount=%r "
                    "for (%s, %s, %s); cannot verify whether the commit "
                    "row was inserted or no-op'd on existing PK collision. "
                    "Insert-mode duplication may be silently masked. "
                    "Check the adbc-driver-bigquery version.",
                    rowcount, run_id, stream_id, batch_seq,
                )

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
                "ADBC-only write requires a configured SchemaContract"
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
                            self._normalize_adbc_schema(schema_name)
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
        qualified = self._adbc_quote_qualified(schema_name, table_name)
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

    def _build_adbc_stage_table_sql(
        self,
        stage_qualified: str,
        target_qualified: str,
    ) -> str:
        """SQL to create an empty staging table shaped like the target.

        Uses ``CREATE TABLE`` (not ``TEMP``) so the table lives in the
        target schema across all drivers and the engine controls
        cleanup explicitly via ``DROP TABLE``. Each driver has its own
        column-copy syntax:

        * Snowflake — ``CREATE TABLE … LIKE`` copies columns only.
        * BigQuery — ``CREATE TABLE … LIKE`` is not supported in
          GoogleSQL; use ``AS SELECT * FROM target WHERE FALSE``.
        * Postgres — ``CREATE TABLE … (LIKE target INCLUDING DEFAULTS)``.

        The previous design used TEMP tables for auto-cleanup, but BQ
        TEMP tables require an explicit session (which the ADBC driver
        does not open by default) and the per-driver TEMP+namespace
        rules diverge enough that explicit DROP is simpler.
        """
        if self._driver == "snowflake":
            return f"CREATE TABLE {stage_qualified} LIKE {target_qualified}"
        if self._driver == "bigquery":
            return (
                f"CREATE TABLE {stage_qualified} AS "
                f"SELECT * FROM {target_qualified} WHERE FALSE"
            )
        # Postgres / Redshift
        return (
            f"CREATE TABLE {stage_qualified} "
            f"(LIKE {target_qualified} INCLUDING DEFAULTS)"
        )

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
        target_qualified = self._adbc_quote_qualified(schema_name, table_name)
        stage_qualified = self._adbc_quote_qualified(schema_name, stage_name)
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
                    self._build_adbc_stage_table_sql(stage_qualified, target_qualified)
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
                        self._normalize_adbc_schema(schema_name)
                        if schema_name else None
                    ),
                )
                conn.commit()
                on_clause = " AND ".join(
                    f"t.{self._adbc_quote_ident(k)} = s.{self._adbc_quote_ident(k)}"
                    for k in conflict_keys
                )
                set_clause = ", ".join(
                    f"t.{self._adbc_quote_ident(c)} = s.{self._adbc_quote_ident(c)}"
                    for c in update_cols
                )
                insert_cols = ", ".join(self._adbc_quote_ident(c) for c in all_columns)
                insert_vals = ", ".join(
                    f"s.{self._adbc_quote_ident(c)}" for c in all_columns
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
