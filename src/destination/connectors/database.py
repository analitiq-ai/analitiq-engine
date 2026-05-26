"""SQLAlchemy-based database destination handler.

This handler provides a unified interface for all SQL databases supported by SQLAlchemy.
The specific database is determined by the `driver` field in the connection config.

Type casting is handled by the Arrow-based SchemaContract, which provides
efficient columnar type conversion for batch operations.
"""

import asyncio
import importlib
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple
from urllib.parse import quote, urlencode

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
from ..sql_types import native_to_snowflake, native_to_sqlalchemy
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
from ...shared.connection_runtime import ConnectionRuntime


logger = logging.getLogger(__name__)


# SQLAlchemy dialect (matches ``self._driver``, set on connect from
# ``runtime.driver``) -> dotted ADBC dbapi module path. Import is lazy
# so a missing package disables the fast path for that dialect.
_ADBC_MODULES: Dict[str, str] = {
    "postgresql": "adbc_driver_postgresql.dbapi",
    "postgres": "adbc_driver_postgresql.dbapi",
}


# Opt-in feature flag. ``ADBC_FAST_PATH=1`` enables the fast path.
_ADBC_ENV_VAR = "ADBC_FAST_PATH"

# Sentinel for ``_adbc_module``: distinct from ``None`` (never tried)
# and from a real module object so an ``is`` check is unambiguous even
# if a third-party module overrides ``__bool__``.
_ADBC_IMPORT_FAILED: Any = object()


class AdbcConfigurationError(RuntimeError):
    """Deterministic ADBC-side misconfiguration.

    Raised when retrying the same batch cannot succeed: missing driver
    package, unsupported dialect, unbuildable URI, missing schema
    contract, malformed SQL, or insufficient privileges. Surfaced as
    ``ACK_STATUS_FATAL_FAILURE`` so the engine stops re-attempting.
    """


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

    _RETRY_SEMANTICS = {
        "insert": "retry will duplicate rows",
        "truncate_insert": "retry is idempotent (truncate + re-ingest)",
        "upsert": "retry is idempotent under conflict keys (re-MERGE)",
    }

    def __init__(self, inner: BaseException, write_mode: str = "insert") -> None:
        retry_note = self._RETRY_SEMANTICS.get(
            write_mode, f"retry semantics for write_mode={write_mode!r} unknown"
        )
        super().__init__(
            f"adbc ingest committed; commit-record failed "
            f"({retry_note}): {inner}"
        )
        self.__cause__ = inner
        self.write_mode = write_mode


def _adbc_flag_enabled() -> bool:
    value = os.environ.get(_ADBC_ENV_VAR, "").strip().lower()
    return value in ("1", "true", "yes", "on")


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
    write_mode: str = "upsert"
    endpoint_document: Dict[str, Any] = field(default_factory=dict)
    schema_contract: Optional[SchemaContract] = None
    metadata: MetaData = field(default_factory=MetaData)


class DatabaseDestinationHandler(BaseDestinationHandler):
    """
    Unified database destination handler using SQLAlchemy.

    Supports any database that SQLAlchemy supports via the `driver` config field.
    The handler automatically:
    - Creates the target table if it doesn't exist
    - Creates the idempotency tracking table
    - Handles upsert operations for supported databases
    - Tracks batch commits for idempotency

    Configuration (connection config):
    - driver: Database driver (postgresql, mysql, sqlite, etc.)
    - host: Database host
    - port: Database port
    - database: Database name
    - username: Database username
    - password: Database password
    - ssl_mode: Optional SSL mode

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

        # Lazily-imported ADBC dbapi module. ``None`` means the import
        # has not been attempted; ``_ADBC_IMPORT_FAILED`` means it was
        # attempted and the package is unavailable in this process — we
        # do not retry within the handler's lifetime.
        self._adbc_module: Any = None
        # Cached ADBC connection. In the Postgres fast path it is
        # opened lazily on the first ingest via ``_open_adbc_connection``
        # (libpq under the hood). In ADBC-only mode (Snowflake today)
        # it is opened eagerly in ``connect()`` via the runtime and
        # poisoned-then-reopened on failure via
        # ``_reopen_adbc_if_needed_sync``. Either way: nulled on any
        # ingest error so the next batch reconnects instead of reusing
        # a poisoned handle.
        self._adbc_conn: Any = None
        # Set of ``(stream_id, reason)`` pairs already logged when the
        # fast path was demoted to SQLAlchemy. Keyed on stream_id so
        # two streams writing to the same physical table each get their
        # own first-demotion log line.
        self._adbc_demotion_logged: Set[Tuple[str, str]] = set()

    def set_endpoint_refs(self, endpoint_refs: Mapping[str, Any]) -> None:
        """Register stream_id → endpoint_ref for each stream writing to this
        destination. Called once by ``src.main`` before the gRPC server starts;
        the handler consults the map per incoming ``SchemaMessage`` to decide
        which ``TypeMapper`` applies (public endpoint → connector's map,
        private endpoint → connection's map).

        Values are dict-shape ``EndpointRef`` payloads
        (``{"scope", "connection_id", "alias"}`` plus optional ``x-*``
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
        """PostgreSQL and MySQL support upsert."""
        return self._driver in ("postgresql", "postgres", "mysql", "mariadb")

    @property
    def supports_bulk_load(self) -> bool:
        """Bulk load support depends on database."""
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
        cancelled mid-close. ``asyncio.CancelledError`` is caught
        separately from ``Exception`` for each release, captured, and
        re-raised after the runtime close still runs — so the caller's
        cancellation is honored without leaving the engine pool
        un-disposed.
        """
        cancelled: Optional[BaseException] = None
        adbc_conn = self._adbc_conn
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
            finally:
                self._adbc_conn = None
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
            write_mode_str = self._get_write_mode(schema_msg.write_mode)
            if self._adbc_only:
                # Snowflake's default schema is account-/role-dependent;
                # falling back to "public" silently writes into a
                # Postgres-shaped namespace that may not exist. Require
                # the endpoint document to declare the schema explicitly.
                if not raw_schema:
                    logger.error(
                        "ADBC destination requires database_object.schema "
                        "for stream %r (no implicit default)",
                        stream_id,
                    )
                    return False
                schema_name = raw_schema
                # ``upsert`` on the ADBC-only path requires a non-empty
                # conflict-key set to drive the MERGE. Without it the
                # write_batch dispatcher silently falls through to plain
                # ingest, which produces duplicate rows on retry while
                # AdbcCommitRecordError claims the retry is idempotent.
                # Refuse the stream up front so the misconfiguration is
                # visible at configure time.
                if write_mode_str == "upsert" and not conflict_keys:
                    logger.error(
                        "ADBC destination stream %r uses write_mode=upsert "
                        "but has no conflict_keys (no primary_keys, no "
                        "_write_conflict_keys); cannot MERGE",
                        stream_id,
                    )
                    return False
            else:
                schema_name = raw_schema or "public"
            state = _StreamState(
                schema_name=schema_name,
                table_name=table_name,
                endpoint_document=dict(endpoint_doc),
                write_mode=write_mode_str,
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
                    # Same flow as the ADBC-only path: a PEP-249 fatal
                    # commit-record failure must stay fatal.
                    raise
                except Exception as commit_exc:
                    # The ingest already committed on the ADBC
                    # connection; the SA ``_batch_commits`` write
                    # failed. For the insert-only fast path a retry
                    # re-ingests, duplicating rows. Surface the
                    # divergence both in the log and in the failure
                    # summary returned to the engine.
                    logger.error(
                        "ADBC ingest committed but commit-record failed "
                        "for %s/%s/%s — retry will duplicate %d row(s)",
                        run_id, stream_id, batch_seq, record_count,
                        exc_info=True,
                    )
                    raise AdbcCommitRecordError(
                        commit_exc, write_mode="insert"
                    ) from commit_exc
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

        Returns the module, or ``_ADBC_IMPORT_FAILED`` when no driver
        is registered for the dialect or the import fails. The result
        is cached so we do not retry an import that already failed
        once in this process.
        """
        if self._adbc_module is not None:
            return self._adbc_module

        module_path = _ADBC_MODULES.get(self._driver)
        if not module_path:
            self._adbc_module = _ADBC_IMPORT_FAILED
            return self._adbc_module

        try:
            self._adbc_module = importlib.import_module(module_path)
        except ImportError as exc:
            # The flag was explicitly opted into, so a missing driver
            # deserves WARNING — the user asked for the fast path and
            # is silently getting the slow one otherwise.
            log_fn = logger.warning if _adbc_flag_enabled() else logger.debug
            log_fn(
                "ADBC fast path disabled for driver=%s: %s not importable (%s). "
                "Install the matching `adbc-driver-*` package to enable.",
                self._driver,
                module_path,
                exc,
            )
            self._adbc_module = _ADBC_IMPORT_FAILED
        return self._adbc_module

    def _build_adbc_uri(self) -> Optional[str]:
        """Render the SQLAlchemy URL as a libpq URI for ADBC.

        Reassembled from ``url`` parts so the leading driver name is
        the bare backend (``postgresql``), credentials are
        percent-encoded, and any non-empty ``url.query`` (e.g.
        ``sslmode=require``, ``application_name=...``) is forwarded so
        TLS / connection settings configured for SQLAlchemy are not
        silently dropped for ADBC.
        """
        if self._engine is None:
            return None
        url = self._engine.url
        backend = (url.get_backend_name() or "").lower()
        if backend not in {"postgresql", "postgres"}:
            return None
        if not url.host or not url.database:
            return None

        userinfo = ""
        if url.username:
            userinfo = quote(url.username, safe="")
            if url.password:
                userinfo += ":" + quote(url.password, safe="")
            userinfo += "@"
        port = f":{url.port}" if url.port else ""

        query = getattr(url, "query", None) or {}
        query_string = ""
        if query:
            # SQLAlchemy ``url.query`` is a dict (or immutabledict)
            # mapping str -> str | tuple[str, ...]. ``urlencode`` with
            # ``doseq=True`` handles both.
            query_string = "?" + urlencode(list(query.items()), doseq=True)

        return f"postgresql://{userinfo}{url.host}{port}/{url.database}{query_string}"

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
        * Stream is append-only (``write_mode == "insert"``); this fast
          path only invokes ``adbc_ingest`` with ``mode="append"``.
          Truncate / upsert modes are handled by the SQLAlchemy backend
          (or, for ADBC-only runtimes, by ``_write_batch_adbc_only``).
        * The dialect is in ``_ADBC_MODULES`` and the matching driver
          package is importable.
        * A connection URI can be built from the engine's URL.
        * The stream has a configured schema contract (needed for the
          vectorized cast before ingest).
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
        """Postgres-fast-path opener: return the cached ADBC connection.

        Builds a libpq URI from ``self._engine.url`` and consults the
        handler-local ``_ADBC_MODULES`` table for the matching driver
        package. ADBC-only mode (Snowflake) uses
        :meth:`_reopen_adbc_if_needed_sync` instead, which goes through
        the runtime's ``open_adbc_connection`` factory and supports
        every dialect registered in the transport factory's shared
        registry. Raises :class:`AdbcConfigurationError` when the
        driver module or URI cannot be produced. The cached handle is
        dropped on any ingest error so a poisoned connection is not
        reused.
        """
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
        """
        conn = self._open_adbc_connection()
        try:
            cursor = conn.cursor()
            try:
                cursor.adbc_ingest(
                    table_name,
                    cast_batch,
                    mode="append",
                    db_schema_name=schema_name or None,
                )
                conn.commit()
            finally:
                try:
                    cursor.close()
                except Exception:
                    logger.debug(
                        "ADBC ingest cursor close failed", exc_info=True
                    )
        except Exception as exc:
            poisoned = self._adbc_conn
            self._adbc_conn = None
            if poisoned is not None:
                try:
                    poisoned.close()
                except Exception:
                    # Best-effort. The conn is being discarded and the
                    # original ingest exception is what the caller
                    # needs to see; the close failure is logged at
                    # DEBUG so a libpq leak still leaves evidence.
                    logger.debug(
                        "Discarded poisoned ADBC connection; close failed",
                        exc_info=True,
                    )
            # Mirror the ADBC-only helpers: PEP-249 fatal errors get
            # reclassified so the engine stops retrying forever on a
            # permission denial or syntax error.
            if _is_fatal_adbc_error(exc):
                raise AdbcConfigurationError(str(exc)) from exc
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

    # ------------------------------------------------------------------
    # ADBC-only mode (DDL + idempotency + writes via ADBC cursor)
    # ------------------------------------------------------------------
    #
    # When the runtime exposes an ADBC transport instead of a SQLAlchemy
    # engine, every DDL/idempotency/write call goes through the cached
    # ADBC DBAPI connection. Snowflake is the only dialect this path
    # supports today; adding another requires (1) registering its driver
    # module in the shared ADBC registry and (2) writing the equivalent
    # of ``arrow_to_snowflake_native`` for the new dialect's DDL types.

    def _adbc_quote_ident(self, name: str) -> str:
        """Quote a SQL identifier for ADBC dialects.

        Snowflake uppercases unquoted identifiers, so we double-quote
        everything to preserve the engine's lower-case-by-convention
        column / table / schema names exactly.
        """
        return '"' + name.replace('"', '""') + '"'

    def _adbc_quote_qualified(self, schema: str, name: str) -> str:
        if schema:
            return f"{self._adbc_quote_ident(schema)}.{self._adbc_quote_ident(name)}"
        return self._adbc_quote_ident(name)

    def _build_snowflake_create_table_ddl(
        self,
        table_name: str,
        schema_name: str,
        endpoint_document: Dict[str, Any],
        primary_keys: List[str],
        type_mapper: TypeMapper,
    ) -> str:
        """Build a Snowflake ``CREATE TABLE IF NOT EXISTS`` statement.

        Mirrors :meth:`_create_table_from_schema` but emits a DDL string
        instead of a SQLAlchemy ``Table``. Always appends ``_synced_at``
        as a server-defaulted TIMESTAMP_TZ when the endpoint document
        does not declare it (matches the SA path's ``DateTime(timezone=True)``).
        """
        column_defs: List[str] = []
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
            snow_type = native_to_snowflake(native_type, type_mapper)
            is_pk = col_name in primary_keys
            nullable = col_def.get("nullable", True) and not is_pk
            parts = [self._adbc_quote_ident(col_name), snow_type]
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
                f"TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP"
            )

        if primary_keys:
            pk_cols = ", ".join(self._adbc_quote_ident(k) for k in primary_keys)
            column_defs.append(f"PRIMARY KEY ({pk_cols})")

        qualified = self._adbc_quote_qualified(schema_name, table_name)
        return (
            f"CREATE TABLE IF NOT EXISTS {qualified} (\n  "
            + ",\n  ".join(column_defs)
            + "\n)"
        )

    def _build_snowflake_batch_commits_ddl(self, schema_name: str) -> str:
        qualified = self._adbc_quote_qualified(schema_name, self.BATCH_COMMITS_TABLE)
        return (
            f"CREATE TABLE IF NOT EXISTS {qualified} (\n"
            f"  {self._adbc_quote_ident('run_id')} VARCHAR(255) NOT NULL,\n"
            f"  {self._adbc_quote_ident('stream_id')} VARCHAR(255) NOT NULL,\n"
            f"  {self._adbc_quote_ident('batch_seq')} INTEGER NOT NULL,\n"
            f"  {self._adbc_quote_ident('committed_cursor')} BINARY,\n"
            f"  {self._adbc_quote_ident('records_written')} INTEGER,\n"
            f"  {self._adbc_quote_ident('committed_at')} TIMESTAMP_NTZ "
            f"DEFAULT CURRENT_TIMESTAMP,\n"
            f"  PRIMARY KEY ({self._adbc_quote_ident('run_id')}, "
            f"{self._adbc_quote_ident('stream_id')}, "
            f"{self._adbc_quote_ident('batch_seq')})\n"
            f")"
        )

    async def _ensure_tables_via_adbc(
        self, state: _StreamState, type_mapper: TypeMapper
    ) -> None:
        """Apply schema + target + ``_batch_commits`` DDL via ADBC cursor.

        Requires ``state.endpoint_document`` (raises
        :class:`AdbcConfigurationError` otherwise). Snowflake is the
        only dialect supported on this path today; adding another
        means writing the equivalent of ``arrow_to_snowflake_native``.
        Emits the three DDL statements in order — schema first (so the
        table CREATE can resolve), then the target table, then the
        idempotency table. Fatal-vs-retryable classification happens
        one level down in :meth:`_execute_adbc_ddl_sync`; the DDL
        lock serialises concurrent streams writing the same catalog.
        """
        if state.endpoint_document is None:
            raise AdbcConfigurationError(
                "ADBC-only mode requires an endpoint document for DDL"
            )
        if self._driver != "snowflake":
            raise AdbcConfigurationError(
                f"ADBC-only mode does not yet support dialect={self._driver!r}; "
                f"Snowflake is the only supported dialect today"
            )
        create_schema_sql = (
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{self._adbc_quote_ident(state.schema_name)}"
        ) if state.schema_name else None
        create_table_sql = self._build_snowflake_create_table_ddl(
            state.table_name,
            state.schema_name,
            state.endpoint_document,
            state.primary_keys,
            type_mapper,
        )
        create_commits_sql = self._build_snowflake_batch_commits_ddl(state.schema_name)
        async with self._ddl_lock:
            await asyncio.to_thread(
                self._execute_adbc_ddl_sync,
                [s for s in (create_schema_sql, create_table_sql, create_commits_sql) if s],
            )
        logger.debug(
            "ADBC-only DDL applied for %s.%s", state.schema_name, state.table_name
        )

    def _execute_adbc_ddl_sync(self, statements: List[str]) -> None:
        """Run a list of DDL statements on the cached ADBC connection.

        SQL-level failures (syntax, permission, missing object) are
        reclassified as :class:`AdbcConfigurationError` so the engine
        treats them as fatal; transient I/O failures still propagate
        as the original exception and stay retryable.
        """
        conn = self._adbc_conn
        if conn is None:
            raise AdbcConfigurationError("ADBC connection not open")
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
                    logger.debug("ADBC DDL cursor close failed", exc_info=True)
        except Exception as exc:
            self._poison_adbc_connection()
            if _is_fatal_adbc_error(exc):
                raise AdbcConfigurationError(str(exc)) from exc
            raise

    def _poison_adbc_connection(self) -> None:
        """Drop and close the cached ADBC connection after a failure.

        The next operation re-opens via ``runtime.open_adbc_connection``.
        Close errors are logged at DEBUG — the original failure is what
        the caller needs to see.
        """
        conn = self._adbc_conn
        self._adbc_conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.debug(
                    "Discarded poisoned ADBC connection; close failed",
                    exc_info=True,
                )

    def _reopen_adbc_if_needed_sync(self) -> Any:
        """Return the cached ADBC connection, opening on demand.

        In ADBC-only mode the connection is opened in ``connect()`` but
        may be poisoned by an earlier failure. This helper transparently
        re-opens via the runtime so each write is self-healing.
        """
        if self._adbc_conn is not None:
            return self._adbc_conn
        if self._runtime is None:
            raise AdbcConfigurationError("Runtime not available for ADBC reconnect")
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
                    logger.debug("ADBC fetch cursor close failed", exc_info=True)
        except Exception as exc:
            self._poison_adbc_connection()
            if _is_fatal_adbc_error(exc):
                raise AdbcConfigurationError(str(exc)) from exc
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
        sql = (
            f"INSERT INTO {qualified} ("
            f"{self._adbc_quote_ident('run_id')}, "
            f"{self._adbc_quote_ident('stream_id')}, "
            f"{self._adbc_quote_ident('batch_seq')}, "
            f"{self._adbc_quote_ident('committed_cursor')}, "
            f"{self._adbc_quote_ident('records_written')}) "
            f"VALUES (?, ?, ?, ?, ?)"
        )
        await asyncio.to_thread(
            self._execute_adbc_dml_sync,
            sql,
            (run_id, stream_id, batch_seq, cursor_bytes, records_written),
        )

    def _execute_adbc_dml_sync(self, sql: str, params: Tuple[Any, ...]) -> None:
        conn = self._reopen_adbc_if_needed_sync()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params)
                conn.commit()
            finally:
                try:
                    cursor.close()
                except Exception:
                    logger.debug("ADBC DML cursor close failed", exc_info=True)
        except Exception as exc:
            self._poison_adbc_connection()
            if _is_fatal_adbc_error(exc):
                raise AdbcConfigurationError(str(exc)) from exc
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
        TABLE then append (two separate commits). Upsert: ingest into
        a session-scoped temp table, then ``MERGE INTO`` the target
        (three separate commits).

        Unlike the SA path's atomicity (write + commit-record in one
        transaction), the ADBC path commits the ingest separately from
        the ``_batch_commits`` INSERT. The retry semantics depend on
        write_mode (see :class:`AdbcCommitRecordError`):
        ``insert`` retries duplicate rows; ``truncate_insert`` is
        idempotent (table empties then re-fills); ``upsert`` is
        idempotent under the conflict-key set.
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
            await asyncio.to_thread(
                self._merge_ingest_sync,
                cast_batch, state.schema_name, state.table_name,
                list(cast_batch.schema.names), conflict_keys,
            )
        else:
            await asyncio.to_thread(
                self._adbc_ingest_sync,
                cast_batch, state.schema_name, state.table_name,
            )

        try:
            await self._record_batch_commit_via_adbc(
                state, run_id, stream_id, batch_seq, cursor_bytes, record_count,
            )
        except AdbcConfigurationError:
            # The commit-record helper already reclassified a PEP-249
            # fatal failure (missing _batch_commits table, permission
            # denial, …). Let it surface as fatal — retrying cannot
            # heal a configuration error even though the ingest itself
            # already committed.
            raise
        except Exception as commit_exc:
            retry_hint = AdbcCommitRecordError._RETRY_SEMANTICS.get(
                state.write_mode, "retry semantics unknown"
            )
            logger.error(
                "ADBC-only ingest committed but commit-record failed "
                "for %s/%s/%s (write_mode=%s; %s; %d row(s))",
                run_id, stream_id, batch_seq, state.write_mode,
                retry_hint, record_count,
                exc_info=True,
            )
            raise AdbcCommitRecordError(
                commit_exc, write_mode=state.write_mode
            ) from commit_exc

    def _truncate_then_ingest_sync(
        self,
        cast_batch: pa.RecordBatch,
        schema_name: str,
        table_name: str,
    ) -> None:
        qualified = self._adbc_quote_qualified(schema_name, table_name)
        conn = self._reopen_adbc_if_needed_sync()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(f"TRUNCATE TABLE {qualified}")
            finally:
                try:
                    cursor.close()
                except Exception:
                    logger.debug(
                        "ADBC truncate cursor close failed", exc_info=True
                    )
            conn.commit()
        except Exception as exc:
            self._poison_adbc_connection()
            if _is_fatal_adbc_error(exc):
                raise AdbcConfigurationError(str(exc)) from exc
            raise
        # ``_adbc_ingest_sync`` handles its own connection poisoning
        # and commit.
        self._adbc_ingest_sync(cast_batch, schema_name, table_name)

    def _merge_ingest_sync(
        self,
        cast_batch: pa.RecordBatch,
        schema_name: str,
        table_name: str,
        all_columns: List[str],
        conflict_keys: List[str],
    ) -> None:
        """Upsert via ingest-to-temp + MERGE INTO target.

        The temp table is qualified with the target's schema so the
        MERGE resolves both sides against the same namespace. Snowflake
        session-scoped temporary tables drop at session close — and
        each handler instance owns its own ADBC connection / session,
        so cross-handler isolation holds today. ``CREATE OR REPLACE``
        defends against the one remaining collision case: two
        consecutive writes to the literal same ``(schema, table)`` on
        retries. Two streams whose targets live in *different* schemas
        produce distinct fully-qualified temp names and do not collide.
        """
        temp_name = f"_analitiq_stage_{table_name}"
        qualified_target = self._adbc_quote_qualified(schema_name, table_name)
        qualified_temp = self._adbc_quote_qualified(schema_name, temp_name)
        conn = self._reopen_adbc_if_needed_sync()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"CREATE OR REPLACE TEMPORARY TABLE {qualified_temp} "
                    f"LIKE {qualified_target}"
                )
                conn.commit()
                cursor.adbc_ingest(
                    temp_name,
                    cast_batch,
                    mode="append",
                    db_schema_name=schema_name or None,
                )
                conn.commit()
                on_clause = " AND ".join(
                    f"t.{self._adbc_quote_ident(k)} = s.{self._adbc_quote_ident(k)}"
                    for k in conflict_keys
                )
                update_cols = [c for c in all_columns if c not in conflict_keys]
                set_clause = ", ".join(
                    f"t.{self._adbc_quote_ident(c)} = s.{self._adbc_quote_ident(c)}"
                    for c in update_cols
                )
                insert_cols = ", ".join(self._adbc_quote_ident(c) for c in all_columns)
                insert_vals = ", ".join(
                    f"s.{self._adbc_quote_ident(c)}" for c in all_columns
                )
                merge_sql = (
                    f"MERGE INTO {qualified_target} t "
                    f"USING {qualified_temp} s "
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
                    logger.debug(
                        "ADBC merge cursor close failed", exc_info=True
                    )
        except Exception as exc:
            self._poison_adbc_connection()
            if _is_fatal_adbc_error(exc):
                raise AdbcConfigurationError(str(exc)) from exc
            raise

    def _prepare_for_sqlalchemy(
        self, state: _StreamState, record_batch: pa.RecordBatch
    ) -> List[Dict[str, Any]]:
        """Materialise a batch for SQLAlchemy via the schema contract.

        SQLAlchemy is the wire-format-aware receiver: ``datetime`` /
        ``Decimal`` / ``dict`` go straight into their column types.
        ``to_db_records`` aligns the batch to the destination schema,
        materialises once, and reverses the Json wire-string encoding so
        a JSONB column receives a real dict, not a quoted string.
        """
        if state.schema_contract is None:
            return record_batch.to_pylist()
        return state.schema_contract.to_db_records(record_batch)

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
        conn = self._adbc_conn
        if conn is None:
            raise RuntimeError("ADBC connection not open")
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        finally:
            cursor.close()
