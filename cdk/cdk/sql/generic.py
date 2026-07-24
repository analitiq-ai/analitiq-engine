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

Writes are stage-then-merge on both transports (ADR sql-write-path-v2):
this facade owns the semantics — write modes, identity and duplicate
rules, refusals, retry verdicts, timeouts — and hands each batch to the
runtime-selected :class:`~cdk.sql.backend.TransportBackend`
(``SqlAlchemyBackend`` or ``AdbcBackend``) as a ``StageWritePlan``.

This base is vendor-neutral: every per-system quirk (quoting, the
merge-form upsert statement, pre-DDL, stage-table syntax, discovery
queries) is delegated to the :class:`~cdk.sql.dialects.SqlDialect`
carried by ``dialect_class`` — which each connector package overrides
with its own dialect next to its connector class. The base never
branches on a driver or connector_id; operations with no portable form
raise ``UnsupportedDialectOperationError`` naming the missing connector
package.
"""

import asyncio
import hashlib
import json
import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, AsyncExitStack, nullcontext
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Literal

import pyarrow as pa
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from cdk.adbc_registry import AdbcConfigurationError
from cdk.base_handler import BaseDestinationHandler, BatchWriteResult, reject_batch
from cdk.connection_runtime import (
    DETERMINISTIC_CONNECT_ERRORS,
    ConnectionRuntime,
    materialize_runtime,
)
from cdk.database_utils import acquire_connection
from cdk.declarations import DECLARED_WRITE_VERDICTS, ErrorMap, parse_declared_error_map
from cdk.query_builder import Filter, ParamsLike, QueryBuilder, QueryConfig
from cdk.schema_contract import SchemaContract
from cdk.type_map import InvalidTypeMapError, TypeMapper, UnmappedTypeError
from cdk.types import (
    AckStatus,
    CheckpointStore,
    Cursor,
    EndpointScope,
    FailureCategory,
    RetrySemantics,
    RetryVerdict,
    SchemaSpec,
)

from ..contract import ColumnDef
from ._adbc_utils import _is_fatal_adbc_error
from .adbc_backend import AdbcBackend
from .adbc_reader import open_adbc_reader
from .backend import SqlAlchemyBackend, TransportBackend
from .capabilities import (
    SqlCapabilities,
    bind_dialect_capabilities,
    undeclared_capability_error,
)
from .ddl import build_create_table_sql
from .ddl import create_table as _sql_create_table
from .dialects import SqlDialect, TableAddress
from .discovery import list_columns as _sql_list_columns
from .discovery import list_schemas as _sql_list_schemas
from .discovery import list_tables as _sql_list_tables
from .exceptions import (
    CatalogAddressingError,
    ReadError,
    SchemaConfigurationError,
    TlsVerificationError,
    UnsupportedDialectOperationError,
)
from .write_plan import build_stage_write_plan

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


WriteMode = Literal["insert", "upsert", "truncate_insert"]


def _page_order_by(
    order_by_field: str | None,
    cursor_field: str | None,
    columns: list[str],
    table_name: str,
) -> str:
    """Resolve the ORDER BY column for an OFFSET-paged ADBC read.

    The stream's declared order_by_field wins (a conflict with the
    incremental cursor is rejected in ``read_batches``), then the cursor,
    then the first projected column (warned once — an undeclared order
    makes OFFSET paging best-effort).
    """
    if order_by_field:
        return order_by_field
    if cursor_field:
        return cursor_field
    _note_order_by_fallback(table_name, columns[0])
    return columns[0]


@dataclass
class _StreamState:
    """Per-stream destination state.

    The handler is a single instance shared across all streams writing to
    this destination, so every field that depends on a specific stream's
    endpoint document lives here, keyed by ``stream_id``. Sharing handler
    instance fields directly across streams would race when the engine
    fires schema/configure calls in parallel.

    ``address`` is the dialect-built :class:`TableAddress` — components
    normalized once at ``configure_schema`` time, so every consumer (DDL,
    DML, ingest kwargs, logs) works from identical components.
    """

    address: TableAddress = field(default_factory=lambda: TableAddress(table=""))
    primary_keys: list[str] = field(default_factory=list)
    # Columns used as the ON CONFLICT / MERGE target for upsert. Set at
    # configure_schema time from the ``set_stream_conflict_keys`` map,
    # which carries the stream's Infra-validated ``write.conflict_keys``
    # verbatim. Empty here means INSERT mode — an upsert always carries
    # an explicit conflict target under the contract.
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
      ``asyncio.to_thread``. Writes are stage-then-merge, executed by
      ``SqlAlchemyBackend`` from a ``StageWritePlan`` this facade builds:
      the batch lands in a per-batch stage table, then exactly one mode
      statement applies it (set-based anti-join for insert, the dialect's
      declared merge form for upsert, plain append for truncate_insert).
      Both flavours run one shared sync-``Connection`` cycle body (the
      async path enters it through ``AsyncConnection.run_sync``), so
      DML/DDL semantics are identical. DDL via rendered ``CREATE TABLE``
      + backend reflection for landing bind types.
    * ``transport_type: "adbc"`` — direct ADBC DBAPI 2.0 connection
      (Snowflake, BigQuery). The same ``StageWritePlan``s execute over
      ``AdbcBackend``: DDL via ``cursor.execute`` of the rendered
      statements, stage landing via the declared bulk mechanism
      (``cursor.adbc_ingest``, a dialect ``bulk_land`` hook, or
      executemany), the mode statement from the same plan.

    Both transports are idempotent on the write mode's own keys (a merge
    on ``conflict_keys`` for upsert, the identity anti-join — contract
    primary key or the synthetic ``_record_hash`` — for insert), not a
    side ledger; they share the schema-contract Arrow cast and the
    per-stream state machine. Configuration is resolved through
    ``ConnectionRuntime`` rather than read off the raw connection JSON —
    the handler never inspects host/port/secret fields directly.

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
    # uniqueness the insert anti-join relies on — one mechanism on both
    # transports; a re-read row is never duplicated (issue #282, issue
    # #285). Absent on streams that carry a primary key or use
    # upsert/truncate_insert.
    RECORD_HASH_COLUMN = "_record_hash"

    # The dialect strategy carrying every vendor-specific piece of SQL:
    # quoting, the merge-form upsert statement, pre-DDL, stage-table
    # syntax, discovery queries. The CDK base is ANSI-neutral; a connector
    # package's class overrides ``dialect_class`` with its own SqlDialect
    # subclass. This is the ONLY per-system extension point — the generic
    # connector never branches on driver or connector_id.
    dialect_class: type[SqlDialect] = SqlDialect

    def __init__(self) -> None:
        """Initialize the database handler."""
        self.dialect: SqlDialect = self.dialect_class()
        # The connector's declared sql_capabilities (issue #390), parsed
        # from the runtime when one binds. None = undeclared: every
        # consumer refuses a needed-but-undeclared fact loudly instead of
        # guessing a default.
        self._capabilities: SqlCapabilities | None = None
        # The connector's declared error taxonomy (issue #401), parsed
        # alongside the capability binding. None = undeclared: the write
        # ack ladder keeps its class-name heuristic (additive absence).
        self._error_map: ErrorMap | None = None
        self._runtime: ConnectionRuntime | None = None
        self._engine: AsyncEngine | None = None
        # Sync SQLAlchemy engine for sync-only drivers (e.g. Redshift's
        # redshift_connector). Mutually exclusive with ``self._engine``
        # and ADBC-only mode; its operations run via asyncio.to_thread,
        # mirroring the ADBC sync-in-thread pattern.
        self._sync_engine: Engine | None = None
        # The transport backend executing StageWritePlans —
        # SqlAlchemyBackend (both engine flavors) or AdbcBackend, selected
        # by the runtime's transport in connect(). None until connect().
        self._backend: TransportBackend | None = None
        self._config: dict[str, Any] = {}
        self._connected: bool = False
        self._driver: str = ""
        # Seconds to bound a destination SQL handler attempt, set by the
        # destination servicer via set_statement_timeout() on each schema
        # handshake from the sender-stamped gRPC ack budget. None (source-role
        # instances, or unset) means unbounded - asyncio.timeout(None) never
        # fires. See _statement_deadline.
        self._statement_timeout_seconds: float | None = None
        # ADBC-only mode: the runtime exposes no SQLAlchemy engine and the
        # backend is AdbcBackend. Set in ``connect()`` from
        # ``runtime.is_adbc``. The facade keeps the flag for the
        # transport-fact rules that stay semantic (statement-deadline
        # enforceability, the explicit-schema addressing requirement) —
        # all DML and cursor mechanics live in the backend.
        self._adbc_only: bool = False

        # Per-stream state derived from the contract endpoint document at
        # configure_schema() time. The SchemaSpec off the wire only
        # carries stream_id, version, and write_mode; everything else comes
        # from the preloaded ``stream_endpoints`` map. Keyed by stream_id
        # so concurrent streams sharing this handler instance do not race
        # on shared mutable fields.
        self._streams: dict[str, _StreamState] = {}

        # Serializes CREATE TABLE statements across streams: two
        # concurrent DDL transactions can race the database's catalog
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

        # Stream-id -> upsert conflict keys (the stream's validated
        # ``write.conflict_keys``). Populated by set_stream_conflict_keys()
        # at startup; absent/empty means INSERT mode.
        self._stream_conflict_keys: dict[str, list[str]] = {}

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

    def set_stream_conflict_keys(
        self, stream_conflict_keys: Mapping[str, list[str]]
    ) -> None:
        """Register stream_id → upsert conflict keys for each stream.

        The keys are the stream's Infra-validated ``write.conflict_keys``,
        forwarded verbatim on their own channel (they are stream
        configuration, not part of the contract endpoint document).
        ``configure_schema`` reads them as the ON CONFLICT / MERGE target;
        absent or empty means INSERT mode.
        """
        self._stream_conflict_keys = {
            sid: list(keys) for sid, keys in stream_conflict_keys.items()
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

        Callers guard the backend's presence before entering the stage
        cycle inside the deadline.
        """
        if self._adbc_only or self._sync_engine is not None:
            return nullcontext()
        return asyncio.timeout(self._statement_timeout_seconds)

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
        # unknown scope, preserving the validation the engine gets from the
        # published contract (``validate_endpoint_ref``) engine-side.
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

    def _bind_capabilities(self, runtime: ConnectionRuntime) -> None:
        """Parse the runtime's declared ``sql_capabilities`` and attach them.

        The facade side of the one binding rule
        (:func:`~cdk.sql.capabilities.bind_dialect_capabilities`): the
        facade keeps the parsed block for write-path gates, and the
        dialect gets the same object for its address-construction catalog
        gate. Runs on ``connect()``, on ``read_batches`` (the
        runtime-taking source entry), and on every control-plane entry
        that takes a runtime directly; the standalone helpers bind
        themselves through the same function. The declared error taxonomy
        (issue #401) binds at the same point so both declarations always
        describe the same connector.
        """
        self._capabilities = bind_dialect_capabilities(self.dialect, runtime)
        self._error_map = parse_declared_error_map(
            runtime.declared_error_map,
            source=f"connector {runtime.connector_id!r}",
        )

    def _dialect_renders_merge_statement(self) -> bool:
        """Whether the active dialect implements the merge-form statement hook.

        A declared-vs-implemented consistency check, not a capability
        guess: the fact that the system CAN upsert is the declaration's
        (``sql_capabilities.merge_form``); whether this connector's dialect
        renders that form is an implementation fact checked at handshake so
        the disagreement fails before DDL, not on the first batch.
        """
        return (
            type(self.dialect).merge_statement_sql is not SqlDialect.merge_statement_sql
        )

    def _dialect_renders_stage_table(self) -> bool:
        """Whether the active dialect implements the stage DDL hook.

        Every SQLAlchemy-path write lands in a stage table, so a
        write-role connector without ``stage_table_sql`` cannot write at
        all — checked at handshake, alongside the declared stage shape.
        """
        return type(self.dialect).stage_table_sql is not SqlDialect.stage_table_sql

    def _stage_ready(self) -> bool:
        """Whether the stage cycle can run for this connector (any transport).

        The same predicate the ``configure_schema`` stage gate enforces —
        declared ``sql_capabilities`` and a stage-rendering dialect — so
        the advertised write modes and the handshake can never disagree:
        GetCapabilities must not offer a mode every stream of which would
        be refused before DDL.
        """
        return self._capabilities is not None and self._dialect_renders_stage_table()

    @property
    def supports_upsert(self) -> bool:
        """True when upsert is declared AND this connector can run it now.

        The declaration (``sql_capabilities.merge_form``) says whether the
        system has a merge statement; the advertisement additionally
        applies the same implementability gates ``configure_schema``
        enforces — the stage predicate and the dialect's merge-statement
        hook, identical on both transports — so ``GetCapabilities`` never
        advertises a mode every stream of which would be refused at
        handshake. Undeclared advertises ``False``; refusals at
        ``configure_schema`` carry the error naming the missing
        declaration or disagreement.
        """
        caps = self._capabilities
        if caps is None or not caps.supports_upsert:
            return False
        return self._dialect_renders_merge_statement() and self._stage_ready()

    @property
    def supports_insert(self) -> bool:
        """True when this connector can run the insert stage cycle now.

        Every write on every transport is a stage cycle, so the stage
        predicate gates the advertisement that gates the handshake.
        """
        return self._stage_ready()

    @property
    def supports_bulk_load(self) -> bool:
        """Bulk-load capability is not advertised here.

        How a batch lands in the stage is the connector's declared
        ``sql_capabilities.bulk_load`` mechanism, consumed by the
        transport backend. Returning False keeps the destination
        protocol unaware of the distinction — the engine always batches
        the same way.
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
        """True when the full-refresh write mode can run on this connector.

        The append phase reads from a stage on both transports, so the
        same stage predicate gates the advertisement that gates the
        handshake.
        """
        return self._stage_ready()

    def retry_semantics(self, stream_id: str) -> RetryVerdict:
        """Retry-safety verdict per write mode and keys (#286).

        Transport-independent under stage-then-merge (ADR
        sql-write-path-v2 §9): upsert merges on its conflict keys; insert
        anti-joins on row identity (contract primary key, or the
        synthetic ``_record_hash`` for a keyless stream) from the stage
        on every transport. The honest-verdict rule for insert: the
        anti-join dedups every sequential replay on its own, but the
        enforced PRIMARY KEY is the structural backstop against writes
        that race it — a system that does not enforce uniqueness
        (``pk_not_enforced``) has a filter, not a guarantee, so its
        insert streams report at-least-once. Truncate-insert truncates on
        the read's first batch only (issue #307) and its append phase is
        a plain insert with no row-identity dedup — so a replayed
        already-committed later batch re-inserts its rows.
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
                    "truncate-insert truncates on the run's first batch "
                    "and appends after that with no row-identity dedup; a "
                    "replayed already-committed later batch re-inserts its "
                    "rows"
                ),
            )
        if self.dialect.pk_not_enforced:
            return RetryVerdict(
                semantics=RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE,
                reason=(
                    f"insert anti-joins on row identity "
                    f"{self._identity_columns(state)}, which dedups every "
                    f"sequential replay, but this system does not enforce "
                    f"the identity constraint — a write racing the "
                    f"anti-join can land a duplicate the system will not "
                    f"reject"
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
        # Bind the declared capability block only once the new runtime's
        # transport is live, alongside the other per-connection state: a
        # failed reconnect must not leave this runtime's declaration bound
        # against the previous runtime's still-connected transport. (A
        # malformed block already failed on the trusted side at config
        # load; this parse re-validates at the process boundary.) Also
        # attached to the dialect: its catalog gate (table_address /
        # information_schema_ref) consults the declared fact at address
        # construction.
        try:
            self._bind_capabilities(runtime)
        except Exception:
            # materialize() already acquired the runtime; the caller does
            # not disconnect a handler whose connect() raised, so release
            # the ref here to keep the lifecycle balanced (same rule as
            # the ADBC eager-open failure below). The close is guarded so
            # a failing release can never mask the binding error the
            # operator actually needs.
            try:
                await runtime.close()
            except Exception:
                logger.warning(
                    "runtime release after a failed capability bind also "
                    "failed; the binding error below is the root cause",
                    exc_info=True,
                )
            raise
        self._driver = runtime.driver or ""
        # Reset prior-connection state so a long-lived handler that
        # reconnects across runtimes (e.g. tests) doesn't carry the
        # previous mode forward.
        self._adbc_only = False
        self._engine = None
        self._sync_engine = None
        self._backend = None
        # The write backend executes StageWritePlans over the runtime's
        # transport. Created after the capability binding so it reads the
        # declared bulk-load fact off the dialect.
        backend: TransportBackend
        if runtime.is_adbc:
            self._adbc_only = True
            transport_name = "ADBC"
            backend = AdbcBackend(self.dialect)
        elif runtime.is_sync_sqlalchemy:
            self._sync_engine = runtime.sync_engine
            transport_name = "sync SQLAlchemy"
            backend = SqlAlchemyBackend(self.dialect)
        else:
            self._engine = runtime.engine
            transport_name = "SQLAlchemy"
            backend = SqlAlchemyBackend(self.dialect)
        try:
            # The ADBC backend opens its connection eagerly so a bad
            # credential fails at connect() time, not on the first batch;
            # the driver-specific exception is wrapped in ConnectionError
            # to match the materialize() failure shape.
            await backend.connect(runtime)
        except Exception as e:
            logger.error(
                "%s backend connect failed: %s", transport_name, e, exc_info=True
            )
            # materialize() already acquired the runtime; the caller does
            # not disconnect a handler whose connect() raised, so release
            # the ref here to keep the lifecycle balanced. The close is
            # guarded so a failing release can never mask the connect
            # error the operator actually needs.
            try:
                await runtime.close()
            except Exception:
                logger.warning(
                    "runtime release after a failed backend connect also "
                    "failed; the connect error below is the root cause",
                    exc_info=True,
                )
            raise ConnectionError(f"{transport_name} connection failed: {e}") from e
        self._backend = backend
        logger.info(
            "GenericSQLConnector connected via %s to %s",
            transport_name,
            self._driver,
        )
        self._connected = True

    async def disconnect(self) -> None:
        """Close database connection.

        Both the transport backend (which closes an ADBC connection
        where one exists) and the runtime are released even if the other
        side fails or the coroutine is cancelled mid-close.
        ``CancelledError`` is re-raised after both releases so the
        caller's cancellation is honored.
        """
        cancelled: BaseException | None = None
        if self._backend is not None:
            try:
                await self._backend.disconnect()
            except asyncio.CancelledError as exc:
                logger.error(
                    "transport backend close cancelled during disconnect; "
                    "server-side resources may remain allocated"
                )
                cancelled = exc
            except Exception:
                logger.error(
                    "Failed to close transport backend during disconnect; "
                    "server-side resources may remain allocated",
                    exc_info=True,
                )
            self._backend = None
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

    def _destination_address(
        self, stream_id: str, database_object: Mapping[str, Any], table_name: str
    ) -> TableAddress | None:
        """Resolve the stream's target :class:`TableAddress`, or ``None`` to reject.

        Owns the per-transport schema rule and the catalog gates so
        ``configure_schema`` stays a lifecycle method. ``None`` means the
        rejection was already logged (missing ADBC schema — the pre-typed
        reject path); deterministic authoring errors raise
        :class:`SchemaConfigurationError` for the SchemaAck translation.
        """
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
                return None
            schema_name = raw_schema
        else:
            schema_name = raw_schema or "public"
        try:
            address = self.dialect.table_address(
                table_name,
                schema=schema_name,
                catalog=database_object.get("catalog") or "",
            )
        except CatalogAddressingError as err:
            # Deterministic authoring error — surface it in the SchemaAck
            # (the gRPC layer translates SchemaConfigurationError) instead
            # of failing the RPC as a wiring defect.
            raise SchemaConfigurationError(str(err)) from err
        if address.catalog and not self._adbc_only:
            # The SQLAlchemy write path reflects the target table and builds
            # DML from the reflected object; SQLAlchemy reflection cannot
            # cross catalogs. Refuse loudly rather than write to whatever
            # the reflected two-part name resolves to.
            raise SchemaConfigurationError(
                f"stream {stream_id!r} targets catalog "
                f"{address.catalog!r}, which the SQLAlchemy write transport "
                f"cannot address; use a connection whose default catalog is "
                f"{address.catalog!r}"
            )
        if address.catalog:
            # table_address passed the address door (declared 'read' or
            # 'full'), but this address is a write/DDL target: 'read'
            # declares discovery/read addressing only. Consult the same
            # declaration object the door consulted (the dialect's), so
            # the door-passed and gate-checked facts cannot diverge.
            caps = self.dialect.capabilities
            if caps is not None and caps.catalog != "full":
                raise SchemaConfigurationError(
                    f"stream {stream_id!r} writes to catalog "
                    f"{address.catalog!r}, but the connector declares "
                    f"sql_capabilities.catalog {caps.catalog!r} — writes and "
                    f"DDL across catalogs require 'full'. Use a connection "
                    f"whose default catalog is {address.catalog!r}."
                )
        return address

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
        # The stream's Infra-validated upsert conflict target, forwarded
        # verbatim via set_stream_conflict_keys(). Absent or empty means
        # no conflict target (INSERT mode); the engine never derives one
        # from ``primary_keys``.
        conflict_keys = list(self._stream_conflict_keys.get(stream_id) or [])
        address = self._destination_address(stream_id, database_object, table_name)
        if address is None:
            return False
        state = _StreamState(
            address=address,
            endpoint_document=dict(endpoint_doc),
            write_mode=self._get_write_mode(schema_spec.write_mode),
            primary_keys=primary_keys,
            conflict_keys=conflict_keys,
        )

        if state.write_mode == "upsert":
            # Mode-specific gate first: its refusal names the upsert fact
            # (merge_form), the more actionable message for the stream.
            self._check_upsert_capabilities(stream_id, address)
        # Every write on every transport lands in a stage table first, so
        # the declared stage shape and the dialect's stage DDL hook are
        # handshake requirements for every write mode, not just upsert.
        self._check_stage_capabilities(stream_id, address)

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
                f"CREATE TABLE for {state.address} did "
                f"not complete within the {self._statement_timeout_seconds:g}s "
                f"destination statement timeout (likely blocked on a lock or a "
                f"slow catalog); the statement was cancelled"
            ) from exc

        state.schema_contract = SchemaContract(state.endpoint_document)

        self._streams[stream_id] = state
        logger.info(
            "Schema configured for stream %r: %s, mode=%s, pk=%s",
            stream_id,
            state.address,
            state.write_mode,
            state.primary_keys,
        )
        return True

    def _check_upsert_capabilities(self, stream_id: str, address: TableAddress) -> None:
        """Refuse an upsert stream at handshake time, before any DDL runs.

        The whole upsert gate in one place: upsert needs the system's
        declared merge form and a dialect that renders it — the same
        statement hook on both transports. Every refusal names the
        missing declaration or the declaration/dialect disagreement.
        """
        caps = self._capabilities
        if caps is None:
            raise SchemaConfigurationError(
                str(
                    undeclared_capability_error(
                        "merge_form",
                        need=f"stream {stream_id!r} writes in upsert "
                        f"mode to {address}",
                    )
                )
            )
        if not caps.supports_upsert:
            raise SchemaConfigurationError(
                f"stream {stream_id!r} writes in upsert mode to "
                f"{address}, but the connector declares "
                f"sql_capabilities.merge_form 'none' — this system has "
                f"no merge statement. Use insert or truncate_insert, or "
                f"fix the connector's declaration."
            )
        if not self._dialect_renders_merge_statement():
            # Declaration/dialect disagreement, caught at handshake
            # instead of on the first batch: the upsert's stage-to-target
            # statement renders through the dialect's merge hook.
            raise SchemaConfigurationError(
                f"stream {stream_id!r} writes in upsert mode to "
                f"{address}: the connector declares "
                f"sql_capabilities.merge_form {caps.merge_form!r}, but "
                f"its dialect {self.dialect.name!r} does not implement "
                f"merge_statement_sql — the declaration and the "
                f"dialect disagree; fix the connector."
            )

    def _check_stage_capabilities(self, stream_id: str, address: TableAddress) -> None:
        """Refuse a write stream the stage cycle cannot run (any transport).

        Stage-then-merge is the only write shape: the batch lands in a
        stage whose scope and transaction shape come from the declared
        ``sql_capabilities.stage`` block and whose DDL renders through the
        dialect's ``stage_table_sql``. Undeclared refuses naming the
        missing declaration; a declaration the dialect cannot render
        refuses naming the disagreement — at handshake, never on the
        first batch.
        """
        if self._capabilities is None:
            raise SchemaConfigurationError(
                str(
                    undeclared_capability_error(
                        "stage",
                        need=f"stream {stream_id!r} writes to {address} "
                        f"through the stage-then-merge primitive",
                    )
                )
            )
        if not self._dialect_renders_stage_table():
            raise SchemaConfigurationError(
                f"stream {stream_id!r} writes to {address}: the connector "
                f"declares sql_capabilities.stage, but its dialect "
                f"{self.dialect.name!r} does not implement stage_table_sql "
                f"— the declaration and the dialect disagree; fix the "
                f"connector."
            )

    def _require_backend(self) -> TransportBackend:
        """Return the transport write backend, or fail loud pre-connect."""
        if self._backend is None:
            raise RuntimeError(
                "transport write backend not available; connect() has not "
                "completed on this handler"
            )
        return self._backend

    def _require_declared_capabilities(self, state: _StreamState) -> SqlCapabilities:
        """Return the declared block, re-checked at write time (defense in depth).

        ``configure_schema`` already refused undeclared streams; a write
        reaching this with ``None`` means the handshake was bypassed —
        refuse with the same loud configuration error, never a guess.
        """
        if self._capabilities is None:
            raise SchemaConfigurationError(
                str(
                    undeclared_capability_error(
                        "stage",
                        need=f"a write to {state.address} must know the "
                        f"declared stage shape",
                    )
                )
            )
        return self._capabilities

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

    def _build_column_defs(self, state: _StreamState) -> list[ColumnDef]:
        """Contract endpoint columns -> ColumnDefs for the shared DDL builder.

        Each column's canonical Arrow type is the document's stored
        ``arrow_type`` — the same declaration ``SchemaContract`` casts
        incoming batches with, so DDL and cast share one source of truth
        (issue #349). The builder renders it through the WRITE map for this
        destination; the stored ``native_type`` is the *source* system's
        type and plays no part in destination DDL. ``_synced_at`` is
        appended as a server-defaulted audit column when the endpoint
        doesn't declare it; a keyless insert stream also gets
        ``_record_hash`` as its synthetic primary key (see
        :meth:`_needs_record_hash`).
        """
        columns: list[ColumnDef] = []
        declared: set[str] = set()
        for index, col_def in enumerate(state.endpoint_document.get("columns") or []):
            col_name = col_def.get("name")
            if not col_name:
                raise SchemaConfigurationError(
                    f"endpoint column at index {index} has no 'name' field"
                )
            arrow_type = col_def.get("arrow_type")
            if not arrow_type:
                raise SchemaConfigurationError(
                    f"column {col_name!r} has no 'arrow_type' field"
                )
            raw_default = col_def.get("default")
            columns.append(
                ColumnDef(
                    name=col_name,
                    canonical_type=arrow_type,
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
                    f"{state.address} declares a column "
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

        A keyless ``insert`` stream: the anti-join matches on the hash
        from the stage, identically on both transports.
        Upsert/truncate_insert dedup on their own keys.
        """
        return state.write_mode == "insert" and not state.primary_keys

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
        dialect, and the rendered statements execute through the transport
        backend. The backend then reports the target's columns — the
        SQLAlchemy backend by reflecting (its landing binds with the
        reflected types), the ADBC backend by a zero-row probe — so the
        engine-managed ``_record_hash`` readiness rule below is one rule
        for every transport.
        """
        if not state.endpoint_document:
            raise SchemaConfigurationError(
                f"destination stream for {state.address} "
                f"has no endpoint document; cannot build DDL"
            )
        target_ddl = build_create_table_sql(
            self.dialect,
            type_mapper,
            state.address,
            self._build_column_defs(state),
            self._identity_columns(state),
            if_not_exists=True,
        )

        # Announce DDL before it runs (and before any lock wait) so a slow
        # CREATE TABLE is attributable to a table address instead of a silent
        # stall between "Received schema" and the SchemaAck. Paired with the
        # "Destination table ready" line below, the gap renders as elapsed
        # time between two INFO logs.
        logger.info(
            "Ensuring destination table exists for %s (executing DDL)",
            state.address,
        )

        if self._backend is None:
            # Silently skipping DDL here would leave the stream unprepared
            # and the write_batch readiness guard returning
            # RETRYABLE_FAILURE forever.
            raise AdbcConfigurationError(
                f"transport write backend is None during DDL for "
                f"{state.address}; "
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
        statements = [
            *self._schema_preparation_statements(state.address),
            target_ddl,
        ]
        async with self._statement_deadline():
            async with self._ddl_lock:
                backend = self._require_backend()
                await backend.run_ddl(statements)
                target_columns = await backend.target_columns(state.address)

        if self._needs_record_hash(state) and (
            self.RECORD_HASH_COLUMN not in target_columns
        ):
            # A keyless insert table created before issue #282 has no
            # _record_hash column; CREATE TABLE IF NOT EXISTS is a no-op and
            # the backend reports it without the column, so every write would
            # then fail on the missing identity column. Fail loud with a clear
            # message instead -- the engine adds no migration (the column is the
            # primary key, so it cannot be back-filled on existing rows).
            raise SchemaConfigurationError(
                f"keyless insert target {state.address} "
                f"has no {self.RECORD_HASH_COLUMN!r} column; it predates the "
                f"content-hash dedup key (issue #282). Recreate the target so the "
                f"engine manages {self.RECORD_HASH_COLUMN} as its primary key."
            )

        logger.info(
            "Destination table ready for %s",
            state.address,
        )

    def _schema_preparation_statements(self, address: TableAddress) -> list[str]:
        """Namespace preparation that precedes the target DDL.

        The target's schema first (per-transport rendering: the dialect's
        ``sqlalchemy_pre_ddl`` hook, or the generic ``CREATE SCHEMA IF NOT
        EXISTS`` the ADBC path composes for a non-implicit schema), and
        before it the declared dedicated stage schema when one exists: the
        write plan places every real-scope stage there, and a handshake
        that reported the target ready while the staging namespace does
        not exist would fail on the first batch instead.
        """
        if self._adbc_only:
            statements = []
            if not self.dialect.schema_is_implicit_default(address.schema):
                # The address components are already normalized, so a
                # case-folding system's conventional lowercase name matches
                # the stored one instead of creating a quoted-lowercase
                # sibling; the schema path is catalog-qualified when the
                # address carries one.
                quoted_schema = self.dialect.quote_schema(address)
                statements = [f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}"]
        else:
            statements = list(self.dialect.sqlalchemy_pre_ddl(address.schema))
        caps = self._capabilities
        if (
            caps is not None
            and caps.stage.scope == "real"
            and caps.stage.schema == "dedicated"
        ):
            # Normalized like every other schema component, so the
            # namespace prepared here is the one the write plan targets.
            dedicated = self.dialect.normalize_ident(str(caps.stage.dedicated_schema))
            if self._adbc_only:
                dedicated_ref = self.dialect.quote_schema(
                    replace(address, schema=dedicated)
                )
                statements = [
                    f"CREATE SCHEMA IF NOT EXISTS {dedicated_ref}",
                    *statements,
                ]
            else:
                statements = [
                    *self.dialect.sqlalchemy_pre_ddl(dedicated),
                    *statements,
                ]
        return statements

    def _reject_if_not_ready(
        self, run_id: str, stream_id: str, batch_seq: int
    ) -> BatchWriteResult | None:
        """Return a rejection for a batch this handler cannot write, else None.

        ``None`` means ``self._streams[stream_id]`` exists and is writable
        on the active transport.
        """
        if not self._connected:
            return reject_batch(
                logger,
                "Handler not connected",
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
            )
        if self._backend is None:
            return reject_batch(
                logger,
                "Handler not connected: no transport write backend",
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
            )

        state = self._streams.get(stream_id)
        if state is None:
            # Stream presence implies configure_schema completed: the
            # state is only registered after DDL and (on the SQLAlchemy
            # path) target reflection succeeded.
            return reject_batch(
                logger,
                "Schema not configured",
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
            )
        return None

    def _timeout_failure(
        self,
        state: _StreamState,
        e: TimeoutError,
        *,
        run_id: str,
        stream_id: str,
        batch_seq: int,
    ) -> BatchWriteResult:
        """Log and classify a ``TimeoutError`` raised out of the write attempt.

        Only the async-SQLAlchemy path is wrapped in ``asyncio.timeout``. The
        ADBC and sync-engine paths run in a worker thread (and a handler with
        no budget set is never bounded), so a TimeoutError from those is a
        driver/socket timeout, not our cancellation - classify it generically
        rather than claiming a statement was cancelled.
        """
        if (
            self._adbc_only
            or self._sync_engine is not None
            or self._statement_timeout_seconds is None
        ):
            logger.error(
                "Error writing batch (run=%s, stream=%s, seq=%s): %s",
                run_id,
                stream_id,
                batch_seq,
                e,
                exc_info=True,
            )
            # A bare TimeoutError often stringifies empty; never ack a
            # reason-less failure.
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=str(e) or f"driver timeout ({type(e).__name__})",
                failure_category=FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED,
            )
        # The bounded SQLAlchemy statement was cancelled (issue #231). A lock
        # or slow write may clear, so stay retryable; carry the reason so it
        # surfaces instead of an empty str(TimeoutError).
        summary = (
            f"destination write for {state.address} "
            f"did not complete within the {self._statement_timeout_seconds:g}s "
            f"statement timeout (likely a lock or slow write); the statement "
            f"was cancelled"
        )
        logger.error(
            "Timeout writing batch (run=%s, stream=%s, seq=%s): %s",
            run_id,
            stream_id,
            batch_seq,
            summary,
        )
        return BatchWriteResult(
            status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            records_written=0,
            failure_summary=summary,
            failure_category=FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED,
        )

    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: list[str],
        cursor: Cursor,
        emitted_at: datetime,
    ) -> BatchWriteResult:
        """Write an Arrow record batch to the database.

        The facade prepares the batch once in Arrow space — schema cast,
        record-hash attachment, intra-batch duplicate collapse — and hands
        the backend Arrow; the backend converts to its own parameter shape
        internally. ``emitted_at`` is part of the write_batch contract for
        time-partitioned sinks; a relational target has no output path, so
        it is unused here.
        """
        rejection = self._reject_if_not_ready(run_id, stream_id, batch_seq)
        if rejection is not None:
            return rejection
        # _reject_if_not_ready proved the stream is configured.
        state = self._streams[stream_id]

        try:
            # One deadline for the whole SQLAlchemy attempt so the write stays
            # under the ack budget. Bounding sub-phases separately would give
            # each its own full budget, which can sum past the ack deadline and
            # let the engine retry while the first write is still running
            # (issue #231).
            async with self._statement_deadline():
                record_count = record_batch.num_rows

                # A full refresh truncates on the read's FIRST batch
                # (batch_seq 1, issue #307) and appends after that.
                # batch_seq is the engine's own statement of a fresh read:
                # it restarts at 1 only when the engine (re)starts the
                # stream read from scratch, and truncate_insert reads are
                # never cursor-resumed. Keying on worker-side memory
                # instead would break when engine and destination restart
                # independently: a fresh worker joining mid-refresh would
                # truncate away committed batches, and a surviving worker
                # would skip the truncate for a restarted read.
                truncate_now = state.write_mode == "truncate_insert" and batch_seq == 1

                if record_count == 0:
                    # Empty batch: nothing to insert. The cursor still
                    # advances (the watermark moved); idempotency lives in
                    # the write itself, so there is no separate marker to
                    # record. An empty FIRST batch (including the synthetic
                    # one the engine sends when the source yields no batches
                    # at all — issue #312) must still truncate.
                    if truncate_now:
                        await self._truncate_only(state)
                    return BatchWriteResult(
                        status=AckStatus.ACK_STATUS_SUCCESS,
                        records_written=0,
                        committed_cursor=cursor,
                    )

                # Stage-then-merge on every transport: the facade
                # prepares the batch once in Arrow space (cast,
                # record-hash attachment, intra-batch duplicate
                # collapse — all semantics), renders the plan, and the
                # backend executes it.
                prepared = self._prepare_write_batch(state, record_batch)
                plan = build_stage_write_plan(
                    self.dialect,
                    self._require_declared_capabilities(state),
                    target=state.address,
                    columns=tuple(prepared.schema.names),
                    write_mode=state.write_mode,
                    conflict_keys=state.conflict_keys,
                    identity=self._identity_columns(state),
                    truncate_now=truncate_now,
                    run_id=run_id,
                    stream_id=stream_id,
                    batch_seq=batch_seq,
                )
                await self._require_backend().execute_write(plan, prepared)

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
            logger.error(
                "Type-map error writing batch (run=%s, stream=%s, seq=%s): %s",
                run_id,
                stream_id,
                batch_seq,
                e,
                exc_info=True,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"type-map: {e}",
                failure_category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
            )
        except UnsupportedDialectOperationError as e:
            # The dialect lacks the requested operation (e.g. upsert with
            # no connector package installed). Deterministic — fail fast.
            logger.error(
                "Dialect operation unsupported writing batch "
                "(run=%s, stream=%s, seq=%s): %s",
                run_id,
                stream_id,
                batch_seq,
                e,
                exc_info=True,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"dialect: {e}",
                failure_category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
            )
        except SchemaConfigurationError as e:
            # The stream is misconfigured for this write (e.g. upsert with
            # no conflict_keys). Deterministic — retrying cannot heal it, so
            # fail fatally instead of silently degrading or looping forever.
            logger.error(
                "Write configuration error (run=%s, stream=%s, seq=%s): %s",
                run_id,
                stream_id,
                batch_seq,
                e,
                exc_info=True,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"write-config: {e}",
                failure_category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
            )
        except TlsVerificationError as e:
            # A pool connection opened for this write failed the declared
            # TLS mode's post-connect check: the endpoint is serving
            # plaintext (or was downgraded). Retrying reconnects to the
            # same endpoint — and under an active MITM is exactly wrong —
            # so fail fatally instead of looping.
            logger.error(
                "TLS verification failed writing batch "
                "(run=%s, stream=%s, seq=%s): %s",
                run_id,
                stream_id,
                batch_seq,
                e,
                exc_info=True,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"tls: {e}",
                failure_category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
            )
        except AdbcConfigurationError as e:
            # ADBC misconfiguration cannot heal between attempts; bail
            # fatally so the engine does not retry forever.
            logger.error(
                "ADBC configuration error writing batch "
                "(run=%s, stream=%s, seq=%s): %s",
                run_id,
                stream_id,
                batch_seq,
                e,
                exc_info=True,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"adbc: {e}",
                failure_category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
            )
        except TimeoutError as e:
            return self._timeout_failure(
                state,
                e,
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
            )
        except Exception as e:
            # The transport (ADBC vs SQLAlchemy) decides which driver raised,
            # so name it -- otherwise the two paths log identically (#328).
            logger.error(
                "Error writing batch (run=%s, stream=%s, seq=%s, transport=%s): %s",
                run_id,
                stream_id,
                batch_seq,
                "adbc" if self._adbc_only else "sqlalchemy",
                e,
                exc_info=True,
            )
            return self._classify_unexpected_write_error(e)

    def _classify_unexpected_write_error(self, e: Exception) -> BatchWriteResult:
        """Ack an exception the typed ladder did not claim.

        Declared map first (issue #401): a connector-declared ``error_map``
        fact (SQLSTATE, vendor code, exception class) claims the failure
        deterministically, and the engine-owned verdict table derives the
        ack — connectors declare facts, never verdicts. Only an unclaimed
        exception falls to the class-name heuristic, and that fallback is
        logged: the deterministic PEP-249 classes (ProgrammingError,
        IntegrityError, DataError, NotSupportedError) are fatal — broken
        rendered SQL, a duplicate conflict key inside one source batch, or
        a constraint violation cannot heal between retries against an
        identical request. The ADBC backend already reclassifies them as
        ``AdbcConfigurationError`` at its boundary when no declared fact
        claims the failure (caught earlier in the ladder); SQLAlchemy's
        wrapper exceptions carry the same class names and are claimed
        here. Everything else stays retryable.
        """
        transport = "adbc" if self._adbc_only else "sqlalchemy"
        match = (
            self._error_map.match_exception(e) if self._error_map is not None else None
        )
        if match is not None:
            status, category = DECLARED_WRITE_VERDICTS[match.category]
            logger.info(
                "declared error_map classified the write failure: " "%s %s -> %s (%s)",
                match.family,
                match.identifier,
                match.category,
                type(e).__name__,
            )
            return BatchWriteResult(
                status=status,
                records_written=0,
                failure_summary=(
                    f"{transport}: declared error_map "
                    f"{match.family}:{match.identifier} -> {match.category}: "
                    f"{type(e).__name__}: {e}"
                ),
                failure_category=category,
            )
        logger.info(
            "no declared error_map fact matched %s; falling back to the "
            "class-name heuristic",
            type(e).__name__,
        )
        if _is_fatal_adbc_error(e):
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=(f"{transport}: " f"{type(e).__name__}: {e}"),
                failure_category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
            )
        return BatchWriteResult(
            status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            records_written=0,
            failure_summary=str(e),
            failure_category=FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED,
        )

    async def _truncate_only(self, state: _StreamState) -> None:
        """Empty the target table with no insert (any transport).

        Runs when a refresh's first batch is delivered with zero rows,
        including the synthetic empty batch the engine sends when the
        source yields no batches at all (issue #312). No stage cycle: there
        is nothing to land, so the emptying statement — the dialect's
        ``empty_table_sql``, never ``TRUNCATE`` — runs on its own.
        """
        await self._require_backend().run_ddl(
            [self.dialect.empty_table_sql(state.address)]
        )

    def _prepare_write_batch(
        self, state: _StreamState, record_batch: pa.RecordBatch
    ) -> pa.RecordBatch:
        """Prepare one batch for the stage cycle, entirely in Arrow space.

        Casts to the destination schema, then applies the mode's identity
        semantics before anything lands — the stage feeds set-based
        statements, so intra-batch duplicates must be resolved here:

        * keyless ``insert`` — attach the content-derived ``_record_hash``
          and collapse duplicate hashes, first occurrence wins (#282);
        * keyed ``insert`` — collapse duplicate primary keys, first
          occurrence wins (two rows sharing an identity would both pass
          the anti-join and then collide on the primary key);
        * ``upsert`` — no collapse: duplicate ``conflict_keys`` inside one
          source batch keep the system's own loud failure — a single page
          carrying the same key twice means the declared keys are not
          actually unique, and no collapse rule can be correct;
        * ``truncate_insert`` — no collapse: deduping a full refresh would
          drop legitimate duplicate rows.
        """
        if state.schema_contract is None:
            raise AdbcConfigurationError(
                f"write for {state.address} "
                "requires a configured SchemaContract; schema alignment was skipped"
            )
        cast_batch = state.schema_contract.cast_arrow_batch(record_batch)
        if state.write_mode != "insert":
            return cast_batch
        if self._needs_record_hash(state):
            return self._attach_record_hash_to_batch(cast_batch, state)
        return self._collapse_first_wins(cast_batch, state.primary_keys)

    @staticmethod
    def _collapse_first_wins(
        batch: pa.RecordBatch, key_columns: list[str]
    ) -> pa.RecordBatch:
        """Drop rows whose *key_columns* tuple already appeared in *batch*.

        The insert contract's intra-batch rule: first occurrence wins.
        The key columns are the table's PRIMARY KEY (NOT NULL), so the
        Python ``None == None`` collapse a nullable key could cause here
        is unreachable in practice.
        """
        if not key_columns:
            return batch
        columns = [batch.column(name) for name in key_columns]
        seen: set[tuple[Any, ...]] = set()
        keep: list[int] = []
        for i in range(batch.num_rows):
            key = tuple(col[i].as_py() for col in columns)
            if key in seen:
                continue
            seen.add(key)
            keep.append(i)
        return batch.take(keep) if len(keep) < batch.num_rows else batch

    def _attach_record_hash_to_batch(
        self,
        batch: pa.RecordBatch,
        state: _StreamState,
    ) -> pa.RecordBatch:
        """Append a ``_record_hash`` column and deduplicate an Arrow batch.

        Computes a per-row SHA-256 digest from the JSON-serialized row
        content — the content-hash identity contract (issue #282): the
        digest is a stable function of the cast row's data, so it matches
        across attempts, runs, and transports — appends it as a new
        column, and removes intra-batch duplicate rows (first occurrence
        wins). Intra-batch dedup is necessary because the stage feeds
        set-based statements keyed on ``_record_hash``: two rows sharing
        the key would both pass the anti-join (or raise "multiple source
        rows match" from a MERGE).

        No-op for streams that don't need the synthetic key.
        """
        if not self._needs_record_hash(state):
            return batch
        hashes: list[str] = []
        seen: set[str] = set()
        keep: list[int] = []
        for i in range(batch.num_rows):
            row = {name: batch.column(name)[i].as_py() for name in batch.schema.names}
            canonical = json.dumps(row, sort_keys=True, default=str)
            digest = hashlib.sha256(canonical.encode()).hexdigest()
            if digest not in seen:
                seen.add(digest)
                keep.append(i)
                hashes.append(digest)
        deduped = batch.take(keep) if len(keep) < batch.num_rows else batch
        hash_col = pa.array(hashes, type=pa.string())
        return pa.RecordBatch.from_arrays(
            list(deduped.columns) + [hash_col],
            names=list(deduped.schema.names) + [self.RECORD_HASH_COLUMN],
        )

    async def health_check(self) -> bool:
        """Check database health through the transport backend's probe."""
        if not self._connected or self._backend is None:
            return False
        try:
            await self._backend.health_check()
            return True
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False

    # ==================================================================
    # Readable role (source reads)
    # ==================================================================
    #
    # ``read_batches`` is self-contained: it materializes the runtime it is
    # handed, pages the table, and releases the runtime on exit. No prior
    # ``connect()`` is required — ``runtime`` is the only connection input,
    # matching the ``Readable`` Protocol. The write role's connection state
    # (``self._engine`` / ``self._backend``) is untouched; a source-role
    # instance and a write-role instance are distinct objects.

    async def read_batches(  # skipcq: PY-R1000
        self,
        runtime: ConnectionRuntime,
        config: dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Read upstream rows as Arrow batches typed via the endpoint contract.

        skipcq PY-R1000: the read entry point's complexity predates this
        change (guard rails + two transports in one generator); splitting
        it is its own refactor, not a side effect of catalog addressing.
        """
        endpoint_doc = config.get("endpoint_document")
        if not endpoint_doc:
            raise ReadError(
                "GenericSQLConnector: source config missing 'endpoint_document'"
            )
        database_object = endpoint_doc.get("database_object") or {}
        table_name = database_object.get("name")
        if not table_name:
            raise ReadError("endpoint document missing database_object.name")
        # read_batches is a runtime-taking entry point (no connect()), so
        # the declared capabilities bind here — before the address gate
        # below consults them.
        self._bind_capabilities(runtime)
        # No default schema: dialects without a schema concept (sqlite,
        # duckdb) would emit invalid ``public.<table>`` references if we
        # forced one. When the endpoint omits ``schema``, QueryBuilder emits
        # an unqualified table name and the driver uses the connection's
        # current schema/database. The address normalizes every component
        # once (catalog, schema, AND table) so both transports and the
        # destination resolve the same physical objects; a catalog the
        # system cannot address fails loud here, before any extraction.
        try:
            address = self.dialect.table_address(
                table_name,
                schema=database_object.get("schema") or "",
                catalog=database_object.get("catalog") or "",
            )
        except CatalogAddressingError as err:
            raise ReadError(str(err)) from err

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
                    address=address,
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
                        schema_name=address.schema or None,
                        table_name=address.table,
                        catalog_name=address.catalog or None,
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
        address: TableAddress,
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

        # The ADBC path quotes every identifier; *address* components were
        # normalized once at construction (the same rule the destination
        # handler applies), so the quoted names target the same physical
        # objects the destination resolves.

        order_by = _page_order_by(order_by_field, cursor_field, columns, address.table)

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
                        schema_name=address.schema or None,
                        table_name=address.table,
                        catalog_name=address.catalog or None,
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

    async def list_schemas(
        self, runtime: ConnectionRuntime, *, catalog: str = ""
    ) -> list[str]:
        self._bind_capabilities(runtime)
        return await _sql_list_schemas(runtime, dialect=self.dialect, catalog=catalog)

    async def list_tables(
        self, runtime: ConnectionRuntime, schema: str, *, catalog: str = ""
    ) -> list[str]:
        self._bind_capabilities(runtime)
        return await _sql_list_tables(
            runtime, schema, dialect=self.dialect, catalog=catalog
        )

    async def list_columns(
        self, runtime: ConnectionRuntime, schema: str, table: str, *, catalog: str = ""
    ) -> tuple[list[ColumnDef], list[str]]:
        self._bind_capabilities(runtime)
        return await _sql_list_columns(
            runtime, schema, table, dialect=self.dialect, catalog=catalog
        )

    async def create_table(
        self,
        runtime: ConnectionRuntime,
        schema: str,
        table: str,
        columns: list[ColumnDef],
        primary_keys: list[str],
        *,
        catalog: str = "",
    ) -> None:
        self._bind_capabilities(runtime)
        await _sql_create_table(
            runtime,
            schema,
            table,
            columns,
            primary_keys,
            dialect=self.dialect,
            catalog=catalog,
        )
