"""Abstract base class for destination handlers.

Destination handlers implement the logic for writing data to specific
destination types (PostgreSQL, MySQL, APIs, etc.). The gRPC server
delegates all data operations to these handlers.
"""

import errno
import inspect
import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from .types import (
    AckStatus,
    BatchWriteResult,
    Cursor,
    FailureCategory,
    RetrySemantics,
    RetryVerdict,
    SchemaSpec,
    WriteMode,
)

if TYPE_CHECKING:
    from .connection_runtime import ConnectionRuntime

# ``BatchWriteResult``, ``Cursor`` and ``SchemaSpec`` are CDK-native value types
# (``cdk.types``), decoupled from the gRPC messages this module once imported —
# the CDK must not depend on ``src/grpc`` (ADR §4.1). The engine's gRPC server
# translates protobuf <-> these types at the wire boundary. ``BatchWriteResult``
# is re-exported here because handlers/tests import it as
# ``from cdk.base_handler import BaseDestinationHandler, BatchWriteResult``.
__all__ = [
    "BaseDestinationHandler",
    "BatchRejected",
    "BatchWriteResult",
    "LandingBatch",
    "os_error_verdict",
    "reject_batch",
]


@dataclass(frozen=True)
class LandingBatch:
    """One batch, past every check the base makes on every sink's behalf.

    Carries the batch both ways round: :attr:`records` for sinks that write
    dicts (file, stdout, HTTP bodies) and :attr:`record_batch` for sinks
    that stay Arrow-native (the SQL write path casts and binds columnar, and
    the worker proxy forwards the Arrow bytes untouched).

    :attr:`records` materialises on first read and is cached, so the
    conversion happens exactly once for a sink that wants it and not at all
    for one that does not -- an Arrow-native sink is not taxed for a
    representation it never looks at.
    """

    run_id: str
    stream_id: str
    batch_seq: int
    record_batch: pa.RecordBatch
    record_ids: list[str]
    cursor: Cursor
    emitted_at: datetime

    @cached_property
    def records(self) -> list[dict[str, Any]]:
        """The batch as dicts, converted once on first read."""
        return list(self.record_batch.to_pylist())


class BatchRejected(Exception):
    """A sink refusing a batch for a reason it can name.

    Raised out of :meth:`BaseDestinationHandler.land` so a sink states the
    refusal once, in the place that detected it, rather than assembling a
    result and threading it back out. The base turns it into the verdict.

    Fatal and destination-owned by default, which is the common case: a
    contract violation the sink detected in what it was handed. A sink that
    means something else -- a transient refusal worth retrying, a defect it
    knows is the configuration's -- says so explicitly.
    """

    def __init__(
        self,
        summary: str,
        *,
        status: AckStatus = AckStatus.ACK_STATUS_FATAL_FAILURE,
        category: FailureCategory = FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED,
        records_written: int = 0,
        failed_record_ids: tuple[str, ...] = (),
    ) -> None:
        super().__init__(summary)
        self.summary = summary
        self.status = status
        self.category = category
        # A sink that lands rows one request at a time can fail partway.
        # Those rows are written, so the count travels with the refusal:
        # the engine must not retry the whole batch and duplicate them, and
        # the failed ids let the DLQ carry exactly what did not land.
        self.records_written = records_written
        self.failed_record_ids = failed_record_ids


# Errno values a retry cannot clear, for every sink that writes through a file
# descriptor. One table, the union of what the file and stdout sinks listed
# separately: they were never two policies, only two incomplete lists. EPIPE
# cannot occur writing to a file and EROFS cannot occur writing to stdout, so
# listing them costs those sinks nothing, and nobody ever decided that EROFS
# was retryable on stdout.
#
# Anything unlisted is retryable. That direction is deliberate: an unknown
# errno retried a bounded number of times costs a delay, while an unknown
# errno treated as fatal dead-letters a batch the sink might well have taken
# on the next attempt.
_FATAL_ERRNOS = frozenset(
    {
        errno.EPIPE,  # the reader closed: nothing downstream is listening
        errno.ENOSPC,  # the volume is full
        errno.EACCES,  # the process may not write here
        errno.EROFS,  # the volume is read-only
        errno.EDQUOT,  # the quota is exhausted
        errno.EBADF,  # writing to a descriptor we already closed
    }
)


def os_error_verdict(
    logger: logging.Logger,
    error: OSError,
    *,
    run_id: str,
    stream_id: str,
    batch_seq: int,
    what: str,
) -> BatchWriteResult:
    """Judge an ``OSError`` raised while writing a batch, and log it.

    *what* names the thing being written ("batch", "stdout") so the log line
    says where the failure happened without each sink re-deciding the rest of
    the wording.

    EBADF is the one fatal errno that is not the destination's fault: a
    descriptor this process already closed is our bug, and saying so keeps it
    from impersonating a full disk or a permissions problem the operator
    would go looking for.
    """
    label = (
        errno.errorcode.get(error.errno, str(error.errno))
        if error.errno is not None
        else "unknown"
    )
    fatal = error.errno in _FATAL_ERRNOS
    status = (
        AckStatus.ACK_STATUS_FATAL_FAILURE
        if fatal
        else AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    )
    category = (
        FailureCategory.FAILURE_CATEGORY_INTERNAL
        if error.errno == errno.EBADF
        else FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED
    )
    logger.error(
        "%s I/O error writing %s (run=%s, stream=%s, seq=%s, errno=%s): %s",
        "Fatal" if fatal else "Retryable",
        what,
        run_id,
        stream_id,
        batch_seq,
        label,
        error,
        exc_info=True,
    )
    return BatchWriteResult(
        status=status,
        records_written=0,
        failure_summary=f"OSError[{label}]: {error}",
        failure_category=category,
    )


def reject_batch(
    logger: logging.Logger,
    reason: str,
    *,
    run_id: str,
    stream_id: str,
    batch_seq: int,
    status: AckStatus = AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
) -> BatchWriteResult:
    """Log and build the result for a batch rejected before any write.

    A ``write_batch`` pre-flight guard rejects without raising, so the
    rejecting process -- the destination -- recorded nothing at all. The
    reason only ever surfaced one hop away, in the engine's retry warning
    (``src/engine/engine.py``), which names neither the handler nor which
    guard fired. Logging here puts the rejection in the log of the process
    that made it (issue #327).

    Routing every guard through one function keeps the log and the returned
    status in step, and means a new guard cannot be added without one. It
    also stamps the NOT_READY failure category once for every guard: a
    pre-flight rejection by definition attempted nothing, and the engine
    must be able to tell that apart from a write the destination actually
    rejected (issue #351).

    *logger* is the calling handler's module logger, so the record names the
    handler that rejected the batch.
    """
    logger.error(
        "write_batch rejected: %s (run=%s, stream=%s, seq=%s)",
        reason,
        run_id,
        stream_id,
        batch_seq,
    )
    return BatchWriteResult(
        status=status,
        records_written=0,
        failure_summary=reason,
        failure_category=FailureCategory.FAILURE_CATEGORY_NOT_READY,
    )


class BaseDestinationHandler(ABC):
    """
    Abstract base class for all destination handlers.

    Implementations must handle:
    1. Connection management (connect/disconnect)
    2. Schema configuration (auto-create tables, indexes, etc.)
    3. Batch writing with idempotency
    4. Cursor storage for checkpointing

    Idempotency Contract:
    - A retried or replayed batch must not duplicate or drop rows; the
      mechanism is the handler's (a SQL handler dedups on the write mode's
      keys -- MERGE on conflict_keys, or the synthetic _record_hash for a
      keyless insert; a file handler content-addresses each batch file so
      a replay overwrites the same bytes)
    - A handler that detects a prior commit may return ALREADY_COMMITTED; a
      sink that writes idempotently returns SUCCESS. The engine advances the
      checkpoint only from a SUCCESS ack, so a cursor on any other status is
      ignored
    - All writes within a batch must be atomic (all-or-nothing)
    """

    #: Why the most recent ``configure_schema`` returned ``False``. The
    #: servicer puts it on the rejected ``SchemaAck`` so the engine reports
    #: the real reason instead of a generic "Schema configuration failed".
    #: Contract members rather than attributes the servicer probes for: a
    #: handler that returns ``False`` without setting them is answering
    #: "no" without saying why, which is a defect the interface should
    #: make visible rather than paper over with a default.
    last_schema_rejection: str | None = None

    #: Who owns that rejection. UNSPECIFIED resolves from the handshake
    #: outcome the engine observed itself, so a handler that knows better
    #: -- a shell that could not reach its connector worker -- says so.
    last_schema_failure_category: FailureCategory = (
        FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
    )

    def set_endpoint_refs(self, endpoint_refs: Mapping[str, Any]) -> None:
        """Register the ``stream_id → endpoint_ref`` index for this handler.

        ``endpoint_refs`` values are dict-shape ``EndpointRef`` payloads
        (``{"scope", "connection_id", "endpoint_id"}`` plus optional ``x-*``
        extension keys). Called once by the
        destination entrypoint before the gRPC server starts. The default
        implementation is a no-op; handlers that need per-stream endpoint
        context (e.g. picking a type-mapper by scope) override it.
        """
        _ = endpoint_refs  # no-op default

    def set_stream_endpoints(
        self, stream_endpoints: Mapping[str, Mapping[str, Any]]
    ) -> None:
        """Register the ``stream_id → contract endpoint document`` index.

        The destination loads its configuration with the same
        ``PipelineConfigPrep`` as the engine, so the contract endpoint
        document (database object, columns, primary keys, API operations,
        …) is already on disk by the time ``configure_schema`` fires for
        an incoming :class:`~cdk.types.SchemaSpec`. Handlers read from this
        map instead of unpacking the spec.

        Called once by the destination entrypoint before the gRPC server
        starts. Default is a no-op; handlers that need the document
        override it.
        """
        _ = stream_endpoints  # no-op default

    # Template method: the no-op default ignores self so handlers can
    # override with instance state (same shape as its siblings above).
    def set_stream_conflict_keys(  # skipcq: PYL-R0201
        self, stream_conflict_keys: Mapping[str, list[str]]
    ) -> None:
        """Register the ``stream_id → upsert conflict keys`` index.

        The conflict keys are the stream's validated ``write.conflict_keys``,
        copied verbatim by the destination entrypoint. They are stream
        configuration, not part of the contract endpoint document, so they
        travel on their own channel instead of being smuggled into the
        document map. An absent or empty entry means INSERT mode — an
        upsert always carries an explicit conflict target under the
        contract.

        Called once by the destination entrypoint before the gRPC server
        starts. Default is a no-op; handlers that upsert override it.
        """
        _ = stream_conflict_keys  # no-op default

    def set_statement_timeout(self, seconds: float | None) -> None:
        """Bound each destination statement to *seconds*.

        Cancelling a statement that blocks lets the engine surface the real
        reason instead of a bare gRPC ACK timeout (issue #231).
        Called by the destination servicer on every schema handshake, before
        ``configure_schema``, with a value derived from the ack budget the
        sender stamped into the schema message (issue #234) — the bound
        always stays at or below the budget of every waiter on the path.
        ``None`` means unbounded. Default is a no-op; only SQL destinations
        honor it.
        """
        _ = seconds  # no-op default

    @abstractmethod
    async def connect(self, runtime: "ConnectionRuntime") -> None:
        """
        Establish connection to the destination.

        Args:
            runtime: ConnectionRuntime that manages connection lifecycle

        Raises:
            ConnectionError: If connection cannot be established
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Close connection to the destination.

        Should be idempotent - safe to call multiple times.
        """
        pass

    async def finalize_run(self, *, succeeded: bool) -> None:
        """Release run-scoped state at the end of a run.

        Invoked from the destination server's ``Shutdown`` handler while the
        handler is still connected. The worker process is torn down (SIGTERM)
        before ``disconnect`` could run connection-bound cleanup, so anything
        needing the live connection at end-of-run belongs here. Default is a
        no-op; handlers with run-scoped state to release (e.g. a connector
        flushing a per-run buffer or temporary object) override it.

        ``succeeded`` is the engine's terminal-run outcome: ``True`` only when
        the pipeline finished successfully. Cleanup that would break a resume
        of a failed run (e.g. pruning the idempotency ledger) must run only
        when ``succeeded`` is ``True``. Best-effort: the server logs and
        swallows any error so teardown never fails.
        """
        return None

    @abstractmethod
    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        """
        Configure destination schema based on the SchemaSpec.

        This may involve:
        - Creating schemas/databases
        - Creating tables
        - Creating indexes
        - Setting up constraints

        Args:
            schema_spec: Schema identification from engine (CDK-native)

        Returns:
            True if schema configuration succeeded, False otherwise
        """
        pass

    def retry_semantics(self, stream_id: str) -> RetryVerdict:
        """Retry-safety verdict for a stream this handler has configured.

        Called by the destination servicer after an accepted
        ``configure_schema``; the verdict rides the SchemaAck so the engine
        can log, per stream, whether a same-run restart re-sends committed
        records (issue #286). The verdict is the handler's to make — write
        mode, keys, transport, and declared idempotency all factor in — so
        handlers override this per stream. The default is the only honest
        claim for a handler that declares nothing: at-least-once.
        """
        _ = stream_id
        return RetryVerdict(
            semantics=RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE,
            reason=(
                "handler declares no retry-safety; a same-run restart may "
                "re-send already-committed records"
            ),
        )

    # skipcq: PYL-R0201 - an overridable hook, not a utility. Every real
    # sink answers from its own instance state; the base's neutral answer
    # reads no attribute, and making one implementation of a hook static
    # would hide that it is an override.
    def not_ready_reason(self, stream_id: str) -> str | None:  # skipcq: PYL-R0201
        """Why this handler cannot take a batch for *stream_id* right now.

        Returns ``None`` when it can. Every sink asked the same question
        before its first write and each spelled the answer differently, so
        the check moves here and each sink answers only for its own state.
        A rejection here attempted nothing, which is what lets
        :func:`reject_batch` stamp NOT_READY for all of them at once.
        """
        _ = stream_id
        return None

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
        """Write a batch of records to the destination.

        This is the shared preamble every sink used to copy: the readiness
        guard, the empty-batch success that still advances the cursor, the
        one materialisation of the Arrow batch, and the mapping from a
        raised failure to a verdict. What is left -- putting the records in
        the sink -- is :meth:`land`.

        A sink with genuinely different framing (a proxy that forwards the
        Arrow batch untouched, one that answers ALREADY_COMMITTED from its
        own ledger) overrides this method instead; the base is the common
        shape, not a cage.

        Idempotency Requirements:
        - A retried/replayed batch must not duplicate or drop rows; dedup on
          row identity (the write mode's keys, or the synthetic _record_hash
          for a keyless insert), never the batch's position
        - Write records atomically (all-or-nothing)

        Args:
            run_id: Unique pipeline run identifier
            stream_id: Stream identifier
            batch_seq: Monotonically increasing batch sequence number
            record_batch: Records as a ``pa.RecordBatch``. Arrow IPC is
                the only supported wire format.
            record_ids: Stable identifiers for each record (for DLQ correlation)
            cursor: Opaque cursor representing max watermark in batch
            emitted_at: Timezone-aware UTC instant the engine emitted this
                batch, stamped once and identical across every retry of the
                same batch. A destination that renders time-based output
                paths (file/S3 partitioning) MUST derive the path from this
                value rather than its own wall clock, so a replayed batch
                resolves the same path instead of drifting across an
                hour/day boundary (issue #353). Sinks without time-based
                partitioning ignore it.

        Returns:
            BatchWriteResult with status, records written, and cursor
        """
        reason = self.not_ready_reason(stream_id)  # skipcq: PYL-E1128
        if reason is not None:
            return reject_batch(
                self._logger,
                reason,
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
            )
        batch = LandingBatch(
            run_id=run_id,
            stream_id=stream_id,
            batch_seq=batch_seq,
            record_batch=record_batch,
            record_ids=record_ids,
            cursor=cursor,
            emitted_at=emitted_at,
        )
        try:
            # Inside the try: a batch this process cannot even materialise is
            # a failed write, not an exception escaping the contract.
            if record_batch.num_rows == 0:
                # Nothing to write, so nothing can fail. The cursor still
                # advances: the engine read this far and found no rows, and
                # withholding the checkpoint would re-read the same empty
                # range forever.
                return await self.land_empty(batch)
            written = await self.land(batch)
        except BatchRejected as rejected:
            # The sink names the defect; the base says which batch it was.
            # Stamping the context here rather than in each raise means the
            # ack and the DLQ entry identify the batch whichever sink
            # refused it, and no sink can forget to.
            summary = (
                f"{rejected.summary} "
                f"(run={run_id}, stream={stream_id}, seq={batch_seq})"
            )
            self._logger.error(summary, exc_info=True)
            return BatchWriteResult(
                status=rejected.status,
                records_written=rejected.records_written,
                failed_record_ids=rejected.failed_record_ids,
                failure_summary=summary,
                failure_category=rejected.category,
            )
        except OSError as error:
            return self.os_error_failure(
                error, run_id=run_id, stream_id=stream_id, batch_seq=batch_seq
            )
        except Exception as error:  # noqa: BLE001 - mapped to a verdict below
            return self.unexpected_write_failure(
                error, run_id=run_id, stream_id=stream_id, batch_seq=batch_seq
            )
        return BatchWriteResult(
            status=AckStatus.ACK_STATUS_SUCCESS,
            records_written=written,
            committed_cursor=cursor,
        )

    # skipcq: PYL-R0201 - an overridable hook (the SQL sink's override
    # reads instance state to truncate); the neutral answer needs none.
    async def land_empty(
        self, batch: "LandingBatch"
    ) -> BatchWriteResult:  # skipcq: PYL-R0201
        """Answer a batch that carried no records.

        Success with the cursor, for every sink that has nothing to do. A
        sink with a per-batch side effect that must happen even when the
        batch is empty -- a full refresh whose truncate is keyed to the
        first batch -- overrides this.
        """
        return BatchWriteResult(
            status=AckStatus.ACK_STATUS_SUCCESS,
            records_written=0,
            committed_cursor=batch.cursor,
        )

    async def land(self, batch: "LandingBatch") -> int:
        """Put ``batch``'s records in the sink; return how many landed.

        Called only for a ready handler and a non-empty batch, so an
        implementation opens on its own distinctive work. Raise
        :class:`BatchRejected` to refuse the batch with a reason and a
        category of your choosing; an ``OSError`` is judged by the shared
        errno table, and anything else becomes a fatal verdict naming the
        exception type. Returning normally is the success path.

        Implement this *or* :meth:`write_batch`, not neither --
        ``__init_subclass__`` refuses a concrete handler that implements
        neither, so the gap is a class-definition error rather than a
        first-batch one.
        """
        raise NotImplementedError(
            f"{type(self).__name__} implements neither land() nor "
            f"write_batch(); a destination handler must provide one"
        )

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Refuse a concrete handler that can neither land nor write.

        The two are alternatives, so neither can be ``@abstractmethod`` and
        ABC cannot enforce the choice. Checking here keeps the failure at
        import, where a missing method is obvious, rather than at the first
        batch of a production run.
        """
        super().__init_subclass__(**kwargs)
        if inspect.isabstract(cls):
            return
        implements_land = cls.land is not BaseDestinationHandler.land
        implements_write = cls.write_batch is not BaseDestinationHandler.write_batch
        if not (implements_land or implements_write):
            raise TypeError(
                f"{cls.__name__} implements neither land() nor write_batch(); "
                f"a destination handler must provide one"
            )

    #: What this sink writes, for the I/O failure log line. A file sink
    #: writes a "batch"; stdout writes "to stdout".
    write_target: str = "batch"

    def os_error_failure(
        self,
        error: OSError,
        *,
        run_id: str,
        stream_id: str,
        batch_seq: int,
    ) -> BatchWriteResult:
        """Judge an ``OSError`` raised while landing a batch.

        A hook rather than a fixed branch because ``OSError`` is a wider net
        than it looks: ``asyncio.TimeoutError`` is a builtin ``TimeoutError``
        on Python 3.11, and aiohttp's connection failures derive from
        ``OSError`` too. A sink whose transport raises those must judge them
        by its own declared taxonomy, not by an errno table written for file
        descriptors -- catching them here would retry a declared config
        defect to exhaustion and turn a mid-request disconnect into a
        dead-lettered batch.
        """
        return os_error_verdict(
            self._logger,
            error,
            run_id=run_id,
            stream_id=stream_id,
            batch_seq=batch_seq,
            what=self.write_target,
        )

    def unexpected_write_failure(
        self,
        error: Exception,
        *,
        run_id: str,
        stream_id: str,
        batch_seq: int,
    ) -> BatchWriteResult:
        """Judge an exception :meth:`land` did not anticipate.

        Fatal by default: an exception nobody declared is a defect, and
        retrying a defect burns the batch's budget to reach the same place.
        A sink whose driver raises a classifiable error tree overrides this
        to consult its declared error map first.
        """
        self._logger.error(
            "Fatal error writing batch (run=%s, stream=%s, seq=%s): %s",
            run_id,
            stream_id,
            batch_seq,
            error,
            exc_info=True,
        )
        return BatchWriteResult(
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            records_written=0,
            failure_summary=f"{type(error).__name__}: {error}",
        )

    @property
    def _logger(self) -> logging.Logger:
        """The implementing module's logger, so a record names its handler."""
        return logging.getLogger(type(self).__module__)

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if destination is healthy and ready to receive data.

        Returns:
            True if healthy, False otherwise
        """
        pass

    @property
    def forwarded_capabilities(self) -> Any | None:
        """A capability declaration this handler forwards rather than makes.

        A handler that proxies to a connector worker already holds the
        worker's own advertisement; returning it here lets every capability
        below read from that one object instead of the proxy re-stating each
        as its own property. Nine such mirrors existed, and each was a place
        for the shell's answer to drift from the worker's.

        ``None`` for a handler that answers for itself, which is every
        in-process one.
        """
        return None

    @property
    def forwards_capabilities(self) -> bool:
        """Whether this handler relays another process's advertisement.

        A forwarding handler with nothing to relay yet advertises nothing:
        the neutral defaults would have it claim, before it has even reached
        its worker, capabilities the worker may not have. A handler that
        answers for itself keeps its own defaults.
        """
        return False

    def _declared(self, name: str, fallback: bool) -> bool:
        """Read one advertised flag, falling back when nothing is declared."""
        declared = self.forwarded_capabilities
        if declared is None:
            return False if self.forwards_capabilities else fallback
        return bool(getattr(declared, name, False))

    def _declares_write_mode(self, mode: int, fallback: bool) -> bool:
        """Whether the relayed advertisement lists ``mode`` as writable.

        Write modes are advertised as a list rather than a flag apiece, so
        they are read by membership -- the one place that knows the list is
        the list itself (issue #388).
        """
        declared = self.forwarded_capabilities
        if declared is None:
            return False if self.forwards_capabilities else fallback
        return mode in getattr(declared, "supported_write_modes", ())

    @property
    @abstractmethod
    def connector_type(self) -> str:
        """Return the connector type identifier (e.g., 'postgresql', 'mysql')."""
        pass

    @property
    def supports_transactions(self) -> bool:
        """Whether this destination supports transactions."""
        return self._declared("supports_transactions", True)

    @property
    def supports_insert(self) -> bool:
        """Whether this destination can run plain-insert writes right now.

        True for every handler that can write at all; a handler whose
        insert path has runtime preconditions (the SQL stage-then-merge
        cycle needs declared capabilities and a stage-rendering dialect)
        overrides this so GetCapabilities never advertises a mode every
        stream of which would be refused at the schema handshake.
        """
        return self._declares_write_mode(WriteMode.WRITE_MODE_INSERT, True)

    @property
    def supports_upsert(self) -> bool:
        """Whether this destination supports upsert operations."""
        return self._declared("supports_upsert", True)

    @property
    def supports_bulk_load(self) -> bool:
        """Whether this destination supports bulk loading (COPY, LOAD DATA, etc.)."""
        return self._declared("supports_bulk_load", False)

    @property
    def supports_auto_create(self) -> bool:
        """Whether this destination can create the target relation if absent.

        Auto-create is a relational/DDL capability; the neutral base cannot
        assume it (an API, file, or stdout destination has no schema to
        create), so it defaults False. A SQL handler that issues CREATE TABLE
        overrides this to True. Advertised capability must follow what the
        handler can actually do, never a constructor literal.
        """
        return self._declared("supports_auto_create", False)

    @property
    def supports_truncate(self) -> bool:
        """Report whether the truncate-insert (full-refresh) mode is supported.

        Only a destination that can truncate the target before insert
        advertises ``WRITE_MODE_TRUNCATE_INSERT``; the neutral base cannot, so
        it defaults False. A SQL handler that can empty the target before the
        insert overrides this to True.
        """
        return self._declares_write_mode(WriteMode.WRITE_MODE_TRUNCATE_INSERT, False)

    def _declared_size(self, name: str, fallback: int) -> int:
        """Read an advertised size, ignoring an unset (zero) declaration."""
        declared = self.forwarded_capabilities
        if declared is None:
            return fallback
        value = getattr(declared, name, 0)
        return int(value) if value else fallback

    @property
    def max_batch_size(self) -> int:
        """Maximum recommended batch size for this destination."""
        return self._declared_size("max_batch_size", 5000)

    @property
    def max_batch_bytes(self) -> int:
        """Maximum recommended batch size in bytes."""
        return self._declared_size("max_batch_bytes", 8 * 1024 * 1024)
