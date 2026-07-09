"""CDK-native value types — zero protobuf / grpc / engine dependency.

These replace the gRPC generated messages (``AckStatus`` / ``Cursor`` /
``SchemaMessage``) in the read/write contracts so the CDK stays
transport-neutral: it must not import ``src/grpc`` (ADR §4.1). The engine's
gRPC server (``src/destination/server.py``) translates protobuf <-> these
types at the wire boundary. The ``AckStatus`` integer values are aligned 1:1
with ``proto/analitiq/v1/stream.proto`` so that translation is mostly identity.
Being IntEnums, members compare equal across enums when their values match
(``AckStatus.ACK_STATUS_SUCCESS == WriteMode.WRITE_MODE_INSERT``) — an
accepted trade-off of the proto alignment; never compare members of
different enums.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import Any, Protocol, runtime_checkable


class AckStatus(IntEnum):
    """Batch acknowledgment status.

    Integer values mirror the ``AckStatus`` enum in ``stream.proto`` exactly,
    so the engine can pass a value straight into a protobuf ``BatchAck`` (and
    vice versa) without a lookup table.
    """

    ACK_STATUS_UNSPECIFIED = 0
    ACK_STATUS_SUCCESS = 1  # All records written, cursor advanced
    ACK_STATUS_ALREADY_COMMITTED = 2  # Idempotent replay, batch already committed
    ACK_STATUS_RETRYABLE_FAILURE = 3  # No commit occurred, safe to retry whole batch
    ACK_STATUS_FATAL_FAILURE = 4  # No commit occurred, do not retry, send to DLQ


class WriteMode(IntEnum):
    """Destination write mode for a stream.

    Integer values mirror the ``WriteMode`` enum in ``stream.proto`` exactly, so
    the engine translates the wire value with ``WriteMode(msg.write_mode)`` and
    the value still reads as its integer when a handler keys a lookup table by
    the raw proto int.
    """

    WRITE_MODE_UNSPECIFIED = 0
    WRITE_MODE_INSERT = 1  # Insert only, fail on conflict
    WRITE_MODE_UPSERT = 2  # Upsert (insert or update on conflict)
    WRITE_MODE_TRUNCATE_INSERT = 3  # Truncate table before insert (full refresh)


class RetrySemantics(IntEnum):
    """Retry safety a destination guarantees on a same-run restart (#286).

    Integer values mirror the ``RetrySemantics`` enum in ``stream.proto``
    exactly, so the servicer passes the value straight into a protobuf
    ``SchemaAck`` without a lookup table.
    """

    RETRY_SEMANTICS_UNSPECIFIED = 0
    # The handler dedups re-sent records by row identity; a restart can
    # neither duplicate nor drop.
    RETRY_SEMANTICS_EXACTLY_ONCE = 1
    # A restart is not replay-safe: committed records are re-applied, or
    # re-batched rows are misclassified (skipped / truncated away). The
    # verdict's reason names the concrete failure mode.
    RETRY_SEMANTICS_AT_LEAST_ONCE = 2


@dataclass(frozen=True)
class RetryVerdict:
    """A handler's retry-safety verdict for one configured stream.

    ``reason`` names the mechanism behind the verdict (the dedup key, the
    manifest, the declared idempotency key, or the gap) so the engine's
    per-stream log line is actionable, not just a label. A verdict must
    commit to exactly-once or at-least-once: UNSPECIFIED is the wire's
    absent value, and letting a handler construct it would silently
    degrade into the base default downstream instead of failing at the
    defective handler.
    """

    semantics: RetrySemantics
    reason: str

    def __post_init__(self) -> None:
        if self.semantics == RetrySemantics.RETRY_SEMANTICS_UNSPECIFIED:
            raise ValueError(
                "RetryVerdict requires exactly-once or at-least-once; "
                "a handler must never claim UNSPECIFIED"
            )


@dataclass(frozen=True)
class Cursor:
    """Opaque checkpoint cursor.

    Produced by the engine, stored and returned verbatim by the destination —
    the destination never interprets ``token``. Mirrors the gRPC ``Cursor``
    message (a single ``bytes token``).
    """

    token: bytes = b""


@dataclass(frozen=True)
class SchemaSpec:
    """Per-stream schema identification, sent once at stream start.

    The CDK-native stand-in for the gRPC ``SchemaMessage``. Carries only the
    fields the wire message does: the destination looks up the full contract
    endpoint document (columns, primary keys, target table) by ``stream_id``
    from configuration it already loaded, not from this object.
    """

    stream_id: str
    version: int
    write_mode: WriteMode
    # The sender's gRPC ack budget (seconds) stamped into the handshake. The
    # destination servicer derives the per-statement timeout from it before
    # configure_schema runs, and the worker proxy forwards it to the connector
    # worker, so the statement bound always tracks the budget the engine
    # actually waits on (issue #234).
    ack_timeout_seconds: int


# Statuses that count as a successful (committed) batch. Public: the
# worker proxy uses it to police the same invariant on acks crossing the
# process boundary, so ``BatchWriteResult.success`` and the proxy cannot
# drift.
SUCCESS_STATUSES = frozenset(
    {AckStatus.ACK_STATUS_SUCCESS, AckStatus.ACK_STATUS_ALREADY_COMMITTED}
)


@dataclass(frozen=True)
class BatchWriteResult:
    """Immutable result of writing a batch to the destination.

    ``success`` is a derived property: a result is successful when its
    ``status`` is SUCCESS or ALREADY_COMMITTED. Modeling it as a property
    (instead of a constructor argument) makes status the single source of
    truth — callers cannot construct an inconsistent result.

    A failure result must not carry a ``committed_cursor``: the engine
    persists the cursor as the stream checkpoint, so a cursor on a failed
    batch would advance the checkpoint past records that were never
    written. ``__post_init__`` rejects the combination at construction.
    """

    status: AckStatus
    records_written: int
    committed_cursor: Cursor | None = None
    failed_record_ids: tuple[str, ...] = ()
    failure_summary: str = ""

    def __post_init__(self) -> None:
        if self.records_written < 0:
            raise ValueError(
                f"records_written must be non-negative, got {self.records_written}"
            )
        if self.committed_cursor is not None and not self.success:
            raise ValueError(
                f"committed_cursor must be None on a failure result "
                f"(status={self.status!r}); a failed batch must never "
                f"advance the checkpoint"
            )
        # Accept any iterable but store a tuple, so the frozen result is
        # immutable all the way down (a list binding would still allow
        # in-place mutation).
        object.__setattr__(self, "failed_record_ids", tuple(self.failed_record_ids))

    @property
    def success(self) -> bool:
        return self.status in SUCCESS_STATUSES


class EndpointScope(StrEnum):
    """Whether an endpoint reference is scoped to a connector or a connection.

    The value read off the engine's ``EndpointRef.scope``. Constructing
    ``EndpointScope(value)`` raises ``ValueError`` on an unknown value, which
    preserves the scope validation the engine gets from the published contract
    (``validate_endpoint_ref``) engine-side. The CDK takes this enum (not the
    engine model) so it never imports ``src/models``.
    """

    CONNECTOR = "connector"
    CONNECTION = "connection"


@runtime_checkable
class CheckpointStore(Protocol):
    """Read-path checkpoint/cursor seam (dependency inversion).

    The CDK declares this Protocol; the engine's ``StateManager`` satisfies it.
    Provisional: the incremental source read path is extracted in a later phase,
    and the final method set (the API source path also persists high-water marks
    / page state) is settled then. Declared now so the ``Readable`` contract is
    boundary-clean without the CDK reaching for engine state.
    """

    async def get_cursor(
        self, stream_name: str, partition: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        ...

    async def save_cursor(
        self,
        stream_name: str,
        partition: dict[str, Any] | None,
        cursor: dict[str, Any],
    ) -> None:
        ...
