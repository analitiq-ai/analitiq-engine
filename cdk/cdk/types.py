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
    # No commit occurred, do not retry: the stream fails whatever the error
    # strategy, and only `dlq` writes the batch out on the way (issue #428).
    ACK_STATUS_FATAL_FAILURE = 4


class FailureCategory(IntEnum):
    """Machine-readable category of a failed batch write (issue #351).

    Answers "who owns the fix" beside ``failure_summary``'s "what went
    wrong", so the engine maps a failure to a customer-facing error code
    without parsing summary text. Integer values mirror the
    ``FailureCategory`` enum in ``stream.proto`` exactly, so the engine
    passes a value straight into a protobuf ``BatchAck`` without a lookup
    table.

    Engine-owned vocabulary: connectors are not asked to self-classify.
    The CDK base classes and engine-owned handler code set it; a thick
    connector that overrides ``write_batch`` leaves it UNSPECIFIED and
    the engine falls back to summary matching. Off the wire the value is
    range-checked (an unrecognised integer degrades to UNSPECIFIED); an
    in-range value is used as declared.
    """

    FAILURE_CATEGORY_UNSPECIFIED = 0  # Nothing declared; engine matches summary text
    FAILURE_CATEGORY_CONFIG_DEFECT = 1  # Deterministic, user-fixable config defect
    FAILURE_CATEGORY_WRITE_REJECTED = 2  # Write attempted and failed at the destination
    FAILURE_CATEGORY_NOT_READY = 3  # Nothing attempted: not connected / not configured


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
    declared idempotency key, or the gap) so the engine's
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


# Statuses that count as a successful (committed) batch.
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
    written. The rule is not enforced here -- raising inside a connector's
    own result object aborts the stream and loses the verdict with it.
    The engine reads the combination off the ack and answers it with a
    connector-contract-violation verdict naming the connector, which is
    what makes a defective connector visible instead of silently repaired
    (issue #428, decision 1.2).

    ``failure_category`` is the machine-readable channel beside
    ``failure_summary`` (issue #351): the site that builds a failure result
    knows the caught exception type, so it declares the category here
    instead of the engine re-deriving it from summary text seven hops
    later. Read only on failure results; UNSPECIFIED means the engine
    falls back to summary matching.
    """

    # `int` as well as the enum, deliberately. proto3 enums are open, so a
    # newer or untrusted connector can ack with a value this build has no
    # member for. Narrowing it here would raise at the boundary and abort
    # the stream; carried through, it reaches the one place that classifies
    # a status and is answered by the policy's unknown-status verdict
    # (issue #428, decision 1.3). Known values still arrive as the enum.
    status: AckStatus | int
    records_written: int
    committed_cursor: Cursor | None = None
    failed_record_ids: tuple[str, ...] = ()
    failure_summary: str = ""
    failure_category: FailureCategory = FailureCategory.FAILURE_CATEGORY_UNSPECIFIED

    def __post_init__(self) -> None:
        if self.records_written < 0:
            raise ValueError(
                f"records_written must be non-negative, got {self.records_written}"
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
