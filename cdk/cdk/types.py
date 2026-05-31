"""CDK-native value types — zero protobuf / grpc / engine dependency.

These replace the gRPC generated messages (``AckStatus`` / ``Cursor`` /
``SchemaMessage``) in the read/write contracts so the CDK stays
transport-neutral: it must not import ``src/grpc`` (ADR §4.1). The engine's
gRPC server (``src/destination/server.py``) translates protobuf <-> these
types at the wire boundary. The ``AckStatus`` integer values are aligned 1:1
with ``proto/analitiq/v1/stream.proto`` so that translation is mostly identity.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import Any, List, Optional, Protocol, runtime_checkable


class AckStatus(IntEnum):
    """Batch acknowledgment status.

    Integer values mirror the ``AckStatus`` enum in ``stream.proto`` exactly,
    so the engine can pass a value straight into a protobuf ``BatchAck`` (and
    vice versa) without a lookup table.
    """

    ACK_STATUS_UNSPECIFIED = 0
    ACK_STATUS_SUCCESS = 1            # All records written, cursor advanced
    ACK_STATUS_ALREADY_COMMITTED = 2  # Idempotent replay, batch already committed
    ACK_STATUS_RETRYABLE_FAILURE = 3  # No commit occurred, safe to retry whole batch
    ACK_STATUS_FATAL_FAILURE = 4      # No commit occurred, do not retry, send to DLQ


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
    identification fields the wire message does: the destination looks up the
    full contract endpoint document (columns, primary keys, target table) by
    ``stream_id`` from configuration it already loaded, not from this object.
    ``write_mode`` is the integer value of the proto ``WriteMode`` enum.
    """

    stream_id: str
    version: int
    write_mode: int


# Statuses that count as a successful (committed) batch. Module-level so the
# ``BatchWriteResult.success`` property is a cheap membership test.
_SUCCESS_STATUSES = frozenset(
    {AckStatus.ACK_STATUS_SUCCESS, AckStatus.ACK_STATUS_ALREADY_COMMITTED}
)


@dataclass(frozen=True)
class BatchWriteResult:
    """Immutable result of writing a batch to the destination.

    ``success`` is a derived property: a result is successful when its
    ``status`` is SUCCESS or ALREADY_COMMITTED. Modeling it as a property
    (instead of a constructor argument) makes status the single source of
    truth — callers cannot construct an inconsistent result.
    """

    status: AckStatus
    records_written: int
    committed_cursor: Optional[Cursor] = None
    failed_record_ids: List[str] = field(default_factory=list)
    failure_summary: str = ""

    def __post_init__(self) -> None:
        if self.records_written < 0:
            raise ValueError(
                f"records_written must be non-negative, got {self.records_written}"
            )

    @property
    def success(self) -> bool:
        return self.status in _SUCCESS_STATUSES


class EndpointScope(StrEnum):
    """Whether an endpoint reference is scoped to a connector or a connection.

    The value read off the engine's ``EndpointRef.scope``. Constructing
    ``EndpointScope(value)`` raises ``ValueError`` on an unknown value, which
    preserves the scope validation that lives in ``EndpointRef.__post_init__``
    engine-side. The CDK takes this enum (not the engine model) so it never
    imports ``src/models``.
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
    ) -> dict[str, Any] | None: ...

    async def save_cursor(
        self,
        stream_name: str,
        partition: dict[str, Any] | None,
        cursor: dict[str, Any],
    ) -> None: ...
