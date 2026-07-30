"""The transport seam of the stage-then-merge write primitive.

Every SQL write is stage-then-merge (spec sql-write-path): land the batch
in a stage table, then run exactly one mode statement from stage to target.
The facade (``GenericSQLConnector``) owns the semantics — write modes,
identity and duplicate rules, refusals, retry verdicts, timeouts — and
expresses one batch write as a :class:`StageWritePlan`. A
:class:`TransportBackend` executes plans; it owns connections, cursors, and
commit calls, and holds no write-mode logic.

The step order those plans run in is not a backend's either: it is
:class:`~cdk.sql.stage_cycle.StageCycle`, one copy above both transports.
A backend supplies the cycle a
:class:`~cdk.sql.stage_cycle.StageConnection` and keeps only what is
genuinely transport-mechanical — connection lifecycle, its own landing
mechanism, and the cancellation discipline of its own lock.
:class:`~cdk.sql.sqlalchemy_backend.SqlAlchemyBackend` is the SQLAlchemy
transport (both engine flavors);
:class:`~cdk.sql.adbc_backend.AdbcBackend` executes the same plans over a
direct ADBC DBAPI connection.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeVar

from .dialects import TableAddress

if TYPE_CHECKING:
    import pyarrow as pa

    from cdk.connection_runtime import ConnectionRuntime

StageScope = Literal["temp", "real"]

_Row = TypeVar("_Row")


@dataclass(frozen=True)
class StageWritePlan:
    """Everything a backend needs to execute one batch write.

    Built by the facade: addresses from the dialect's ``TableAddress``
    factory, SQL text from the dialect's rendering hooks, scope and
    transaction shape from the connector's declared capabilities. The
    plan is complete for every dialect-specific *write* statement, so a
    backend renders none of them; the generic ANSI probes around the
    write are rendered above the transport seam
    (``cdk.sql.stage_cycle``), leaving the backends nothing to render at
    all.
    """

    stage: TableAddress
    target: TableAddress
    scope: StageScope
    transactional: bool
    create_stage_sql: str
    #: Set on a truncate_insert read's first batch only: the dialect's
    #: target-emptying statement (``empty_table_sql``), run after landing
    #: and before the mode statement.
    truncate_sql: str | None
    #: The one mode statement from stage to target (anti-join insert,
    #: merge-form upsert, or plain append).
    mode_sql: str
    drop_stage_sql: str
    #: Landing column order, identity columns included.
    columns: tuple[str, ...]
    #: Rows one landing statement may carry under the connector's declared
    #: ``sql_capabilities.limits.max_bind_params``
    #: (``floor(cap / column_count)``, issue #401); ``None`` when no cap is
    #: declared — the executemany landing then sends the whole batch in one
    #: statement, as before.
    rows_per_statement: int | None = None


def iter_landing_chunks(
    rows: Sequence[_Row], per_statement: int | None
) -> Iterator[Sequence[_Row]]:
    """Split landed rows so no statement exceeds the declared bind cap.

    The one chunking rule both transport backends apply in their
    executemany landing (``StageWritePlan.rows_per_statement``, issue
    #401); ``None`` (no declared cap) yields the whole batch unchanged.
    """
    if per_statement is None:
        yield rows
        return
    for start in range(0, len(rows), per_statement):
        yield rows[start : start + per_statement]


class TransportBackend(ABC):
    """Executes :class:`StageWritePlan`s; owns connections and commits.

    Holds no write-mode logic: which statement runs is the plan's, decided
    by the facade. Success of :meth:`execute_write` is returning without
    raising — it deliberately returns nothing, because database rowcounts
    lie about the batch (an idempotent replay's anti-join affects 0 rows;
    MySQL upserts count 2 per updated row); the facade reports
    ``records_written`` from the batch's own row count.
    """

    @abstractmethod
    async def connect(self, runtime: ConnectionRuntime) -> None:
        """Take ownership of the runtime's transport for this backend."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Release backend-held transport state.

        The ``ConnectionRuntime`` itself (engine disposal) stays owned by
        the facade's lifecycle, which materialized it.
        """

    @abstractmethod
    async def run_ddl(self, statements: Sequence[str]) -> None:
        """Run *statements* committed together (one transaction)."""

    @abstractmethod
    async def target_columns(self, target: TableAddress) -> tuple[str, ...]:
        """Return *target*'s column names as the database reports them.

        Called by the facade after DDL, outside any write cycle: the
        names serve its readiness checks (the engine-managed
        ``_record_hash`` column, one rule for every transport). The
        mechanism is the transport's own — SQLAlchemy reflects, ADBC
        reads the description of a zero-row probe — which is why this
        stays on the backend rather than moving above the seam. A backend
        may also cache what it learns here for the landing step's own use.
        """

    @abstractmethod
    async def health_check(self) -> None:
        """Probe transport liveness; success is returning without raising."""

    @abstractmethod
    async def execute_write(self, plan: StageWritePlan, batch: pa.RecordBatch) -> None:
        """Run one stage cycle for *batch* under the backend's write lock.

        The body is :meth:`~cdk.sql.stage_cycle.StageCycle.run` on every
        transport; what a backend adds here is the cancellation
        discipline of its own lock. At most one stage cycle runs at a
        time per backend — the whole cycle holds that lock, so a retry
        can only ever meet a completed or abandoned stage, never a live
        one.
        """
