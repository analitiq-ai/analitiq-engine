"""One verdict for a batch, and the policy that reaches it.

A batch's fate used to be re-derived at every hop -- the client, the worker
proxy and the stream processor each mapped the same event to their own
answer, and the copies disagreed (issue #428). This module is the single
place that turns acks into a decision:

* :class:`BatchPolicy` is bound once per stream from ``error_strategy`` +
  ``max_retries`` and owns the whole send -> ack -> backoff-retry loop. It
  also disposes of a batch the transform rejected on a validation rule
  (:meth:`BatchPolicy.reject`), under the strategy that rule carries, so a
  rejected batch and an exhausted one reach their verdict through the same
  strategy ladder.
* The loop ends in exactly one :data:`Disposition`, a sum type whose success
  variant is the only one carrying a cursor. A failed batch cannot advance
  the checkpoint because a failure has nowhere to put one.

The policy decides; the caller acts. :class:`DeadLetter` does not write the
dead letter and :class:`Committed` does not persist the cursor -- those need
collaborators (DLQ, state manager, metrics) the policy deliberately does not
hold, which is also what keeps its test surface a scripted list of acks.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from enum import Enum, StrEnum, auto
from typing import Final

from cdk.types import FailureCategory

from ..grpc.client import BatchResult
from ..grpc.cursor import Cursor
from ..grpc.generated.analitiq.v1 import AckStatus

logger = logging.getLogger(__name__)


class ErrorStrategy(StrEnum):
    """What a stream does with a batch the destination would not take.

    This repo's ``errors.tolerance``: it belongs to policy, not to a failure
    report. The values are the published pipeline contract's
    ``runtime.error_handling.strategy`` vocabulary; the typed enum replaces
    the bare-string comparisons that used to need a defensive "unhandled
    strategy" raise at the bottom of the ladder.
    """

    FAIL = "fail"
    DLQ = "dlq"
    SKIP = "skip"

    @classmethod
    def strictest(cls, strategies: Iterable[ErrorStrategy]) -> ErrorStrategy:
        """Pick the strategy that gives up the most, among *strategies*.

        A batch is one unit: when several validation rules fail on the same
        batch under different strategies, the batch takes the strictest
        one -- fail over dlq over skip -- so no rule's verdict is quietly
        relaxed by a looser neighbour. Raises ``ValueError`` on an empty
        input, since there is no verdict to pick.
        """
        return max(strategies, key=_STRICTNESS.index)


#: Loosest first; ``ErrorStrategy.strictest`` picks the last one present.
_STRICTNESS: Final = (ErrorStrategy.SKIP, ErrorStrategy.DLQ, ErrorStrategy.FAIL)


class FailureKind(Enum):
    """Which terminal failure the ack protocol reached.

    The strategy collapses three distinct endings into the same disposition,
    so the ending itself rides the report: exhausting bounded retries is not
    the same event as a connector's fatal verdict, and neither is an ack the
    engine cannot interpret.
    """

    RETRIES_EXHAUSTED = auto()
    FATAL = auto()
    UNKNOWN_STATUS = auto()
    #: The transform rejected the batch on a validation rule; it was never
    #: sent.
    VALIDATION_FAILED = auto()


@dataclass(frozen=True)
class FailureReport:
    """Why a batch failed, in the terms the caller reports it in."""

    kind: FailureKind
    summary: str
    #: The destination's machine-readable category, or UNSPECIFIED when it
    #: declared none -- and always UNSPECIFIED for an ack whose status the
    #: engine could not interpret, whose advisory fields are not trustworthy.
    category: FailureCategory
    #: Sends made, including the first: ``1`` unless retries were spent,
    #: ``0`` for a batch rejected before any send.
    attempts: int


@dataclass(frozen=True)
class Committed:
    """The destination wrote the batch and returned its watermark."""

    cursor: Cursor | None
    records_written: int


@dataclass(frozen=True)
class AlreadyCommitted:
    """An earlier attempt had already committed this batch.

    Carries no cursor: the checkpoint is an artifact of a confirmed commit
    this run made, and this run made none.
    """


@dataclass(frozen=True)
class DeadLetter:
    """Preserve the records out of band, then continue the stream."""

    report: FailureReport


@dataclass(frozen=True)
class Skipped:
    """Drop the records and continue the stream."""

    report: FailureReport


@dataclass(frozen=True)
class Failed:
    """Stop the stream."""

    report: FailureReport
    #: Whether the records are preserved before the stream stops. True only
    #: under the dlq strategy: a fatal verdict fails the stream whatever the
    #: strategy, but dlq still owes the operator the rows.
    dead_letter: bool


#: The terminal answer for one batch. Only ``Committed`` carries a cursor.
Disposition = Committed | AlreadyCommitted | DeadLetter | Skipped | Failed


class BatchPolicy:
    """Drives one batch to a terminal disposition.

    Bound once per stream. ``send`` is a zero-argument thunk the caller
    binds to one batch, so the policy re-issues the identical send on every
    retry and never touches the payload -- the caller's per-batch values
    (the Arrow batch, the record ids, the once-stamped emit instant) cannot
    drift across attempts because the policy cannot see them.

    ``sleep`` is injected so a test can exercise a full backoff ladder
    without spending it.
    """

    def __init__(
        self,
        *,
        max_retries: int,
        retry_delay: float,
        error_strategy: ErrorStrategy,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Bind the stream's retry budget and failure tolerance.

        The strategy is re-read through the enum so a value outside the
        vocabulary raises here, at the boundary, instead of quietly taking
        whichever branch falls through at the bottom of the ladder.
        """
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._error_strategy = ErrorStrategy(error_strategy)
        self._sleep = sleep

    async def run(
        self, send: Callable[[], Awaitable[BatchResult]], *, label: str
    ) -> Disposition:
        """Send until the batch reaches a terminal answer, and return it.

        Retries a retryable ack with exponential backoff up to
        ``max_retries``. ``label`` names the batch in the retry log line.
        """
        attempts = 0
        while True:
            result = await send()
            attempts += 1

            if result.status == AckStatus.ACK_STATUS_SUCCESS:
                return Committed(
                    cursor=result.committed_cursor,
                    records_written=result.records_written,
                )
            if result.status == AckStatus.ACK_STATUS_ALREADY_COMMITTED:
                return AlreadyCommitted()
            if result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE:
                if attempts > self._max_retries:
                    return self._terminal(
                        FailureKind.RETRIES_EXHAUSTED, result, attempts
                    )
                delay = self._retry_delay * (2 ** (attempts - 1))
                logger.warning(
                    "%s retryable failure, retry %s/%s after %.2fs: %s",
                    label,
                    attempts,
                    self._max_retries,
                    delay,
                    result.failure_summary,
                )
                await self._sleep(delay)
                continue
            if result.status == AckStatus.ACK_STATUS_FATAL_FAILURE:
                return self._terminal(FailureKind.FATAL, result, attempts)
            return self._terminal(FailureKind.UNKNOWN_STATUS, result, attempts)

    def _terminal(
        self, kind: FailureKind, result: BatchResult, attempts: int
    ) -> Disposition:
        """Apply the error strategy to a failure the retry loop is done with."""
        if kind is FailureKind.UNKNOWN_STATUS:
            # An ack the engine cannot interpret says nothing trustworthy
            # about the failure either, so its advisory fields are dropped
            # rather than reported as the destination's claim.
            report = FailureReport(
                kind=kind,
                summary=f"unknown ACK status: {result.status}",
                category=FailureCategory.FAILURE_CATEGORY_UNSPECIFIED,
                attempts=attempts,
            )
        else:
            report = FailureReport(
                kind=kind,
                summary=result.failure_summary,
                category=result.failure_category,
                attempts=attempts,
            )

        if kind is FailureKind.RETRIES_EXHAUSTED:
            return _dispose(self._error_strategy, report)

        # A verdict the connector rendered (or an ack the engine cannot
        # read) is not made good by continuing: the stream stops whatever
        # the strategy, and only dlq preserves the rows on the way out.
        return Failed(report, dead_letter=self._error_strategy is ErrorStrategy.DLQ)

    @staticmethod
    def reject(*, strategy: ErrorStrategy, summary: str) -> Disposition:
        """Dispose of a batch the transform rejected on a validation rule.

        The rule's own effective strategy decides -- the assignment's
        ``validate.error_handling.strategy`` override, else the pipeline
        default the transform was compiled with -- through the same ladder
        an exhausted batch takes. There is nothing to retry: a rule is a
        pure function of the batch, so re-running it would fail the same
        rows the same way.
        """
        report = FailureReport(
            kind=FailureKind.VALIDATION_FAILED,
            summary=summary,
            category=FailureCategory.FAILURE_CATEGORY_UNSPECIFIED,
            attempts=0,
        )
        return _dispose(ErrorStrategy(strategy), report)


def _dispose(strategy: ErrorStrategy, report: FailureReport) -> Disposition:
    """Apply the strategy ladder to a batch the destination will not get."""
    if strategy is ErrorStrategy.DLQ:
        return DeadLetter(report)
    if strategy is ErrorStrategy.SKIP:
        return Skipped(report)
    return Failed(report, dead_letter=False)
