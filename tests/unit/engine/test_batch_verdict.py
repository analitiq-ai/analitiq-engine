"""One verdict for a failing batch (issue #428).

Each class here pins one of the four decisions the ticket settles, driven
through :class:`BatchPolicy`'s own interface: a scripted list of acks, an
injected sleep, no wire and no mocks. ``send`` is a thunk, so a test writes
the ack sequence the destination would have produced and reads back the one
terminal disposition the policy reached.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import fields

import pytest
from analitiq.contracts.pipelines.config import ErrorHandling as ContractErrorHandling

from cdk.types import FailureCategory
from src.engine.batch_policy import (
    AlreadyCommitted,
    BatchPolicy,
    Committed,
    DeadLetter,
    ErrorStrategy,
    Failed,
    FailureKind,
    Skipped,
)
from src.grpc.client import BatchResult, _transport_failure
from src.grpc.cursor import encode_cursor
from src.grpc.generated.analitiq.v1 import AckStatus, BatchAck, Cursor

pytestmark = pytest.mark.unit


def _ack(
    status: AckStatus,
    *,
    summary: str = "",
    records_written: int = 0,
    cursor: Cursor | None = None,
    category: FailureCategory = FailureCategory.FAILURE_CATEGORY_UNSPECIFIED,
) -> BatchResult:
    """One ack, as the client hands it to the policy."""
    return BatchResult(
        success=status
        in (AckStatus.ACK_STATUS_SUCCESS, AckStatus.ACK_STATUS_ALREADY_COMMITTED),
        status=status,
        records_written=records_written,
        committed_cursor=cursor,
        failed_record_ids=[],
        failure_summary=summary,
        failure_category=category,
    )


def _script(*acks: BatchResult) -> tuple[Callable[[], Awaitable[BatchResult]], list]:
    """Turn a list of acks into a send thunk, plus the log of calls made."""
    calls: list[int] = []
    remaining = list(acks)

    async def send() -> BatchResult:
        calls.append(len(calls) + 1)
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    return send, calls


def _policy(
    *,
    strategy: ErrorStrategy = ErrorStrategy.FAIL,
    max_retries: int = 3,
    slept: list[float] | None = None,
) -> BatchPolicy:
    async def _sleep(delay: float) -> None:
        if slept is not None:
            slept.append(delay)

    return BatchPolicy(
        max_retries=max_retries,
        retry_delay=1.0,
        error_strategy=strategy,
        sleep=_sleep,
    )


class TestTransportFailureIsRetryable:
    """Decision 1.1 — a send that reached no ack is retryable, everywhere.

    A lost ack means *unknown*, not *failed*: no connector verdict exists to
    trust, so a fatal one would be fabricated and a batch nobody rejected
    would be thrown away. The engine path used to call it fatal while the
    proxied path remapped it to retryable, so the same event cost the stream
    two different things.
    """

    @pytest.mark.asyncio
    async def test_the_detection_site_answers_retryable(self):
        """The one place that builds a no-ack verdict never fabricates fatal;
        that is what makes the two paths agree without either remapping."""
        result = _transport_failure("destination closed the stream")
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.success is False

    @pytest.mark.asyncio
    async def test_a_lost_ack_is_retried_and_the_batch_still_commits(self):
        """Two transport failures then an ack: the batch commits rather than
        dying on the first dropped connection."""
        cursor = encode_cursor("updated_at", "2026-01-01T00:00:00Z")
        send, calls = _script(
            _transport_failure("reader task exited before ACK"),
            _transport_failure("timeout waiting for ACK"),
            _ack(AckStatus.ACK_STATUS_SUCCESS, records_written=7, cursor=cursor),
        )

        disposition = await _policy().run(send, label="batch 1")

        assert disposition == Committed(cursor=cursor, records_written=7)
        assert len(calls) == 3

    @pytest.mark.asyncio
    async def test_retries_stay_bounded(self):
        """Retrying forever is not the alternative to failing early: the
        budget is max_retries, then the strategy decides."""
        send, calls = _script(_transport_failure("timeout waiting for ACK"))

        disposition = await _policy(max_retries=2).run(send, label="batch 1")

        assert len(calls) == 3  # the first send plus two retries
        assert isinstance(disposition, Failed)
        assert disposition.report.kind is FailureKind.RETRIES_EXHAUSTED
        assert disposition.report.attempts == 3


class TestRetryIgnoresDeclaredSemantics:
    """Decision 1.1b — retry does not consult the stream's RetrySemantics.

    The declaration tells an operator which streams may duplicate on a
    restart; gating the retry on it would fail a batch on an at-least-once
    sink to avoid a duplicate, trading a possible duplicate for a certain
    data loss.
    """

    def test_the_policy_cannot_see_the_declaration(self):
        """No semantics argument: the decision is structural, not a branch
        someone could re-add a condition to."""
        parameters = set(inspect.signature(BatchPolicy.__init__).parameters)
        assert parameters == {"self", "max_retries", "retry_delay", "error_strategy",
                              "sleep"}

    @pytest.mark.asyncio
    async def test_the_retry_budget_is_the_stream_s_alone(self):
        """Two streams, identical acks, identical outcome -- whatever either
        destination declared about replay safety."""
        exactly_once = _policy(max_retries=1)
        at_least_once = _policy(max_retries=1)
        acks = (
            _transport_failure("timeout waiting for ACK"),
            _ack(AckStatus.ACK_STATUS_SUCCESS, records_written=2),
        )

        first_send, first_calls = _script(*acks)
        second_send, second_calls = _script(*acks)

        assert await exactly_once.run(first_send, label="a") == await at_least_once.run(
            second_send, label="b"
        )
        assert len(first_calls) == len(second_calls) == 2


class TestCursorRidesTheSuccessVariant:
    """Decision 1.2 — only a confirmed commit carries a checkpoint.

    The cursor used to be a nullable field on every outcome plus a rule
    saying not to read it on a failure, policed in two places and repaired
    in a third. A failed batch now has nowhere to put one.
    """

    @pytest.mark.asyncio
    async def test_a_commit_carries_its_watermark(self):
        cursor = encode_cursor("updated_at", "2026-01-01T00:00:00Z")
        send, _ = _script(
            _ack(AckStatus.ACK_STATUS_SUCCESS, records_written=3, cursor=cursor)
        )

        disposition = await _policy().run(send, label="batch 1")

        assert isinstance(disposition, Committed)
        assert disposition.cursor == cursor

    @pytest.mark.parametrize(
        "variant", [AlreadyCommitted, DeadLetter, Skipped, Failed]
    )
    def test_no_other_variant_has_anywhere_to_put_one(self, variant):
        """The sum type is the enforcement: a cursor on a failure is not
        dropped at runtime, it is unrepresentable."""
        assert "cursor" not in {field.name for field in fields(variant)}

    @pytest.mark.asyncio
    async def test_a_replay_ack_advances_no_checkpoint(self):
        """ALREADY_COMMITTED says an earlier attempt committed; this run
        confirmed nothing, so it hands back no watermark even when the
        destination volunteered one."""
        send, _ = _script(
            _ack(
                AckStatus.ACK_STATUS_ALREADY_COMMITTED,
                cursor=encode_cursor("updated_at", "2026-01-01T00:00:00Z"),
            )
        )

        assert await _policy().run(send, label="batch 1") == AlreadyCommitted()

    def test_a_cursor_on_a_failure_ack_is_a_connector_contract_violation(self):
        """Silently dropping it repairs the symptom and leaves the connector
        defective and undetectable, so the engine answers the combination
        with an explicit verdict naming the peer that sent it."""
        from src.grpc.client import DestinationGRPCClient

        client = DestinationGRPCClient(host="dest", port=50051)
        ack = BatchAck(
            run_id="run-1",
            stream_id="s1",
            batch_seq=4,
            status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            records_written=0,
            committed_cursor=Cursor(token=b"watermark"),
            failure_summary="db unavailable",
        )

        result = client._process_ack(ack)

        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.committed_cursor is None
        assert "contract violation" in result.failure_summary
        assert "dest:50051" in result.failure_summary

    def test_an_honest_failure_ack_is_left_alone(self):
        """The violation verdict must not swallow ordinary failures: without
        a cursor the connector's own verdict and summary survive."""
        from src.grpc.client import DestinationGRPCClient

        client = DestinationGRPCClient(host="dest", port=50051)
        ack = BatchAck(
            run_id="run-1",
            stream_id="s1",
            batch_seq=4,
            status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
            failure_summary="db unavailable",
        )

        result = client._process_ack(ack)

        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.committed_cursor is None
        assert result.failure_summary == "db unavailable"


class TestOneLoopOneVerdict:
    """Decision 1.3 — the policy owns the loop and returns one terminal answer.

    The retry loop had two call sites and the reaction to its outcome had
    two ladders, which already disagreed. The policy executes the send, so
    there is no second call site to diverge from.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "strategy,expected",
        [
            (ErrorStrategy.DLQ, DeadLetter),
            (ErrorStrategy.SKIP, Skipped),
            (ErrorStrategy.FAIL, Failed),
        ],
    )
    async def test_exhaustion_is_resolved_by_the_strategy(self, strategy, expected):
        send, calls = _script(
            _ack(
                AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                summary="connection reset by peer",
                category=FailureCategory.FAILURE_CATEGORY_NOT_READY,
            )
        )

        disposition = await _policy(strategy=strategy, max_retries=1).run(
            send, label="batch 1"
        )

        assert isinstance(disposition, expected)
        assert disposition.report.kind is FailureKind.RETRIES_EXHAUSTED
        assert disposition.report.summary == "connection reset by peer"
        assert (
            disposition.report.category
            is FailureCategory.FAILURE_CATEGORY_NOT_READY
        )
        assert len(calls) == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "strategy,dead_letter",
        [
            (ErrorStrategy.DLQ, True),
            (ErrorStrategy.SKIP, False),
            (ErrorStrategy.FAIL, False),
        ],
    )
    async def test_a_fatal_verdict_stops_the_stream_under_every_strategy(
        self, strategy, dead_letter
    ):
        """Whole-batch DLQ on fatal is unchanged: dlq still preserves the
        rows, but no strategy turns a fatal verdict into a continuation."""
        send, calls = _script(
            _ack(AckStatus.ACK_STATUS_FATAL_FAILURE, summary="constraint violated")
        )

        disposition = await _policy(strategy=strategy).run(send, label="batch 1")

        assert disposition == Failed(disposition.report, dead_letter=dead_letter)
        assert disposition.report.kind is FailureKind.FATAL
        assert len(calls) == 1  # a fatal verdict is never retried

    @pytest.mark.asyncio
    async def test_an_uninterpretable_ack_keeps_none_of_its_advice(self):
        """An ack whose status the engine cannot read says nothing
        trustworthy about the failure either."""
        send, _ = _script(
            _ack(
                AckStatus.ACK_STATUS_UNSPECIFIED,
                summary="looks fine to me",
                category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
            )
        )

        disposition = await _policy(strategy=ErrorStrategy.DLQ).run(
            send, label="batch 1"
        )

        assert isinstance(disposition, Failed)
        assert disposition.report.kind is FailureKind.UNKNOWN_STATUS
        assert (
            disposition.report.category
            is FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
        )
        assert "unknown ACK status" in disposition.report.summary

    @pytest.mark.asyncio
    async def test_backoff_is_exponential_and_the_sleep_is_injected(self):
        """The whole ladder is exercised without spending it -- which is why
        the sleep is a constructor argument."""
        slept: list[float] = []
        send, _ = _script(_transport_failure("timeout waiting for ACK"))

        await _policy(max_retries=4, slept=slept).run(send, label="batch 1")

        assert slept == [1.0, 2.0, 4.0, 8.0]


class TestErrorStrategyVocabulary:
    """The strategy enum answers to the published contract, not the other way
    round: a value the contract permits must be a value the engine can run."""

    def test_the_enum_is_exactly_the_contract_s_vocabulary(self):
        contract_values = set(
            ContractErrorHandling.model_fields["strategy"].annotation.__args__
        )
        assert {strategy.value for strategy in ErrorStrategy} == contract_values

    def test_a_value_outside_it_is_refused_at_construction(self):
        with pytest.raises(ValueError, match="tolerate-everything"):
            BatchPolicy(
                max_retries=1,
                retry_delay=0,
                error_strategy="tolerate-everything",  # type: ignore[arg-type]
            )
