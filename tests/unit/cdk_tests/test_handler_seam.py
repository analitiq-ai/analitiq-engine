"""What a destination adapter has to write, and what the base writes for it.

Every sink used to copy the same preamble before its one distinctive act:
a readiness guard, an empty-batch success that still advances the cursor,
one materialisation of the Arrow batch, and a pair of except arms turning a
raised failure into a verdict. Four copies meant four chances to drift, and
they had: one sink caught ``to_pylist`` inside its try and another outside,
two declared a failure category and two did not.

The preamble now lives on the base and each adapter implements ``land``.
"""

from __future__ import annotations

import errno
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pytest

from cdk.base_handler import BaseDestinationHandler, BatchRejected, LandingBatch
from cdk.types import AckStatus, Cursor, FailureCategory, SchemaSpec

pytestmark = pytest.mark.unit

_EMITTED_AT = datetime(2026, 7, 21, 9, 0, tzinfo=timezone.utc)


class _Sink(BaseDestinationHandler):
    """The least a sink can implement: one land()."""

    def __init__(self, *, landed: int = 2, raises: BaseException | None = None) -> None:
        self.landed = landed
        self.raises = raises
        self.not_ready: str | None = None
        self.materialised = False

    def not_ready_reason(self, stream_id: str) -> str | None:
        """Report the staged readiness answer."""
        _ = stream_id
        return self.not_ready

    async def land(self, batch: LandingBatch) -> int:
        """Record what the base handed over, then answer as staged."""
        if self.raises is not None:
            raise self.raises
        return self.landed

    async def connect(self, runtime: Any) -> None:
        """No connection to make."""

    async def disconnect(self) -> None:
        """No connection to drop."""

    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        """Accept every schema."""
        return True

    async def health_check(self) -> bool:
        """Always healthy."""
        return True

    @property
    def connector_type(self) -> str:
        """Name this test sink."""
        return "test-sink"


async def _write(handler: BaseDestinationHandler, rows: list[dict[str, Any]]):
    batch = (
        pa.RecordBatch.from_pylist(rows)
        if rows
        else pa.record_batch([], schema=pa.schema([]))
    )
    return await handler.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
        record_batch=batch,
        record_ids=[str(i) for i in range(len(rows))],
        cursor=Cursor(token=b"tok"),
        emitted_at=_EMITTED_AT,
    )


class TestTheBaseWritesThePreamble:
    @pytest.mark.asyncio
    async def test_a_not_ready_sink_rejects_without_landing(self) -> None:
        sink = _Sink()
        sink.not_ready = "Handler not connected"
        result = await _write(sink, [{"a": 1}])
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        # Nothing was attempted, which is the whole meaning of NOT_READY.
        assert result.failure_category == FailureCategory.FAILURE_CATEGORY_NOT_READY
        assert "Handler not connected" in result.failure_summary

    @pytest.mark.asyncio
    async def test_an_empty_batch_succeeds_and_still_advances_the_cursor(self) -> None:
        result = await _write(_Sink(), [])
        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.records_written == 0
        # Withholding the checkpoint would re-read the same empty range
        # forever; the engine read this far and found no rows.
        assert result.committed_cursor == Cursor(token=b"tok")

    @pytest.mark.asyncio
    async def test_a_landed_batch_succeeds_with_the_cursor(self) -> None:
        result = await _write(_Sink(landed=2), [{"a": 1}, {"a": 2}])
        assert result.status == AckStatus.ACK_STATUS_SUCCESS
        assert result.records_written == 2
        assert result.committed_cursor == Cursor(token=b"tok")

    @pytest.mark.asyncio
    async def test_an_oserror_is_judged_by_the_shared_errno_table(self) -> None:
        sink = _Sink(raises=OSError(errno.ENOSPC, "disk full"))
        result = await _write(sink, [{"a": 1}])
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "ENOSPC" in result.failure_summary

    @pytest.mark.asyncio
    async def test_an_unlisted_errno_stays_retryable(self) -> None:
        sink = _Sink(raises=OSError(errno.EIO, "transient"))
        result = await _write(sink, [{"a": 1}])
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE

    @pytest.mark.asyncio
    async def test_a_named_refusal_carries_its_reason_and_category(self) -> None:
        sink = _Sink(
            raises=BatchRejected(
                "formatter returned empty bytes",
                category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
            )
        )
        result = await _write(sink, [{"a": 1}])
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.failure_category == (
            FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT
        )
        assert "formatter returned empty bytes" in result.failure_summary
        # The sink names the defect; the base says which batch it was, so no
        # sink can forget to identify it.
        assert "run=r1" in result.failure_summary
        assert "seq=1" in result.failure_summary

    @pytest.mark.asyncio
    async def test_a_partial_refusal_keeps_what_landed(self) -> None:
        # A sink that writes row by row can fail partway. Retrying the whole
        # batch would duplicate the rows that did land, so the count and the
        # failed ids travel with the refusal.
        sink = _Sink(
            raises=BatchRejected(
                "2/3 records failed",
                records_written=1,
                failed_record_ids=("1", "2"),
            )
        )
        result = await _write(sink, [{"a": 1}, {"a": 2}, {"a": 3}])
        assert result.records_written == 1
        assert result.failed_record_ids == ("1", "2")
        assert result.committed_cursor is None

    @pytest.mark.asyncio
    async def test_an_undeclared_exception_is_fatal_and_named(self) -> None:
        sink = _Sink(raises=KeyError("missing"))
        result = await _write(sink, [{"a": 1}])
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "KeyError" in result.failure_summary


class TestTheBatchIsMaterialisedOnce:
    @pytest.mark.asyncio
    async def test_an_arrow_native_sink_pays_no_conversion(self) -> None:
        # The worker proxy forwards Arrow bytes untouched; converting every
        # batch to dicts for a sink that never reads them would be a real
        # cost on the sandboxed path.
        batch = LandingBatch(
            run_id="r1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"a": 1}]),
            record_ids=["0"],
            cursor=Cursor(),
            emitted_at=_EMITTED_AT,
        )
        assert "records" not in batch.__dict__  # untouched
        assert batch.records == [{"a": 1}]
        assert "records" in batch.__dict__  # cached after one read

    @pytest.mark.asyncio
    async def test_records_convert_once_however_often_they_are_read(self) -> None:
        batch = LandingBatch(
            run_id="r1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"a": 1}]),
            record_ids=["0"],
            cursor=Cursor(),
            emitted_at=_EMITTED_AT,
        )
        first = batch.records
        assert batch.records is first  # the same list, not a second conversion


class TestASinkMustBeAbleToWrite:
    def test_a_handler_implementing_neither_is_refused_at_definition(self) -> None:
        # A gap here is a class-definition error rather than a first-batch
        # one, which is the difference between a failed import and a failed
        # production run.
        with pytest.raises(TypeError, match="neither land"):

            class _Useless(BaseDestinationHandler):
                """Every lifecycle member, and no way to write a batch."""

                async def connect(self, runtime: Any) -> None:
                    """No connection to make."""

                async def disconnect(self) -> None:
                    """No connection to drop."""

                async def configure_schema(self, spec: SchemaSpec) -> bool:
                    """Accept every schema."""
                    return True

                async def health_check(self) -> bool:
                    """Always healthy."""
                    return True

                @property
                def connector_type(self) -> str:
                    """Name this sink."""
                    return "useless"

            assert _Useless  # unreachable: the class body raised

    def test_overriding_write_batch_alone_is_allowed(self) -> None:
        # The base is the common shape, not a cage: a sink whose framing
        # genuinely differs replaces the whole method.

        class _Framed(BaseDestinationHandler):
            """A sink whose framing differs, so it replaces write_batch."""

            async def connect(self, runtime: Any) -> None:
                """No connection to make."""

            async def disconnect(self) -> None:
                """No connection to drop."""

            async def configure_schema(self, spec: SchemaSpec) -> bool:
                """Accept every schema."""
                return True

            async def health_check(self) -> bool:
                """Always healthy."""
                return True

            @property
            def connector_type(self) -> str:
                return "framed"

            async def write_batch(self, *args: Any, **kwargs: Any) -> Any:
                return None

        assert _Framed() is not None


class TestForwardedCapabilities:
    def test_a_forwarding_handler_advertises_nothing_before_it_connects(self) -> None:
        # The neutral defaults would have it claim, before reaching its
        # worker, capabilities the worker may not have.
        class _Forwarder(_Sink):
            @property
            def forwards_capabilities(self) -> bool:
                return True

        forwarder = _Forwarder()
        assert forwarder.supports_upsert is False
        assert forwarder.supports_insert is False
        assert forwarder.supports_truncate is False

    def test_a_self_answering_handler_keeps_its_own_defaults(self) -> None:
        sink = _Sink()
        assert sink.supports_upsert is True
        assert sink.supports_insert is True
