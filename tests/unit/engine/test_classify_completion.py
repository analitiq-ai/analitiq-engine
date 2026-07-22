"""Unit tests for StreamProcessor._classify_completion.

The partial-completion classifier is the only place a dlq/skip stream's
exhausted-batch causes become the stream's reported outcome (issue #351):
nothing raises on that path, so if this method misreports, the runner falls
back to its load-stage default while every test stays green.
"""

from unittest.mock import MagicMock

import pytest

from src.engine.stream_processor import StreamProcessor
from src.state.error_classification import ErrorCode, customer_message


def _processor() -> StreamProcessor:
    return StreamProcessor(
        stream_id="s1",
        stream_config={"name": "s1"},
        pipeline_config={},
        pipeline_id="p1",
        state_manager=MagicMock(),
        pipeline_metrics=MagicMock(),
        worker_readable=MagicMock(),
        dlq_root=".",
        batch_size=1,
        buffer_size=1,
        max_retries=0,
        retry_delay=0,
        error_strategy="dlq",
    )


@pytest.mark.unit
class TestClassifyCompletion:
    def test_no_failures_reports_success(self):
        processor = _processor()
        assert processor._classify_completion() == ("success", None, None, None)

    def test_partial_reports_dominant_exhausted_code(self):
        processor = _processor()
        processor.metrics.records_failed = 2
        processor.exhausted_failure_codes.extend(
            [ErrorCode.INTERNAL, ErrorCode.DESTINATION_WRITE_FAILED]
        )

        status, code, message, detail = processor._classify_completion()

        assert status == "partial"
        assert code is ErrorCode.DESTINATION_WRITE_FAILED
        assert message == customer_message(ErrorCode.DESTINATION_WRITE_FAILED)
        assert "dead-lettered" in detail

    def test_partial_with_no_stashed_codes_takes_load_stage_default(self):
        # Defense-in-depth: records_failed without stashed codes (a future
        # accounting bug) must still name a destination write failure, not
        # crash or report success.
        processor = _processor()
        processor.metrics.records_failed = 1

        status, code, _, _ = processor._classify_completion()

        assert status == "partial"
        assert code is ErrorCode.DESTINATION_WRITE_FAILED

    def test_skip_strategy_wording_never_implies_dead_lettering(self):
        # 'skip' drops records unrecoverably; the detail must say dropped,
        # not dead-lettered (partial-run reporting honesty).
        processor = _processor()
        processor.metrics.records_failed = 3
        processor.metrics.records_skipped = 3
        processor.exhausted_failure_codes.append(ErrorCode.INTERNAL)

        status, code, _, detail = processor._classify_completion()

        assert status == "partial"
        assert code is ErrorCode.INTERNAL
        assert "skipped (dropped)" in detail
        assert "dead-lettered" not in detail
