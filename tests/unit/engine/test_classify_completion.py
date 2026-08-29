"""Unit tests for StreamProcessor._classify_completion.

The partial-completion classifier is the only place a dlq/skip stream's
exhausted-batch causes become the stream's reported outcome (issue #351):
nothing raises on that path, so if this method misreports, the runner falls
back to its load-stage default while every test stays green.
"""

from unittest.mock import MagicMock

import pytest

from src.engine.mapping import MappingDocument
from src.engine.stream_processor import DroppedBatch, StreamProcessor
from src.state.error_classification import ErrorCode, FailureStage, customer_message


def _processor() -> StreamProcessor:
    return StreamProcessor(
        stream_id="s1",
        stream_config={"name": "s1"},
        mapping=MappingDocument(),
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


def _load(code: ErrorCode) -> DroppedBatch:
    return DroppedBatch(code=code, stage=FailureStage.DESTINATION_LOAD)


@pytest.mark.unit
class TestClassifyCompletion:
    def test_no_failures_reports_success(self):
        processor = _processor()
        assert processor._classify_completion() == ("success", None, None, None)

    def test_partial_reports_dominant_exhausted_code(self):
        processor = _processor()
        processor.metrics.records_failed = 2
        processor.dropped_batches.extend(
            [_load(ErrorCode.INTERNAL), _load(ErrorCode.DESTINATION_WRITE_FAILED)]
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
        processor.dropped_batches.append(_load(ErrorCode.INTERNAL))

        status, code, _, detail = processor._classify_completion()

        assert status == "partial"
        assert code is ErrorCode.INTERNAL
        assert "skipped (dropped)" in detail
        assert "dead-lettered" not in detail

    def test_validation_drop_reports_the_transform_stage(self):
        # A batch a validation rule rejected under dlq/skip (issue #468) is
        # classified as the transform stage's failure, not a write failure.
        processor = _processor()
        processor.metrics.records_failed = 2
        processor.dropped_batches.append(
            DroppedBatch(code=ErrorCode.CONFIG_INVALID, stage=FailureStage.TRANSFORM)
        )

        status, code, message, detail = processor._classify_completion()

        assert status == "partial"
        assert code is ErrorCode.CONFIG_INVALID
        assert message == customer_message(ErrorCode.CONFIG_INVALID)
        assert detail == (
            "transform/CONFIG_INVALID: "
            "records dead-lettered after failing validation"
        )

    def test_mixed_skip_and_dlq_wording_names_both(self):
        # Rules carry their own strategies, so one stream can skip one batch
        # and dead-letter another; the detail must not claim only one.
        processor = _processor()
        processor.metrics.records_failed = 5
        processor.metrics.records_skipped = 2
        processor.dropped_batches.append(
            DroppedBatch(code=ErrorCode.CONFIG_INVALID, stage=FailureStage.TRANSFORM)
        )

        _, _, _, detail = processor._classify_completion()

        assert "dead-lettered or skipped (dropped)" in detail
