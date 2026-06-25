"""Unit tests for the typed resolved-runtime models and their invariants."""

from unittest.mock import MagicMock

import pytest

from src.engine.pipeline_config_prep import _parse_runtime_config
from src.models.resolved import (
    BatchingConfig,
    ErrorHandlingConfig,
    PipelineConnections,
    ResolvedPipeline,
    ResolvedStream,
    RuntimeConfig,
)


class TestBatchingConfig:
    def test_defaults(self):
        cfg = BatchingConfig()
        assert cfg.batch_size == 1000
        assert cfg.max_concurrent_batches == 3

    @pytest.mark.parametrize("batch_size", [0, -1])
    def test_rejects_non_positive_batch_size(self, batch_size):
        with pytest.raises(ValueError, match="batch_size must be positive"):
            BatchingConfig(batch_size=batch_size)

    @pytest.mark.parametrize("value", [0, -5])
    def test_rejects_non_positive_concurrency(self, value):
        with pytest.raises(ValueError, match="max_concurrent_batches must be positive"):
            BatchingConfig(max_concurrent_batches=value)


class TestErrorHandlingConfig:
    def test_defaults(self):
        cfg = ErrorHandlingConfig()
        assert cfg.strategy == "fail"
        assert cfg.max_retries == 3
        assert cfg.retry_delay_seconds == 5

    def test_accepts_dlq_strategy(self):
        assert ErrorHandlingConfig(strategy="dlq").strategy == "dlq"

    def test_rejects_unknown_strategy(self):
        with pytest.raises(ValueError, match="Unknown error strategy"):
            ErrorHandlingConfig(strategy="retry-forever")

    def test_rejects_negative_max_retries(self):
        with pytest.raises(ValueError, match="max_retries must be non-negative"):
            ErrorHandlingConfig(max_retries=-1)

    def test_rejects_negative_retry_delay(self):
        with pytest.raises(ValueError, match="retry_delay_seconds must be non-neg"):
            ErrorHandlingConfig(retry_delay_seconds=-1)


class TestRuntimeConfig:
    def test_defaults_compose_sub_configs(self):
        cfg = RuntimeConfig()
        assert isinstance(cfg.batching, BatchingConfig)
        assert isinstance(cfg.error_handling, ErrorHandlingConfig)
        assert cfg.buffer_size == 5000

    def test_rejects_non_positive_buffer(self):
        with pytest.raises(ValueError, match="buffer_size must be positive"):
            RuntimeConfig(buffer_size=0)

    def test_to_dict_round_trips_nested_shape(self):
        cfg = RuntimeConfig(
            batching=BatchingConfig(batch_size=250, max_concurrent_batches=7),
            error_handling=ErrorHandlingConfig(
                strategy="dlq", max_retries=1, retry_delay_seconds=2
            ),
            buffer_size=4096,
        )
        assert cfg.to_dict() == {
            "batching": {"batch_size": 250, "max_concurrent_batches": 7},
            "error_handling": {
                "strategy": "dlq",
                "max_retries": 1,
                "retry_delay_seconds": 2,
            },
            "buffer_size": 4096,
        }


class TestPipelineConnections:
    def test_holds_source_and_destinations(self):
        conns = PipelineConnections(source="src", destinations=["a", "b"])
        assert conns.source == "src"
        assert conns.destinations == ["a", "b"]

    def test_rejects_empty_source(self):
        with pytest.raises(ValueError, match="source cannot be empty"):
            PipelineConnections(source="", destinations=["a"])


class TestResolvedModelGuards:
    def _pipeline(self, pipeline_id="p1"):
        return ResolvedPipeline(
            pipeline_id=pipeline_id,
            name="n",
            display_name=None,
            description=None,
            status="active",
            connections=PipelineConnections(source="src", destinations=["d"]),
        )

    def test_resolved_pipeline_accepts_valid_id(self):
        assert self._pipeline().pipeline_id == "p1"

    def test_resolved_pipeline_rejects_empty_id(self):
        with pytest.raises(ValueError, match="pipeline_id cannot be empty"):
            self._pipeline(pipeline_id="")

    def _stream(self, stream_id="s1"):
        return ResolvedStream(
            stream_id=stream_id,
            stream_version=1,
            pipeline_id="p1",
            display_name=None,
            description=None,
            status="active",
            is_enabled=True,
            tags=[],
            source=MagicMock(),
            destinations=[MagicMock()],
            mapping={},
        )

    def test_resolved_stream_rejects_empty_id(self):
        with pytest.raises(ValueError, match="stream_id cannot be empty"):
            self._stream(stream_id="")


class TestParseRuntimeConfig:
    def test_empty_block_yields_defaults(self):
        cfg = _parse_runtime_config({})
        assert cfg.batching.batch_size == 1000
        assert cfg.error_handling.strategy == "fail"
        assert cfg.buffer_size == 5000

    def test_partial_block_merges_with_defaults(self):
        cfg = _parse_runtime_config({"batching": {"batch_size": 50}})
        assert cfg.batching.batch_size == 50
        assert cfg.batching.max_concurrent_batches == 3  # default preserved

    def test_full_block_is_typed(self):
        cfg = _parse_runtime_config(
            {
                "batching": {"batch_size": 200, "max_concurrent_batches": 4},
                "error_handling": {
                    "strategy": "dlq",
                    "max_retries": 9,
                    "retry_delay_seconds": 1,
                },
                "buffer_size": 1234,
            }
        )
        assert cfg.batching.max_concurrent_batches == 4
        assert cfg.error_handling.strategy == "dlq"
        assert cfg.buffer_size == 1234

    def test_invalid_value_fails_loud(self):
        with pytest.raises(ValueError, match="Unknown error strategy"):
            _parse_runtime_config({"error_handling": {"strategy": "nope"}})
