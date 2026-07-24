"""Tests for the engine-side ``WorkerReadable``.

Covers the engine half of the worker read contract: cursor relay into the
engine's checkpoint store, the retryability classification of worker error
events, the worker-died-mid-read crash path, and the channel options that
must match the worker server's message-size ceiling.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from cdk.sql.exceptions import ReadError
from src.grpc import DEFAULT_MAX_MESSAGE_SIZE
from src.grpc.generated.analitiq.v1.source_service_pb2 import (
    CursorSave,
    ReadBatchChunk,
    ReadComplete,
)
from src.grpc.generated.analitiq.v1.source_service_pb2 import ReadError as ReadErrorMsg
from src.grpc.generated.analitiq.v1.source_service_pb2 import ReadResponse
from src.state.error_classification import ErrorCode, FailureStage, read_failure_tag
from src.worker.readable import WorkerReadable
from src.worker.source_service import _encode_arrow_ipc


class _FakeCheckpoint:
    def __init__(self, initial=None):
        self._initial = initial
        self.saved = []

    async def get_cursor(self, stream_name, partition=None):
        return self._initial

    async def save_cursor(self, stream_name, partition, cursor):
        self.saved.append(cursor)


def _responses_to_stream(responses, captured_requests=None):
    async def _iter(request):
        if captured_requests is not None:
            captured_requests.append(request)
        for response in responses:
            yield response

    return _iter


def _readable():
    return WorkerReadable(
        connectors_dir=Path("/nonexistent/connectors"),
        connections_dir=Path("/nonexistent/connections"),
    )


def _runtime():
    """Runtime double declaring no error taxonomy (issue #401)."""
    runtime = MagicMock()
    runtime.connector_id = "demo"
    runtime.declared_error_map = None
    runtime.close = AsyncMock()
    return runtime


async def _run(responses, *, checkpoint=None, captured_requests=None):
    """Drive read_batches against a canned worker response stream."""
    checkpoint = checkpoint or _FakeCheckpoint()
    runtime = _runtime()
    handle = MagicMock()
    handle.target = "unix:/tmp/w/worker.sock"
    handle.close = AsyncMock()
    channel = MagicMock()
    channel.close = AsyncMock()
    stub = MagicMock()
    stub.ReadStream = _responses_to_stream(responses, captured_requests)

    with (
        patch("src.worker.readable.build_bootstrap", AsyncMock(return_value={})),
        patch("src.worker.readable.spawn_worker", AsyncMock(return_value=handle)),
        patch(
            "src.worker.readable.grpc.aio.insecure_channel", return_value=channel
        ) as channel_factory,
        patch("src.worker.readable.SourceServiceStub", return_value=stub),
    ):
        batches = [
            b
            async for b in _readable().read_batches(
                runtime,
                {"stream_source": {}},
                checkpoint=checkpoint,
                stream_name="s1",
            )
        ]
    return batches, checkpoint, handle, runtime, channel, channel_factory


def _batch_response(rows):
    batch = pa.RecordBatch.from_pylist(rows)
    return ReadResponse(
        batch=ReadBatchChunk(
            payload=_encode_arrow_ipc(batch), record_count=batch.num_rows
        )
    )


class TestWorkerReadable:
    async def test_batches_and_cursor_saves_relay_to_engine(self):
        responses = [
            _batch_response([{"id": 1}]),
            ReadResponse(
                cursor_save=CursorSave(cursor_json=json.dumps({"cursor": "c1"}))
            ),
            ReadResponse(complete=ReadComplete(total_records=1, total_batches=1)),
        ]
        batches, checkpoint, handle, runtime, channel, _ = await _run(responses)
        assert len(batches) == 1
        assert batches[0].to_pylist() == [{"id": 1}]
        assert checkpoint.saved == [{"cursor": "c1"}]
        # Lifecycle: worker, channel, and runtime are all released.
        handle.close.assert_awaited_once()
        channel.close.assert_awaited_once()
        runtime.close.assert_awaited_once()

    async def test_datetime_cursor_round_trips_both_directions(self):
        # Persisted timestamp cursors come back from the store as datetime
        # objects; both wire hops must round-trip them losslessly.
        ts = datetime(2024, 6, 1, 12, 30, 0, tzinfo=timezone.utc)
        captured_requests = []
        responses = [
            ReadResponse(
                cursor_save=CursorSave(
                    cursor_json=json.dumps(
                        {"cursor": {"__type__": "datetime", "value": ts.isoformat()}}
                    )
                )
            ),
            ReadResponse(complete=ReadComplete()),
        ]
        checkpoint = _FakeCheckpoint(initial={"cursor": ts})
        _, checkpoint, *_ = await _run(
            responses, checkpoint=checkpoint, captured_requests=captured_requests
        )

        # Outbound: the initial datetime cursor is tagged, not str()-ed.
        sent = json.loads(captured_requests[0].initial_cursor_json)
        assert sent == {"cursor": {"__type__": "datetime", "value": ts.isoformat()}}
        # Inbound: the relayed save lands in the store as a datetime again.
        assert checkpoint.saved == [{"cursor": ts}]
        assert isinstance(checkpoint.saved[0]["cursor"], datetime)

    async def test_channel_options_match_worker_server_ceiling(self):
        responses = [ReadResponse(complete=ReadComplete())]
        *_, channel_factory = await _run(responses)
        _, kwargs = channel_factory.call_args
        options = dict(kwargs["options"])
        assert options["grpc.max_receive_message_length"] == DEFAULT_MAX_MESSAGE_SIZE
        assert options["grpc.max_send_message_length"] == DEFAULT_MAX_MESSAGE_SIZE

    async def test_deterministic_error_raises_fatal_read_error(self):
        responses = [
            ReadResponse(
                error=ReadErrorMsg(
                    message="no rule for FANCYTYPE",
                    deterministic=True,
                    error_type="UnmappedTypeError",
                )
            )
        ]
        with pytest.raises(ReadError, match="UnmappedTypeError.*deterministic"):
            await _run(responses)

    async def test_deterministic_error_carries_structured_tag_across_boundary(self):
        # The worker's `deterministic` flag is the structured signal that retrying
        # cannot help. It must survive the gRPC boundary as a FailureTag so the
        # engine classifies the collapsed ReadError by its real cause, not the
        # wrapper type. Type-map miss, secret error, and an opaque deterministic
        # error all floor to CONFIG_INVALID (a setup defect); the connector class
        # rides the error_type prefix.
        for error_type, message in [
            ("UnmappedTypeError", "no rule for FANCYTYPE"),
            ("SecretNotFoundError", "missing ${password}"),
            ("RuntimeError", "boom"),
        ]:
            responses = [
                ReadResponse(
                    error=ReadErrorMsg(
                        message=message,
                        deterministic=True,
                        error_type=error_type,
                    )
                )
            ]
            with pytest.raises(ReadError) as exc_info:
                await _run(responses)
            tag = read_failure_tag(exc_info.value)
            assert tag is not None, error_type
            assert tag.stage is FailureStage.SOURCE_EXTRACT, error_type
            assert tag.code is ErrorCode.CONFIG_INVALID, error_type

    async def test_declared_category_names_the_code_across_the_boundary(self):
        # The worker classified at the birth site and sent the declared
        # category on the wire (issue #401); the engine derives the
        # published code from it instead of flooring to CONFIG_INVALID.
        responses = [
            ReadResponse(
                error=ReadErrorMsg(
                    message="status 401",
                    deterministic=True,
                    error_type="ReadError",
                    declared_category="auth",
                )
            )
        ]
        with pytest.raises(ReadError) as exc_info:
            await _run(responses)
        tag = read_failure_tag(exc_info.value)
        assert tag is not None
        assert tag.code is ErrorCode.SOURCE_AUTH_FAILED

    async def test_declared_category_tags_a_retryable_error(self):
        # A declared rate_limited 403 that exhausts retries must report
        # RATE_LIMITED, not the text split's auth reading of "403". The
        # tag rides the retryable error; retry machinery ignores tags.
        responses = [
            ReadResponse(
                error=ReadErrorMsg(
                    message="status 403",
                    deterministic=False,
                    error_type="TransientReadError",
                    declared_category="rate_limited",
                )
            )
        ]
        with pytest.raises(RuntimeError) as exc_info:
            await _run(responses)
        tag = read_failure_tag(exc_info.value)
        assert tag is not None
        assert tag.code is ErrorCode.RATE_LIMITED

    async def test_retryable_error_raises_runtime_error(self):
        responses = [
            ReadResponse(
                error=ReadErrorMsg(
                    message="db went away",
                    deterministic=False,
                    error_type="ConnectionResetError",
                )
            )
        ]
        with pytest.raises(RuntimeError, match="ConnectionResetError: db went away"):
            await _run(responses)

    async def test_stream_end_without_terminal_event_is_a_crash(self):
        # A worker that dies mid-read just ends the stream; the engine must
        # surface that as a retryable failure, never as a clean completion.
        responses = [_batch_response([{"id": 1}])]
        with pytest.raises(RuntimeError, match="worker crash"):
            await _run(responses)

    async def test_worker_closed_even_when_read_fails(self):
        responses = [
            ReadResponse(
                error=ReadErrorMsg(message="x", deterministic=False, error_type="E")
            )
        ]
        checkpoint = _FakeCheckpoint()
        runtime = _runtime()
        handle = MagicMock()
        handle.target = "unix:/tmp/w/worker.sock"
        handle.close = AsyncMock()
        channel = MagicMock()
        channel.close = AsyncMock()
        stub = MagicMock()
        stub.ReadStream = _responses_to_stream(responses)

        with (
            patch("src.worker.readable.build_bootstrap", AsyncMock(return_value={})),
            patch("src.worker.readable.spawn_worker", AsyncMock(return_value=handle)),
            patch(
                "src.worker.readable.grpc.aio.insecure_channel", return_value=channel
            ),
            patch("src.worker.readable.SourceServiceStub", return_value=stub),
        ):
            with pytest.raises(RuntimeError):
                async for _ in _readable().read_batches(
                    runtime, {}, checkpoint=checkpoint, stream_name="s1"
                ):
                    pass
        handle.close.assert_awaited_once()
        channel.close.assert_awaited_once()
        runtime.close.assert_awaited_once()
