"""Tests for the source worker servicer.

Pins the worker-side read contract: Arrow IPC encoding, ordered cursor
relay, and — most load-bearing — the deterministic-vs-retryable error
classification the engine uses to decide between failing a stream fatally
and retrying it.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from cdk.exceptions import ReadError, TransientReadError, TransportSpecError
from cdk.type_map import UnmappedTypeError
from src.grpc.generated.analitiq.v1.source_service_pb2 import ReadRequest
from src.state.store import decode_cursor_state
from src.worker.readable import _decode_arrow_ipc
from src.worker.source_service import (
    SourceWorkerServicer,
    _encode_arrow_ipc,
    _RelayCheckpoint,
)


class _FakeReadable:
    """A Readable that yields canned batches and drives the checkpoint."""

    def __init__(
        self,
        batches,
        *,
        cursor_values=None,
        error=None,
        trailing_cursor=None,
        classify_error=None,
    ):
        self._batches = batches
        self._cursor_values = cursor_values or []
        self._error = error
        self._trailing_cursor = trailing_cursor
        self._classify_error = classify_error

    async def read_batches(
        self,
        runtime,
        config,
        *,
        checkpoint,
        stream_name,
        partition=None,
        batch_size=1000,
    ):
        for i, batch in enumerate(self._batches):
            yield batch
            if i < len(self._cursor_values):
                await checkpoint.save_cursor(
                    stream_name, partition, {"cursor": self._cursor_values[i]}
                )
        if self._error is not None:
            raise self._error
        if self._trailing_cursor is not None:
            await checkpoint.save_cursor(
                stream_name, partition, {"cursor": self._trailing_cursor}
            )

    def classify_error(self, exc):
        """The connector-owned code escape hatch (issue #513); None by default."""
        if self._classify_error is None:
            return None
        return self._classify_error(exc)


def _batch(rows):
    return pa.RecordBatch.from_pylist(rows)


def _runtime():
    """Runtime double declaring no error taxonomy (issue #401)."""
    runtime = MagicMock()
    runtime.connector_id = "demo"
    runtime.declared_error_map = None
    return runtime


class OperationalError(Exception):
    """Bears a driver exception name for the declared-error-map tests."""


class _BareReadable:
    """A Readable satisfying only the protocol's read_batches -- no
    classify_error at all, since BaseDestinationHandler isn't a required
    base for a source-only connector (issue #513)."""

    def __init__(self, error):
        self._error = error

    async def read_batches(self, runtime, config, *, checkpoint, stream_name, **kw):
        raise self._error
        yield  # pragma: no cover -- makes this an async generator


async def _collect(servicer, request=None):
    request = request or ReadRequest(stream_name="s1", batch_size=10)
    return [r async for r in servicer.ReadStream(request, MagicMock())]


class TestRelayCheckpoint:
    async def test_get_cursor_answers_from_initial(self):
        relay = _RelayCheckpoint({"cursor": "2024-01-01"})
        assert await relay.get_cursor("s1") == {"cursor": "2024-01-01"}
        relay_empty = _RelayCheckpoint(None)
        assert await relay_empty.get_cursor("s1") is None

    async def test_save_cursor_queues_in_order(self):
        relay = _RelayCheckpoint(None)
        await relay.save_cursor("s1", {}, {"cursor": 1})
        await relay.save_cursor("s1", {}, {"cursor": 2})
        assert relay.pending == [{"cursor": 1}, {"cursor": 2}]


class TestArrowIpcRoundTrip:
    def test_round_trip(self):
        batch = _batch([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        decoded = _decode_arrow_ipc(_encode_arrow_ipc(batch))
        assert decoded.equals(batch)

    def test_empty_batch_round_trip(self):
        batch = pa.RecordBatch.from_pylist([], schema=pa.schema([("id", pa.int64())]))
        decoded = _decode_arrow_ipc(_encode_arrow_ipc(batch))
        assert decoded.num_rows == 0
        assert decoded.schema == batch.schema


class TestReadStream:
    async def test_batches_cursor_saves_and_complete(self):
        readable = _FakeReadable(
            [_batch([{"id": 1}]), _batch([{"id": 2}, {"id": 3}])],
            cursor_values=["c1", "c2"],
        )
        servicer = SourceWorkerServicer(readable, _runtime(), {})
        responses = await _collect(servicer)

        kinds = [r.WhichOneof("message") for r in responses]
        # Pull-based generators drain batch N's save one iteration late;
        # what matters is order preservation and the trailing drain.
        assert kinds == ["batch", "batch", "cursor_save", "cursor_save", "complete"]
        saved = [
            json.loads(r.cursor_save.cursor_json)
            for r in responses
            if r.WhichOneof("message") == "cursor_save"
        ]
        assert saved == [{"cursor": "c1"}, {"cursor": "c2"}]
        complete = responses[-1].complete
        assert complete.total_records == 3
        assert complete.total_batches == 2

    async def test_initial_cursor_reaches_the_connector(self):
        captured = {}

        class _CursorProbe(_FakeReadable):
            async def read_batches(
                self,
                runtime,
                config,
                *,
                checkpoint,
                stream_name,
                partition=None,
                batch_size=1000,
            ):
                captured["initial"] = await checkpoint.get_cursor(stream_name)
                return
                yield  # pragma: no cover — makes this an async generator

        servicer = SourceWorkerServicer(_CursorProbe([]), _runtime(), {})
        request = ReadRequest(
            stream_name="s1",
            initial_cursor_json=json.dumps({"cursor": "2024-06-01"}),
        )
        await _collect(servicer, request)
        assert captured["initial"] == {"cursor": "2024-06-01"}

    @pytest.mark.parametrize(
        "exc",
        [
            ReadError("bad endpoint document"),
            UnmappedTypeError("demo", "forward", "FANCYTYPE"),
            # A transport-spec/value-expression authoring defect that
            # escapes a connector unwrapped is deterministic by its own
            # contract — retrying cannot heal a config error.
            TransportSpecError("conflicting expression markers"),
            KeyError("endpoint_document"),
            TypeError("x"),
            ValueError("y"),
        ],
    )
    async def test_deterministic_errors_marked_fatal(self, exc):
        readable = _FakeReadable([_batch([{"id": 1}])], error=exc)
        servicer = SourceWorkerServicer(readable, _runtime(), {})
        responses = await _collect(servicer)
        terminal = responses[-1]
        assert terminal.WhichOneof("message") == "error"
        assert terminal.error.deterministic is True
        assert terminal.error.error_type == type(exc).__name__

    @pytest.mark.parametrize(
        "exc",
        [
            ConnectionResetError("db went away"),
            # Transient API failures (429/5xx) heal on retry and must not
            # classify deterministic alongside ReadError.
            TransientReadError("API request failed: status 503"),
        ],
    )
    async def test_runtime_errors_marked_retryable(self, exc):
        readable = _FakeReadable([], error=exc)
        servicer = SourceWorkerServicer(readable, _runtime(), {})
        responses = await _collect(servicer)
        terminal = responses[-1]
        assert terminal.WhichOneof("message") == "error"
        assert terminal.error.deterministic is False
        assert str(exc) in terminal.error.message

    async def test_declared_transient_marks_retryable(self):
        # Wiring, not helper logic: the map parsed in __init__ must reach
        # ReadStream's classification without private-attr injection.
        runtime = _runtime()
        runtime.declared_error_map = {
            "key_attrs": ["__exception_class__"],
            "codes": {"OperationalError": "transient"},
        }
        readable = _FakeReadable([], error=OperationalError("server went away"))
        servicer = SourceWorkerServicer(readable, runtime, {})
        responses = await _collect(servicer)
        terminal = responses[-1]
        assert terminal.WhichOneof("message") == "error"
        assert terminal.error.deterministic is False

    async def test_declared_config_marks_deterministic(self):
        # The load-bearing inverse: OperationalError is outside the
        # deterministic type ladder, so deterministic=True can only come
        # from the declaration having reached the classification.
        runtime = _runtime()
        runtime.declared_error_map = {
            "key_attrs": ["__exception_class__"],
            "codes": {"OperationalError": "config"},
        }
        readable = _FakeReadable([], error=OperationalError("bad search_path"))
        servicer = SourceWorkerServicer(readable, runtime, {})
        responses = await _collect(servicer)
        terminal = responses[-1]
        assert terminal.WhichOneof("message") == "error"
        assert terminal.error.deterministic is True

    async def test_classify_error_reaches_the_classification_through_readstream(self):
        # Wiring for the classify_error hook (issue #513): no declared map
        # at all, so only the connector instance's classify_error can be
        # the source of a deterministic=True verdict here.
        runtime = _runtime()
        readable = _FakeReadable(
            [],
            error=OperationalError("bad search_path"),
            classify_error=lambda exc: "config",
        )
        servicer = SourceWorkerServicer(readable, runtime, {})
        responses = await _collect(servicer)
        terminal = responses[-1]
        assert terminal.WhichOneof("message") == "error"
        assert terminal.error.deterministic is True
        assert terminal.error.declared_category == "config"

    async def test_a_crashing_classify_error_does_not_crash_the_stream(self):
        # A connector's classify_error is untrusted, potentially AI-authored
        # code; a bug in it must not replace the original read failure with
        # an unhandled exception out of ReadStream.
        runtime = _runtime()

        def _broken(exc):
            raise RuntimeError("connector bug")

        readable = _FakeReadable(
            [], error=OperationalError("bad search_path"), classify_error=_broken
        )
        servicer = SourceWorkerServicer(readable, runtime, {})
        responses = await _collect(servicer)
        terminal = responses[-1]
        assert terminal.WhichOneof("message") == "error"
        assert "OperationalError" in terminal.error.error_type

    async def test_a_readable_with_no_classify_error_at_all_does_not_crash(self):
        # The Readable protocol declares only read_batches -- classify_error
        # comes from BaseDestinationHandler, which a source-only connector
        # need not inherit. Reading the attribute must not itself raise,
        # one expression before the guard that exists precisely to stop a
        # hook from displacing the failure being reported.
        runtime = _runtime()
        readable = _BareReadable(OperationalError("bad search_path"))
        servicer = SourceWorkerServicer(readable, runtime, {})
        responses = await _collect(servicer)
        terminal = responses[-1]
        assert terminal.WhichOneof("message") == "error"
        assert "OperationalError" in terminal.error.error_type

    async def test_an_off_vocabulary_declared_category_still_reaches_the_stream(self):
        # ReadError accepts any declared_category with no vocabulary check;
        # a connector raising one with a typo'd value must still produce
        # the typed ReadResponse(error=...) event, not an unhandled
        # exception out of ReadStream itself.
        runtime = _runtime()
        readable = _FakeReadable(
            [], error=ReadError("quota exceeded", declared_category="quota_exceeded")
        )
        servicer = SourceWorkerServicer(readable, runtime, {})
        responses = await _collect(servicer)
        terminal = responses[-1]
        assert terminal.WhichOneof("message") == "error"
        assert "quota exceeded" in terminal.error.message

    async def test_error_ends_stream_without_complete(self):
        readable = _FakeReadable([_batch([{"id": 1}])], error=ValueError("x"))
        servicer = SourceWorkerServicer(readable, _runtime(), {})
        responses = await _collect(servicer)
        kinds = [r.WhichOneof("message") for r in responses]
        assert kinds[-1] == "error"
        assert "complete" not in kinds

    async def test_trailing_cursor_save_after_generator_end(self):
        readable = _FakeReadable([_batch([{"id": 1}])], trailing_cursor="final")
        servicer = SourceWorkerServicer(readable, _runtime(), {})
        responses = await _collect(servicer)
        kinds = [r.WhichOneof("message") for r in responses]
        assert kinds == ["batch", "cursor_save", "complete"]
        assert json.loads(responses[1].cursor_save.cursor_json) == {"cursor": "final"}

    async def test_datetime_cursor_survives_the_json_relay(self):
        # Database timestamp cursors arrive as datetime objects; plain
        # json.dumps would raise TypeError (and, being in the deterministic
        # tuple, fail the stream fatally). The relay must tag them instead.
        ts = datetime(2024, 6, 1, 12, 30, 0, tzinfo=timezone.utc)
        readable = _FakeReadable([_batch([{"id": 1}])], cursor_values=[ts])
        servicer = SourceWorkerServicer(readable, _runtime(), {})
        responses = await _collect(servicer)

        kinds = [r.WhichOneof("message") for r in responses]
        assert kinds == ["batch", "cursor_save", "complete"]
        relayed = decode_cursor_state(json.loads(responses[1].cursor_save.cursor_json))
        assert relayed == {"cursor": ts}
        assert isinstance(relayed["cursor"], datetime)

    async def test_datetime_initial_cursor_decoded_for_the_connector(self):
        ts = datetime(2024, 6, 1, 12, 30, 0, tzinfo=timezone.utc)
        captured = {}

        class _CursorProbe(_FakeReadable):
            async def read_batches(
                self,
                runtime,
                config,
                *,
                checkpoint,
                stream_name,
                partition=None,
                batch_size=1000,
            ):
                captured["initial"] = await checkpoint.get_cursor(stream_name)
                return
                yield  # pragma: no cover — makes this an async generator

        servicer = SourceWorkerServicer(_CursorProbe([]), _runtime(), {})
        request = ReadRequest(
            stream_name="s1",
            initial_cursor_json=json.dumps(
                {"cursor": {"__type__": "datetime", "value": ts.isoformat()}}
            ),
        )
        await _collect(servicer, request)
        assert captured["initial"] == {"cursor": ts}
        assert isinstance(captured["initial"]["cursor"], datetime)
