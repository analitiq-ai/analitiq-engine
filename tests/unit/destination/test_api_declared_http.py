"""Declared error_map consumption in the API destination + source (#401).

The http family classifies a status before the built-in 4xx heuristics;
the declared verdict carries the failure category onto the ack, so the
engine classifies structurally instead of from text.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pyarrow as pa
import pytest

from cdk.declarations import parse_declared_error_map
from cdk.types import AckStatus, Cursor, FailureCategory
from src.destination.connectors.api import (
    ApiDestinationHandler,
    _classify_http_error,
    _http_verdict,
    _StreamState,
)
from src.source.connectors.api import (
    ReadError,
    TransientReadError,
    _read_error_for_status,
)


def _map(block):
    parsed = parse_declared_error_map(block)
    assert parsed is not None
    return parsed


def _response_error(status: int) -> aiohttp.ClientResponseError:
    return aiohttp.ClientResponseError(
        request_info=MagicMock(), history=(), status=status
    )


class TestDestinationHttpVerdict:
    def test_declared_rate_limited_is_retryable(self):
        # 402 would be FATAL under the heuristic; the declaration wins.
        error_map = _map({"http": {"402": "rate_limited"}})
        status, category = _http_verdict(_response_error(402), error_map)
        assert status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert category == FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED

    def test_declared_auth_is_fatal_config_defect(self):
        error_map = _map({"http": {"401": "auth"}})
        status, category = _http_verdict(_response_error(401), error_map)
        assert status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT

    def test_undeclared_status_keeps_heuristic_with_unspecified_category(self):
        error_map = _map({"http": {"401": "auth"}})
        status, category = _http_verdict(_response_error(404), error_map)
        assert status == _classify_http_error(_response_error(404))
        assert category == FailureCategory.FAILURE_CATEGORY_UNSPECIFIED

    def test_no_map_keeps_heuristic(self):
        status, category = _http_verdict(_response_error(429), None)
        assert status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert category == FailureCategory.FAILURE_CATEGORY_UNSPECIFIED

    def test_non_response_errors_never_consult_the_map(self):
        error_map = _map({"http": {"401": "auth"}})
        status, category = _http_verdict(
            aiohttp.ClientConnectionError("refused"), error_map
        )
        assert status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert category == FailureCategory.FAILURE_CATEGORY_UNSPECIFIED


class TestDestinationConnectWiring:
    """The declaration flows runtime -> connect() -> write ack.

    The verdict tests above feed ``_http_verdict`` directly, so a dropped
    ``error_map_for`` line in ``connect()`` would pass them all while
    silently disabling the feature.
    """

    async def test_declared_category_rides_the_write_ack(self):
        handler = ApiDestinationHandler()
        runtime = MagicMock()
        runtime.connector_id = "demo"
        runtime.declared_error_map = {"http": {"402": "rate_limited"}}
        runtime.materialize = AsyncMock()
        runtime.raw_config = {}
        # connect() lowercases the session's default header names.
        runtime.session.headers = {}
        await handler.connect(runtime)

        # Stream state is orthogonal plumbing; the wiring under test is
        # the declaration's path from the runtime into the ack.
        handler._streams["s1"] = _StreamState(endpoint="/things")
        handler._send_request = AsyncMock(side_effect=_response_error(402))

        result = await handler.write_batch(
            run_id="r1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["rec-1"],
            cursor=Cursor(),
            emitted_at=datetime.now(timezone.utc),
        )
        # 402 would be FATAL under the heuristic; the declared category
        # makes the ack RETRYABLE and carries the structured category.
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert (
            result.failure_category == FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED
        )

    async def test_per_record_fatal_failure_keeps_the_declared_category(self):
        # A declared fatal status handled inside single-record mode must not
        # lose its category to the batch verdict's UNSPECIFIED default —
        # the engine would then fall back to text for a declared failure.
        handler = ApiDestinationHandler()
        runtime = MagicMock()
        runtime.connector_id = "demo"
        runtime.declared_error_map = {"http": {"401": "auth"}}
        runtime.materialize = AsyncMock()
        runtime.raw_config = {}
        runtime.session.headers = {}
        await handler.connect(runtime)

        handler._streams["s1"] = _StreamState(endpoint="/things")
        handler._send_request = AsyncMock(side_effect=_response_error(401))

        result = await handler.write_batch(
            run_id="r1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}, {"id": 2}]),
            record_ids=["rec-1", "rec-2"],
            cursor=Cursor(),
            emitted_at=datetime.now(timezone.utc),
        )
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.failure_category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT


class TestSourceHttpReadError:
    def test_declared_transient_status_retries(self):
        # 403 is deterministic under the heuristic; declared rate_limited
        # makes it retryable.
        error_map = _map({"http": {"403": "rate_limited"}})
        err = _read_error_for_status(403, "detail", error_map)
        assert isinstance(err, TransientReadError)

    def test_declared_auth_status_is_deterministic(self):
        # 503 retries under the heuristic; declared auth pins it fatal.
        error_map = _map({"http": {"503": "auth"}})
        err = _read_error_for_status(503, "detail", error_map)
        assert isinstance(err, ReadError)
        assert not isinstance(err, TransientReadError)

    @pytest.mark.parametrize(
        ("status", "expected"),
        [(429, TransientReadError), (404, ReadError), (500, TransientReadError)],
    )
    def test_undeclared_statuses_keep_the_heuristic(self, status, expected):
        error_map = _map({"http": {"402": "auth"}})
        err = _read_error_for_status(status, "detail", error_map)
        assert type(err) is expected

    def test_no_map_keeps_the_heuristic(self):
        assert isinstance(
            _read_error_for_status(429, "detail", None), TransientReadError
        )
        err = _read_error_for_status(400, "detail", None)
        assert isinstance(err, ReadError)
