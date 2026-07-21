"""Every ``write_batch`` pre-flight guard must say why it rejected a batch.

A guard rejects without raising, so nothing downstream records the reason:
the engine sees a RETRYABLE ack and retries, and the first readable signal
is retry exhaustion (issue #327). These tests pin the invariant across all
five destination handlers, so a new guard added without a log fails CI.

The assertions are deliberately on the log *content* (run/stream/seq and a
reason), not on an exact message: an operator correlating a DLQ entry with
a log needs those three identifiers, whatever the surrounding wording.
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from cdk.sql.generic import GenericSQLConnector
from cdk.types import AckStatus, Cursor
from src.destination.connectors.api import ApiDestinationHandler
from src.destination.connectors.file import FileDestinationHandler
from src.destination.connectors.stream import StreamDestinationHandler
from src.worker.proxy import WorkerProxyHandler

RUN_ID = "run-guard"
STREAM_ID = "stream-guard"
BATCH_SEQ = 7


def _record_batch() -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([{"id": 1}])


async def _write(handler) -> object:
    return await handler.write_batch(
        run_id=RUN_ID,
        stream_id=STREAM_ID,
        batch_seq=BATCH_SEQ,
        record_batch=_record_batch(),
        record_ids=["1"],
        cursor=Cursor(token=b""),
    )


def _assert_rejection_logged(caplog: pytest.LogCaptureFixture) -> str:
    """Return the single ERROR record's message, asserting it identifies the batch."""
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1, f"expected one ERROR record, got {errors}"
    message = errors[0].getMessage()
    for identifier in (RUN_ID, STREAM_ID, str(BATCH_SEQ)):
        assert identifier in message, f"{identifier!r} missing from {message!r}"
    return message


def _disconnected_file() -> FileDestinationHandler:
    handler = FileDestinationHandler()
    handler._connected = False
    return handler


def _uninitialized_file() -> FileDestinationHandler:
    """Connected, but connect() never built the components."""
    handler = FileDestinationHandler()
    handler._connected = True
    handler._storage = None
    handler._formatter = None
    handler._manifest = None
    return handler


def _disconnected_stream() -> StreamDestinationHandler:
    handler = StreamDestinationHandler()
    handler._connected = False
    return handler


def _disconnected_api() -> ApiDestinationHandler:
    handler = ApiDestinationHandler()
    handler._connected = False
    handler._session = None
    return handler


def _unconfigured_api() -> ApiDestinationHandler:
    handler = ApiDestinationHandler()
    handler._connected = True
    handler._session = MagicMock()
    handler._streams = {}
    return handler


def _disconnected_sql() -> GenericSQLConnector:
    handler = GenericSQLConnector()
    handler._connected = False
    return handler


def _engineless_sql() -> GenericSQLConnector:
    """Connected on the SQLAlchemy path, but no engine was ever built."""
    handler = GenericSQLConnector()
    handler._connected = True
    handler._adbc_only = False
    handler._engine = None
    handler._sync_engine = None
    return handler


def _unconfigured_sql() -> GenericSQLConnector:
    handler = GenericSQLConnector()
    handler._connected = True
    handler._adbc_only = True
    handler._streams = {}
    return handler


def _unconfigured_proxy() -> WorkerProxyHandler:
    handler = WorkerProxyHandler(
        connectors_dir=Path("/nonexistent/connectors"),
        connections_dir=Path("/nonexistent/connections"),
    )
    handler._streams = {}
    return handler


ALL_GUARDS = [
    pytest.param(_disconnected_file, id="file-disconnected"),
    pytest.param(_uninitialized_file, id="file-uninitialized"),
    pytest.param(_disconnected_stream, id="stream-disconnected"),
    pytest.param(_disconnected_api, id="api-disconnected"),
    pytest.param(_unconfigured_api, id="api-unconfigured"),
    pytest.param(_disconnected_sql, id="sql-disconnected"),
    pytest.param(_engineless_sql, id="sql-engineless"),
    pytest.param(_unconfigured_sql, id="sql-unconfigured"),
    pytest.param(_unconfigured_proxy, id="proxy-unconfigured"),
]


@pytest.mark.parametrize("build_handler", ALL_GUARDS)
async def test_guard_logs_and_returns_retryable(
    build_handler, caplog: pytest.LogCaptureFixture
):
    with caplog.at_level(logging.ERROR):
        result = await _write(build_handler())

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    assert result.records_written == 0
    assert result.failure_summary
    # The ack and the log must carry the same reason, so an operator reading
    # either one reaches the same conclusion.
    assert result.failure_summary in _assert_rejection_logged(caplog)


async def test_file_uninitialized_guard_names_the_missing_components(
    caplog: pytest.LogCaptureFixture,
):
    """A bare "components not initialized" is useless without naming them (#328)."""
    handler = _uninitialized_file()
    handler._storage = MagicMock()  # only formatter and manifest are missing

    with caplog.at_level(logging.ERROR):
        result = await _write(handler)

    message = _assert_rejection_logged(caplog)
    assert "formatter" in message and "manifest" in message
    assert "storage" not in result.failure_summary
