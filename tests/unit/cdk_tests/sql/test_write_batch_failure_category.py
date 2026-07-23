"""write_batch failure acks must declare their failure category (issue #351).

The except chain in ``GenericSQLConnector.write_batch`` is the site that
knows the caught exception type; historically it flattened that knowledge
into a ``failure_summary`` prose prefix (``type-map:`` / ``dialect:`` /
``write-config:`` / ``adbc:``) the engine substring-matched seven hops
later. These tests pin the machine-readable channel: every deterministic
config-defect except declares CONFIG_DEFECT, every attempted-write failure
declares WRITE_REJECTED, and the summary text stays what it was.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pyarrow as pa
import pytest

from cdk.adbc_registry import AdbcConfigurationError
from cdk.sql.dialects import TableAddress
from cdk.sql.exceptions import (
    SchemaConfigurationError,
    UnsupportedDialectOperationError,
)
from cdk.sql.generic import GenericSQLConnector, _StreamState
from cdk.type_map import InvalidTypeMapError, UnmappedTypeError
from cdk.types import AckStatus, Cursor, FailureCategory

pytestmark = pytest.mark.unit

STREAM_ID = "stream-cat"


def _ready_handler(write_error: Exception) -> GenericSQLConnector:
    """A connected ADBC-only handler whose write attempt raises ``write_error``."""
    handler = GenericSQLConnector()
    handler._connected = True
    handler._adbc_only = True
    handler._streams = {STREAM_ID: _StreamState(address=TableAddress(table="t"))}
    handler._write_batch_adbc_only = AsyncMock(side_effect=write_error)
    return handler


async def _write(handler: GenericSQLConnector):
    return await handler.write_batch(
        run_id="run-cat",
        stream_id=STREAM_ID,
        batch_seq=2,
        record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
        record_ids=["1"],
        cursor=Cursor(token=b""),
        emitted_at=datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc),
    )


@pytest.mark.parametrize(
    ("error", "summary_prefix"),
    [
        pytest.param(
            UnmappedTypeError("pg", "reverse", "FANCYTYPE"),
            "type-map:",
            id="unmapped-type",
        ),
        pytest.param(InvalidTypeMapError("bad map"), "type-map:", id="invalid-map"),
        pytest.param(
            UnsupportedDialectOperationError("upsert", dialect="SqlDialect"),
            "dialect:",
            id="dialect",
        ),
        pytest.param(
            SchemaConfigurationError("no conflict keys"),
            "write-config:",
            id="write-config",
        ),
        pytest.param(AdbcConfigurationError("no driver"), "adbc:", id="adbc"),
    ],
)
async def test_deterministic_config_defects_declare_config_defect(
    error, summary_prefix
):
    result = await _write(_ready_handler(error))

    assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
    assert result.failure_category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT
    # The human-readable channel keeps its wording; the category rides beside
    # it, it does not replace it.
    assert result.failure_summary.startswith(summary_prefix)


async def test_generic_write_failure_declares_write_rejected():
    result = await _write(_ready_handler(RuntimeError("duplicate key value")))

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    assert result.failure_category == FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED
    assert "duplicate key value" in result.failure_summary


async def test_driver_timeout_declares_write_rejected():
    # The ADBC path's TimeoutError is a driver/socket timeout on an attempted
    # write, not a pre-flight rejection: WRITE_REJECTED, retryable.
    result = await _write(_ready_handler(TimeoutError()))

    assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
    assert result.failure_category == FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED
