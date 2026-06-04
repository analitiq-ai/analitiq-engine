"""Tests for the destination-side worker proxy.

Covers the connect-failure paths (worker spawned but unreachable, worker
not answering GetCapabilities) — both must tear the worker down and raise,
never leave an orphan process — and the unconfigured-stream write path.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cdk.types import AckStatus
from src.worker.proxy import WorkerProxyHandler


def _proxy():
    return WorkerProxyHandler(
        connectors_dir=Path("/nonexistent/connectors"),
        connections_dir=Path("/nonexistent/connections"),
    )


def _runtime():
    runtime = MagicMock()
    runtime.connector_id = "demo"
    return runtime


def _handle():
    handle = MagicMock()
    handle.target = "unix:/tmp/w/worker.sock"
    handle.close = AsyncMock()
    return handle


class TestProxyConnectFailures:
    async def test_channel_never_connects_tears_down_and_raises(self):
        handle = _handle()
        control = MagicMock()
        control.connect = AsyncMock(return_value=False)
        control.disconnect = AsyncMock()
        control.send_shutdown = AsyncMock()

        with (
            patch("src.worker.proxy.build_bootstrap", AsyncMock(return_value={})),
            patch("src.worker.proxy.spawn_worker", AsyncMock(return_value=handle)),
            patch("src.worker.proxy.DestinationGRPCClient", return_value=control),
        ):
            with pytest.raises(ConnectionError, match="did not connect"):
                await _proxy().connect(_runtime())
        # The spawned worker must not be orphaned.
        handle.close.assert_awaited_once()

    async def test_no_capabilities_answer_tears_down_and_raises(self):
        handle = _handle()
        control = MagicMock()
        control.connect = AsyncMock(return_value=True)
        control.get_capabilities = AsyncMock(return_value=None)
        control.disconnect = AsyncMock()
        control.send_shutdown = AsyncMock()

        with (
            patch("src.worker.proxy.build_bootstrap", AsyncMock(return_value={})),
            patch("src.worker.proxy.spawn_worker", AsyncMock(return_value=handle)),
            patch("src.worker.proxy.DestinationGRPCClient", return_value=control),
        ):
            with pytest.raises(ConnectionError, match="GetCapabilities"):
                await _proxy().connect(_runtime())
        handle.close.assert_awaited_once()


class TestProxyWriteBatch:
    async def test_write_before_configure_is_retryable_not_silent(self):
        import pyarrow as pa

        result = await _proxy().write_batch(
            run_id="r1",
            stream_id="s1",
            batch_seq=0,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["a"],
            cursor=None,
        )
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.records_written == 0
        assert "not configured" in result.failure_summary


class TestProxyCapabilities:
    def test_capability_passthrough_with_fallbacks(self):
        proxy = _proxy()
        # Before connect: safe defaults, no crash.
        assert proxy.connector_type == "unknown"
        assert proxy.supports_upsert is False
        caps = MagicMock(
            connector_type="database",
            supports_transactions=True,
            supports_upsert=True,
            supports_bulk_load=True,
            max_batch_size=500,
            max_batch_bytes=0,
        )
        proxy._capabilities = caps
        assert proxy.connector_type == "database"
        assert proxy.supports_upsert is True
        assert proxy.max_batch_size == 500
        # Zero means "worker did not declare" — fall back to the base default.
        assert proxy.max_batch_bytes == 8 * 1024 * 1024
