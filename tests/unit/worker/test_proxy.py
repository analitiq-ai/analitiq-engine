"""Tests for the destination-side worker proxy.

Covers the connect-failure paths (worker spawned but unreachable, worker
not answering GetCapabilities) — both must tear the worker down and raise,
never leave an orphan process — and the unconfigured-stream write path.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cdk.types import AckStatus, RetrySemantics, SchemaSpec, WriteMode
from src.worker.proxy import WorkerProxyHandler

# A fixed, timezone-aware emit instant for write_batch/send_batch calls; the
# engine stamps this per batch (issue #353). Value is arbitrary for sinks
# that ignore it.
_EMITTED_AT = datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc)


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
        # get_capabilities now raises on a transport/RPC failure instead of
        # collapsing to None; the proxy must still tear down and re-raise.
        control.get_capabilities = AsyncMock(
            side_effect=ConnectionError("worker channel dropped")
        )
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


class TestProxyConfigureSchema:
    async def test_forwards_engine_ack_budget_to_worker(self):
        """The proxy must forward the engine-stamped ack budget on the UDS
        hop AND adopt it as that hop's own ack wait, so the worker's bound
        and the hop's patience both track the budget the engine actually
        waits on — not the destination container's own environment
        (issue #234)."""
        proxy = _proxy()
        proxy._handle = _handle()
        client = MagicMock()
        client.connect = AsyncMock(return_value=True)
        client.start_stream = AsyncMock(return_value=True)
        client.stream_retry_semantics = None

        with patch(
            "src.worker.proxy.DestinationGRPCClient", return_value=client
        ) as client_cls:
            accepted = await proxy.configure_schema(
                SchemaSpec(
                    stream_id="s1",
                    version=1,
                    write_mode=WriteMode.WRITE_MODE_UPSERT,
                    ack_timeout_seconds=300,
                )
            )

        assert accepted is True
        # The UDS client waits the engine's budget, not the local env default,
        # so an engine-raised budget is honored on the worker hop.
        assert client_cls.call_args.kwargs["timeout_seconds"] == 300
        schema_config = client.start_stream.call_args.kwargs["schema_config"]
        assert schema_config["ack_timeout_seconds"] == 300
        assert schema_config["write_mode"] == "upsert"

    async def test_re_reports_worker_retry_verdict(self):
        """The shell's engine-facing ack must carry the worker's verdict
        (issue #286), not the base handler default."""
        proxy = _proxy()
        proxy._handle = _handle()
        client = MagicMock()
        client.connect = AsyncMock(return_value=True)
        client.start_stream = AsyncMock(return_value=True)
        client.stream_retry_semantics = (
            int(RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE),
            "upsert merges on conflict keys",
        )

        with patch("src.worker.proxy.DestinationGRPCClient", return_value=client):
            accepted = await proxy.configure_schema(
                SchemaSpec(
                    stream_id="s1",
                    version=1,
                    write_mode=WriteMode.WRITE_MODE_UPSERT,
                    ack_timeout_seconds=300,
                )
            )

        assert accepted is True
        verdict = proxy.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE
        assert verdict.reason == "upsert merges on conflict keys"

    async def test_unspecified_worker_verdict_falls_back_to_base_default(self):
        proxy = _proxy()
        proxy._handle = _handle()
        client = MagicMock()
        client.connect = AsyncMock(return_value=True)
        client.start_stream = AsyncMock(return_value=True)
        client.stream_retry_semantics = None

        with patch("src.worker.proxy.DestinationGRPCClient", return_value=client):
            await proxy.configure_schema(
                SchemaSpec(
                    stream_id="s1",
                    version=1,
                    write_mode=WriteMode.WRITE_MODE_INSERT,
                    ack_timeout_seconds=300,
                )
            )

        verdict = proxy.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE
        assert "declares no retry-safety" in verdict.reason


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
            emitted_at=_EMITTED_AT,
        )
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert result.records_written == 0
        assert "not configured" in result.failure_summary

    async def test_worker_death_before_ack_is_retryable_not_fatal(self):
        # A worker that dies before answering rendered no verdict on the
        # batch; the proxy must classify that retryable (idempotency
        # resolves committed-before-crash on resend), never fatal/DLQ.
        import pyarrow as pa

        from src.grpc.client import BatchResult
        from src.grpc.generated.analitiq.v1 import AckStatus as ProtoAckStatus

        proxy = _proxy()
        stream_client = MagicMock()
        stream_client.send_batch = AsyncMock(
            return_value=BatchResult(
                success=False,
                status=ProtoAckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                committed_cursor=None,
                failed_record_ids=[],
                failure_summary="Stream reader/writer task exited before ACK",
                transport_failure=True,
            )
        )
        proxy._streams["s1"] = stream_client

        result = await proxy.write_batch(
            run_id="r1",
            stream_id="s1",
            batch_seq=0,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["a"],
            cursor=None,
            emitted_at=_EMITTED_AT,
        )
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert "before ACK" in result.failure_summary

    async def test_connector_fatal_verdict_stays_fatal(self):
        import pyarrow as pa

        from cdk.types import FailureCategory
        from src.grpc.client import BatchResult
        from src.grpc.generated.analitiq.v1 import AckStatus as ProtoAckStatus

        proxy = _proxy()
        stream_client = MagicMock()
        stream_client.send_batch = AsyncMock(
            return_value=BatchResult(
                success=False,
                status=ProtoAckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                committed_cursor=None,
                failed_record_ids=["a"],
                failure_summary="type-map: no rule for FANCYTYPE",
                failure_category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
            )
        )
        proxy._streams["s1"] = stream_client

        result = await proxy.write_batch(
            run_id="r1",
            stream_id="s1",
            batch_seq=0,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["a"],
            cursor=None,
            emitted_at=_EMITTED_AT,
        )
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        # The declared category is forwarded across the hop like the summary
        # (issue #351); dropping it here would put the engine back on text
        # matching for every worker-run connector.
        assert result.failure_category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT
        # #353: emitted_at must ride the forwarded batch unchanged across the
        # shell->worker hop, or a sandboxed file connector would see the
        # epoch-0 default and fail every partitioned write. It is the last
        # positional arg of the forwarded send_batch.
        assert stream_client.send_batch.await_args.args[-1] == _EMITTED_AT

    async def test_cursor_on_failure_ack_is_dropped_not_fatal_to_stream(self):
        # The ack crosses an untrusted process boundary: a worker pairing
        # a failure status with a cursor violates the BatchWriteResult
        # contract. The proxy must drop the cursor (a failed batch never
        # advances the checkpoint) while preserving the connector's
        # verdict and failure_summary — not let the constructor's
        # invariant abort the whole stream (#129).
        import pyarrow as pa

        from src.grpc.client import BatchResult
        from src.grpc.generated.analitiq.v1 import AckStatus as ProtoAckStatus
        from src.grpc.generated.analitiq.v1 import Cursor as ProtoCursor

        proxy = _proxy()
        stream_client = MagicMock()
        stream_client.send_batch = AsyncMock(
            return_value=BatchResult(
                success=False,
                status=ProtoAckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                committed_cursor=ProtoCursor(token=b"stale"),
                failed_record_ids=["a"],
                failure_summary="connector verdict: bad record",
            )
        )
        proxy._streams["s1"] = stream_client

        result = await proxy.write_batch(
            run_id="r1",
            stream_id="s1",
            batch_seq=0,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["a"],
            cursor=None,
            emitted_at=_EMITTED_AT,
        )
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert result.committed_cursor is None
        assert result.failure_summary == "connector verdict: bad record"
        assert result.failed_record_ids == ("a",)

    async def test_transport_failure_returns_retryable_then_client_self_heals(self):
        # Lazy self-heal: a timeout teardown sets transport_failure and the
        # proxy returns a retryable ACK immediately — it does NOT rebuild during
        # the call. The cached client self-heals on the NEXT write_batch (its
        # own send_batch rebuilds from the params cached at start_stream), so the
        # engine's retry of the same batch succeeds.
        import pyarrow as pa

        from src.grpc.client import BatchResult
        from src.grpc.generated.analitiq.v1 import AckStatus as ProtoAckStatus
        from src.grpc.generated.analitiq.v1 import Cursor as ProtoCursor

        proxy = _proxy()
        proxy._handle = _handle()

        # One cached client across both calls. First send_batch: timeout
        # teardown -> retryable transport_failure. Second send_batch (the
        # client having self-healed internally): clean success.
        client = MagicMock()
        client.send_batch = AsyncMock(
            side_effect=[
                BatchResult(
                    success=False,
                    status=ProtoAckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                    records_written=0,
                    committed_cursor=None,
                    failed_record_ids=[],
                    failure_summary="Timeout waiting for ACK on batch 0",
                    transport_failure=True,
                ),
                BatchResult(
                    success=True,
                    status=ProtoAckStatus.ACK_STATUS_SUCCESS,
                    records_written=1,
                    committed_cursor=ProtoCursor(token=b"c1"),
                    failed_record_ids=[],
                    failure_summary="",
                ),
            ]
        )
        client.disconnect = AsyncMock()
        proxy._streams["s1"] = client

        batch = pa.RecordBatch.from_pylist([{"id": 1}])

        first = await proxy.write_batch(
            run_id="r1",
            stream_id="s1",
            batch_seq=0,
            record_batch=batch,
            record_ids=["a"],
            cursor=None,
            emitted_at=_EMITTED_AT,
        )
        # First attempt: retryable. No rebuild happened in the proxy — the same
        # cached client is retained, untouched, for the engine's retry.
        assert first.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        client.disconnect.assert_not_awaited()
        assert proxy._streams["s1"] is client

        # Retry: same cached client, which has self-healed internally.
        retry = await proxy.write_batch(
            run_id="r1",
            stream_id="s1",
            batch_seq=0,
            record_batch=batch,
            record_ids=["a"],
            cursor=None,
            emitted_at=_EMITTED_AT,
        )
        assert retry.status == AckStatus.ACK_STATUS_SUCCESS
        assert retry.records_written == 1
        assert client.send_batch.await_count == 2

    async def test_write_batch_does_not_block_on_rebuild_before_returning_ack(self):
        # The retryable ACK must return as soon as send_batch resolves — the
        # proxy must not drive any rebuild/open_stream during the call. Patch
        # _open_stream to fail the test if write_batch ever calls it.
        import pyarrow as pa

        from src.grpc.client import BatchResult
        from src.grpc.generated.analitiq.v1 import AckStatus as ProtoAckStatus

        proxy = _proxy()
        proxy._handle = _handle()

        client = MagicMock()
        client.send_batch = AsyncMock(
            return_value=BatchResult(
                success=False,
                status=ProtoAckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                committed_cursor=None,
                failed_record_ids=[],
                failure_summary="Timeout waiting for ACK on batch 0",
                transport_failure=True,
            )
        )
        proxy._streams["s1"] = client

        async def _fail_open_stream(*args, **kwargs):
            raise AssertionError(
                "write_batch must not rebuild the stream before returning the ACK"
            )

        with patch.object(proxy, "_open_stream", _fail_open_stream):
            result = await proxy.write_batch(
                run_id="r1",
                stream_id="s1",
                batch_seq=0,
                record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
                record_ids=["a"],
                cursor=None,
                emitted_at=_EMITTED_AT,
            )
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE


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

    def test_auto_create_and_truncate_forwarded_from_capabilities(self):
        proxy = _proxy()
        # Before connect: both default False, no crash.
        assert proxy.supports_auto_create is False
        assert proxy.supports_truncate is False
        # supports_truncate is derived from the advertised write-mode list, not
        # a dedicated bool — mirror exactly what the worker advertised.
        caps = MagicMock(
            supports_auto_create=True,
            supported_write_modes=[
                WriteMode.WRITE_MODE_INSERT,
                WriteMode.WRITE_MODE_TRUNCATE_INSERT,
            ],
        )
        proxy._capabilities = caps
        assert proxy.supports_auto_create is True
        assert proxy.supports_truncate is True

    def test_truncate_absent_and_auto_create_false_when_not_advertised(self):
        proxy = _proxy()
        caps = MagicMock(
            supports_auto_create=False,
            supported_write_modes=[WriteMode.WRITE_MODE_INSERT],
        )
        proxy._capabilities = caps
        assert proxy.supports_auto_create is False
        assert proxy.supports_truncate is False
