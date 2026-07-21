"""Tests for DestinationServicer protocol behavior.

Focuses on contract-level responses the engine relies on:
when ``configure_schema`` raises a deterministic type-map error, the
servicer must yield a ``SchemaAck(accepted=False, message="type-map: …")``
rather than aborting the stream. A regression that re-wraps the exception
or drops the prefix would ship silently without this test.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from cdk.base_handler import BatchWriteResult
from cdk.type_map import InvalidTypeMapError, UnmappedTypeError
from cdk.types import Cursor as CdkCursor
from cdk.types import FailureCategory as CdkFailureCategory
from cdk.types import RetrySemantics, RetryVerdict
from src.destination.connectors.api import ApiDestinationHandler
from src.destination.server import DestinationServicer
from src.grpc.generated.analitiq.v1 import (
    AckStatus,
    Cursor,
    GetCapabilitiesRequest,
    PayloadFormat,
    RecordBatch,
)
from src.grpc.generated.analitiq.v1 import RetrySemantics as WireRetrySemantics
from src.grpc.generated.analitiq.v1 import SchemaMessage, StreamRequest, WriteMode


def _stub_retry_semantics(handler) -> None:
    """Give a MagicMock handler a real verdict: the servicer stamps it into
    the accepted SchemaAck, and protobuf rejects a MagicMock value."""
    handler.retry_semantics = MagicMock(
        return_value=RetryVerdict(
            semantics=RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE,
            reason="test stub",
        )
    )


async def _iter_once(msg: StreamRequest) -> AsyncIterator[StreamRequest]:
    yield msg


async def _iter_many(*msgs: StreamRequest) -> AsyncIterator[StreamRequest]:
    for msg in msgs:
        yield msg


def _schema_request(
    stream_id: str = "s1",
    *,
    version: int = 1,
    write_mode: int = WriteMode.WRITE_MODE_INSERT,
    ack_timeout_seconds: int = 30,
) -> StreamRequest:
    return StreamRequest(
        schema=SchemaMessage(
            stream_id=stream_id,
            version=version,
            write_mode=write_mode,
            ack_timeout_seconds=ack_timeout_seconds,
        )
    )


def _arrow_ipc(batch: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _batch_request(*, stream_id: str = "s1", token: bytes = b"") -> StreamRequest:
    batch = pa.RecordBatch.from_pydict({"id": [1]})
    return StreamRequest(
        batch=RecordBatch(
            run_id="r1",
            stream_id=stream_id,
            batch_seq=0,
            format=PayloadFormat.PAYLOAD_FORMAT_ARROW_IPC,
            payload=_arrow_ipc(batch),
            record_count=1,
            record_ids=["1"],
            cursor=Cursor(token=token),
        )
    )


class TestSchemaAckTypeMapError:
    @pytest.mark.asyncio
    async def test_unmapped_type_error_is_surfaced_in_schema_ack(self):
        handler = MagicMock()
        handler.configure_schema = AsyncMock(
            side_effect=UnmappedTypeError("pg", "forward", "MONEY")
        )

        servicer = DestinationServicer(handler, server=MagicMock())
        responses = []
        async for resp in servicer.StreamRecords(
            _iter_once(_schema_request()), context=MagicMock()
        ):
            responses.append(resp)

        assert len(responses) == 1
        ack = responses[0].schema_ack
        assert ack.stream_id == "s1"
        assert ack.accepted is False
        assert ack.message.startswith("type-map: ")
        assert "MONEY" in ack.message

    @pytest.mark.asyncio
    async def test_invalid_type_map_error_is_surfaced_in_schema_ack(self):
        handler = MagicMock()
        handler.configure_schema = AsyncMock(
            side_effect=InvalidTypeMapError("rule 3 uses lookahead")
        )

        servicer = DestinationServicer(handler, server=MagicMock())
        responses = []
        async for resp in servicer.StreamRecords(
            _iter_once(_schema_request("s2")), context=MagicMock()
        ):
            responses.append(resp)

        assert len(responses) == 1
        ack = responses[0].schema_ack
        assert ack.accepted is False
        assert ack.message.startswith("type-map: ")
        assert "lookahead" in ack.message

    @pytest.mark.asyncio
    async def test_generic_false_return_gets_generic_message(self):
        """Non-type-map ``False`` returns still get the generic message so
        the type-map-specific path is distinguishable from other config
        failures."""
        handler = MagicMock()
        handler.configure_schema = AsyncMock(return_value=False)
        # A bare MagicMock auto-creates a truthy last_schema_rejection, which
        # the servicer would forward instead of the generic message; a handler
        # with no recorded reason exposes None.
        handler.last_schema_rejection = None

        servicer = DestinationServicer(handler, server=MagicMock())
        responses = []
        async for resp in servicer.StreamRecords(
            _iter_once(_schema_request("s3")), context=MagicMock()
        ):
            responses.append(resp)

        assert responses[0].schema_ack.accepted is False
        assert responses[0].schema_ack.message == "Schema configuration failed"

    @pytest.mark.asyncio
    async def test_generic_exception_still_aborts_stream(self):
        """Non-type-map exceptions continue to abort the stream via the
        outer except — we don't want to paper over unknown failures."""
        handler = MagicMock()
        handler.configure_schema = AsyncMock(
            side_effect=RuntimeError("something else broke")
        )

        servicer = DestinationServicer(handler, server=MagicMock())
        with pytest.raises(RuntimeError, match="something else broke"):
            async for _ in servicer.StreamRecords(
                _iter_once(_schema_request("s4")), context=MagicMock()
            ):
                pass

    @pytest.mark.asyncio
    async def test_adbc_configuration_error_is_surfaced_in_schema_ack(self):
        """configure_schema deliberately propagates AdbcConfigurationError
        (e.g. _engine None during DDL); the servicer must translate it to a
        rejected SchemaAck instead of crashing the stream."""
        from cdk.adbc_registry import AdbcConfigurationError

        handler = MagicMock()
        handler.configure_schema = AsyncMock(
            side_effect=AdbcConfigurationError(
                "SQLAlchemy engine is None during DDL for public.events; "
                "connect() must be called before configure_schema()"
            )
        )

        servicer = DestinationServicer(handler, server=MagicMock())
        responses = []
        async for resp in servicer.StreamRecords(
            _iter_once(_schema_request("s5")), context=MagicMock()
        ):
            responses.append(resp)

        assert len(responses) == 1
        ack = responses[0].schema_ack
        assert ack.accepted is False
        assert ack.message.startswith("AdbcConfigurationError: ")
        assert "public.events" in ack.message

    @pytest.mark.asyncio
    async def test_schema_configuration_error_is_surfaced_in_schema_ack(self):
        """SchemaConfigurationError is the typed signal for intentional
        config errors in configure_schema (unsupported write mode, column
        without name/native_type); the servicer must translate it to a
        rejected SchemaAck instead of crashing the stream."""
        from cdk.sql.exceptions import SchemaConfigurationError

        handler = MagicMock()
        handler.configure_schema = AsyncMock(
            side_effect=SchemaConfigurationError(
                "Unsupported proto write_mode=99; expected one of [1, 2, 3]"
            )
        )

        servicer = DestinationServicer(handler, server=MagicMock())
        responses = []
        async for resp in servicer.StreamRecords(
            _iter_once(_schema_request("s7")), context=MagicMock()
        ):
            responses.append(resp)

        assert len(responses) == 1
        ack = responses[0].schema_ack
        assert ack.accepted is False
        assert ack.message.startswith("SchemaConfigurationError: ")
        assert "write_mode=99" in ack.message

    @pytest.mark.asyncio
    async def test_unsupported_dialect_error_is_surfaced_in_schema_ack(self):
        """UnsupportedDialectOperationError is in configure_schema's
        propagate tuple; it must land in the SchemaAck as well."""
        from cdk.sql.exceptions import UnsupportedDialectOperationError

        handler = MagicMock()
        handler.configure_schema = AsyncMock(
            side_effect=UnsupportedDialectOperationError(
                "build_sqlalchemy_upsert", dialect="ansi"
            )
        )

        servicer = DestinationServicer(handler, server=MagicMock())
        responses = []
        async for resp in servicer.StreamRecords(
            _iter_once(_schema_request("s6")), context=MagicMock()
        ):
            responses.append(resp)

        assert len(responses) == 1
        ack = responses[0].schema_ack
        assert ack.accepted is False
        assert ack.message.startswith("UnsupportedDialectOperationError: ")


class TestSchemaAckBudget:
    """The sender stamps its gRPC ack budget on the handshake; the servicer
    derives the destination statement timeout from it before configure_schema
    runs DDL (issues #231, #234). A regression here either re-opens the
    orphaned-statement window or lets a budget-less handshake run unbounded.
    """

    @pytest.mark.asyncio
    async def test_statement_timeout_set_from_wire_budget_before_configure(self):
        calls: list[str] = []
        handler = MagicMock()
        handler.set_statement_timeout = MagicMock(
            side_effect=lambda _s: calls.append("set_statement_timeout")
        )

        async def _configure(_spec):
            calls.append("configure_schema")
            return True

        handler.configure_schema = AsyncMock(side_effect=_configure)
        _stub_retry_semantics(handler)
        servicer = DestinationServicer(handler, server=MagicMock())

        async for _ in servicer.StreamRecords(
            _iter_once(_schema_request(ack_timeout_seconds=30)),
            context=MagicMock(),
        ):
            pass

        # Derived from the wire budget (30 - 5s margin), not from any env var.
        handler.set_statement_timeout.assert_called_once_with(25.0)
        # The bound must be in place before configure_schema runs its DDL.
        assert calls == ["set_statement_timeout", "configure_schema"]

    @pytest.mark.asyncio
    async def test_missing_ack_budget_rejects_schema(self):
        """A handshake without the stamped budget cannot bound statements
        below the sender's wait — reject loudly instead of running
        unbounded."""
        handler = MagicMock()
        handler.configure_schema = AsyncMock(return_value=True)
        servicer = DestinationServicer(handler, server=MagicMock())

        responses = []
        async for resp in servicer.StreamRecords(
            _iter_once(_schema_request(ack_timeout_seconds=0)),
            context=MagicMock(),
        ):
            responses.append(resp)

        ack = responses[0].schema_ack
        assert ack.accepted is False
        assert "ack_timeout_seconds" in ack.message
        handler.configure_schema.assert_not_called()
        handler.set_statement_timeout.assert_not_called()

    def test_derive_keeps_margin_for_large_budgets(self):
        from src.destination.server import derive_statement_timeout_seconds

        assert derive_statement_timeout_seconds(30) == 25.0
        assert derive_statement_timeout_seconds(300) == 295.0

    def test_derive_stays_below_small_budgets(self):
        from src.destination.server import derive_statement_timeout_seconds

        # Too small to spare the full 5s margin: half the budget instead.
        assert derive_statement_timeout_seconds(8) == 4.0
        assert derive_statement_timeout_seconds(1) == 0.5


class TestGetCapabilities:
    def _make_handler(
        self,
        *,
        supports_upsert: bool,
        supports_truncate: bool = True,
        supports_auto_create: bool = True,
    ) -> MagicMock:
        handler = MagicMock()
        handler.supports_upsert = supports_upsert
        handler.supports_truncate = supports_truncate
        handler.supports_auto_create = supports_auto_create
        handler.connector_type = "database"
        handler.supports_transactions = True
        handler.supports_bulk_load = False
        handler.max_batch_size = 1000
        handler.max_batch_bytes = 0
        return handler

    @pytest.mark.asyncio
    async def test_insert_always_present(self):
        servicer = DestinationServicer(
            self._make_handler(supports_upsert=False), server=MagicMock()
        )
        resp = await servicer.GetCapabilities(GetCapabilitiesRequest(), MagicMock())
        assert WriteMode.WRITE_MODE_INSERT in resp.supported_write_modes

    @pytest.mark.asyncio
    async def test_upsert_absent_when_not_supported(self):
        servicer = DestinationServicer(
            self._make_handler(supports_upsert=False), server=MagicMock()
        )
        resp = await servicer.GetCapabilities(GetCapabilitiesRequest(), MagicMock())
        assert WriteMode.WRITE_MODE_UPSERT not in resp.supported_write_modes
        assert resp.supports_upsert is False

    @pytest.mark.asyncio
    async def test_upsert_present_when_supported(self):
        servicer = DestinationServicer(
            self._make_handler(supports_upsert=True), server=MagicMock()
        )
        resp = await servicer.GetCapabilities(GetCapabilitiesRequest(), MagicMock())
        assert WriteMode.WRITE_MODE_UPSERT in resp.supported_write_modes
        assert WriteMode.WRITE_MODE_INSERT in resp.supported_write_modes
        assert resp.supports_upsert is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize("supports_upsert", [True, False])
    async def test_truncate_insert_present_when_handler_supports_it(
        self, supports_upsert: bool
    ):
        servicer = DestinationServicer(
            self._make_handler(supports_upsert=supports_upsert, supports_truncate=True),
            server=MagicMock(),
        )
        resp = await servicer.GetCapabilities(GetCapabilitiesRequest(), MagicMock())
        assert WriteMode.WRITE_MODE_TRUNCATE_INSERT in resp.supported_write_modes

    @pytest.mark.asyncio
    async def test_truncate_insert_absent_when_handler_lacks_it(self):
        """A handler that cannot truncate (API/file/stdout) must not advertise
        WRITE_MODE_TRUNCATE_INSERT — the capability follows the handler
        property, never a constructor literal."""
        servicer = DestinationServicer(
            self._make_handler(supports_upsert=False, supports_truncate=False),
            server=MagicMock(),
        )
        resp = await servicer.GetCapabilities(GetCapabilitiesRequest(), MagicMock())
        assert WriteMode.WRITE_MODE_TRUNCATE_INSERT not in resp.supported_write_modes

    @pytest.mark.asyncio
    @pytest.mark.parametrize("supports_auto_create", [True, False])
    async def test_auto_create_follows_handler_property(
        self, supports_auto_create: bool
    ):
        servicer = DestinationServicer(
            self._make_handler(
                supports_upsert=False, supports_auto_create=supports_auto_create
            ),
            server=MagicMock(),
        )
        resp = await servicer.GetCapabilities(GetCapabilitiesRequest(), MagicMock())
        assert resp.supports_auto_create is supports_auto_create

    @pytest.mark.asyncio
    async def test_missing_capability_attribute_aborts_with_detail(self):
        """A handler that omits a capability attribute must abort the RPC
        with INTERNAL and a message naming the handler and the missing
        attribute, not surface as a bare AttributeError (issue #73)."""
        import grpc

        handler = MagicMock(spec=[])  # every attribute access raises
        context = MagicMock()
        # Real grpc.aio abort raises and never returns; mirror that so
        # the servicer cannot fall through to an implicit return.
        context.abort = AsyncMock(side_effect=RuntimeError("aborted"))

        servicer = DestinationServicer(handler, server=MagicMock())
        with pytest.raises(RuntimeError, match="aborted"):
            await servicer.GetCapabilities(GetCapabilitiesRequest(), context)

        context.abort.assert_awaited_once()
        code, detail = context.abort.await_args.args
        assert code == grpc.StatusCode.INTERNAL
        assert "MagicMock" in detail
        assert "supports_upsert" in detail


class TestGetCapabilitiesApiHandlerIntegration:
    """End-to-end: a real ApiDestinationHandler composed with the servicer.

    Guards the PR's central claim that advertised capability follows the
    endpoint contract. A mock handler can't catch drift between the
    handler property and the servicer field reference; this can.
    """

    def _doc(self, *, modes):
        """Minimal contract-valid api-endpoint document with write modes.

        ``set_stream_endpoints`` validates into ``ApiEndpointDoc``
        (issue #349), so the blocks carry the contract-required input
        schema, ``from_input`` body, and (for upsert) ``conflict_keys``.
        """
        write: dict = {}
        for mode in modes:
            block = {
                "request": {
                    "method": "POST",
                    "path": f"/v1/{mode}",
                    "body": {"from_input": "record"},
                },
                "input": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "id": {
                                "type": "integer",
                                "native_type": "integer",
                                "arrow_type": "Int64",
                            }
                        },
                    }
                },
            }
            if mode == "upsert":
                block["conflict_keys"] = ["id"]
            write[mode] = block
        return {
            "endpoint_id": "things",
            "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
            "operations": {"write": write},
        }

    @pytest.mark.asyncio
    async def test_upsert_advertised_when_endpoint_declares_it(self):
        handler = ApiDestinationHandler()
        handler.set_stream_endpoints({"s1": self._doc(modes=("insert", "upsert"))})
        servicer = DestinationServicer(handler, server=MagicMock())
        resp = await servicer.GetCapabilities(GetCapabilitiesRequest(), MagicMock())
        assert WriteMode.WRITE_MODE_UPSERT in resp.supported_write_modes
        assert resp.supports_upsert is True

    @pytest.mark.asyncio
    async def test_upsert_not_advertised_for_insert_only_endpoint(self):
        handler = ApiDestinationHandler()
        handler.set_stream_endpoints({"s1": self._doc(modes=("insert",))})
        servicer = DestinationServicer(handler, server=MagicMock())
        resp = await servicer.GetCapabilities(GetCapabilitiesRequest(), MagicMock())
        assert WriteMode.WRITE_MODE_UPSERT not in resp.supported_write_modes
        assert WriteMode.WRITE_MODE_INSERT in resp.supported_write_modes
        assert resp.supports_upsert is False


class TestWireToCdkTranslation:
    """The servicer is the one place protobuf <-> CDK-native types are
    translated (ADR §4.1: the CDK never imports gRPC). These pin that the
    translation is faithful — a dropped or mistranslated field would let the
    handler see the wrong schema/cursor, or advance the wrong checkpoint.
    """

    @pytest.mark.asyncio
    async def test_schema_message_becomes_cdk_schema_spec(self):
        from cdk.types import SchemaSpec
        from cdk.types import WriteMode as CdkWriteMode

        handler = MagicMock()
        handler.configure_schema = AsyncMock(return_value=True)
        _stub_retry_semantics(handler)
        servicer = DestinationServicer(handler, server=MagicMock())

        async for _ in servicer.StreamRecords(
            _iter_once(
                _schema_request("s9", version=7, write_mode=WriteMode.WRITE_MODE_UPSERT)
            ),
            context=MagicMock(),
        ):
            pass

        spec = handler.configure_schema.call_args.args[0]
        assert isinstance(spec, SchemaSpec)
        assert spec.stream_id == "s9"
        assert spec.version == 7
        assert spec.write_mode is CdkWriteMode.WRITE_MODE_UPSERT
        assert spec.ack_timeout_seconds == 30

    @pytest.mark.asyncio
    async def test_batch_cursor_is_translated_in_and_committed_cursor_out(self):
        handler = MagicMock()
        handler.configure_schema = AsyncMock(return_value=True)
        handler.write_batch = AsyncMock(
            return_value=BatchWriteResult(
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=1,
                committed_cursor=CdkCursor(token=b"committed-xyz"),
            )
        )
        _stub_retry_semantics(handler)
        servicer = DestinationServicer(handler, server=MagicMock())

        responses = []
        async for resp in servicer.StreamRecords(
            _iter_many(
                _schema_request("s1"),
                _batch_request(stream_id="s1", token=b"inbound-abc"),
            ),
            context=MagicMock(),
        ):
            responses.append(resp)

        # Inbound: wire Cursor.token -> CDK-native Cursor on the handler call.
        passed_cursor = handler.write_batch.call_args.kwargs["cursor"]
        assert isinstance(passed_cursor, CdkCursor)
        assert passed_cursor.token == b"inbound-abc"

        # Outbound: handler's CDK Cursor -> wire Cursor on the ack.
        ack = responses[-1].ack
        assert ack.status == AckStatus.ACK_STATUS_SUCCESS
        assert ack.HasField("committed_cursor")
        assert ack.committed_cursor.token == b"committed-xyz"

    @pytest.mark.asyncio
    async def test_no_committed_cursor_leaves_wire_field_unset(self):
        handler = MagicMock()
        handler.configure_schema = AsyncMock(return_value=True)
        handler.write_batch = AsyncMock(
            return_value=BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                committed_cursor=None,
                failure_summary="transient",
            )
        )
        _stub_retry_semantics(handler)
        servicer = DestinationServicer(handler, server=MagicMock())

        responses = []
        async for resp in servicer.StreamRecords(
            _iter_many(_schema_request("s1"), _batch_request(stream_id="s1")),
            context=MagicMock(),
        ):
            responses.append(resp)

        ack = responses[-1].ack
        assert ack.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        # A None CDK cursor must not materialize a committed_cursor on the wire
        # (a checkpoint advance on a failed batch).
        assert not ack.HasField("committed_cursor")

    @pytest.mark.asyncio
    async def test_failure_category_crosses_the_wire(self):
        # The category declared on the CDK result must land on the BatchAck
        # unchanged (issue #351): the CDK IntEnum values mirror the proto
        # enum 1:1, so a mistranslation here would silently change the
        # customer-facing error code the engine derives from it.
        from src.grpc.generated.analitiq.v1 import FailureCategory

        handler = MagicMock()
        handler.configure_schema = AsyncMock(return_value=True)
        handler.write_batch = AsyncMock(
            return_value=BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary="type-map: no rule for FANCYTYPE",
                failure_category=CdkFailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
            )
        )
        _stub_retry_semantics(handler)
        servicer = DestinationServicer(handler, server=MagicMock())

        responses = []
        async for resp in servicer.StreamRecords(
            _iter_many(_schema_request("s1"), _batch_request(stream_id="s1")),
            context=MagicMock(),
        ):
            responses.append(resp)

        ack = responses[-1].ack
        assert ack.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert ack.failure_category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT

    @pytest.mark.asyncio
    async def test_batch_before_schema_acks_not_ready(self):
        # A batch that arrives before any schema handshake attempted nothing;
        # the servicer's own rejection must declare NOT_READY like every
        # handler pre-flight guard does (issue #351).
        from src.grpc.generated.analitiq.v1 import FailureCategory

        handler = MagicMock()
        servicer = DestinationServicer(handler, server=MagicMock())

        responses = []
        async for resp in servicer.StreamRecords(
            _iter_once(_batch_request(stream_id="s1")),
            context=MagicMock(),
        ):
            responses.append(resp)

        ack = responses[-1].ack
        assert ack.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert ack.failure_summary == "Schema not configured"
        assert ack.failure_category == FailureCategory.FAILURE_CATEGORY_NOT_READY
        handler.write_batch.assert_not_called()


class TestServerPingProtection:
    """The gRPC server must advertise ping-flood protection options.

    If a client re-adds keepalive settings (incident: PR #85), these server-side
    limits apply regardless, preventing ``GOAWAY: Too many pings`` loops.
    """

    @pytest.mark.asyncio
    async def test_server_options_include_ping_protection(self):
        """grpc.http2.min_ping_interval_without_data_ms and
        grpc.http2.max_ping_strikes must be present in the server options."""
        from unittest.mock import MagicMock, patch

        from src.destination.server import DestinationGRPCServer

        captured_options = []

        class _FakeServer:
            def add_insecure_port(self, addr):
                pass

            async def start(self):
                pass

        def fake_grpc_server(options=None, **kwargs):
            captured_options.extend(options or [])
            return _FakeServer()

        handler = MagicMock()
        server = DestinationGRPCServer(handler, port=9999)

        with patch(
            "src.destination.server.grpc_aio.server", side_effect=fake_grpc_server
        ), patch("src.destination.server.add_DestinationServiceServicer_to_server"):
            await server.start()

        option_map = dict(captured_options)
        assert option_map.get("grpc.http2.min_ping_interval_without_data_ms") == 300_000
        assert option_map.get("grpc.http2.max_ping_strikes") == 2


class TestServerUdsBind:
    @pytest.mark.asyncio
    async def test_explicit_address_binds_uds_not_tcp(self):
        """A worker server bound to ``unix:...`` must not open a TCP port —
        the UDS path is the isolation mechanism (filesystem permissions as
        access control)."""
        import shutil
        import tempfile
        from pathlib import Path

        from src.destination.server import DestinationGRPCServer

        # UDS paths are capped at ~104 chars; pytest's tmp_path can exceed
        # that on macOS, so build a short one the way spawn_worker does.
        workdir = Path(tempfile.mkdtemp(prefix="uds-test-", dir="/tmp"))
        sock = workdir / "worker.sock"
        server = DestinationGRPCServer(handler=MagicMock(), address=f"unix:{sock}")
        await server.start()
        try:
            assert sock.exists()
        finally:
            await server.stop(grace_period=0)
            shutil.rmtree(workdir, ignore_errors=True)


class TestShutdownFinalizeRun:
    """Shutdown runs the handler's optional finalize_run hook (connection-
    dependent run-completion cleanup) while still connected -- the worker is
    SIGTERM'd right after, so this is the last reliable point for that cleanup.
    """

    @pytest.mark.asyncio
    async def test_success_reason_finalizes_with_succeeded_true(self):
        handler = MagicMock()
        handler.finalize_run = AsyncMock()
        server = MagicMock()
        servicer = DestinationServicer(handler, server=server)

        ack = await servicer.Shutdown(
            MagicMock(reason="pipeline_completed"), MagicMock()
        )

        handler.finalize_run.assert_awaited_once_with(succeeded=True)
        server.signal_shutdown.assert_called_once()
        assert ack.acknowledged is True

    @pytest.mark.asyncio
    async def test_non_success_reason_finalizes_with_succeeded_false(self):
        # A failed/aborted (or generic teardown) reason must not signal success,
        # so the handler can skip success-only cleanup for a possible resume.
        handler = MagicMock()
        handler.finalize_run = AsyncMock()
        server = MagicMock()
        servicer = DestinationServicer(handler, server=server)

        ack = await servicer.Shutdown(MagicMock(reason="pipeline_failed"), MagicMock())

        handler.finalize_run.assert_awaited_once_with(succeeded=False)
        server.signal_shutdown.assert_called_once()
        assert ack.acknowledged is True

    @pytest.mark.asyncio
    async def test_finalize_failure_does_not_block_shutdown(self):
        handler = MagicMock()
        handler.finalize_run = AsyncMock(side_effect=RuntimeError("prune failed"))
        server = MagicMock()
        servicer = DestinationServicer(handler, server=server)

        ack = await servicer.Shutdown(MagicMock(reason="x"), MagicMock())

        server.signal_shutdown.assert_called_once()
        assert ack.acknowledged is True

    @pytest.mark.asyncio
    async def test_finalize_cancelled_still_signals_shutdown(self):
        # The client's send_shutdown deadline can fire while the prune is still
        # running, cancelling this handler. CancelledError is not an Exception,
        # so without the finally signal_shutdown would be skipped and the server
        # would keep running after the engine has finished.
        handler = MagicMock()
        handler.finalize_run = AsyncMock(side_effect=asyncio.CancelledError())
        server = MagicMock()
        servicer = DestinationServicer(handler, server=server)

        with pytest.raises(asyncio.CancelledError):
            await servicer.Shutdown(MagicMock(reason="pipeline_completed"), MagicMock())

        server.signal_shutdown.assert_called_once()


class TestSchemaAckRetrySemantics:
    """An accepted SchemaAck carries the handler's retry-safety verdict
    (issue #286); a rejected one carries none (unspecified on the wire)."""

    @pytest.mark.asyncio
    async def test_accepted_ack_carries_handler_verdict(self):
        handler = MagicMock()
        handler.configure_schema = AsyncMock(return_value=True)
        handler.retry_semantics = MagicMock(
            return_value=RetryVerdict(
                semantics=RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE,
                reason="writes are idempotent by content",
            )
        )

        servicer = DestinationServicer(handler, server=MagicMock())
        responses = []
        async for resp in servicer.StreamRecords(
            _iter_once(_schema_request()), context=MagicMock()
        ):
            responses.append(resp)

        ack = responses[0].schema_ack
        assert ack.accepted is True
        assert ack.retry_semantics == WireRetrySemantics.Value(
            "RETRY_SEMANTICS_EXACTLY_ONCE"
        )
        assert ack.retry_semantics_reason == "writes are idempotent by content"
        handler.retry_semantics.assert_called_once_with("s1")

    @pytest.mark.asyncio
    async def test_rejected_ack_carries_no_verdict(self):
        handler = MagicMock()
        handler.configure_schema = AsyncMock(return_value=False)
        handler.last_schema_rejection = None

        servicer = DestinationServicer(handler, server=MagicMock())
        responses = []
        async for resp in servicer.StreamRecords(
            _iter_once(_schema_request()), context=MagicMock()
        ):
            responses.append(resp)

        ack = responses[0].schema_ack
        assert ack.accepted is False
        assert ack.retry_semantics == WireRetrySemantics.Value(
            "RETRY_SEMANTICS_UNSPECIFIED"
        )
        assert ack.retry_semantics_reason == ""
        handler.retry_semantics.assert_not_called()
