"""Tests for DestinationServicer protocol behavior.

Focuses on contract-level responses the engine relies on:
when ``configure_schema`` raises a deterministic type-map error, the
servicer must yield a ``SchemaAck(accepted=False, message="type-map: …")``
rather than aborting the stream. A regression that re-wraps the exception
or drops the prefix would ship silently without this test.
"""

from __future__ import annotations

from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.destination.server import DestinationServicer
from src.engine.type_map import InvalidTypeMapError, UnmappedTypeError
from src.grpc.generated.analitiq.v1 import (
    GetCapabilitiesRequest,
    SchemaMessage,
    StreamRequest,
    WriteMode,
)


async def _iter_once(msg: StreamRequest) -> AsyncIterator[StreamRequest]:
    yield msg


def _schema_request(stream_id: str = "s1") -> StreamRequest:
    return StreamRequest(
        schema=SchemaMessage(
            stream_id=stream_id,
            version=1,
            write_mode=WriteMode.WRITE_MODE_INSERT,
        )
    )


class TestGetCapabilities:
    @pytest.mark.asyncio
    async def test_insert_always_included(self):
        handler = MagicMock()
        handler.supports_upsert = False
        handler.supports_transactions = False
        handler.supports_bulk_load = False
        handler.connector_type = "database"
        handler.max_batch_size = 1000
        handler.max_batch_bytes = 0

        servicer = DestinationServicer(handler, server=MagicMock())
        resp = await servicer.GetCapabilities(GetCapabilitiesRequest(), context=MagicMock())

        assert WriteMode.WRITE_MODE_INSERT in resp.supported_write_modes
        assert WriteMode.WRITE_MODE_UPSERT not in resp.supported_write_modes
        assert WriteMode.WRITE_MODE_TRUNCATE_INSERT in resp.supported_write_modes
        assert WriteMode.WRITE_MODE_UNSPECIFIED not in resp.supported_write_modes

    @pytest.mark.asyncio
    async def test_upsert_only_when_supported(self):
        handler = MagicMock()
        handler.supports_upsert = True
        handler.supports_transactions = True
        handler.supports_bulk_load = True
        handler.connector_type = "database"
        handler.max_batch_size = 1000
        handler.max_batch_bytes = 0

        servicer = DestinationServicer(handler, server=MagicMock())
        resp = await servicer.GetCapabilities(GetCapabilitiesRequest(), context=MagicMock())

        assert WriteMode.WRITE_MODE_INSERT in resp.supported_write_modes
        assert WriteMode.WRITE_MODE_UPSERT in resp.supported_write_modes
        assert WriteMode.WRITE_MODE_TRUNCATE_INSERT in resp.supported_write_modes
        assert WriteMode.WRITE_MODE_UNSPECIFIED not in resp.supported_write_modes


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
