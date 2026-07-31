"""What GetCapabilities advertises follows the handler, never a literal.

Every advertised write mode is read off a handler property, so a handler whose
schema handshake would refuse a mode never has it advertised on its behalf
(issue #388). The servicer method takes a request and a context, so this needs
no channel.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

import grpc
from src.destination.server import DestinationServicer
from src.grpc.generated.analitiq.v1 import GetCapabilitiesRequest, WriteMode

pytestmark = pytest.mark.unit


def _handler(
    *,
    supports_upsert: bool,
    supports_insert: bool = True,
    supports_truncate: bool = True,
    supports_auto_create: bool = True,
) -> MagicMock:
    handler = MagicMock()
    handler.supports_insert = supports_insert
    handler.supports_upsert = supports_upsert
    handler.supports_truncate = supports_truncate
    handler.supports_auto_create = supports_auto_create
    handler.connector_type = "database"
    handler.supports_transactions = True
    handler.supports_bulk_load = False
    handler.max_batch_size = 1000
    handler.max_batch_bytes = 0
    return handler


async def _advertise(handler: MagicMock):
    servicer = DestinationServicer(handler, server=MagicMock())
    return await servicer.GetCapabilities(GetCapabilitiesRequest(), MagicMock())


@pytest.mark.asyncio
async def test_insert_present_when_handler_supports_it():
    resp = await _advertise(_handler(supports_upsert=False))
    assert WriteMode.WRITE_MODE_INSERT in resp.supported_write_modes


@pytest.mark.asyncio
async def test_insert_absent_when_handler_lacks_it():
    """INSERT follows the handler property too (issue #388): a SQL handler
    whose stage cycle cannot run must not advertise a mode the schema
    handshake would refuse."""
    resp = await _advertise(_handler(supports_upsert=False, supports_insert=False))
    assert WriteMode.WRITE_MODE_INSERT not in resp.supported_write_modes


@pytest.mark.asyncio
async def test_upsert_absent_when_not_supported():
    resp = await _advertise(_handler(supports_upsert=False))
    assert WriteMode.WRITE_MODE_UPSERT not in resp.supported_write_modes
    assert resp.supports_upsert is False


@pytest.mark.asyncio
async def test_upsert_present_when_supported():
    resp = await _advertise(_handler(supports_upsert=True))
    assert WriteMode.WRITE_MODE_UPSERT in resp.supported_write_modes
    assert WriteMode.WRITE_MODE_INSERT in resp.supported_write_modes
    assert resp.supports_upsert is True


@pytest.mark.asyncio
@pytest.mark.parametrize("supports_upsert", [True, False])
async def test_truncate_insert_present_when_handler_supports_it(supports_upsert: bool):
    resp = await _advertise(
        _handler(supports_upsert=supports_upsert, supports_truncate=True)
    )
    assert WriteMode.WRITE_MODE_TRUNCATE_INSERT in resp.supported_write_modes


@pytest.mark.asyncio
async def test_truncate_insert_absent_when_handler_lacks_it():
    """A handler that cannot truncate (API/file/stdout) must not advertise
    WRITE_MODE_TRUNCATE_INSERT -- the capability follows the handler property,
    never a constructor literal."""
    resp = await _advertise(_handler(supports_upsert=False, supports_truncate=False))
    assert WriteMode.WRITE_MODE_TRUNCATE_INSERT not in resp.supported_write_modes


@pytest.mark.asyncio
@pytest.mark.parametrize("supports_auto_create", [True, False])
async def test_auto_create_follows_handler_property(supports_auto_create: bool):
    resp = await _advertise(
        _handler(supports_upsert=False, supports_auto_create=supports_auto_create)
    )
    assert resp.supports_auto_create is supports_auto_create


@pytest.mark.asyncio
async def test_missing_capability_attribute_aborts_with_detail():
    """A handler that omits a capability attribute must abort the RPC with
    INTERNAL and a message naming the handler and the missing attribute, not
    surface as a bare AttributeError (issue #73)."""
    handler = MagicMock(spec=[])  # every attribute access raises
    context = MagicMock()
    # Real grpc.aio abort raises and never returns; mirror that so the
    # servicer cannot fall through to an implicit return.
    context.abort = AsyncMock(side_effect=RuntimeError("aborted"))

    servicer = DestinationServicer(handler, server=MagicMock())
    with pytest.raises(RuntimeError, match="aborted"):
        await servicer.GetCapabilities(GetCapabilitiesRequest(), context)

    context.abort.assert_awaited_once()
    code, detail = context.abort.await_args.args
    assert code == grpc.StatusCode.INTERNAL
    assert "MagicMock" in detail
    # The first capability the servicer consults is named in the detail.
    assert "supports_insert" in detail
