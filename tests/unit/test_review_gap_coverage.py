"""Coverage for paths flagged in the PR review as untested.

Each test name maps to a specific review finding so the rationale is
discoverable from the test output.
"""

from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock


# --------------------------------------------------------------------------- #
# Gap #12: _write_conflict_keys threads end-to-end through configure_schema   #
# --------------------------------------------------------------------------- #


class TestWriteConflictKeysWiring:
    """The destination handler must use ``_write_conflict_keys`` from the
    enriched endpoint document when present, falling back to primary_keys
    only when the key is absent entirely (not just empty)."""

    @pytest.mark.asyncio
    async def test_explicit_conflict_keys_override_primary_keys(self):
        from cdk.sql.generic import (
            GenericSQLConnector,
        )
        from cdk.type_map import TypeMapper
        from cdk.type_map.rules import parse_rules

        handler = GenericSQLConnector()
        handler._connected = True
        handler._engine = MagicMock()
        handler._runtime = MagicMock()
        handler._runtime.type_mapper_for = MagicMock(
            return_value=TypeMapper(
                "pg",
                parse_rules(
                    [{"match": "exact", "native": "BIGINT", "canonical": "Int64"}],
                    source="<t>",
                ),
            ),
        )
        handler._endpoint_refs = {
            "s1": {"scope": "connector", "connection_id": "pg", "endpoint_id": "x"},
        }
        handler._stream_endpoints = {
            "s1": {
                "database_object": {"name": "orders", "schema": "public"},
                "columns": [
                    {"name": "tenant_id", "native_type": "BIGINT", "arrow_type": "Int64", "nullable": False},
                    {"name": "id", "native_type": "BIGINT", "arrow_type": "Int64", "nullable": False},
                ],
                "primary_keys": ["id"],
                "_write_conflict_keys": ["tenant_id", "id"],
            },
        }
        handler._ensure_tables_exist = AsyncMock()

        from src.grpc.generated.analitiq.v1 import SchemaMessage, WriteMode
        msg = SchemaMessage(
            stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_UPSERT,
        )
        ok = await handler.configure_schema(msg)

        assert ok is True
        assert handler._streams["s1"].conflict_keys == ["tenant_id", "id"]
        assert handler._streams["s1"].primary_keys == ["id"]

    @pytest.mark.asyncio
    async def test_missing_write_conflict_keys_falls_back_to_primary_keys(self):
        from cdk.sql.generic import (
            GenericSQLConnector,
        )
        from cdk.type_map import TypeMapper
        from cdk.type_map.rules import parse_rules

        handler = GenericSQLConnector()
        handler._connected = True
        handler._engine = MagicMock()
        handler._runtime = MagicMock()
        handler._runtime.type_mapper_for = MagicMock(
            return_value=TypeMapper(
                "pg",
                parse_rules(
                    [{"match": "exact", "native": "BIGINT", "canonical": "Int64"}],
                    source="<t>",
                ),
            ),
        )
        handler._endpoint_refs = {
            "s1": {"scope": "connector", "connection_id": "pg", "endpoint_id": "x"},
        }
        handler._stream_endpoints = {
            "s1": {
                "database_object": {"name": "orders", "schema": "public"},
                "columns": [{"name": "id", "native_type": "BIGINT", "arrow_type": "Int64", "nullable": False}],
                "primary_keys": ["id"],
            },
        }
        handler._ensure_tables_exist = AsyncMock()

        from src.grpc.generated.analitiq.v1 import SchemaMessage, WriteMode
        msg = SchemaMessage(
            stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_UPSERT,
        )
        await handler.configure_schema(msg)

        assert handler._streams["s1"].conflict_keys == ["id"]


# --------------------------------------------------------------------------- #
# Gap #13: main.py unknown write_mode propagates ValueError                   #
# --------------------------------------------------------------------------- #


class TestMainConflictKeysWiring:
    """Misconfigured pipelines must fail fast at destination startup —
    silently downgrading UPSERT to INSERT corrupts replication
    semantics."""

    def test_unknown_mode_raises(self):
        from src.models.stream import WriteConfig, WriteMode

        with pytest.raises(ValueError):
            WriteMode("merge-typo")

        # Confirm the helper's contract: ValueError on UPSERT without keys
        wc = WriteConfig(mode=WriteMode.UPSERT)
        with pytest.raises(ValueError, match="UPSERT requires"):
            wc.effective_conflict_keys([])

    def test_effective_conflict_keys_flattens_for_database_handler(self):
        """The database handler uses a single flat list. The first
        composite returned by ``effective_conflict_keys`` is what
        main.py threads through ``_write_conflict_keys``."""
        from src.models.stream import WriteConfig, WriteMode

        wc = WriteConfig(
            mode=WriteMode.UPSERT,
            conflict_keys=[["tenant_id", "id"]],
        )
        composites = wc.effective_conflict_keys(["id"]) or []
        assert composites == [["tenant_id", "id"]]
        assert list(composites[0]) == ["tenant_id", "id"]


# --------------------------------------------------------------------------- #
# Gap #14: server.py configure_schema deterministic-exception SchemaAck       #
# --------------------------------------------------------------------------- #


class TestServerConfigureSchemaDeterministicErrors:
    """KeyError/TypeError/ValueError from a malformed endpoint document
    must surface in SchemaAck.message with the exception type rather
    than crashing the stream with a generic 'configuration failed'."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("exc", [
        KeyError("database_object"),
        TypeError("expected dict"),
        ValueError("malformed columns"),
    ])
    async def test_deterministic_exception_in_schema_ack(self, exc):
        from src.destination.server import DestinationServicer

        handler = MagicMock()
        handler.configure_schema = AsyncMock(side_effect=exc)
        servicer = DestinationServicer(handler, MagicMock())

        from src.grpc.generated.analitiq.v1 import (
            SchemaMessage, StreamRequest, WriteMode,
        )
        schema_msg = SchemaMessage(
            stream_id="s1", version=1, write_mode=WriteMode.WRITE_MODE_UPSERT,
        )
        request = StreamRequest(schema=schema_msg)

        async def _one_request():
            yield request

        ctx = MagicMock()
        responses = []
        async for resp in servicer.StreamRecords(_one_request(), ctx):
            responses.append(resp)
            break  # Only need the SchemaAck

        assert len(responses) == 1
        ack = responses[0].schema_ack
        assert ack.accepted is False
        assert type(exc).__name__ in ack.message
        assert str(exc) in ack.message


# --------------------------------------------------------------------------- #
# Gap #15: start_stream clears stale failure state                            #
# --------------------------------------------------------------------------- #


class TestStartStreamStateReset:
    """A previous failed run must not poison the next run's diagnostic."""

    @pytest.mark.asyncio
    async def test_start_stream_clears_stale_failure_state(self):
        from src.grpc.client import DestinationGRPCClient

        client = DestinationGRPCClient()
        client._connected = True
        client._stub = MagicMock()
        client._stub.StreamRecords = MagicMock(return_value=MagicMock())

        # Seed stale state from a hypothetical prior failed run.
        client._task_failure = RuntimeError("prev run")
        client._peer_closed_stream = True

        # Make start_stream's schema-ack wait complete immediately via a
        # task that fails — we only care about the pre-send reset.
        import asyncio as _asyncio
        original_create_task = _asyncio.create_task

        def _capture_create_task(coro, name=None):
            # State should already have been reset before any tasks fire.
            assert client._task_failure is None
            assert client._peer_closed_stream is False
            coro.close()
            return MagicMock()

        _asyncio.create_task = _capture_create_task  # type: ignore[assignment]
        try:
            # start_stream will block on _response_queue.get; cancel via timeout.
            with pytest.raises((asyncio.TimeoutError, Exception)):
                await asyncio.wait_for(
                    client.start_stream("run-1", "stream-1", {"write_mode": "upsert"}),
                    timeout=0.1,
                )
        finally:
            _asyncio.create_task = original_create_task  # type: ignore[assignment]

        # Final assertion: reset persisted after start_stream returned.
        assert client._task_failure is None
        assert client._peer_closed_stream is False


# --------------------------------------------------------------------------- #
# Gap #16: _parse_iso_timestamp / _parse_iso_to_datetime_object raise         #
# --------------------------------------------------------------------------- #


class TestIsoTimestampStrictRaise:
    """Returning ``datetime.now()`` on unparseable cursor input would
    silently re-window incremental replication. These parsers must
    raise so the engine routes via error_strategy."""

    @pytest.mark.asyncio
    async def test_parse_iso_timestamp_raises_on_unparseable(self):
        from src.engine.data_transformer import DataTransformer

        t = DataTransformer()
        with pytest.raises(ValueError):
            await t._parse_iso_timestamp("not-a-timestamp")

    @pytest.mark.asyncio
    async def test_parse_iso_timestamp_handles_zulu(self):
        from datetime import datetime
        from src.engine.data_transformer import DataTransformer

        t = DataTransformer()
        result = await t._parse_iso_timestamp("2026-05-12T10:30:00Z")
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    @pytest.mark.asyncio
    async def test_parse_iso_to_datetime_object_raises_on_none(self):
        from src.engine.data_transformer import DataTransformer

        t = DataTransformer()
        with pytest.raises(ValueError):
            await t._parse_iso_to_datetime_object(None)

    @pytest.mark.asyncio
    async def test_parse_iso_to_datetime_object_raises_on_unparseable(self):
        from src.engine.data_transformer import DataTransformer

        t = DataTransformer()
        with pytest.raises(ValueError):
            await t._parse_iso_to_datetime_object("garbage")

    @pytest.mark.asyncio
    async def test_to_int_raises_on_unparseable(self):
        from src.engine.data_transformer import DataTransformer
        from src.engine.exceptions import TransformationError

        t = DataTransformer()
        with pytest.raises(TransformationError, match="to_int"):
            await t._apply_field_transformations("abc", ["to_int"])

    @pytest.mark.asyncio
    async def test_to_float_raises_on_unparseable(self):
        from src.engine.data_transformer import DataTransformer
        from src.engine.exceptions import TransformationError

        t = DataTransformer()
        with pytest.raises(TransformationError, match="to_float"):
            await t._apply_field_transformations("xyz", ["to_float"])
