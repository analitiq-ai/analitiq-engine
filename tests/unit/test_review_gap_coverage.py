"""Coverage for paths flagged in the PR review as untested.

Each test name maps to a specific review finding so the rationale is
discoverable from the test output.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest


def _merge_capable_caps():
    """A declared merge-capable sql_capabilities object (issue #390): the
    upsert configure gate needs a declared merge_form before any DDL runs."""
    from cdk.sql.capabilities import SqlCapabilities

    return SqlCapabilities.from_declaration(
        {
            "catalog": "none",
            "session_targeting": "per_statement",
            "merge_form": "merge",
            "bulk_load": {},
            "stage": {"scope": "real", "schema": "target", "transactional_ddl": True},
        }
    )


def _rendering_connector_cls():
    """A GenericSQLConnector whose dialect implements the stage-then-merge
    rendering hooks — the configure gates check declaration/dialect
    agreement (#390/#388)."""
    from cdk.sql.dialects import SqlDialect
    from cdk.sql.generic import GenericSQLConnector

    class _RenderingDialect(SqlDialect):
        name = "rendering"

        def stage_table_sql(self, stage, target, *, temp):
            return (
                f"CREATE TABLE {self.quote_table(stage)} AS SELECT * FROM "
                f"{self.quote_table(target)} WHERE FALSE"
            )

        def merge_statement_sql(self, stage, target, conflict_keys, columns):
            return "MERGE ..."

    class _RenderingConnector(GenericSQLConnector):
        dialect_class = _RenderingDialect

    return _RenderingConnector


# --------------------------------------------------------------------------- #
# Gap #12: stream conflict keys thread end-to-end through configure_schema    #
# --------------------------------------------------------------------------- #


class TestWriteConflictKeysWiring:
    """The destination handler consumes the stream's conflict keys verbatim
    from the ``set_stream_conflict_keys`` channel. When the stream has no
    entry the conflict target is empty — the engine never derives one from
    ``primary_keys``."""

    @pytest.mark.asyncio
    async def test_conflict_keys_consumed_verbatim(self):
        """Registered conflict keys land verbatim in the stream state."""
        from cdk.type_map import TypeMapper
        from cdk.type_map.rules import parse_rules

        handler = _rendering_connector_cls()()
        handler._capabilities = _merge_capable_caps()
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
                    {
                        "name": "tenant_id",
                        "native_type": "BIGINT",
                        "arrow_type": "Int64",
                        "nullable": False,
                    },
                    {
                        "name": "id",
                        "native_type": "BIGINT",
                        "arrow_type": "Int64",
                        "nullable": False,
                    },
                ],
                "primary_keys": ["id"],
            },
        }
        handler.set_stream_conflict_keys({"s1": ["tenant_id", "id"]})
        handler._ensure_tables_exist = AsyncMock()

        from src.grpc.generated.analitiq.v1 import SchemaMessage, WriteMode

        msg = SchemaMessage(
            stream_id="s1",
            version=1,
            write_mode=WriteMode.WRITE_MODE_UPSERT,
        )
        ok = await handler.configure_schema(msg)

        assert ok is True
        assert handler._streams["s1"].conflict_keys == ["tenant_id", "id"]
        assert handler._streams["s1"].primary_keys == ["id"]

    @pytest.mark.asyncio
    async def test_absent_write_conflict_keys_yields_empty_no_primary_keys_fallback(
        self,
    ):
        """No registered entry means an empty conflict target, no PK fallback."""
        from cdk.type_map import TypeMapper
        from cdk.type_map.rules import parse_rules

        handler = _rendering_connector_cls()()
        handler._capabilities = _merge_capable_caps()
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
                    {
                        "name": "id",
                        "native_type": "BIGINT",
                        "arrow_type": "Int64",
                        "nullable": False,
                    }
                ],
                "primary_keys": ["id"],
            },
        }
        handler._ensure_tables_exist = AsyncMock()

        from src.grpc.generated.analitiq.v1 import SchemaMessage, WriteMode

        msg = SchemaMessage(
            stream_id="s1",
            version=1,
            write_mode=WriteMode.WRITE_MODE_UPSERT,
        )
        await handler.configure_schema(msg)

        # No set_stream_conflict_keys entry for the stream: the conflict
        # target is empty. The engine does NOT fabricate one from
        # ``primary_keys`` (the misconfiguration surfaces loudly later, at
        # the write path).
        assert handler._streams["s1"].conflict_keys == []


# --------------------------------------------------------------------------- #
# Gap #13: main.py unknown write_mode propagates ValueError                   #
# --------------------------------------------------------------------------- #


class TestMainConflictKeysWiring:
    """An unknown ``write.mode`` must fail fast at destination startup
    (format validation). Conflict-key enforcement is no longer the
    engine's job — Infra validates it, so the engine only rejects a mode
    string it cannot parse."""

    def test_unknown_mode_raises(self):
        from src.models.stream import WriteMode

        with pytest.raises(ValueError):
            WriteMode("merge-typo")


# --------------------------------------------------------------------------- #
# Gap #14: server.py configure_schema deterministic-exception SchemaAck       #
# --------------------------------------------------------------------------- #


class TestServerConfigureSchemaDeterministicErrors:
    """KeyError/TypeError/ValueError from a malformed endpoint document
    must surface in SchemaAck.message with the exception type rather
    than crashing the stream with a generic 'configuration failed'."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exc",
        [
            KeyError("database_object"),
            TypeError("expected dict"),
            ValueError("malformed columns"),
        ],
    )
    async def test_deterministic_exception_in_schema_ack(self, exc):
        from src.destination.server import DestinationServicer

        handler = MagicMock()
        handler.configure_schema = AsyncMock(side_effect=exc)
        servicer = DestinationServicer(handler, MagicMock())

        from src.grpc.generated.analitiq.v1 import (
            SchemaMessage,
            StreamRequest,
            WriteMode,
        )

        schema_msg = SchemaMessage(
            stream_id="s1",
            version=1,
            write_mode=WriteMode.WRITE_MODE_UPSERT,
            ack_timeout_seconds=30,
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
# Gap #16: AssignmentTransformer iso_to_datetime must raise on bad input      #
# --------------------------------------------------------------------------- #


class TestIsoTimestampStrictRaise:
    """Returning ``datetime.now()`` on unparseable cursor input would
    silently re-window incremental replication. The vectorized iso_* kernels
    must raise so the engine routes via error_strategy."""

    def test_iso_to_datetime_raises_on_unparseable(self):
        # A datetime.now() fallback would fabricate timestamps that are
        # indistinguishable from valid data (issue #138).
        import pyarrow as pa

        from src.engine.data_transformer import _FUNCTION_CATALOG
        from src.engine.exceptions import TransformationError

        with pytest.raises(TransformationError, match="iso_to_datetime"):
            _FUNCTION_CATALOG["iso_to_datetime"][1](pa.array(["not-a-timestamp"]))

    def test_iso_to_datetime_parses_valid_input_and_passes_null(self):
        import pyarrow as pa

        from src.engine.data_transformer import _FUNCTION_CATALOG

        out = _FUNCTION_CATALOG["iso_to_datetime"][1](
            pa.array(["2026-05-12T10:30:00Z", None])
        )
        assert out.type == pa.timestamp("us", tz="UTC")
        rows = out.to_pylist()
        assert rows[0].tzinfo is not None
        assert rows[1] is None

    def test_iso_to_date_raises_on_unparseable(self):
        import pyarrow as pa

        from src.engine.data_transformer import _FUNCTION_CATALOG
        from src.engine.exceptions import TransformationError

        with pytest.raises(TransformationError, match="iso_to_date"):
            _FUNCTION_CATALOG["iso_to_date"][1](pa.array(["not-a-date"]))

    def test_iso_to_date_parses_valid_input_and_passes_null(self):
        import pyarrow as pa

        from src.engine.data_transformer import _FUNCTION_CATALOG

        out = _FUNCTION_CATALOG["iso_to_date"][1](
            pa.array(["2026-05-12T10:30:00", "2026-05-12", None])
        )
        assert out.to_pylist() == ["2026-05-12", "2026-05-12", None]
