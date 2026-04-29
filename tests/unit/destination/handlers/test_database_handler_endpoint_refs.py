"""Tests for DatabaseDestinationHandler per-stream type-mapper dispatch.

The handler registers a stream_id → endpoint_ref index at startup; each
incoming SchemaMessage picks its mapper by scope. These tests lock in
that contract without the full gRPC/materialization stack.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.destination.connectors.database import DatabaseDestinationHandler
from src.engine.type_map import TypeMapper
from src.engine.type_map.rules import parse_rules
from src.shared.connection_runtime import ConnectionRuntime


def _mapper(label: str) -> TypeMapper:
    return TypeMapper(
        label,
        parse_rules(
            [{"match": "exact", "native": "BIGINT", "canonical": "Int64"}],
            source="<test>",
        ),
    )


def _runtime(
    *,
    connector_mapper: TypeMapper | None = None,
    connection_mapper: TypeMapper | None = None,
) -> ConnectionRuntime:
    return ConnectionRuntime(
        raw_config={},
        connection_id="dest-conn",
        connector_type="database",
        driver="postgresql",
        resolver=AsyncMock(),
        connector_type_mapper=connector_mapper,
        connection_type_mapper=connection_mapper,
    )


class TestEndpointRefDispatch:
    def test_pre_connect_raises(self):
        handler = DatabaseDestinationHandler()
        handler.set_endpoint_refs({"s1": {"scope": "connector", "identifier": "pg", "endpoint": "transfers"}})
        with pytest.raises(RuntimeError, match="called before connect"):
            handler._type_mapper_for_stream("s1")

    def test_unknown_stream_id_raises(self):
        handler = DatabaseDestinationHandler()
        handler._runtime = _runtime(connector_mapper=_mapper("pg"))
        handler.set_endpoint_refs({"s1": {"scope": "connector", "identifier": "pg", "endpoint": "transfers"}})
        with pytest.raises(RuntimeError, match="no endpoint_ref registered"):
            handler._type_mapper_for_stream("unregistered-stream")

    def test_connector_scoped_uses_connector_mapper(self):
        handler = DatabaseDestinationHandler()
        connector_map = _mapper("pg")
        handler._runtime = _runtime(
            connector_mapper=connector_map,
            connection_mapper=_mapper("connection:dest-conn"),
        )
        handler.set_endpoint_refs({"s1": {"scope": "connector", "identifier": "pg", "endpoint": "transfers"}})
        assert handler._type_mapper_for_stream("s1") is connector_map

    def test_connection_scoped_uses_connection_mapper(self):
        handler = DatabaseDestinationHandler()
        connection_map = _mapper("connection:dest-conn")
        handler._runtime = _runtime(
            connector_mapper=_mapper("pg"),
            connection_mapper=connection_map,
        )
        handler.set_endpoint_refs({"s1": {"scope": "connection", "identifier": "dest-conn", "endpoint": "orders"}})
        assert handler._type_mapper_for_stream("s1") is connection_map

    def test_set_endpoint_refs_copies_mapping(self):
        """External mutations must not leak into the handler's state."""
        handler = DatabaseDestinationHandler()
        source = {"s1": {"scope": "connector", "identifier": "pg", "endpoint": "transfers"}}
        handler.set_endpoint_refs(source)
        source["s1"] = {"scope": "connector", "identifier": "evil", "endpoint": "injected"}
        handler._runtime = _runtime(connector_mapper=_mapper("pg"))
        # Original registration wins — set_endpoint_refs took a defensive copy.
        assert handler._endpoint_refs["s1"] == {
            "scope": "connector", "identifier": "pg", "endpoint": "transfers",
        }


class TestCreateTableFromSchemaStrictness:
    """``_create_table_from_schema`` must refuse malformed payloads rather
    than silently dropping columns. Covers the sibling raise of the
    Arrow-side check in ``schema_contract``."""

    def test_unnamed_column_raises(self):
        handler = DatabaseDestinationHandler()
        handler._runtime = _runtime(connector_mapper=_mapper("pg"))
        handler._schema_name = "public"

        schema = {
            "columns": [
                {"type": "BIGINT"},  # missing 'name'
                {"name": "valid", "type": "BIGINT"},
            ]
        }
        with pytest.raises(ValueError, match="has no 'name' field"):
            handler._create_table_from_schema(
                "t", schema, [], _mapper("pg")
            )


class TestJsonSchemaDdlMapping:
    """``_json_type_to_sqlalchemy`` must agree with the Arrow path on
    JSON-Schema formats — otherwise DDL and casting disagree silently."""

    def test_date_format_is_date_not_datetime(self):
        from sqlalchemy import Date, DateTime

        handler = DatabaseDestinationHandler()
        sa_type = handler._json_type_to_sqlalchemy({"type": "string", "format": "date"})
        # Date, not DateTime — pa.date32() on the Arrow side has no time
        # component, so a DateTime column would round-trip wrong.
        assert isinstance(sa_type, Date)
        assert not isinstance(sa_type, DateTime)

    def test_date_time_format_is_tz_aware_datetime(self):
        from sqlalchemy import DateTime

        handler = DatabaseDestinationHandler()
        sa_type = handler._json_type_to_sqlalchemy(
            {"type": "string", "format": "date-time"}
        )
        assert isinstance(sa_type, DateTime)
        assert sa_type.timezone is True

    def test_unknown_type_raises(self):
        handler = DatabaseDestinationHandler()
        with pytest.raises(ValueError, match="unsupported type/format"):
            handler._json_type_to_sqlalchemy({"type": "polygon"})


class TestWriteBatchFatalOnTypeMapError:
    """Deterministic type-map errors in write_batch must not be retried."""

    @pytest.mark.asyncio
    async def test_type_map_error_classified_as_fatal(self):
        from contextlib import asynccontextmanager
        from unittest.mock import AsyncMock, MagicMock

        from src.destination.connectors.database import DatabaseDestinationHandler
        from src.engine.type_map import UnmappedTypeError
        from src.grpc.generated.analitiq.v1 import AckStatus, Cursor

        handler = DatabaseDestinationHandler()
        # Preconditions: connected, schema configured, idempotency check
        # clean. We don't actually hit the DB because the schema-contract
        # prepare_records call raises before any SQL runs.
        handler._engine = MagicMock()
        handler._connected = True
        handler._table = MagicMock()
        handler._batch_commits_table = MagicMock()
        handler._write_mode = "insert"
        handler._primary_keys = []

        @asynccontextmanager
        async def _fake_begin():
            yield AsyncMock()

        handler._engine.begin = _fake_begin

        async def _not_committed(*_args, **_kwargs):
            return False

        handler._check_batch_committed = _not_committed  # type: ignore[method-assign]

        # The schema contract's prepare_records is called inside _insert_records;
        # route the UnmappedTypeError through that entry.
        async def _raising_insert(_conn, _records):
            raise UnmappedTypeError("pg", "forward", "MONEY")

        handler._insert_records = _raising_insert  # type: ignore[method-assign]

        result = await handler.write_batch(
            run_id="run-1",
            stream_id="s1",
            batch_seq=1,
            records=[{"id": 1}],
            record_ids=["1"],
            cursor=Cursor(token=b""),
        )

        assert result.success is False
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert "type-map" in result.failure_summary
