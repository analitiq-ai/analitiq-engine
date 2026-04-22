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
        handler.set_endpoint_refs({"s1": "connector:pg/transfers"})
        with pytest.raises(RuntimeError, match="called before connect"):
            handler._type_mapper_for_stream("s1")

    def test_unknown_stream_id_raises(self):
        handler = DatabaseDestinationHandler()
        handler._runtime = _runtime(connector_mapper=_mapper("pg"))
        handler.set_endpoint_refs({"s1": "connector:pg/transfers"})
        with pytest.raises(RuntimeError, match="no endpoint_ref registered"):
            handler._type_mapper_for_stream("unregistered-stream")

    def test_connector_scoped_uses_connector_mapper(self):
        handler = DatabaseDestinationHandler()
        connector_map = _mapper("pg")
        handler._runtime = _runtime(
            connector_mapper=connector_map,
            connection_mapper=_mapper("connection:dest-conn"),
        )
        handler.set_endpoint_refs({"s1": "connector:pg/transfers"})
        assert handler._type_mapper_for_stream("s1") is connector_map

    def test_connection_scoped_uses_connection_mapper(self):
        handler = DatabaseDestinationHandler()
        connection_map = _mapper("connection:dest-conn")
        handler._runtime = _runtime(
            connector_mapper=_mapper("pg"),
            connection_mapper=connection_map,
        )
        handler.set_endpoint_refs({"s1": "connection:dest-conn/orders"})
        assert handler._type_mapper_for_stream("s1") is connection_map

    def test_set_endpoint_refs_copies_mapping(self):
        """External mutations must not leak into the handler's state."""
        handler = DatabaseDestinationHandler()
        source = {"s1": "connector:pg/transfers"}
        handler.set_endpoint_refs(source)
        source["s1"] = "connector:evil/injected"
        handler._runtime = _runtime(connector_mapper=_mapper("pg"))
        # Original registration wins — set_endpoint_refs took a defensive copy.
        assert handler._endpoint_refs["s1"] == "connector:pg/transfers"
