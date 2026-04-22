"""Tests for ``ConnectionRuntime.type_mapper_for`` — per-endpoint mapper dispatch."""

from unittest.mock import Mock

import pytest

from src.engine.type_map import TypeMapper
from src.engine.type_map.rules import parse_rules
from src.shared.connection_runtime import ConnectionRuntime


RULES = [{"match": "exact", "native": "BIGINT", "canonical": "Int64"}]


def _runtime(
    *,
    connector_mapper: TypeMapper | None = None,
    connection_mapper: TypeMapper | None = None,
) -> ConnectionRuntime:
    return ConnectionRuntime(
        raw_config={},
        connection_id="test-conn",
        connector_type="database",
        driver="postgresql",
        resolver=Mock(),
        connector_type_mapper=connector_mapper,
        connection_type_mapper=connection_mapper,
    )


class TestTypeMapperFor:
    def test_connector_scope_returns_connector_mapper(self):
        cmapper = TypeMapper("pg", parse_rules(RULES, source="<t>"))
        rt = _runtime(connector_mapper=cmapper)
        assert rt.type_mapper_for("connector:pg/transfers") is cmapper

    def test_connection_scope_returns_connection_mapper(self):
        cmapper = TypeMapper("pg", parse_rules(RULES, source="<t>"))
        nmapper = TypeMapper("connection:test-conn", parse_rules(RULES, source="<t>"))
        rt = _runtime(connector_mapper=cmapper, connection_mapper=nmapper)
        assert rt.type_mapper_for("connection:test-conn/orders") is nmapper

    def test_connection_scope_without_mapper_raises(self):
        cmapper = TypeMapper("pg", parse_rules(RULES, source="<t>"))
        rt = _runtime(connector_mapper=cmapper, connection_mapper=None)
        with pytest.raises(RuntimeError, match="has no type-map"):
            rt.type_mapper_for("connection:test-conn/orders")

    def test_unscoped_ref_rejected(self):
        rt = _runtime(connector_mapper=TypeMapper("pg", parse_rules(RULES, source="<t>")))
        with pytest.raises(ValueError, match="not a scoped endpoint_ref"):
            rt.type_mapper_for("transfers")

    def test_unknown_scope_rejected(self):
        rt = _runtime(connector_mapper=TypeMapper("pg", parse_rules(RULES, source="<t>")))
        with pytest.raises(ValueError, match="unknown endpoint scope"):
            rt.type_mapper_for("pipeline:foo/bar")

    def test_connector_mapper_required(self):
        """Accessing ``connector_type_mapper`` when it was not supplied raises."""
        rt = _runtime(connector_mapper=None)
        with pytest.raises(RuntimeError, match="connector_type_mapper not available"):
            _ = rt.connector_type_mapper
