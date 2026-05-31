"""Tests for ``ConnectionRuntime.type_mapper_for`` — per-endpoint mapper dispatch.

``type_mapper_for`` takes the already-resolved :class:`~cdk.types.EndpointScope`
(the engine maps its ``EndpointRef.scope`` to it at the boundary). Parsing /
validating the raw ``endpoint_ref`` dict — and rejecting a non-object or an
unknown scope — is the engine's job now and is covered where that boundary lives
(``DatabaseDestinationHandler._type_mapper_for_stream`` and the ``EndpointRef``
model tests), not here.
"""

from unittest.mock import Mock

import pytest

from cdk.type_map import TypeMapper
from cdk.type_map.rules import parse_rules
from cdk.connection_runtime import ConnectionRuntime
from cdk.types import EndpointScope


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
        assert rt.type_mapper_for(scope=EndpointScope.CONNECTOR) is cmapper

    def test_connection_scope_returns_connection_mapper(self):
        cmapper = TypeMapper("pg", parse_rules(RULES, source="<t>"))
        nmapper = TypeMapper("connection:test-conn", parse_rules(RULES, source="<t>"))
        rt = _runtime(connector_mapper=cmapper, connection_mapper=nmapper)
        assert rt.type_mapper_for(scope=EndpointScope.CONNECTION) is nmapper

    def test_connection_scope_without_mapper_falls_back_to_connector(self):
        cmapper = TypeMapper("pg", parse_rules(RULES, source="<t>"))
        rt = _runtime(connector_mapper=cmapper, connection_mapper=None)
        assert rt.type_mapper_for(scope=EndpointScope.CONNECTION) is cmapper

    def test_connector_mapper_required(self):
        """Accessing ``connector_type_mapper`` when it was not supplied raises."""
        rt = _runtime(connector_mapper=None)
        with pytest.raises(RuntimeError, match="connector_type_mapper not available"):
            _ = rt.connector_type_mapper
