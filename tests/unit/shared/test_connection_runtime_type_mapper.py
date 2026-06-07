"""Tests for ``ConnectionRuntime.type_mapper_for`` — per-endpoint mapper dispatch.

``type_mapper_for`` takes the already-resolved :class:`~cdk.types.EndpointScope`
(the engine maps its ``EndpointRef.scope`` to it at the boundary). Parsing /
validating the raw ``endpoint_ref`` dict — and rejecting a non-object or an
unknown scope — is the engine's job now and is covered where that boundary lives
(``GenericSQLConnector._type_mapper_for_stream`` and the ``EndpointRef``
model tests), not here.

Connection maps compose with the connector map per-type (issue #126): the
connection's rules take precedence; the connector's rules are consulted on a
miss, for both read and write directions.
"""

from unittest.mock import Mock

import pytest

from cdk.type_map import TypeMapper
from cdk.type_map.rules import parse_rules, parse_write_rules
from cdk.connection_runtime import ConnectionRuntime
from cdk.types import EndpointScope


CONNECTOR_READ_RULES = [{"match": "exact", "native": "BIGINT", "canonical": "Int64"}]
CONNECTION_READ_RULES = [{"match": "exact", "native": "CUSTOM_TYPE", "canonical": "Utf8"}]
CONNECTOR_WRITE_RULES = [{"match": "exact", "canonical": "Int64", "native": "BIGINT"}]
CONNECTION_WRITE_RULES = [{"match": "exact", "canonical": "Utf8", "native": "CUSTOM_NATIVE"}]


def _connector_mapper(*, with_write: bool = False) -> TypeMapper:
    write = parse_write_rules(CONNECTOR_WRITE_RULES, source="<cw>") if with_write else None
    return TypeMapper("pg", parse_rules(CONNECTOR_READ_RULES, source="<cr>"), write)


def _connection_mapper(*, with_write: bool = False) -> TypeMapper:
    write = parse_write_rules(CONNECTION_WRITE_RULES, source="<nw>") if with_write else None
    return TypeMapper(
        "connection:test-conn",
        parse_rules(CONNECTION_READ_RULES, source="<nr>"),
        write,
    )


def _runtime(
    *,
    connector_mapper: TypeMapper | None = None,
    connection_mapper: TypeMapper | None = None,
) -> ConnectionRuntime:
    return ConnectionRuntime(
        raw_config={},
        connection_id="test-conn",
        connector_id="test-connector",
        connector_type="database",
        driver="postgresql",
        resolver=Mock(),
        connector_type_mapper=connector_mapper,
        connection_type_mapper=connection_mapper,
    )


class TestTypeMapperFor:
    def test_connector_scope_returns_connector_mapper(self):
        cmapper = _connector_mapper()
        rt = _runtime(connector_mapper=cmapper)
        assert rt.type_mapper_for(scope=EndpointScope.CONNECTOR) is cmapper

    def test_connection_scope_without_mapper_falls_back_to_connector(self):
        cmapper = _connector_mapper()
        rt = _runtime(connector_mapper=cmapper, connection_mapper=None)
        assert rt.type_mapper_for(scope=EndpointScope.CONNECTION) is cmapper

    def test_connector_mapper_required(self):
        """Accessing ``connector_type_mapper`` when it was not supplied raises."""
        rt = _runtime(connector_mapper=None)
        with pytest.raises(RuntimeError, match="connector_type_mapper not available"):
            _ = rt.connector_type_mapper


class TestConnectionScopeComposition:
    """Connection maps compose with the connector map per-type (issue #126)."""

    def test_connection_read_rule_overrides_connector(self):
        rt = _runtime(
            connector_mapper=_connector_mapper(),
            connection_mapper=_connection_mapper(),
        )
        composed = rt.type_mapper_for(scope=EndpointScope.CONNECTION)
        assert composed.to_arrow_type("CUSTOM_TYPE") == "Utf8"

    def test_connector_read_rule_available_as_fallback(self):
        rt = _runtime(
            connector_mapper=_connector_mapper(),
            connection_mapper=_connection_mapper(),
        )
        composed = rt.type_mapper_for(scope=EndpointScope.CONNECTION)
        # BIGINT is only in the connector map — fallback must find it.
        assert composed.to_arrow_type("BIGINT") == "Int64"

    def test_connection_write_rule_overrides_connector(self):
        rt = _runtime(
            connector_mapper=_connector_mapper(with_write=True),
            connection_mapper=_connection_mapper(with_write=True),
        )
        composed = rt.type_mapper_for(scope=EndpointScope.CONNECTION)
        # Connection overrides Utf8 → CUSTOM_NATIVE; connector maps Int64 → BIGINT.
        assert composed.to_native_type("Utf8") == "CUSTOM_NATIVE"

    def test_connector_write_rule_available_as_fallback(self):
        rt = _runtime(
            connector_mapper=_connector_mapper(with_write=True),
            connection_mapper=_connection_mapper(with_write=True),
        )
        composed = rt.type_mapper_for(scope=EndpointScope.CONNECTION)
        # Int64 write rule is only in the connector map.
        assert composed.to_native_type("Int64") == "BIGINT"

    def test_connection_read_only_map_inherits_connector_write_rules(self):
        # A connection with a read override but no write map still supports DDL
        # generation via the connector's write rules (the core gap from #126).
        rt = _runtime(
            connector_mapper=_connector_mapper(with_write=True),
            connection_mapper=_connection_mapper(with_write=False),
        )
        composed = rt.type_mapper_for(scope=EndpointScope.CONNECTION)
        assert composed.has_write_map is True
        assert composed.to_native_type("Int64") == "BIGINT"

    def test_composed_mapper_is_not_either_original(self):
        cmapper = _connector_mapper()
        nmapper = _connection_mapper()
        rt = _runtime(connector_mapper=cmapper, connection_mapper=nmapper)
        composed = rt.type_mapper_for(scope=EndpointScope.CONNECTION)
        assert composed is not cmapper
        assert composed is not nmapper
