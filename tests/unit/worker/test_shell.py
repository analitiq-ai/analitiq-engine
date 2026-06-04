"""Tests for the trusted-shell bootstrap assembly.

``read_type_map_payloads`` must perform the same directory lookup and JSON
validation as the file loaders (one parser, one error surface), and
``build_bootstrap`` must pack only resolved values — the payload it returns
is what crosses the stdin pipe into the untrusted worker.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from cdk.type_map import InvalidTypeMapError
from cdk.type_map.loader import build_type_mapper
from src.worker.shell import build_bootstrap, read_type_map_payloads

_RULES = [{"match": "exact", "native": "BIGINT", "canonical": "Int64"}]
_WRITE_RULES = [{"match": "exact", "canonical": "Int64", "native": "BIGINT"}]


def _write_definition(base, *, rules=None, write_rules=None):
    definition = base / "definition"
    definition.mkdir(parents=True)
    if rules is not None:
        (definition / "type-map-read.json").write_text(json.dumps(rules))
    if write_rules is not None:
        (definition / "type-map-write.json").write_text(json.dumps(write_rules))
    return definition


class TestReadTypeMapPayloads:
    def test_connector_and_connection_blocks(self, tmp_path):
        connectors = tmp_path / "connectors"
        connections = tmp_path / "connections"
        _write_definition(
            connectors / "postgres", rules=_RULES, write_rules=_WRITE_RULES
        )
        _write_definition(connections / "my-pg", rules=_RULES)

        payloads = read_type_map_payloads(
            connectors, "postgres", connections, "my-pg"
        )
        assert payloads["connector"] == {
            "rules": _RULES,
            "write_rules": _WRITE_RULES,
        }
        assert payloads["connection"] == {"rules": _RULES, "write_rules": None}

    def test_alternate_connector_layout(self, tmp_path):
        connectors = tmp_path / "connectors"
        connections = tmp_path / "connections"
        _write_definition(connectors / "connector-postgres", rules=_RULES)
        connections.mkdir()

        payloads = read_type_map_payloads(
            connectors, "postgres", connections, "my-pg"
        )
        assert payloads["connector"] == {"rules": _RULES, "write_rules": None}
        assert payloads["connection"] is None

    def test_absent_maps_are_none(self, tmp_path):
        payloads = read_type_map_payloads(
            tmp_path / "connectors", "postgres", tmp_path / "connections", "my-pg"
        )
        assert payloads == {"connector": None, "connection": None}

    def test_malformed_map_raises_typed_error(self, tmp_path):
        connectors = tmp_path / "connectors"
        definition = (connectors / "postgres") / "definition"
        definition.mkdir(parents=True)
        (definition / "type-map-read.json").write_text("{not json")

        # Same error type and surface as the file loaders.
        with pytest.raises(InvalidTypeMapError, match="not valid JSON"):
            read_type_map_payloads(
                connectors, "postgres", tmp_path / "connections", "my-pg"
            )


class TestBuildTypeMapper:
    def test_rebuilds_mapper_from_raw_arrays(self):
        mapper = build_type_mapper("postgres", _RULES, _WRITE_RULES)
        assert mapper is not None

    def test_non_list_rules_payload_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="JSON array"):
            build_type_mapper("postgres", {"match": "exact"})

    def test_non_list_write_rules_payload_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="write-type-map"):
            build_type_mapper("postgres", _RULES, {"match": "exact"})


class TestBuildBootstrap:
    async def test_packs_resolved_payload_and_type_maps(self, tmp_path):
        connectors = tmp_path / "connectors"
        connections = tmp_path / "connections"
        _write_definition(connectors / "postgres", rules=_RULES)
        connections.mkdir()

        runtime = MagicMock()
        runtime.connector_type = "database"
        runtime.connector_id = "postgres"
        runtime.connection_id = "my-pg"
        runtime.resolve_spec = AsyncMock(
            return_value={"connection_id": "my-pg", "transport_spec": {"dsn": "x"}}
        )

        bootstrap = await build_bootstrap(
            runtime,
            role="source",
            connectors_dir=connectors,
            connections_dir=connections,
            source_config={"stream_source": {}},
        )
        assert bootstrap["role"] == "source"
        assert bootstrap["kind"] == "database"
        assert bootstrap["connector_id"] == "postgres"
        assert bootstrap["connection"]["transport_spec"] == {"dsn": "x"}
        assert bootstrap["type_maps"]["connector"] == {
            "rules": _RULES,
            "write_rules": None,
        }
        assert bootstrap["source_config"] == {"stream_source": {}}
        # The whole bootstrap is JSON-safe — it crosses the stdin pipe.
        assert json.loads(json.dumps(bootstrap)) == bootstrap
