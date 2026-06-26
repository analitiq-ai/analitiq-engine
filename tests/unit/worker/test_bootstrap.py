"""Tests for the worker launch bootstrap (parse + stdin reader).

The bootstrap is the only input a worker receives and it carries resolved
credentials, so two properties are load-bearing: shape validation rejects a
malformed launch before any connector code runs, and parse errors never echo
the document (which would put secrets in the shell's logs).
"""

from __future__ import annotations

import io
import json

import pytest

from cdk.type_map import TypeMapper
from src.worker.bootstrap import (
    WorkerBootstrap,
    parse_bootstrap,
    read_bootstrap_from_stdin,
)


def _minimal_raw(**overrides):
    raw = {
        "role": "source",
        "kind": "database",
        "connector_id": "postgres",
        "uds_path": "/tmp/w/worker.sock",
        "connection": {
            "connection_id": "my-pg",
            "connector_id": "postgres",
            "connector_type": "database",
            "driver_hint": "postgresql",
            "transport_spec": None,
            "resolved_config": {"host": "db"},
        },
    }
    raw.update(overrides)
    return raw


class TestParseBootstrap:
    def test_minimal_source_payload(self):
        bootstrap = parse_bootstrap(_minimal_raw())
        assert isinstance(bootstrap, WorkerBootstrap)
        assert bootstrap.role == "source"
        assert bootstrap.kind == "database"
        assert bootstrap.connector_id == "postgres"
        assert bootstrap.uds_path == "/tmp/w/worker.sock"
        assert bootstrap.connection_payload["connection_id"] == "my-pg"
        # Optional blocks default to empty, not None.
        assert bootstrap.endpoint_refs == {}
        assert bootstrap.stream_endpoints == {}
        assert bootstrap.source_config == {}
        assert bootstrap.connector_type_mapper is None
        assert bootstrap.connection_type_mapper is None

    def test_destination_role_accepted(self):
        bootstrap = parse_bootstrap(
            _minimal_raw(
                role="destination",
                endpoint_refs={"s1": {"endpoint_id": "t"}},
                stream_endpoints={"s1": {"table": "t"}},
            )
        )
        assert bootstrap.role == "destination"
        assert bootstrap.endpoint_refs == {"s1": {"endpoint_id": "t"}}
        assert bootstrap.stream_endpoints == {"s1": {"table": "t"}}

    @pytest.mark.parametrize("role", [None, "", "engine", "SOURCE"])
    def test_invalid_role_rejected(self, role):
        with pytest.raises(ValueError, match="bootstrap.role"):
            parse_bootstrap(_minimal_raw(role=role))

    @pytest.mark.parametrize("key", ["kind", "connector_id", "uds_path", "connection"])
    def test_missing_required_field_rejected(self, key):
        raw = _minimal_raw()
        del raw[key]
        with pytest.raises(ValueError, match=f"bootstrap.{key} is required"):
            parse_bootstrap(raw)

    def test_type_map_blocks_build_mappers(self):
        raw = _minimal_raw(
            type_maps={
                "connector": {
                    "rules": [
                        {"match": "exact", "native": "BIGINT", "canonical": "Int64"},
                    ],
                    "write_rules": None,
                },
                "connection": None,
            }
        )
        bootstrap = parse_bootstrap(raw)
        assert isinstance(bootstrap.connector_type_mapper, TypeMapper)
        assert bootstrap.connection_type_mapper is None

    def test_build_runtime_carries_pre_resolved_payload(self):
        runtime = parse_bootstrap(_minimal_raw()).build_runtime()
        assert runtime.connection_id == "my-pg"
        assert runtime.connector_id == "postgres"
        assert runtime.connector_type == "database"


class TestReadBootstrapFromStdin:
    def _with_stdin(self, monkeypatch, text: str):
        monkeypatch.setattr("sys.stdin", io.StringIO(text))

    def test_valid_payload_round_trips(self, monkeypatch):
        self._with_stdin(monkeypatch, json.dumps(_minimal_raw()))
        bootstrap = read_bootstrap_from_stdin()
        assert bootstrap.connector_id == "postgres"

    def test_empty_stdin_rejected(self, monkeypatch):
        self._with_stdin(monkeypatch, "  \n")
        with pytest.raises(ValueError, match="without a bootstrap"):
            read_bootstrap_from_stdin()

    def test_non_object_rejected(self, monkeypatch):
        self._with_stdin(monkeypatch, "[1, 2]")
        with pytest.raises(ValueError, match="must be a JSON object"):
            read_bootstrap_from_stdin()

    def test_malformed_json_error_never_echoes_the_document(self, monkeypatch):
        # The document carries resolved credentials; a parse error must
        # report the shape problem only, never the text.
        secret = "hunter2-super-secret"
        self._with_stdin(monkeypatch, f'{{"password": "{secret}", broken')
        with pytest.raises(ValueError, match="not valid JSON") as excinfo:
            read_bootstrap_from_stdin()
        assert secret not in str(excinfo.value)
        assert secret not in repr(excinfo.value.__cause__)
