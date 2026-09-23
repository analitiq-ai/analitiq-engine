"""End-to-end tests for :class:`src.engine.pipeline_config_prep.PipelineConfigPrep`.

``create_config()`` is the single seam every container hits at startup:
walk up to find ``pipelines/manifest.json``, locate the requested pipeline,
gate the run on the validator's verdict over its workspace, build the
in-memory ``ConnectionRuntime`` objects, and return the assembled config.

These tests stand up a complete on-disk pipeline tree under ``tmp_path``.
Which document the verdict refuses, and why, is the validator's to test;
these pin how the engine loads what it passed, and that it runs nothing the
verdict refused.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from analitiq.contracts.endpoint_identity import derive_db_endpoint_id
from analitiq.contracts.stream import StreamMapping

from cdk.conformance.fakes import type_map_document
from cdk.types import EndpointScope
from src.config.run_workspace import WorkspaceRejectedError
from src.engine.batch_policy import ErrorStrategy
from src.engine.mapping import compile_mapping
from src.engine.pipeline_config_prep import PipelineConfigPrep, _split_stream_ref

# ---------------------------------------------------------------------------
# On-disk fixture builders
#
# The documents are kept valid against the real published contracts so
# the real-schema tests below can run the same tree; ids therefore match
# the schemas' RFC-4122 pattern (version nibble 4, variant nibble 8).
# ---------------------------------------------------------------------------


CONNECTOR_ID = "demo-api"
CONNECTION_SRC_ID = "00000000-0000-4000-8000-000000000001"
CONNECTION_DST_ID = "00000000-0000-4000-8000-000000000002"
PIPELINE_ID = "00000000-0000-4000-8000-0000000000aa"
STREAM_ID = "00000000-0000-4000-8000-0000000000bb"
ENDPOINT_SRC = "src_endpoint"
ENDPOINT_DST = "dst_endpoint"

# Connection-scoped destination: a private database endpoint. Its endpoint_id
# is server-derived from database_object (never client-authored for
# scope="connection"), and the connection package holds the endpoint doc
# under that derived handle.
DST_DATABASE_OBJECT = {"schema": "public", "name": "dst_table"}
ENDPOINT_DST_CONNECTION = derive_db_endpoint_id(None, "public", "dst_table")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def _connector_doc() -> dict[str, Any]:
    return {
        "$schema": "https://schemas.analitiq.ai/connector/latest.json",
        "kind": "api",
        "connector_id": CONNECTOR_ID,
        "display_name": "Demo API",
        "version": "1.0.0",
        "auth": {"type": "none"},
        "connection_contract": {},
        "default_transport": "api",
        "transports": {
            "api": {
                "transport_type": "http",
                "base_url": "https://api.example.test",
            },
        },
    }


def _connection_doc(connection_id: str) -> dict[str, Any]:
    return {
        "$schema": "https://schemas.analitiq.ai/connection/latest.json",
        "connection_id": connection_id,
        "connector_id": CONNECTOR_ID,
        "display_name": f"Connection {connection_id}",
        "parameters": {"host": "https://api.example.test"},
    }


def _endpoint_doc(endpoint_id: str) -> dict[str, Any]:
    return {
        "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
        "endpoint_id": endpoint_id,
        "display_name": endpoint_id,
        "operations": {
            "read": {
                "request": {"method": "GET", "path": f"/{endpoint_id}"},
                "response": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "integer",
                                    "arrow_type": "Int64",
                                    "native_type": "integer",
                                },
                            },
                        },
                    },
                    "records": {"ref": "response.body"},
                },
            },
        },
    }


def _database_endpoint_doc(database_object: dict[str, Any]) -> dict[str, Any]:
    """A private (connection-scoped) database endpoint document, keyed by the
    server-derived endpoint_id over its database_object."""
    endpoint_id = derive_db_endpoint_id(
        database_object.get("catalog"),
        database_object["schema"],
        database_object["name"],
    )
    return {
        "$schema": "https://schemas.analitiq.ai/database-endpoint/latest.json",
        "endpoint_id": endpoint_id,
        "database_object": database_object,
        "columns": [
            {
                "name": "id",
                "native_type": "BIGINT",
                "arrow_type": "Int64",
                "nullable": False,
            },
        ],
        "primary_keys": ["id"],
    }


def _stream_doc(stream_id: str, *, dst_scope: str = "connector") -> dict[str, Any]:
    if dst_scope == "connection":
        # A connection-scoped ref carries database_object plus the server-derived
        # endpoint_id.
        dst_endpoint_ref = {
            "scope": "connection",
            "connection_id": CONNECTION_DST_ID,
            "endpoint_id": ENDPOINT_DST_CONNECTION,
            "database_object": DST_DATABASE_OBJECT,
        }
    else:
        dst_endpoint_ref = {
            "scope": "connector",
            "connection_id": CONNECTION_DST_ID,
            "endpoint_id": ENDPOINT_DST,
        }
    return {
        "$schema": "https://schemas.analitiq.ai/stream/latest.json",
        "stream_id": stream_id,
        "pipeline_id": PIPELINE_ID,
        "display_name": "Demo stream",
        "status": "active",
        "source": {
            "endpoint_ref": {
                "scope": "connector",
                "connection_id": CONNECTION_SRC_ID,
                "endpoint_id": ENDPOINT_SRC,
            },
            "primary_keys": ["id"],
        },
        "destinations": [
            {
                "endpoint_ref": dst_endpoint_ref,
                "write": {"mode": "insert"},
            },
        ],
        "mapping": {"assignments": []},
    }


def _pipeline_doc() -> dict[str, Any]:
    return {
        "$schema": "https://schemas.analitiq.ai/pipeline/latest.json",
        "pipeline_id": PIPELINE_ID,
        "display_name": "Demo Pipeline",
        "status": "active",
        "connections": {
            "source": CONNECTION_SRC_ID,
            "destinations": [CONNECTION_DST_ID],
        },
        "streams": [STREAM_ID],
        "schedule": {"type": "manual"},
    }


def _manifest(status: str = "active") -> dict[str, Any]:
    return {
        "pipelines": [
            {
                "pipeline_id": PIPELINE_ID,
                "path": f"{PIPELINE_ID}/pipeline.json",
                "status": status,
            },
        ],
    }


def _build_tree(
    root: Path,
    *,
    manifest_status: str = "active",
    include_stream_file: bool = True,
    include_manifest: bool = True,
    dst_endpoint_scope: str = "connector",
) -> Path:
    """Materialize a complete pipeline tree under ``root``. Returns ``root``.

    Knobs let individual tests inject specific defects (missing manifest,
    inactive status, stream-id mismatch).
    ``dst_endpoint_scope="connection"`` places the destination endpoint
    (plus a connection-scoped type-map) under the destination connection's
    ``definition/`` tree instead of the connector's, and points the stream's
    destination ``endpoint_ref`` at it with ``scope: "connection"``.
    """
    if include_manifest:
        _write_json(
            root / "pipelines" / "manifest.json", _manifest(status=manifest_status)
        )
    _write_json(root / "pipelines" / PIPELINE_ID / "pipeline.json", _pipeline_doc())
    if include_stream_file:
        stream_doc = _stream_doc(STREAM_ID, dst_scope=dst_endpoint_scope)
        _write_json(
            root / "pipelines" / PIPELINE_ID / "streams" / f"{STREAM_ID}.json",
            stream_doc,
        )

    for connection_id in (CONNECTION_SRC_ID, CONNECTION_DST_ID):
        _write_json(
            root / "connections" / connection_id / "connection.json",
            _connection_doc(connection_id),
        )
        _write_json(
            root / "connections" / connection_id / ".secrets" / "credentials.json",
            {},
        )

    _write_json(
        root / "connectors" / CONNECTOR_ID / "definition" / "connector.json",
        _connector_doc(),
    )
    _write_json(
        root / "connectors" / CONNECTOR_ID / "definition" / "type-map.json",
        type_map_document(
            read=[
                {"match": "exact", "native_type": "VARCHAR", "arrow_type": "Utf8"},
                {"match": "exact", "native_type": "BIGINT", "arrow_type": "Int64"},
                {"match": "exact", "native_type": "integer", "arrow_type": "Int64"},
            ],
        ),
    )
    connector_endpoints = [ENDPOINT_SRC]
    if dst_endpoint_scope == "connector":
        connector_endpoints.append(ENDPOINT_DST)
    else:
        dst_definition = root / "connections" / CONNECTION_DST_ID / "definition"
        private_doc = _database_endpoint_doc(DST_DATABASE_OBJECT)
        private_doc["description"] = "connection-scoped private endpoint"
        _write_json(
            dst_definition / "endpoints" / f"{ENDPOINT_DST_CONNECTION}.json",
            private_doc,
        )
        _write_json(
            dst_definition / "type-map.json",
            # Carries a rule the connector's map does not, so the test can
            # tell which mapper actually resolved.
            type_map_document(
                read=[{"match": "exact", "native_type": "JSONB", "arrow_type": "Utf8"}],
            ),
        )
    for endpoint_id in connector_endpoints:
        _write_json(
            root
            / "connectors"
            / CONNECTOR_ID
            / "definition"
            / "endpoints"
            / f"{endpoint_id}.json",
            _endpoint_doc(endpoint_id),
        )
    return root


@pytest.fixture
def pipeline_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "project"
    root.mkdir()
    _build_tree(root)
    monkeypatch.chdir(root)
    monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
    return root


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestCreateConfigHappyPath:
    def test_returns_five_tuple_with_expected_shapes(self, pipeline_tree: Path) -> None:
        """create_config returns the typed five-tuple with resolved shapes."""
        prep = PipelineConfigPrep()
        (
            pipeline_config,
            stream_configs,
            connections,
            endpoints,
            connectors,
        ) = prep.create_config()

        assert pipeline_config.pipeline_id == PIPELINE_ID
        assert pipeline_config.display_name == "Demo Pipeline"
        assert pipeline_config.connections.source == CONNECTION_SRC_ID
        assert pipeline_config.connections.destinations == [CONNECTION_DST_ID]

        assert len(stream_configs) == 1
        stream = stream_configs[0]
        assert stream.stream_id == STREAM_ID
        # A bare reference (no ``_v{n}`` suffix) resolves to version 1.
        assert stream.stream_version == 1
        assert stream.source.connection_ref == CONNECTION_SRC_ID
        assert stream.source.runtime is connections[CONNECTION_SRC_ID]
        assert stream.source.endpoint_document.endpoint_id == ENDPOINT_SRC
        assert stream.source.primary_keys == ["id"]
        assert stream.source.replication is None  # fixture stream omits replication
        assert stream.destinations[0].runtime is connections[CONNECTION_DST_ID]
        assert stream.destinations[0].endpoint_document.endpoint_id == ENDPOINT_DST

        assert set(connections) == {CONNECTION_SRC_ID, CONNECTION_DST_ID}
        assert len(endpoints) == 2

        assert len(connectors) == 1
        assert connectors[0].connector_id == CONNECTOR_ID

    def test_runtime_injection_is_shared_across_source_and_destination(
        self, pipeline_tree: Path
    ) -> None:
        """Each ``connection_id`` resolves to exactly one ``ConnectionRuntime``,
        and the instance injected as ``source._runtime`` / ``destinations[]._runtime``
        is the *same object* held in the returned ``connections`` map (identity,
        not a copy) — the engine relies on this to share TLS / connection pools.
        Distinct connection ids yield distinct runtimes."""
        prep = PipelineConfigPrep()
        _, stream_configs, connections, _, _ = prep.create_config()

        src_runtime = stream_configs[0].source.runtime
        dst_runtime = stream_configs[0].destinations[0].runtime
        assert src_runtime is connections[CONNECTION_SRC_ID]
        assert dst_runtime is connections[CONNECTION_DST_ID]
        assert src_runtime is not dst_runtime

    def test_resolved_source_to_source_config_is_json_safe(
        self, pipeline_tree: Path
    ) -> None:
        """``ResolvedSource.to_source_config()`` must not contain the
        ``ConnectionRuntime`` object — the result is passed directly to
        ``build_bootstrap`` and must be JSON-serialisable."""
        from cdk.connection_runtime import ConnectionRuntime

        prep = PipelineConfigPrep()
        _, stream_configs, _, _, _ = prep.create_config()

        source_config = stream_configs[0].source.to_source_config()
        assert "_runtime" not in source_config
        assert "_runtime" not in source_config.get("stream_source", {})
        # Must be JSON-serialisable (no ConnectionRuntime objects inside)
        for v in source_config.values():
            assert not isinstance(
                v, ConnectionRuntime
            ), f"to_source_config() must not embed ConnectionRuntime; got {type(v)}"
        json.dumps(source_config)  # raises if not serialisable


# ---------------------------------------------------------------------------
# The mapping crosses the config boundary typed
# ---------------------------------------------------------------------------


class TestStreamMappingReachesTheTransform:
    """A stream's `mapping` arrives as a compiled-ready document, not a dict.

    This is the seam the all-null nested read lived in: contract validation
    accepted the token-array path, and the transform read it, but the config
    layer in between rewrote paths. The document below is authorable — every
    node is in the published stream grammar (`get`, `pipe`, `fn: to_string`) —
    so the workspace verdict passes it on the way through.
    """

    def _write_mapping(self, root: Path, mapping: dict[str, Any]) -> None:
        stream_file = root / "pipelines" / PIPELINE_ID / "streams" / f"{STREAM_ID}.json"
        document = json.loads(stream_file.read_text())
        document["mapping"] = mapping
        _write_json(stream_file, document)

    def test_nested_get_inside_pipe_survives_config_prep(
        self, pipeline_tree: Path
    ) -> None:
        self._write_mapping(
            pipeline_tree,
            {
                "assignments": [
                    {
                        "target": {
                            "path": "city",
                            "arrow_type": "Utf8",
                            "nullable": False,
                        },
                        "value": {
                            "kind": "expression",
                            "expression": {
                                "op": "pipe",
                                "args": [
                                    {"op": "get", "path": ["address", "city"]},
                                    {"op": "fn", "name": "to_string"},
                                ],
                            },
                        },
                        "validate": {
                            "rules": [{"type": "not_null", "field": ["city"]}]
                        },
                    },
                ],
            },
        )

        prep = PipelineConfigPrep()
        _, stream_configs, _, _, _ = prep.create_config()

        mapping = stream_configs[0].mapping
        assert isinstance(mapping, StreamMapping)

        batch = pa.record_batch(
            [pa.array([{"city": "Berlin"}, {"city": "Kyiv"}])], names=["address"]
        )
        out = compile_mapping(mapping, default_strategy=ErrorStrategy.FAIL).run(batch)
        assert out.to_pylist() == [{"city": "Berlin"}, {"city": "Kyiv"}]


# ---------------------------------------------------------------------------
# Stream version parsing — the ``_v{n}`` suffix rides onto the checkpoint line
# ---------------------------------------------------------------------------


class TestStreamVersionParsing:
    @pytest.mark.parametrize(
        "ref,expected",
        [
            ("abc-123", ("abc-123", 1)),
            ("abc-123_v2", ("abc-123", 2)),
            ("abc-123_v17", ("abc-123", 17)),
            # A uuid with internal underscores keeps everything but the suffix.
            ("a_b_c_v3", ("a_b_c", 3)),
            # No trailing integer -> treated as bare (version 1).
            ("abc_vX", ("abc_vX", 1)),
            ("abc_v", ("abc_v", 1)),
        ],
    )
    def test_split_stream_ref(self, ref, expected) -> None:
        assert _split_stream_ref(ref) == expected

    def test_versioned_ref_resolves_bare_record_and_carries_version(
        self, pipeline_tree: Path
    ) -> None:
        """A ``{uuid}_v{n}`` reference in pipeline.streams resolves the bare
        stream document and surfaces version ``n`` on the resolved stream."""
        pipeline_doc = _pipeline_doc()
        pipeline_doc["streams"] = [f"{STREAM_ID}_v4"]
        _write_json(
            pipeline_tree / "pipelines" / PIPELINE_ID / "pipeline.json", pipeline_doc
        )

        prep = PipelineConfigPrep()
        _, stream_configs, _, _, _ = prep.create_config()

        assert len(stream_configs) == 1
        assert stream_configs[0].stream_id == STREAM_ID  # bare, unchanged
        assert stream_configs[0].stream_version == 4

    def test_source_replication_is_typed(self, pipeline_tree: Path) -> None:
        """A stream source's replication block is parsed into a typed
        ReplicationConfig on the resolved source (engine-internal view).

        No ``tie_breaker_fields`` here: the fixture source is connector-scoped
        (API), and the contract reserves that field for database sources
        (RULE-STRM-014), so it carries through as ``None``.
        """
        stream_doc = _stream_doc(STREAM_ID)
        stream_doc["source"]["replication"] = {
            "method": "incremental",
            "cursor_field": "updated_at",
        }
        _write_json(
            pipeline_tree / "pipelines" / PIPELINE_ID / "streams" / f"{STREAM_ID}.json",
            stream_doc,
        )

        prep = PipelineConfigPrep()
        _, stream_configs, _, _, _ = prep.create_config()

        src = stream_configs[0].source
        assert src.primary_keys == ["id"]
        assert src.replication is not None
        assert src.replication.method == "incremental"
        assert src.replication.cursor_field == "updated_at"
        assert src.replication.tie_breaker_fields is None


# ---------------------------------------------------------------------------
# Error paths — each should raise loudly with a message that names the offender
# ---------------------------------------------------------------------------


class TestCreateConfigErrorPaths:
    def test_missing_pipeline_id_env_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root)
        monkeypatch.chdir(root)
        monkeypatch.delenv("PIPELINE_ID", raising=False)
        with pytest.raises(RuntimeError, match="PIPELINE_ID environment variable"):
            PipelineConfigPrep()

    def test_a_document_the_verdict_refuses_stops_the_run(
        self, pipeline_tree: Path
    ) -> None:
        """The wiring: the run is gated on the workspace verdict, which is
        asked with the pipeline named to run -- so a draft pipeline, whatever
        its manifest entry says, runs nothing."""
        pipeline = _pipeline_doc()
        pipeline["status"] = "draft"
        _write_json(
            pipeline_tree / "pipelines" / PIPELINE_ID / "pipeline.json", pipeline
        )
        with pytest.raises(WorkspaceRejectedError, match="status"):
            PipelineConfigPrep().create_config()

    def test_the_manifest_status_does_not_decide_the_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root, manifest_status="draft")
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        pipeline, _, _, _, _ = PipelineConfigPrep().create_config()
        assert pipeline.pipeline_id == PIPELINE_ID

    def test_the_run_is_identified_by_the_graded_pipeline_document(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The manifest only locates the pipeline; its id is the document's."""
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root)
        locator = "00000000-0000-4000-8000-0000000000bb"
        manifest = _manifest()
        manifest["pipelines"][0]["pipeline_id"] = locator
        _write_json(root / "pipelines" / "manifest.json", manifest)
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", locator)
        pipeline, _, _, _, _ = PipelineConfigPrep().create_config()
        assert pipeline.pipeline_id == PIPELINE_ID

    def test_an_unreferenced_broken_package_does_not_block_the_run(
        self, pipeline_tree: Path
    ) -> None:
        for broken in (
            pipeline_tree / "connections" / "unrelated" / "connection.json",
            pipeline_tree
            / "connectors"
            / "unrelated"
            / "definition"
            / "connector.json",
        ):
            broken.parent.mkdir(parents=True)
            broken.write_text("{ not json")
        pipeline, _, _, _, _ = PipelineConfigPrep().create_config()
        assert pipeline.pipeline_id == PIPELINE_ID

    def test_unknown_pipeline_id_rejected(
        self, pipeline_tree: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PIPELINE_ID", "does-not-exist")
        prep = PipelineConfigPrep()
        with pytest.raises(ValueError, match="is not located by"):
            prep.create_config()

    def test_missing_manifest_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        root = tmp_path / "project"
        (root / "pipelines").mkdir(parents=True)
        # No manifest.json — _discover_paths walks the whole way up. Make
        # sure it can't find one above ``root`` either by parking ourselves
        # in a synthetic subdirectory whose parents have no manifest.
        deep = root / "nested"
        deep.mkdir()
        monkeypatch.chdir(deep)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        with pytest.raises(
            RuntimeError, match="Could not find pipelines/manifest.json"
        ):
            PipelineConfigPrep()


# ---------------------------------------------------------------------------
# Connection-scoped endpoints (#94)
# ---------------------------------------------------------------------------


class TestConnectionScopedEndpoints:
    """``scope: "connection"`` endpoint_refs resolve from the connection's
    own ``definition/`` tree, and the connection-scoped type-map wins over
    the connector's when present. Runs against the real published schemas
    so the private-endpoint layout is also contract-checked."""

    @pytest.fixture
    def connection_scoped_tree(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> Path:
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root, dst_endpoint_scope="connection")
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        return root

    def test_destination_endpoint_resolves_from_connection_tree(
        self, connection_scoped_tree: Path
    ) -> None:
        """A connection-scoped ref resolves the private endpoint document."""
        prep = PipelineConfigPrep()
        _, stream_configs, _, _, _ = prep.create_config()

        dest = stream_configs[0].destinations[0]
        assert dest.endpoint_ref.scope == "connection"
        # endpoint_id is server-derived from database_object.
        assert dest.endpoint_ref.endpoint_id == ENDPOINT_DST_CONNECTION
        assert dest.endpoint_ref.database_object is not None
        assert dest.endpoint_document.endpoint_id == ENDPOINT_DST_CONNECTION
        # The marker proves the connection-scoped file was read, not a
        # same-named connector endpoint.
        assert (
            dest.endpoint_document.description == "connection-scoped private endpoint"
        )
        # The source side still resolves from the connector tree.
        source = stream_configs[0].source
        assert source.endpoint_ref.scope == "connector"
        assert source.endpoint_document.description is None

    def test_connection_type_map_preferred_for_connection_scope(
        self, connection_scoped_tree: Path
    ) -> None:
        prep = PipelineConfigPrep()
        _, _, connections, _, _ = prep.create_config()

        runtime = connections[CONNECTION_DST_ID]
        assert runtime.connection_type_mapper is not None
        mapper = runtime.type_mapper_for(scope=EndpointScope.CONNECTION)
        # JSONB exists only in the connection-scoped map; composition means it
        # still resolves correctly (connection rules take precedence).
        assert mapper.to_arrow_type("JSONB") == "Utf8"

    def test_connection_scope_falls_back_to_connector_map_when_absent(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root, dst_endpoint_scope="connection")
        (
            root / "connections" / CONNECTION_DST_ID / "definition" / "type-map.json"
        ).unlink()
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)

        prep = PipelineConfigPrep()
        _, _, connections, _, _ = prep.create_config()

        runtime = connections[CONNECTION_DST_ID]
        assert runtime.connection_type_mapper is None
        mapper = runtime.type_mapper_for(scope=EndpointScope.CONNECTION)
        assert mapper is runtime.connector_type_mapper
        assert mapper.to_arrow_type("BIGINT") == "Int64"
