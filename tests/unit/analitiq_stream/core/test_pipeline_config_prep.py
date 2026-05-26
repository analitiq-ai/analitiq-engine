"""End-to-end tests for :class:`src.engine.pipeline_config_prep.PipelineConfigPrep`.

``create_config()`` is the single seam every container hits at startup:
walk up to find ``pipelines/manifest.json``, resolve the requested
pipeline_id, validate manifest entry + pipeline doc + stream docs +
connection docs + connector docs against the published JSON Schemas,
build the in-memory ``ConnectionRuntime`` objects, and return the
assembled config.

These tests stand up a complete on-disk pipeline tree under ``tmp_path``
plus a local ``file://`` schema mirror so contract validation runs
without network access.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pytest

from src.config.schema_validator import _load_schema
from src.engine.pipeline_config_prep import PipelineConfigPrep


# ---------------------------------------------------------------------------
# Schema-mirror infrastructure
# ---------------------------------------------------------------------------


_PERMISSIVE_SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
}

_ARTIFACT_KINDS = (
    "connector",
    "connection",
    "pipeline",
    "stream",
    "endpoint",
    "api-endpoint",
    "database-endpoint",
)


@pytest.fixture(autouse=True)
def _reset_schema_cache():
    """``_load_schema`` is ``@lru_cache``d process-wide; reset between tests
    so each test's ``file://`` mirror is honoured."""
    _load_schema.cache_clear()
    yield
    _load_schema.cache_clear()


@pytest.fixture
def schema_mirror(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Serve permissive ``type: "object"`` schemas for every artifact kind
    from a local ``file://`` mirror. Validation runs but accepts anything
    object-shaped, so the tests don't need to track schema-side breakage."""
    mirror = tmp_path / "schemas"
    for kind in _ARTIFACT_KINDS:
        kind_dir = mirror / kind
        kind_dir.mkdir(parents=True, exist_ok=True)
        (kind_dir / "latest.json").write_text(json.dumps(_PERMISSIVE_SCHEMA))
    monkeypatch.setenv("ANALITIQ_SCHEMA_BASE_URL", mirror.as_uri())
    return mirror


# ---------------------------------------------------------------------------
# On-disk fixture builders
# ---------------------------------------------------------------------------


CONNECTOR_ID = "demo-api"
CONNECTION_SRC_ID = "00000000-0000-0000-0000-000000000001"
CONNECTION_DST_ID = "00000000-0000-0000-0000-000000000002"
PIPELINE_ID = "00000000-0000-0000-0000-0000000000aa"
STREAM_ID = "00000000-0000-0000-0000-0000000000bb"
ENDPOINT_SRC = "src_endpoint"
ENDPOINT_DST = "dst_endpoint"


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def _connector_doc() -> Dict[str, Any]:
    return {
        "$schema": "https://schemas.analitiq.ai/connector/latest.json",
        "kind": "api",
        "connector_id": CONNECTOR_ID,
        "display_name": "Demo API",
        "auth": {"type": "none"},
    }


def _connection_doc(connection_id: str) -> Dict[str, Any]:
    return {
        "$schema": "https://schemas.analitiq.ai/connection/latest.json",
        "connection_id": connection_id,
        "connector_id": CONNECTOR_ID,
        "display_name": f"Connection {connection_id}",
        "host": "https://api.example.test",
        "parameters": {},
    }


def _endpoint_doc(endpoint_id: str) -> Dict[str, Any]:
    return {
        "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
        "endpoint_id": endpoint_id,
        "display_name": endpoint_id,
        "operations": {
            "read": {
                "request": {"method": "GET", "path": f"/{endpoint_id}"},
                "response": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "integer", "arrow_type": "Int64"},
                        },
                    },
                    "records": {"ref": "response.body"},
                },
            },
        },
    }


def _type_map_rules() -> list:
    return [
        {"match": "exact", "native": "VARCHAR", "canonical": "Utf8"},
        {"match": "exact", "native": "BIGINT", "canonical": "Int64"},
    ]


def _stream_doc(stream_id: str) -> Dict[str, Any]:
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
                "endpoint_ref": {
                    "scope": "connector",
                    "connection_id": CONNECTION_DST_ID,
                    "endpoint_id": ENDPOINT_DST,
                },
                "write": {"mode": "insert"},
            },
        ],
        "mapping": {"assignments": []},
    }


def _pipeline_doc() -> Dict[str, Any]:
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


def _manifest(status: str = "active") -> Dict[str, Any]:
    return {
        "pipelines": [
            {
                "pipeline_id": PIPELINE_ID,
                "display_name": "Demo Pipeline",
                "path": f"{PIPELINE_ID}/pipeline.json",
                "status": status,
                "streams": [STREAM_ID],
            },
        ],
    }


def _build_tree(
    root: Path,
    *,
    manifest_status: str = "active",
    include_stream_file: bool = True,
    stream_id_in_file: str = STREAM_ID,
    include_manifest: bool = True,
) -> Path:
    """Materialize a complete pipeline tree under ``root``. Returns ``root``.

    Knobs let individual tests inject specific defects (missing manifest,
    inactive status, stream-id mismatch, missing stream file).
    """
    if include_manifest:
        _write_json(root / "pipelines" / "manifest.json", _manifest(status=manifest_status))
    _write_json(root / "pipelines" / PIPELINE_ID / "pipeline.json", _pipeline_doc())
    if include_stream_file:
        stream_doc = _stream_doc(stream_id_in_file)
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
        _type_map_rules(),
    )
    for endpoint_id in (ENDPOINT_SRC, ENDPOINT_DST):
        _write_json(
            root / "connectors" / CONNECTOR_ID / "definition" / "endpoints" / f"{endpoint_id}.json",
            _endpoint_doc(endpoint_id),
        )
    return root


@pytest.fixture
def pipeline_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema_mirror: Path
) -> Path:
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
        prep = PipelineConfigPrep()
        pipeline_config, stream_configs, connections, endpoints, connectors = prep.create_config()

        assert pipeline_config["pipeline_id"] == PIPELINE_ID
        assert pipeline_config["display_name"] == "Demo Pipeline"
        assert pipeline_config["status"] == "active"
        assert pipeline_config["connections"]["source"] == CONNECTION_SRC_ID
        assert pipeline_config["connections"]["destinations"] == [CONNECTION_DST_ID]

        assert len(stream_configs) == 1
        stream = stream_configs[0]
        assert stream["stream_id"] == STREAM_ID
        assert stream["source"]["connection_ref"] == CONNECTION_SRC_ID
        assert stream["source"]["_runtime"] is connections[CONNECTION_SRC_ID]
        assert stream["source"]["_endpoint"]["endpoint_id"] == ENDPOINT_SRC
        assert stream["destinations"][0]["_runtime"] is connections[CONNECTION_DST_ID]
        assert stream["destinations"][0]["_endpoint"]["endpoint_id"] == ENDPOINT_DST

        assert set(connections) == {CONNECTION_SRC_ID, CONNECTION_DST_ID}
        assert len(endpoints) == 2

        assert len(connectors) == 1
        assert connectors[0]["connector_id"] == CONNECTOR_ID

    def test_runtime_injection_is_shared_across_source_and_destination(
        self, pipeline_tree: Path
    ) -> None:
        """Same connection used in two streams should yield the *same*
        ``ConnectionRuntime`` instance — not a fresh copy. The destination
        engine relies on this to share TLS / connection pools."""
        prep = PipelineConfigPrep()
        _, stream_configs, connections, _, _ = prep.create_config()

        src_runtime = stream_configs[0]["source"]["_runtime"]
        dst_runtime = stream_configs[0]["destinations"][0]["_runtime"]
        assert src_runtime is connections[CONNECTION_SRC_ID]
        assert dst_runtime is connections[CONNECTION_DST_ID]
        assert src_runtime is not dst_runtime


# ---------------------------------------------------------------------------
# Error paths — each should raise loudly with a message that names the offender
# ---------------------------------------------------------------------------


class TestCreateConfigErrorPaths:
    def test_missing_pipeline_id_env_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema_mirror: Path
    ) -> None:
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root)
        monkeypatch.chdir(root)
        monkeypatch.delenv("PIPELINE_ID", raising=False)
        with pytest.raises(RuntimeError, match="PIPELINE_ID environment variable"):
            PipelineConfigPrep()

    def test_inactive_pipeline_status_rejected(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        schema_mirror: Path,
    ) -> None:
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root, manifest_status="draft")
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        prep = PipelineConfigPrep()
        with pytest.raises(ValueError, match="only 'active' pipelines"):
            prep.create_config()

    def test_unknown_pipeline_id_rejected(
        self, pipeline_tree: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PIPELINE_ID", "does-not-exist")
        prep = PipelineConfigPrep()
        with pytest.raises(ValueError, match="not found in manifest"):
            prep.create_config()

    def test_missing_manifest_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema_mirror: Path
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
        with pytest.raises(RuntimeError, match="Could not find pipelines/manifest.json"):
            PipelineConfigPrep()

    def test_stream_id_mismatch_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema_mirror: Path
    ) -> None:
        """Stream file at ``streams/<STREAM_ID>.json`` whose document
        carries a different ``stream_id`` should be silently re-keyed by
        the document. Pipeline.streams references the document id, so a
        mismatch surfaces as ``pipeline.streams references X but no stream
        file declares that id``."""
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root, stream_id_in_file="different-stream-id")
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        prep = PipelineConfigPrep()
        with pytest.raises(
            ValueError, match=r"pipeline\.streams references .* but no stream file"
        ):
            prep.create_config()

    def test_missing_stream_file_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema_mirror: Path
    ) -> None:
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root, include_stream_file=False)
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        prep = PipelineConfigPrep()
        with pytest.raises(FileNotFoundError, match="Streams directory not found|stream file"):
            prep.create_config()
