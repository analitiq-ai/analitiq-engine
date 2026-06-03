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
from src.engine.pipeline_config_prep import (
    PipelineConfigPrep,
    _parse_mapping_config,
    _parse_replication_config,
    _parse_stream_filter,
    _parse_write_config,
)
from src.models.resolved import (
    BatchingConfig,
    ErrorHandlingConfig,
    ResolvedDestination,
    ResolvedPipeline,
    ResolvedSource,
    ResolvedStream,
    RuntimeConfig,
)
from src.models.stream import ReplicationMethod, SourceConfig, WriteMode

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
        _write_json(
            root / "pipelines" / "manifest.json", _manifest(status=manifest_status)
        )
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
    def test_returns_resolved_pipeline_with_expected_shapes(self, pipeline_tree: Path) -> None:
        prep = PipelineConfigPrep()
        pipeline = prep.create_config()

        assert isinstance(pipeline, ResolvedPipeline)
        assert pipeline.pipeline_id == PIPELINE_ID
        assert pipeline.display_name == "Demo Pipeline"
        assert pipeline.status == "active"
        assert pipeline.source_connection_id == CONNECTION_SRC_ID
        assert pipeline.destination_connection_ids == [CONNECTION_DST_ID]

        assert len(pipeline.streams) == 1
        stream = pipeline.streams[0]
        assert isinstance(stream, ResolvedStream)
        assert stream.stream_id == STREAM_ID
        assert stream.source.runtime.connection_id == CONNECTION_SRC_ID
        assert stream.source.runtime is pipeline.connections[CONNECTION_SRC_ID]
        assert stream.source.endpoint["endpoint_id"] == ENDPOINT_SRC
        assert stream.destinations[0].runtime is pipeline.connections[CONNECTION_DST_ID]
        assert stream.destinations[0].endpoint["endpoint_id"] == ENDPOINT_DST

        assert set(pipeline.connections) == {CONNECTION_SRC_ID, CONNECTION_DST_ID}

    def test_source_config_is_typed(self, pipeline_tree: Path) -> None:
        """Stream source config is parsed into a typed SourceConfig."""
        prep = PipelineConfigPrep()
        pipeline = prep.create_config()

        source_config = pipeline.streams[0].source.config
        assert source_config.primary_keys == ["id"]
        assert source_config.replication.method == ReplicationMethod.FULL_REFRESH
        assert source_config.filters == []

    def test_runtime_injection_is_shared_across_source_and_destination(
        self, pipeline_tree: Path
    ) -> None:
        """Each ``connection_id`` resolves to exactly one ``ConnectionRuntime``;
        the same instance appears in both the stream's source/destination and
        the ``connections`` map — the engine relies on identity to share pools."""
        prep = PipelineConfigPrep()
        pipeline = prep.create_config()

        stream = pipeline.streams[0]
        src_runtime = stream.source.runtime
        dst_runtime = stream.destinations[0].runtime
        assert src_runtime is pipeline.connections[CONNECTION_SRC_ID]
        assert dst_runtime is pipeline.connections[CONNECTION_DST_ID]
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
        with pytest.raises(
            RuntimeError, match="Could not find pipelines/manifest.json"
        ):
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
        with pytest.raises(
            FileNotFoundError, match="Streams directory not found|stream file"
        ):
            prep.create_config()


# ---------------------------------------------------------------------------
# Parser unit tests — exercise the module-level typed parsers directly
# ---------------------------------------------------------------------------


class TestParserErrorPaths:
    def test_parse_replication_config_unknown_method_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown replication method"):
            _parse_replication_config({"method": "cdc_bogus"})

    def test_parse_replication_config_valid_incremental(self) -> None:
        cfg = _parse_replication_config({"method": "incremental", "cursor_field": "updated_at"})
        assert cfg.method == ReplicationMethod.INCREMENTAL
        assert cfg.cursor_field == "updated_at"

    def test_parse_write_config_unknown_mode_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown write mode"):
            _parse_write_config({"mode": "typo_mode"})

    def test_parse_write_config_valid_insert(self) -> None:
        cfg = _parse_write_config({"mode": "insert"})
        assert cfg.mode == WriteMode.INSERT

    def test_parse_stream_filter_missing_field_raises(self) -> None:
        with pytest.raises(ValueError, match="missing required 'field' key"):
            _parse_stream_filter({"operator": "eq", "value": "x"})

    def test_parse_stream_filter_valid(self) -> None:
        f = _parse_stream_filter({"field": "status", "operator": "eq", "value": "active"})
        assert f.field == "status"
        assert f.operator == "eq"
        assert f.value == "active"

    def test_parse_mapping_config_expression_assignment(self) -> None:
        raw = {
            "assignments": [
                {
                    "target": {"path": "name", "arrow_type": "Utf8"},
                    "value": {"expression": "$.source_name"},
                }
            ]
        }
        cfg = _parse_mapping_config(raw)
        assert len(cfg.assignments) == 1
        assert cfg.assignments[0].value.expression == "$.source_name"
        assert cfg.assignments[0].value.constant is None

    def test_parse_mapping_config_constant_assignment(self) -> None:
        raw = {
            "assignments": [
                {
                    "target": {"path": "region", "arrow_type": "Utf8"},
                    "value": {"constant": {"arrow_type": "Utf8", "value": "EU"}},
                }
            ]
        }
        cfg = _parse_mapping_config(raw)
        assert len(cfg.assignments) == 1
        assert cfg.assignments[0].value.constant is not None
        assert cfg.assignments[0].value.constant.value == "EU"

    def test_parse_mapping_config_no_expression_or_constant_raises(self) -> None:
        raw = {
            "assignments": [
                {
                    "target": {"path": "x", "arrow_type": "Utf8"},
                    "value": {},
                }
            ]
        }
        with pytest.raises(ValueError, match="non-null 'expression' or 'constant'"):
            _parse_mapping_config(raw)

    def test_stream_missing_pipeline_id_logs_warning(
        self, pipeline_tree: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        stream_doc = _stream_doc(STREAM_ID)
        del stream_doc["pipeline_id"]
        _write_json(
            pipeline_tree / "pipelines" / PIPELINE_ID / "streams" / f"{STREAM_ID}.json",
            stream_doc,
        )
        import logging
        with caplog.at_level(logging.WARNING, logger="src.engine.pipeline_config_prep"):
            prep = PipelineConfigPrep()
            pipeline = prep.create_config()
        assert pipeline.streams[0].pipeline_id == ""
        assert any("pipeline_id" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# ResolvedStream method tests
# ---------------------------------------------------------------------------


class TestResolvedStreamMethods:
    """Direct tests for ResolvedStream helper methods."""

    def _make_runtime(self):
        from unittest.mock import MagicMock
        rt = MagicMock()
        rt.connection_id = "test-conn"
        return rt

    def _make_destination(self) -> ResolvedDestination:
        from src.models.stream import DestinationConfig, EndpointRef
        return ResolvedDestination(
            config=DestinationConfig(
                endpoint_ref=EndpointRef(
                    scope="connector", connection_id="test-conn", endpoint_id="ep"
                )
            ),
            runtime=self._make_runtime(),
            endpoint={"endpoint_id": "ep"},
        )

    def _make_stream(self, destinations) -> ResolvedStream:
        from unittest.mock import MagicMock
        runtime = self._make_runtime()
        source = ResolvedSource(
            config=SourceConfig(),
            runtime=runtime,
            endpoint={},
        )
        return ResolvedStream(
            stream_id="s1",
            pipeline_id="test-pipeline",
            display_name=None,
            description=None,
            status="active",
            tags=[],
            source=source,
            destinations=destinations,
            mapping=__import__(
                "src.models.stream", fromlist=["MappingConfig"]
            ).MappingConfig(),
        )

    def test_get_primary_destination_returns_first(self) -> None:
        dest = self._make_destination()
        stream = self._make_stream([dest])
        assert stream.get_primary_destination() is dest

    def test_get_primary_destination_multiple_returns_first(self) -> None:
        dest1 = self._make_destination()
        dest2 = self._make_destination()
        stream = self._make_stream([dest1, dest2])
        assert stream.get_primary_destination() is dest1

    def test_get_primary_destination_empty_raises(self) -> None:
        stream = self._make_stream([])
        with pytest.raises(ValueError, match="has no destinations configured"):
            stream.get_primary_destination()


# ---------------------------------------------------------------------------
# Additional happy-path coverage
# ---------------------------------------------------------------------------


class TestCreateConfigRuntimeNonDefaults:
    def test_runtime_non_defaults_propagate(
        self, pipeline_tree: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Explicit runtime block in pipeline.json is parsed into RuntimeConfig."""
        pipeline_doc = _pipeline_doc()
        pipeline_doc["runtime"] = {
            "batching": {"batch_size": 250, "max_concurrent_batches": 5},
            "error_handling": {"strategy": "dlq", "max_retries": 1},
            "buffer_size": 8000,
        }
        _write_json(
            pipeline_tree / "pipelines" / PIPELINE_ID / "pipeline.json", pipeline_doc
        )
        prep = PipelineConfigPrep()
        pipeline = prep.create_config()

        assert pipeline.runtime.batching.batch_size == 250
        assert pipeline.runtime.batching.max_concurrent_batches == 5
        assert pipeline.runtime.error_handling.strategy == "dlq"
        assert pipeline.runtime.error_handling.max_retries == 1
        assert pipeline.runtime.buffer_size == 8000


# ---------------------------------------------------------------------------
# Additional error paths
# ---------------------------------------------------------------------------


class TestCreateConfigAdditionalErrorPaths:
    def test_no_destinations_in_pipeline_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        schema_mirror: Path,
    ) -> None:
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root)
        pipeline_doc = _pipeline_doc()
        pipeline_doc["connections"]["destinations"] = []
        _write_json(root / "pipelines" / PIPELINE_ID / "pipeline.json", pipeline_doc)
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        prep = PipelineConfigPrep()
        with pytest.raises(ValueError, match="at least one destination"):
            prep.create_config()

    def test_connection_id_mismatch_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        schema_mirror: Path,
    ) -> None:
        """connection.json whose connection_id disagrees with its directory name is rejected."""
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root)
        # Overwrite src connection doc with a mismatched connection_id.
        bad_doc = _connection_doc("different-id-entirely")
        _write_json(
            root / "connections" / CONNECTION_SRC_ID / "connection.json", bad_doc
        )
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        prep = PipelineConfigPrep()
        with pytest.raises(ValueError, match="Connection id mismatch"):
            prep.create_config()

    def test_duplicate_stream_id_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        schema_mirror: Path,
    ) -> None:
        """Two stream files in the same directory sharing a stream_id are rejected."""
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root)
        # Write a second stream file that declares the same stream_id.
        _write_json(
            root / "pipelines" / PIPELINE_ID / "streams" / "duplicate.json",
            _stream_doc(STREAM_ID),
        )
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        prep = PipelineConfigPrep()
        with pytest.raises(ValueError, match="Duplicate stream_id"):
            prep.create_config()


# ---------------------------------------------------------------------------
# BatchingConfig / ErrorHandlingConfig invariant tests
# ---------------------------------------------------------------------------


class TestConfigInvariants:
    def test_batching_config_zero_batch_size_raises(self) -> None:
        with pytest.raises(ValueError, match="batch_size must be positive"):
            BatchingConfig(batch_size=0)

    def test_batching_config_negative_batch_size_raises(self) -> None:
        with pytest.raises(ValueError, match="batch_size must be positive"):
            BatchingConfig(batch_size=-1)

    def test_batching_config_zero_concurrent_raises(self) -> None:
        with pytest.raises(ValueError, match="max_concurrent_batches must be positive"):
            BatchingConfig(max_concurrent_batches=0)

    def test_error_handling_config_unknown_strategy_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown error strategy"):
            ErrorHandlingConfig(strategy="ignore")

    def test_error_handling_config_valid_strategies(self) -> None:
        assert ErrorHandlingConfig(strategy="fail").strategy == "fail"
        assert ErrorHandlingConfig(strategy="dlq").strategy == "dlq"

    def test_resolved_pipeline_empty_pipeline_id_raises(self) -> None:
        with pytest.raises(ValueError, match="pipeline_id cannot be empty"):
            ResolvedPipeline(
                pipeline_id="",
                display_name=None,
                description=None,
                status="active",
                tags=[],
                source_connection_id="src",
                destination_connection_ids=[],
                streams=[],
                runtime=RuntimeConfig(),
                schedule={},
                engine_config={},
                connections={},
            )
