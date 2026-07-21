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
from typing import Any

import pytest
from analitiq.contracts.endpoint_identity import derive_db_endpoint_id

from cdk.types import EndpointScope
from src.config.schema_validator import BundleValidationError, ContractValidationError
from src.engine.pipeline_config_prep import PipelineConfigPrep, _split_stream_ref

# ---------------------------------------------------------------------------
# Contract validation
#
# Validation is offline against the pinned analitiq-contract-models
# (src/config/schema_validator.py) -- no schema mirror, no network fetch.
# Every fixture document below is kept valid against the real published
# contracts, so the tests exercise actual contract validation. The two
# fixtures are retained as no-ops so test signatures stay stable.
# ---------------------------------------------------------------------------


@pytest.fixture
def schema_mirror() -> None:
    """No-op: contract validation runs offline against the pinned models."""
    return None


@pytest.fixture
def real_schema_mirror() -> None:
    """No-op: contract validation runs offline against the pinned models."""
    return None


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
# scope="connection"), and the bundle writes the endpoint doc under that
# derived handle.
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


def _type_map_rules() -> list:
    return [
        {"match": "exact", "native": "VARCHAR", "canonical": "Utf8"},
        {"match": "exact", "native": "BIGINT", "canonical": "Int64"},
    ]


def _connection_type_map_rules() -> list:
    """Connection-scoped override map: carries a rule the connector's
    map does not, so tests can tell which mapper actually resolved."""
    return [
        {"match": "exact", "native": "JSONB", "canonical": "Utf8"},
    ]


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
        # endpoint_id (the run bundle carries both, keyed by the derived handle).
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
    dst_endpoint_scope: str = "connector",
) -> Path:
    """Materialize a complete pipeline tree under ``root``. Returns ``root``.

    Knobs let individual tests inject specific defects (missing manifest,
    inactive status, stream-id mismatch, missing stream file).
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
        stream_doc = _stream_doc(stream_id_in_file, dst_scope=dst_endpoint_scope)
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
        root / "connectors" / CONNECTOR_ID / "definition" / "type-map-read.json",
        _type_map_rules(),
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
        _write_json(dst_definition / "type-map-read.json", _connection_type_map_rules())
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
        assert pipeline_config.status == "active"
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
        assert connectors[0]["connector_id"] == CONNECTOR_ID

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

    def test_versioned_ref_without_matching_record_raises_naming_both_ids(
        self, pipeline_tree: Path
    ) -> None:
        """A versioned ref whose bare id has no stream file fails loud, and the
        message names both the full ref and the bare id it looked up."""
        missing_bare = "00000000-0000-4000-8000-000000000000"
        pipeline_doc = _pipeline_doc()
        pipeline_doc["streams"] = [f"{missing_bare}_v4"]
        _write_json(
            pipeline_tree / "pipelines" / PIPELINE_ID / "pipeline.json", pipeline_doc
        )

        prep = PipelineConfigPrep()
        with pytest.raises(ValueError) as exc:
            prep.create_config()
        message = str(exc.value)
        assert f"{missing_bare}_v4" in message  # the full reference
        assert missing_bare in message  # the bare id actually looked up

    def test_two_versioned_refs_of_one_stream_fail_loud(
        self, pipeline_tree: Path
    ) -> None:
        """Distinct versioned refs that strip to the same bare id must be
        rejected instead of silently dropping one. The pipeline contract
        enforces this: versioned IDs collapse to their base for the
        duplicate check."""
        pipeline_doc = _pipeline_doc()
        pipeline_doc["streams"] = [f"{STREAM_ID}_v1", f"{STREAM_ID}_v2"]
        _write_json(
            pipeline_tree / "pipelines" / PIPELINE_ID / "pipeline.json", pipeline_doc
        )

        prep = PipelineConfigPrep()
        with pytest.raises(ContractValidationError, match="duplicate") as exc:
            prep.create_config()
        # The contract reports the collapsed base id.
        assert STREAM_ID in str(exc.value)

    def test_omitted_pipeline_id_falls_back_to_manifest_id(
        self, pipeline_tree: Path
    ) -> None:
        """pipeline_id is nullable in the contract; an authored pipeline.json
        that omits it must resolve to the manifest/env id (the executable
        identity), not be rejected by ResolvedPipeline's non-empty guard."""
        pipeline_doc = _pipeline_doc()
        pipeline_doc.pop("pipeline_id", None)
        _write_json(
            pipeline_tree / "pipelines" / PIPELINE_ID / "pipeline.json", pipeline_doc
        )

        prep = PipelineConfigPrep()
        pipeline_config, _, _, _, _ = prep.create_config()

        assert pipeline_config.pipeline_id == PIPELINE_ID

    def test_source_replication_is_typed(self, pipeline_tree: Path) -> None:
        """A stream source's replication block is parsed into a typed
        ReplicationConfig on the resolved source (engine-internal view)."""
        stream_doc = _stream_doc(STREAM_ID)
        stream_doc["source"]["replication"] = {
            "method": "incremental",
            "cursor_field": "updated_at",
            "tie_breaker_fields": ["id"],
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
        assert src.replication.tie_breaker_fields == ["id"]


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
        # A valid-but-different stream_id (the contract requires a UUID) so the
        # mismatch surfaces from bundle referential validation (the pipeline ref
        # resolves to no bundled stream document), not from id-shape validation.
        _build_tree(root, stream_id_in_file="00000000-0000-4000-8000-0000000000cc")
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        prep = PipelineConfigPrep()
        with pytest.raises(
            BundleValidationError,
            match=r"no bundled stream document declares id",
        ):
            prep.create_config()

    def test_stream_declaring_foreign_pipeline_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema_mirror: Path
    ) -> None:
        """A bundled stream that names a different parent pipeline is a
        cross-document defect the published bundle validator catches (the engine
        did not check this before delegating referential validation)."""
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root)
        stream_doc = _stream_doc(STREAM_ID)
        stream_doc["pipeline_id"] = "00000000-0000-4000-8000-0000000000ff"
        _write_json(
            root / "pipelines" / PIPELINE_ID / "streams" / f"{STREAM_ID}.json",
            stream_doc,
        )
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        prep = PipelineConfigPrep()
        with pytest.raises(BundleValidationError, match="different pipeline"):
            prep.create_config()

    def test_source_wired_to_destination_connection_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema_mirror: Path
    ) -> None:
        """A stream whose source is wired to the destination connection (role
        mis-wiring) is caught by the bundle validator — the engine has no other
        check for source/destination role correctness."""
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root)
        stream_doc = _stream_doc(STREAM_ID)
        stream_doc["source"]["endpoint_ref"]["connection_id"] = CONNECTION_DST_ID
        _write_json(
            root / "pipelines" / PIPELINE_ID / "streams" / f"{STREAM_ID}.json",
            stream_doc,
        )
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        with pytest.raises(BundleValidationError, match="connections.source"):
            PipelineConfigPrep().create_config()

    def test_active_pipeline_with_no_runnable_stream_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema_mirror: Path
    ) -> None:
        """An active pipeline whose only stream is not itself active runs
        nothing; the bundle validator's active-gate rejects it."""
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root)
        stream_doc = _stream_doc(STREAM_ID)
        stream_doc["status"] = "draft"
        _write_json(
            root / "pipelines" / PIPELINE_ID / "streams" / f"{STREAM_ID}.json",
            stream_doc,
        )
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        with pytest.raises(BundleValidationError, match="runnable stream"):
            PipelineConfigPrep().create_config()

    def test_connection_scoped_endpoint_missing_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema_mirror: Path
    ) -> None:
        """A connection-scoped endpoint_ref with no matching endpoint document
        on disk is caught by the bundle validator, before file resolution."""
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root, dst_endpoint_scope="connection")
        (
            root
            / "connections"
            / CONNECTION_DST_ID
            / "definition"
            / "endpoints"
            / f"{ENDPOINT_DST_CONNECTION}.json"
        ).unlink()
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        with pytest.raises(BundleValidationError, match="no matching bundled endpoint"):
            PipelineConfigPrep().create_config()

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

    @pytest.mark.parametrize("side", ["source", "destination"])
    def test_missing_endpoint_ref_names_stream_and_side(
        self, pipeline_tree: Path, side: str
    ) -> None:
        """A stream side without ``endpoint_ref`` must fail naming which side
        (source vs destination) is malformed. The stream contract requires
        endpoint_ref on both sides, so this surfaces at contract validation."""
        stream_doc = _stream_doc(STREAM_ID)
        location = "source" if side == "source" else "destinations/0"
        if side == "source":
            del stream_doc["source"]["endpoint_ref"]
        else:
            del stream_doc["destinations"][0]["endpoint_ref"]
        _write_json(
            pipeline_tree / "pipelines" / PIPELINE_ID / "streams" / f"{STREAM_ID}.json",
            stream_doc,
        )
        prep = PipelineConfigPrep()
        with pytest.raises(ContractValidationError, match=f"{location}/endpoint_ref"):
            prep.create_config()

    @pytest.mark.parametrize("kind", ["", None])
    def test_unusable_connector_kind_rejected(self, pipeline_tree: Path, kind) -> None:
        """A connector document whose ``kind`` is missing or empty must fail
        loudly. ``kind`` is the connector contract's discriminator, so a
        missing/empty value is rejected at contract validation."""
        connector_doc = _connector_doc()
        if kind is None:
            del connector_doc["kind"]
        else:
            connector_doc["kind"] = kind
        _write_json(
            pipeline_tree
            / "connectors"
            / CONNECTOR_ID
            / "definition"
            / "connector.json",
            connector_doc,
        )
        prep = PipelineConfigPrep()
        with pytest.raises(ContractValidationError):
            prep.create_config()


# ---------------------------------------------------------------------------
# Registry-discovered kinds (#137)
# ---------------------------------------------------------------------------


class TestRegistryDiscoveredKinds:
    def test_kind_outside_contract_enum_rejected(self, pipeline_tree: Path) -> None:
        """Config prep hardcodes no kind enum of its own -- it defers the
        authoritative kind set to the published connector contract (#137).
        The contract's ``kind`` is a closed discriminator, so a kind outside
        it (e.g. an entry-point package inventing ``graphql``) is rejected at
        contract validation rather than assembling."""
        connector_doc = _connector_doc()
        connector_doc["kind"] = "graphql"
        _write_json(
            pipeline_tree
            / "connectors"
            / CONNECTOR_ID
            / "definition"
            / "connector.json",
            connector_doc,
        )
        prep = PipelineConfigPrep()
        with pytest.raises(ContractValidationError, match="graphql|discriminator"):
            prep.create_config()


# ---------------------------------------------------------------------------
# Endpoint schema dispatch (#165)
# ---------------------------------------------------------------------------


class TestEndpointSchemaDispatch:
    """``_resolve_endpoint`` extracts the endpoint variant from ``$schema`` URL
    instead of a hard-coded two-branch check, so non-built-in endpoint kinds
    fail at schema validation rather than at URL parsing (#165)."""

    def test_missing_endpoint_path_segment_rejected(self, pipeline_tree: Path) -> None:
        """An endpoint whose $schema URL has no *-endpoint path segment must
        raise with a message pointing to the problem."""
        bad_endpoint = _endpoint_doc(ENDPOINT_SRC)
        bad_endpoint["$schema"] = "https://schemas.analitiq.ai/connector/latest.json"
        _write_json(
            pipeline_tree
            / "connectors"
            / CONNECTOR_ID
            / "definition"
            / "endpoints"
            / f"{ENDPOINT_SRC}.json",
            bad_endpoint,
        )
        prep = PipelineConfigPrep()
        with pytest.raises(ValueError, match=r"\*-endpoint path segment"):
            prep.create_config()

    def test_empty_schema_url_rejected(self, pipeline_tree: Path) -> None:
        """An endpoint with an empty $schema field must raise."""
        bad_endpoint = _endpoint_doc(ENDPOINT_SRC)
        bad_endpoint["$schema"] = ""
        _write_json(
            pipeline_tree
            / "connectors"
            / CONNECTOR_ID
            / "definition"
            / "endpoints"
            / f"{ENDPOINT_SRC}.json",
            bad_endpoint,
        )
        prep = PipelineConfigPrep()
        with pytest.raises(ValueError, match=r"\*-endpoint path segment"):
            prep.create_config()

    def test_non_built_in_endpoint_kind_fails_at_schema_validation(
        self, pipeline_tree: Path
    ) -> None:
        """A non-built-in *-endpoint $schema URL has its variant name extracted
        correctly; failure comes from schema validation (unknown kind), not
        from the URL-parsing step — confirming the extraction succeeded."""
        bad_endpoint = _endpoint_doc(ENDPOINT_SRC)
        bad_endpoint[
            "$schema"
        ] = "https://schemas.analitiq.ai/nosql-endpoint/latest.json"
        _write_json(
            pipeline_tree
            / "connectors"
            / CONNECTOR_ID
            / "definition"
            / "endpoints"
            / f"{ENDPOINT_SRC}.json",
            bad_endpoint,
        )
        prep = PipelineConfigPrep()
        # Kind "nosql-endpoint" was extracted; validate_artifact raises because
        # it is not in ARTIFACT_KINDS — the error names the extracted kind.
        with pytest.raises(ValueError, match="nosql-endpoint"):
            prep.create_config()

    def test_api_endpoint_schema_still_resolves(self, pipeline_tree: Path) -> None:
        """Built-in api-endpoint still resolves correctly after the refactor."""
        prep = PipelineConfigPrep()
        _, stream_configs, _, _, _ = prep.create_config()
        src = stream_configs[0].source
        assert "api-endpoint" in src.endpoint_document.schema_url

    def test_url_without_schema_key_rejected(self, pipeline_tree: Path) -> None:
        """An endpoint document with no $schema key must raise."""
        bad_endpoint = _endpoint_doc(ENDPOINT_SRC)
        del bad_endpoint["$schema"]
        _write_json(
            pipeline_tree
            / "connectors"
            / CONNECTOR_ID
            / "definition"
            / "endpoints"
            / f"{ENDPOINT_SRC}.json",
            bad_endpoint,
        )
        prep = PipelineConfigPrep()
        with pytest.raises(ValueError, match=r"\*-endpoint path segment"):
            prep.create_config()

    def test_schema_url_without_trailing_slash_still_extracts_kind(
        self, pipeline_tree: Path
    ) -> None:
        """A $schema URL where the variant is the final path component (no
        trailing slash) must still have its kind extracted correctly."""
        bad_endpoint = _endpoint_doc(ENDPOINT_SRC)
        # No trailing slash — regex must still match via the (?:/|$) boundary.
        bad_endpoint["$schema"] = "https://schemas.analitiq.ai/nosql-endpoint"
        _write_json(
            pipeline_tree
            / "connectors"
            / CONNECTOR_ID
            / "definition"
            / "endpoints"
            / f"{ENDPOINT_SRC}.json",
            bad_endpoint,
        )
        prep = PipelineConfigPrep()
        # The kind is extracted; failure comes from schema validation, not URL parsing.
        with pytest.raises(ValueError, match="nosql-endpoint"):
            prep.create_config()


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
        real_schema_mirror: Path,
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
        _, stream_configs, connections, _, _ = prep.create_config()

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
        real_schema_mirror: Path,
    ) -> None:
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root, dst_endpoint_scope="connection")
        (
            root
            / "connections"
            / CONNECTION_DST_ID
            / "definition"
            / "type-map-read.json"
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

    def test_connection_without_connection_id_still_resolves(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema_mirror: Path
    ) -> None:
        """A connection.json may omit connection_id (it is server-assigned; the
        engine falls back to the directory name). The assembled bundle must
        still resolve it against pipeline.connections, not report it missing."""
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root)
        conn_doc = _connection_doc(CONNECTION_SRC_ID)
        del conn_doc["connection_id"]
        _write_json(
            root / "connections" / CONNECTION_SRC_ID / "connection.json", conn_doc
        )
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        # Assembles with no false "no connection document bundled" error.
        _, _, connections, _, _ = PipelineConfigPrep().create_config()
        assert CONNECTION_SRC_ID in connections

    def test_stale_connection_endpoint_document_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema_mirror: Path
    ) -> None:
        """A connection-scoped endpoint file named for the ref's derived id but
        whose contents describe a different table is rejected at resolution,
        before the SQL code consumes its database_object."""
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root, dst_endpoint_scope="connection")
        # Overwrite the endpoint file (named for the ref's derived id) with a
        # doc for a DIFFERENT table -- internally consistent, wrong identity.
        stale = _database_endpoint_doc({"schema": "public", "name": "other_table"})
        _write_json(
            root
            / "connections"
            / CONNECTION_DST_ID
            / "definition"
            / "endpoints"
            / f"{ENDPOINT_DST_CONNECTION}.json",
            stale,
        )
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)
        with pytest.raises(ValueError, match="does not match"):
            PipelineConfigPrep().create_config()


# ---------------------------------------------------------------------------
# Real published schema contracts (#96)
# ---------------------------------------------------------------------------


class TestRealSchemaContract:
    """Run config prep against vendored snapshots of the real published
    schemas, so drift between the document shapes these tests author and
    the schemas.analitiq.ai contracts fails in CI instead of slipping
    through the permissive mirror (#96)."""

    def test_fixture_tree_assembles_under_real_schemas(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        real_schema_mirror: Path,
    ) -> None:
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root)
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)

        prep = PipelineConfigPrep()
        (
            pipeline_config,
            stream_configs,
            connections,
            endpoints,
            connectors,
        ) = prep.create_config()

        assert pipeline_config.pipeline_id == PIPELINE_ID
        assert [s.stream_id for s in stream_configs] == [STREAM_ID]
        assert set(connections) == {CONNECTION_SRC_ID, CONNECTION_DST_ID}
        assert len(endpoints) == 2
        assert connectors[0]["connector_id"] == CONNECTOR_ID

    def test_real_schema_rejects_contract_violation(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        real_schema_mirror: Path,
    ) -> None:
        """The vendored mirror actually validates: a connection document
        carrying a property the published contract forbids fails loudly.
        The permissive mirror would have accepted it — this pins that the
        real-schema tests exercise contract validation at all."""
        root = tmp_path / "project"
        root.mkdir()
        _build_tree(root)
        bad_connection = _connection_doc(CONNECTION_SRC_ID)
        bad_connection["host"] = "https://api.example.test"
        _write_json(
            root / "connections" / CONNECTION_SRC_ID / "connection.json",
            bad_connection,
        )
        monkeypatch.chdir(root)
        monkeypatch.setenv("PIPELINE_ID", PIPELINE_ID)

        prep = PipelineConfigPrep()
        with pytest.raises(ContractValidationError, match="host"):
            prep.create_config()
