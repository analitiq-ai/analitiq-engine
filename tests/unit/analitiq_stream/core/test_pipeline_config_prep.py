"""Unit tests for PipelineConfigPrep module — modular config architecture."""

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

import pytest

from src.engine.pipeline_config_prep import PipelineConfigPrep


class ModularConfigHelper:
    """Helper to create the modular directory structure for tests."""

    @staticmethod
    def setup_connector(
        connectors_dir: Path,
        slug: str,
        connector_type: str = "api",
        driver: str = None,
        extra: dict = None,
    ):
        """Create a connector directory with connector.json and manifest.json."""
        definition_dir = connectors_dir / slug / "definition"
        endpoints_dir = definition_dir / "endpoints"
        endpoints_dir.mkdir(parents=True, exist_ok=True)

        connector = {
            "connector_id": f"{slug}-connector-id",
            "connector_name": slug.title(),
            "connector_type": connector_type,
            "slug": slug,
            **(extra or {}),
        }
        if connector_type == "database" and driver:
            # Spec-compliant transports block so PipelineConfigPrep can
            # derive the base dialect without a top-level ``driver`` field.
            connector.setdefault("default_transport", "database")
            connector.setdefault(
                "transports",
                {
                    "database": {
                        "kind": "sqlalchemy",
                        "driver": f"{driver}+asyncpg",
                        "dsn": {"template": f"{driver}+asyncpg://u:p@h:5432/d"},
                    }
                },
            )
        elif connector_type == "api":
            connector.setdefault("default_transport", "api")
            connector.setdefault(
                "transports",
                {
                    "api": {
                        "kind": "http",
                        "base_url": "https://api.example.com",
                    }
                },
            )
        (definition_dir / "connector.json").write_text(json.dumps(connector))

        manifest = {
            "connector_id": connector["connector_id"],
            "connector_name": connector["connector_name"],
            "slug": slug,
            "version": "1.0.0",
            "endpoints": [],
        }
        (definition_dir / "manifest.json").write_text(json.dumps(manifest))

        # Every connector must ship a type-map.json (per GH #28). A minimal
        # map is fine for tests that only exercise config loading.
        type_map = [
            {"match": "exact", "native": "BIGINT", "canonical": "Int64"},
            {"match": "exact", "native": "TEXT", "canonical": "Utf8"},
        ]
        (definition_dir / "type-map.json").write_text(json.dumps(type_map))
        return definition_dir

    @staticmethod
    def setup_connector_endpoint(
        connectors_dir: Path, slug: str, endpoint_name: str, endpoint_data: dict
    ):
        """Create a public endpoint file under a connector."""
        ep_dir = connectors_dir / slug / "definition" / "endpoints"
        ep_dir.mkdir(parents=True, exist_ok=True)
        (ep_dir / f"{endpoint_name}.json").write_text(json.dumps(endpoint_data))

    @staticmethod
    def setup_connection(
        connections_dir: Path,
        alias: str,
        connector_slug: str,
        host: str = "localhost",
        parameters: dict = None,
    ):
        """Create a connection directory with connection.json."""
        conn_dir = connections_dir / alias
        conn_dir.mkdir(parents=True, exist_ok=True)

        connection = {
            "connector_slug": connector_slug,
            "host": host,
            **({"parameters": parameters} if parameters else {}),
        }
        (conn_dir / "connection.json").write_text(json.dumps(connection))
        return conn_dir

    @staticmethod
    def setup_connection_endpoint(
        connections_dir: Path, alias: str, endpoint_name: str, endpoint_data: dict
    ):
        """Create a private endpoint file under a connection."""
        ep_dir = connections_dir / alias / "definition" / "endpoints"
        ep_dir.mkdir(parents=True, exist_ok=True)
        (ep_dir / f"{endpoint_name}.json").write_text(json.dumps(endpoint_data))

    @staticmethod
    def setup_secrets(connections_dir: Path, alias: str, secrets: dict):
        """Create .secrets/credentials.json for a connection."""
        secrets_dir = connections_dir / alias / ".secrets"
        secrets_dir.mkdir(parents=True, exist_ok=True)
        (secrets_dir / "credentials.json").write_text(json.dumps(secrets))

    @staticmethod
    def write_pipeline(
        pipelines_dir: Path,
        pipeline_id: str,
        pipeline_config: dict,
        streams: list,
    ):
        """Write a pipeline in manifest-based directory structure.

        Creates manifest.json (or appends to existing), pipeline directory
        with pipeline.json, and individual stream files.
        """
        # Read existing manifest or create new one
        manifest_path = pipelines_dir / "manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
        else:
            manifest = {"pipelines": []}

        # Add/update entry for this pipeline
        stream_ids = [s.get("stream_id", "unknown") for s in streams]
        new_entry = {
            "pipeline_id": pipeline_id,
            "name": pipeline_config.get("name", pipeline_id),
            "path": f"{pipeline_id}/pipeline.json",
            "status": pipeline_config.get("status", "active"),
            "schedule": pipeline_config.get("schedule"),
            "streams": stream_ids,
        }
        # Replace existing entry for this pipeline_id or append
        manifest["pipelines"] = [
            e for e in manifest["pipelines"]
            if e.get("pipeline_id") != pipeline_id
        ] + [new_entry]
        manifest_path.write_text(json.dumps(manifest))

        # Write pipeline directory with pipeline.json (empty streams array)
        pipeline_dir = pipelines_dir / pipeline_id
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        content = {"pipeline": pipeline_config, "streams": []}
        (pipeline_dir / "pipeline.json").write_text(json.dumps(content))

        # Write individual stream files
        streams_dir = pipeline_dir / "streams"
        streams_dir.mkdir(parents=True, exist_ok=True)
        for stream in streams:
            sid = stream.get("stream_id", "unknown")
            (streams_dir / f"{sid}.json").write_text(json.dumps(stream))


@pytest.fixture
def temp_config_dir():
    """Create temporary directory with modular config structure."""
    with tempfile.TemporaryDirectory() as temp_dir:
        Path(temp_dir, "connectors").mkdir()
        Path(temp_dir, "connections").mkdir()
        Path(temp_dir, "pipelines").mkdir()
        yield temp_dir


@pytest.fixture
def mock_paths(temp_config_dir):
    """Patch _discover_paths to use the temp directory paths."""
    paths = {
        "connectors": Path(temp_config_dir) / "connectors",
        "connections": Path(temp_config_dir) / "connections",
        "pipelines": Path(temp_config_dir) / "pipelines",
    }
    with patch.object(PipelineConfigPrep, "_discover_paths", return_value=paths):
        yield paths


@pytest.fixture
def helper():
    return ModularConfigHelper()


@pytest.fixture
def standard_setup(temp_config_dir, helper):
    """Set up a standard test environment with one API source and one DB destination."""
    connectors_dir = Path(temp_config_dir, "connectors")
    connections_dir = Path(temp_config_dir, "connections")
    pipelines_dir = Path(temp_config_dir, "pipelines")

    # Connectors
    helper.setup_connector(connectors_dir, "wise", connector_type="api")
    helper.setup_connector(connectors_dir, "postgresql", connector_type="database", driver="postgresql")

    # Public endpoint
    helper.setup_connector_endpoint(connectors_dir, "wise", "transfers", {
        "endpoint_id": "wise-transfers-ep",
        "endpoint": "/v1/transfers",
        "method": "GET",
        "endpoint_schema": {"type": "array", "items": {"type": "object"}},
    })

    # Connections
    helper.setup_connection(connections_dir, "my-wise", "wise", host="https://api.wise.com")
    helper.setup_connection(
        connections_dir, "prod-pg", "postgresql",
        host="${DB_HOST}",
        parameters={"database": "${DB_NAME}", "username": "${DB_USER}", "password": "${DB_PASSWORD}", "port": 5432},
    )

    # Private endpoint (DB table schema)
    helper.setup_connection_endpoint(connections_dir, "prod-pg", "public_transfers", {
        "endpoint": "public/transfers",
        "method": "DATABASE",
        "endpoint_schema": {
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "status", "type": "VARCHAR(50)", "nullable": True},
            ],
            "primary_keys": ["id"],
        },
    })

    # Secrets
    helper.setup_secrets(connections_dir, "my-wise", {"access_token": "test-token"})
    helper.setup_secrets(connections_dir, "prod-pg", {
        "DB_HOST": "localhost", "DB_NAME": "testdb", "DB_USER": "user", "DB_PASSWORD": "pass"
    })

    # Pipeline
    pipeline_config = {
        "pipeline_id": "test-pipeline-123",
        "name": "Wise to Postgres",
        "status": "active",
        "version": 1,
        "connections": {
            "source": "my-wise",
            "destinations": ["prod-pg"],
        },
        "streams": ["stream-1"],
        "schedule": {"type": "interval", "interval_minutes": 60},
        "engine": {"vcpu": 1, "memory": 8192},
        "runtime": {
            "buffer_size": 5000,
            "batching": {"batch_size": 100, "max_concurrent_batches": 3},
            "logging": {"log_level": "INFO", "metrics_enabled": True},
            "error_handling": {"strategy": "dlq", "max_retries": 3, "retry_delay": 5},
        },
    }

    stream_config = {
        "stream_id": "stream-1",
        "is_enabled": True,
        "source": {
            "connection_ref": "my-wise",
            "endpoint_ref": {"scope": "connector", "identifier": "wise", "endpoint": "transfers"},
            "primary_key": ["id"],
            "replication": {"method": "incremental", "cursor_field": ["created"]},
        },
        "destinations": [{
            "connection_ref": "prod-pg",
            "endpoint_ref": {"scope": "connection", "identifier": "prod-pg", "endpoint": "public_transfers"},
            "write": {"mode": "upsert", "conflict_keys": ["id"]},
        }],
        "mapping": {
            "assignments": [
                {
                    "target": {"path": ["id"], "type": "integer", "nullable": False},
                    "value": {"kind": "expr", "expr": {"op": "get", "path": ["id"]}},
                }
            ]
        },
    }

    helper.write_pipeline(pipelines_dir, "test-pipeline-123", pipeline_config, [stream_config])

    return {
        "connectors_dir": connectors_dir,
        "connections_dir": connections_dir,
        "pipelines_dir": pipelines_dir,
        "pipeline_id": "test-pipeline-123",
    }


class TestPipelineConfigPrepLocal:
    """Test PipelineConfigPrep initialization and environment validation."""

    def test_initialization(self, temp_config_dir, mock_paths):
        with patch.dict(os.environ, {"PIPELINE_ID": "test-pipeline"}):
            prep = PipelineConfigPrep()
            assert prep.pipeline_id == "test-pipeline"

    def test_validate_environment_success(self, temp_config_dir, mock_paths):
        with patch.dict(os.environ, {"PIPELINE_ID": "test-pipeline"}):
            prep = PipelineConfigPrep()
            prep.validate_environment()

    def test_missing_pipeline_id_raises(self, temp_config_dir, mock_paths):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PIPELINE_ID", None)
            with pytest.raises(RuntimeError, match="PIPELINE_ID"):
                PipelineConfigPrep()

    def test_missing_pipelines_directory_raises(self, temp_config_dir):
        paths = {
            "connectors": Path(temp_config_dir) / "connectors",
            "connections": Path(temp_config_dir) / "connections",
            "pipelines": Path("/nonexistent/pipelines"),
        }
        with patch.object(PipelineConfigPrep, "_discover_paths", return_value=paths):
            with patch.dict(os.environ, {"PIPELINE_ID": "test-pipeline"}):
                with pytest.raises(RuntimeError, match="Pipelines directory not found"):
                    PipelineConfigPrep()


class TestConfigurationLoading:
    """Test loading pipeline, connections, and endpoints from modular files."""

    def test_load_pipeline_and_streams(
        self, temp_config_dir, mock_paths, standard_setup
    ):
        with patch.dict(os.environ, {"PIPELINE_ID": "test-pipeline-123"}):
            prep = PipelineConfigPrep()
            pipeline_config, stream_configs = prep.load_pipeline_config()

            assert pipeline_config["pipeline_id"] == "test-pipeline-123"
            assert pipeline_config["name"] == "Wise to Postgres"
            assert len(stream_configs) == 1
            assert stream_configs[0]["stream_id"] == "stream-1"

    def test_connection_resolution(
        self, temp_config_dir, mock_paths, standard_setup
    ):
        with patch.dict(os.environ, {"PIPELINE_ID": "test-pipeline-123"}):
            prep = PipelineConfigPrep()
            pipeline, streams, connections, endpoints, connectors = prep.create_config()

            assert "my-wise" in connections
            assert connections["my-wise"].connector_type == "api"
            assert "prod-pg" in connections
            assert connections["prod-pg"].connector_type == "database"
            assert connections["prod-pg"].driver == "postgresql"

    def test_endpoint_resolution(
        self, temp_config_dir, mock_paths, standard_setup
    ):
        from src.models.stream import EndpointRef

        with patch.dict(os.environ, {"PIPELINE_ID": "test-pipeline-123"}):
            prep = PipelineConfigPrep()
            pipeline, streams, connections, endpoints, connectors = prep.create_config()

            wise_ref = EndpointRef(
                scope="connector", identifier="wise", endpoint="transfers",
            )
            pg_ref = EndpointRef(
                scope="connection", identifier="prod-pg", endpoint="public_transfers",
            )
            assert wise_ref in endpoints
            assert endpoints[wise_ref]["endpoint"] == "/v1/transfers"
            assert pg_ref in endpoints
            assert endpoints[pg_ref]["method"] == "DATABASE"

    def test_connectors_collected(
        self, temp_config_dir, mock_paths, standard_setup
    ):
        with patch.dict(os.environ, {"PIPELINE_ID": "test-pipeline-123"}):
            prep = PipelineConfigPrep()
            pipeline, streams, connections, endpoints, connectors = prep.create_config()

            slugs = {c["slug"] for c in connectors}
            assert "wise" in slugs
            assert "postgresql" in slugs

    def test_pipeline_file_cached(
        self, temp_config_dir, mock_paths, standard_setup
    ):
        with patch.dict(os.environ, {"PIPELINE_ID": "test-pipeline-123"}):
            prep = PipelineConfigPrep()
            file1 = prep._load_pipeline_file()
            file2 = prep._load_pipeline_file()
            assert file1 is file2

    def test_missing_pipeline_raises(
        self, temp_config_dir, mock_paths
    ):
        """Test that a pipeline not in the manifest raises ValueError."""
        # Write an empty manifest so the file exists
        manifest_path = Path(temp_config_dir) / "pipelines" / "manifest.json"
        manifest_path.write_text(json.dumps({"pipelines": []}))

        with patch.dict(os.environ, {"PIPELINE_ID": "nonexistent-pipeline"}):
            prep = PipelineConfigPrep()
            with pytest.raises(ValueError, match="not found in manifest"):
                prep.load_pipeline_config()

    def test_missing_manifest_raises(
        self, temp_config_dir, mock_paths
    ):
        """Test that a missing manifest.json raises FileNotFoundError."""
        with patch.dict(os.environ, {"PIPELINE_ID": "some-pipeline"}):
            prep = PipelineConfigPrep()
            with pytest.raises(FileNotFoundError, match="Pipeline manifest not found"):
                prep.load_pipeline_config()

    def test_inactive_pipeline_raises(
        self, temp_config_dir, mock_paths, standard_setup, helper
    ):
        """Test that a pipeline with non-active status raises ValueError."""
        pipelines_dir = Path(temp_config_dir, "pipelines")
        pipeline_config = {
                "name": "Draft Pipeline",
            "status": "draft",
            "version": 1,
            "connections": {"source": "my-wise", "destinations": ["prod-pg"]},
            "streams": ["stream-1"],
            "engine": {"vcpu": 1, "memory": 8192},
            "runtime": {
                "buffer_size": 5000,
                "batching": {"batch_size": 100, "max_concurrent_batches": 3},
                "logging": {"log_level": "INFO", "metrics_enabled": True},
                "error_handling": {"strategy": "dlq", "max_retries": 3, "retry_delay": 5},
            },
        }
        stream = {
            "stream_id": "stream-1",
            "source": {
                "connection_ref": "my-wise",
                "endpoint_ref": {"scope": "connector", "identifier": "wise", "endpoint": "transfers"},
            },
            "destinations": [{
                "connection_ref": "prod-pg",
                "endpoint_ref": {"scope": "connection", "identifier": "prod-pg", "endpoint": "public_transfers"},
            }],
        }
        helper.write_pipeline(pipelines_dir, "draft-pipeline", pipeline_config, [stream])

        with patch.dict(os.environ, {"PIPELINE_ID": "draft-pipeline"}):
            prep = PipelineConfigPrep()
            with pytest.raises(ValueError, match="Only 'active' pipelines"):
                prep.load_pipeline_config()

    def test_missing_stream_file_raises(
        self, temp_config_dir, mock_paths, standard_setup, helper
    ):
        """Test that a missing stream file raises FileNotFoundError."""
        pipelines_dir = Path(temp_config_dir, "pipelines")
        pipeline_config = {
                "name": "Bad",
            "status": "active",
            "version": 1,
            "connections": {"source": "my-wise", "destinations": ["prod-pg"]},
            "streams": ["nonexistent-stream"],
            "engine": {"vcpu": 1, "memory": 8192},
            "runtime": {
                "buffer_size": 5000,
                "batching": {"batch_size": 100, "max_concurrent_batches": 3},
                "logging": {"log_level": "INFO", "metrics_enabled": True},
                "error_handling": {"strategy": "dlq", "max_retries": 3, "retry_delay": 5},
            },
        }
        # Write pipeline with no streams (so directory exists but file doesn't)
        helper.write_pipeline(pipelines_dir, "bad-pipeline", pipeline_config, [])
        # Override pipeline.json to reference a stream that has no file
        pipeline_dir = pipelines_dir / "bad-pipeline"
        content = {"pipeline": pipeline_config, "streams": []}
        (pipeline_dir / "pipeline.json").write_text(json.dumps(content))

        with patch.dict(os.environ, {"PIPELINE_ID": "bad-pipeline"}):
            prep = PipelineConfigPrep()
            with pytest.raises(FileNotFoundError, match="Stream config not found"):
                prep.load_pipeline_config()

    def test_stream_id_mismatch_raises(
        self, temp_config_dir, mock_paths, standard_setup, helper
    ):
        """Test that a stream file with mismatched stream_id raises ValueError."""
        pipelines_dir = Path(temp_config_dir, "pipelines")
        pipeline_config = {
                "name": "Mismatch",
            "status": "active",
            "version": 1,
            "connections": {"source": "my-wise", "destinations": ["prod-pg"]},
            "streams": ["expected-stream"],
            "engine": {"vcpu": 1, "memory": 8192},
            "runtime": {
                "buffer_size": 5000,
                "batching": {"batch_size": 100, "max_concurrent_batches": 3},
                "logging": {"log_level": "INFO", "metrics_enabled": True},
                "error_handling": {"strategy": "dlq", "max_retries": 3, "retry_delay": 5},
            },
        }
        # Write pipeline with empty streams list first
        helper.write_pipeline(pipelines_dir, "mismatch-pipeline", pipeline_config, [])

        # Manually write a stream file named expected-stream.json but with wrong stream_id
        streams_dir = pipelines_dir / "mismatch-pipeline" / "streams"
        streams_dir.mkdir(parents=True, exist_ok=True)
        wrong_stream = {
            "stream_id": "wrong-stream-id",
            "source": {
                "connection_ref": "my-wise",
                "endpoint_ref": {"scope": "connector", "identifier": "wise", "endpoint": "transfers"},
            },
            "destinations": [{
                "connection_ref": "prod-pg",
                "endpoint_ref": {"scope": "connection", "identifier": "prod-pg", "endpoint": "public_transfers"},
            }],
        }
        (streams_dir / "expected-stream.json").write_text(json.dumps(wrong_stream))

        with patch.dict(os.environ, {"PIPELINE_ID": "mismatch-pipeline"}):
            prep = PipelineConfigPrep()
            with pytest.raises(ValueError, match="not listed in pipeline.streams"):
                prep.load_pipeline_config()

    def test_connections_format_in_output(
        self, temp_config_dir, mock_paths, standard_setup
    ):
        """Test that pipeline_config.connections uses alias format."""
        with patch.dict(os.environ, {"PIPELINE_ID": "test-pipeline-123"}):
            prep = PipelineConfigPrep()
            pipeline_config, _ = prep.load_pipeline_config()

            assert pipeline_config["connections"]["source"] == "my-wise"
            assert pipeline_config["connections"]["destinations"] == ["prod-pg"]

    def test_stream_source_has_endpoint_ref(
        self, temp_config_dir, mock_paths, standard_setup
    ):
        """Test that normalized stream source contains a structured endpoint_ref."""
        with patch.dict(os.environ, {"PIPELINE_ID": "test-pipeline-123"}):
            prep = PipelineConfigPrep()
            _, stream_configs = prep.load_pipeline_config()

            source = stream_configs[0]["source"]
            assert source["endpoint_ref"] == {
                "scope": "connector",
                "identifier": "wise",
                "endpoint": "transfers",
            }
            assert source["connection_ref"] == "my-wise"

    def test_stream_destination_has_endpoint_ref(
        self, temp_config_dir, mock_paths, standard_setup
    ):
        """Test that normalized stream destination contains a structured endpoint_ref."""
        with patch.dict(os.environ, {"PIPELINE_ID": "test-pipeline-123"}):
            prep = PipelineConfigPrep()
            _, stream_configs = prep.load_pipeline_config()

            dest = stream_configs[0]["destinations"][0]
            assert dest["endpoint_ref"] == {
                "scope": "connection",
                "identifier": "prod-pg",
                "endpoint": "public_transfers",
            }
            assert dest["connection_ref"] == "prod-pg"
