"""
Unit tests for docker/config_fetcher.py

Tests the consolidated configuration file output:
- Single file per pipeline: {pipelines}/{pipeline_id}.json
- Contains: pipeline, connections, connectors, endpoints, streams
- Secrets written separately to {secrets}/{connection_id}.json
"""
import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Import after path setup
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "docker"))

from config_fetcher import ConfigFetcher


@pytest.fixture
def sample_batch_job_data():
    """Load sample batch-job-data response."""
    data_path = Path(__file__).parent.parent / "data" / "batch-job-data-response.json"
    with open(data_path) as f:
        return json.load(f)


@pytest.fixture
def config_fetcher(tmp_path):
    """Create a ConfigFetcher instance with mocked environment.

    Uses a temporary directory for output and patches analitiq.yaml paths.
    """
    project_root = Path(__file__).parent.parent.parent
    original_cwd = os.getcwd()

    # Create temp directories
    pipelines_dir = tmp_path / "pipelines"
    secrets_dir = tmp_path / ".secrets"
    pipelines_dir.mkdir(parents=True, exist_ok=True)
    secrets_dir.mkdir(parents=True, exist_ok=True)

    try:
        os.chdir(project_root)
        with patch.dict("os.environ", {
            "PIPELINE_ID": "22ab7b76-b4df-4c68-8b27-82c307436661",
            "CLIENT_ID": "d7a11991-2795-49d1-a858-c7e58ee5ecc6",
            "ENV": "dev",
        }):
            fetcher = ConfigFetcher(region="eu-central-1")
            # Override paths to use temp directory
            fetcher.pipelines_path = pipelines_dir
            fetcher.secrets_path = secrets_dir
            yield fetcher
    finally:
        os.chdir(original_cwd)


class TestConsolidatedFileOutput:
    """Test that a single consolidated file is written per pipeline."""

    def test_consolidated_file_structure(
        self, config_fetcher, sample_batch_job_data
    ):
        """Verify consolidated file contains all required sections."""
        # Mock Lambda and S3 calls
        config_fetcher.fetch_batch_job_data = MagicMock(
            return_value=sample_batch_job_data["data"]
        )
        config_fetcher.fetch_secrets_parallel = MagicMock(return_value={})

        # Run fetch_and_write_all
        pipeline_path = config_fetcher.fetch_and_write_all()

        # Verify file exists
        assert pipeline_path.exists(), f"Consolidated file not found at {pipeline_path}"

        # Load and verify structure
        with open(pipeline_path) as f:
            consolidated = json.load(f)

        # Check all required sections exist
        assert "pipeline" in consolidated, "Missing 'pipeline' section"
        assert "connections" in consolidated, "Missing 'connections' section"
        assert "connectors" in consolidated, "Missing 'connectors' section"
        assert "endpoints" in consolidated, "Missing 'endpoints' section"
        assert "streams" in consolidated, "Missing 'streams' section"

        # Verify types
        assert isinstance(consolidated["pipeline"], dict)
        assert isinstance(consolidated["connections"], list)
        assert isinstance(consolidated["connectors"], list)
        assert isinstance(consolidated["endpoints"], list)
        assert isinstance(consolidated["streams"], list)

    def test_consolidated_file_has_correct_counts(
        self, config_fetcher, sample_batch_job_data
    ):
        """Verify consolidated file contains correct number of entities."""
        # Mock Lambda and S3 calls
        config_fetcher.fetch_batch_job_data = MagicMock(
            return_value=sample_batch_job_data["data"]
        )
        config_fetcher.fetch_secrets_parallel = MagicMock(return_value={})

        # Run fetch_and_write_all
        pipeline_path = config_fetcher.fetch_and_write_all()

        with open(pipeline_path) as f:
            consolidated = json.load(f)

        # Verify counts match input data
        assert len(consolidated["connections"]) == len(sample_batch_job_data["data"]["connections"])
        assert len(consolidated["connectors"]) == len(sample_batch_job_data["data"]["connectors"])
        assert len(consolidated["endpoints"]) == len(sample_batch_job_data["data"]["endpoints"])
        assert len(consolidated["streams"]) == len(sample_batch_job_data["data"]["streams"])

    def test_pipeline_data_preserved(
        self, config_fetcher, sample_batch_job_data
    ):
        """Verify pipeline data is preserved in consolidated file."""
        config_fetcher.fetch_batch_job_data = MagicMock(
            return_value=sample_batch_job_data["data"]
        )
        config_fetcher.fetch_secrets_parallel = MagicMock(return_value={})

        pipeline_path = config_fetcher.fetch_and_write_all()

        with open(pipeline_path) as f:
            consolidated = json.load(f)

        # Verify pipeline data matches input
        input_pipeline = sample_batch_job_data["data"]["pipeline"]
        output_pipeline = consolidated["pipeline"]

        assert output_pipeline["pipeline_id"] == input_pipeline["pipeline_id"]
        assert output_pipeline["name"] == input_pipeline["name"]
        assert output_pipeline["connections"] == input_pipeline["connections"]


class TestSecretsWriting:
    """Test that secrets are written to separate files."""

    def test_secrets_written_to_secrets_dir(
        self, config_fetcher, sample_batch_job_data
    ):
        """Secrets should be written to .secrets/{connection_id}.json"""
        # Mock Lambda call
        config_fetcher.fetch_batch_job_data = MagicMock(
            return_value=sample_batch_job_data["data"]
        )

        # Mock S3 secrets fetch with actual secrets
        mock_secrets = {
            "b4904c77-0a4a-4a8d-a768-4a8b5f2f2414": {"token": "test-token-123"},
            "b452b1b2-34f9-4175-a47b-3dd6a4012230": {"api_key": "test-api-key-456"},
        }
        config_fetcher.fetch_secrets_parallel = MagicMock(return_value=mock_secrets)

        # Run fetch_and_write_all
        config_fetcher.fetch_and_write_all()

        # Check secrets files were written
        for conn_id, secrets in mock_secrets.items():
            secrets_path = config_fetcher.secrets_path / f"{conn_id}.json"
            assert secrets_path.exists(), f"Secrets not found at {secrets_path}"

            with open(secrets_path) as f:
                saved_secrets = json.load(f)
            assert saved_secrets == secrets

    def test_no_secrets_file_for_empty_secrets(
        self, config_fetcher, sample_batch_job_data
    ):
        """No secrets file should be written if no secrets exist for a connection."""
        config_fetcher.fetch_batch_job_data = MagicMock(
            return_value=sample_batch_job_data["data"]
        )
        # Return empty secrets
        config_fetcher.fetch_secrets_parallel = MagicMock(return_value={})

        config_fetcher.fetch_and_write_all()

        # Verify no secrets files were written
        secrets_files = list(config_fetcher.secrets_path.glob("*.json"))
        assert len(secrets_files) == 0, "No secrets files should be written when secrets are empty"


class TestStreamDataPreservation:
    """Test that stream data is preserved (not transformed)."""

    def test_stream_endpoint_id_preserved(
        self, config_fetcher, sample_batch_job_data
    ):
        """Stream source.endpoint_id should be preserved (not transformed to path)."""
        config_fetcher.fetch_batch_job_data = MagicMock(
            return_value=sample_batch_job_data["data"]
        )
        config_fetcher.fetch_secrets_parallel = MagicMock(return_value={})

        pipeline_path = config_fetcher.fetch_and_write_all()

        with open(pipeline_path) as f:
            consolidated = json.load(f)

        # Verify streams have endpoint_id (not endpoint path)
        stream = consolidated["streams"][0]
        assert "endpoint_id" in stream["source"], "source.endpoint_id should be preserved"
        assert "endpoint_id" in stream["destinations"][0], "destinations[].endpoint_id should be preserved"


class TestConnectionDataPreservation:
    """Test that connection data is preserved (not transformed)."""

    def test_connection_ids_preserved(
        self, config_fetcher, sample_batch_job_data
    ):
        """Connection IDs in pipeline should remain as UUIDs."""
        config_fetcher.fetch_batch_job_data = MagicMock(
            return_value=sample_batch_job_data["data"]
        )
        config_fetcher.fetch_secrets_parallel = MagicMock(return_value={})

        pipeline_path = config_fetcher.fetch_and_write_all()

        with open(pipeline_path) as f:
            consolidated = json.load(f)

        # Verify pipeline connections are still UUID strings
        pipeline = consolidated["pipeline"]
        source_conn = pipeline["connections"]["source"]["conn_1"]

        # Should be a string UUID, not a dict with connector/credentials
        assert isinstance(source_conn, str), "Source connection should be a UUID string"

        dest_conn = pipeline["connections"]["destinations"][0]["conn_2"]
        assert isinstance(dest_conn, str), "Destination connection should be a UUID string"


class TestPipelineIdExtraction:
    """Test pipeline ID extraction from versioned IDs."""

    def test_pipeline_id_without_version(self, config_fetcher):
        """Pipeline ID without version suffix should be used as-is."""
        config_fetcher.pipeline_id = "abc123"
        assert config_fetcher._get_pipeline_id_without_version() == "abc123"

    def test_pipeline_id_with_version(self, config_fetcher):
        """Pipeline ID with version suffix should have version removed."""
        config_fetcher.pipeline_id = "abc123:v1.2.3"
        assert config_fetcher._get_pipeline_id_without_version() == "abc123"


class TestConnectionIdCollection:
    """Test collection of connection IDs from connections list."""

    def test_collect_connection_ids(self, config_fetcher):
        """Should collect all connection_id values from connections list."""
        connections = [
            {"connection_id": "conn-1", "name": "Connection 1"},
            {"connection_id": "conn-2", "name": "Connection 2"},
            {"name": "Connection without ID"},  # Should be skipped
        ]

        result = config_fetcher._collect_connection_ids(connections)

        assert result == {"conn-1", "conn-2"}
