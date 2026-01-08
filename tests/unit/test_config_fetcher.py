"""
Unit tests for docker/config_fetcher.py

Tests the transformation logic for:
- Connectors saved to connectors/{connector_id}/connector.json
- Endpoints saved to connectors/{connector_id}/endpoints/{endpoint_id}.json
- Stream source.endpoint_id -> source.endpoint transformation
- Pipeline connections transformation to path-based format

All directory paths are loaded from analitiq.yaml (single source of truth).
"""
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

# Import after path setup
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "docker"))

from config_fetcher import ConfigFetcher


def load_analitiq_paths() -> dict:
    """Load paths from analitiq.yaml at project root."""
    project_root = Path(__file__).parent.parent.parent
    yaml_path = project_root / "analitiq.yaml"
    with open(yaml_path) as f:
        config = yaml.safe_load(f)
    return config.get("paths", {})


# Load paths from analitiq.yaml once at module level
ANALITIQ_PATHS = load_analitiq_paths()


@pytest.fixture
def sample_batch_job_data():
    """Load sample batch-job-data response."""
    data_path = Path(__file__).parent.parent / "data" / "batch-job-data-response.json"
    with open(data_path) as f:
        return json.load(f)


@pytest.fixture
def config_fetcher(tmp_path):
    """Create a ConfigFetcher instance with mocked environment.

    Tests run from project root, so analitiq.yaml is found automatically.
    """
    # Change to project root so analitiq.yaml is found
    project_root = Path(__file__).parent.parent.parent
    original_cwd = os.getcwd()

    try:
        os.chdir(project_root)
        with patch.dict("os.environ", {
            "PIPELINE_ID": "22ab7b76-b4df-4c68-8b27-82c307436661",
            "CLIENT_ID": "d7a11991-2795-49d1-a858-c7e58ee5ecc6",
            "ENV": "dev",
        }):
            fetcher = ConfigFetcher(config_dir=tmp_path, region="eu-central-1")
            return fetcher
    finally:
        os.chdir(original_cwd)


class TestConfigFetcherDirectoryStructure:
    """Test that files are written to correct directory structure."""

    def test_connectors_written_with_connector_id(
        self, config_fetcher, sample_batch_job_data, tmp_path
    ):
        """Connectors should be saved under connectors/{connector_id}/connector.json"""
        connectors = sample_batch_job_data["data"]["connectors"]

        written_ids = config_fetcher._write_connectors(connectors)

        # Check both connectors were written
        assert len(written_ids) == 2

        # Check connector directories use connector_id (not slug)
        for connector in connectors:
            connector_id = connector["connector_id"]
            connector_path = tmp_path / "connectors" / connector_id / "connector.json"
            assert connector_path.exists(), f"Connector not found at {connector_path}"

            # Verify content
            with open(connector_path) as f:
                saved = json.load(f)
            assert saved["connector_id"] == connector_id

    def test_endpoints_written_with_connector_id_and_endpoint_id(
        self, config_fetcher, sample_batch_job_data, tmp_path
    ):
        """Endpoints should be saved under connectors/{connector_id}/endpoints/{endpoint_id}.json"""
        connectors = sample_batch_job_data["data"]["connectors"]
        endpoints = sample_batch_job_data["data"]["endpoints"]

        # First write connectors
        written_connector_ids = config_fetcher._write_connectors(connectors)

        # Then write endpoints
        endpoint_path_map = config_fetcher._write_endpoints(endpoints, written_connector_ids)

        # Check both endpoints were written
        assert len(endpoint_path_map) == 2

        # Check endpoint paths use connector_id and endpoint_id
        for endpoint in endpoints:
            endpoint_id = endpoint["endpoint_id"]
            connector_id = endpoint["connector_id"]

            # Check the path map returns correct format
            expected_relative_path = f"{connector_id}/endpoints/{endpoint_id}.json"
            assert endpoint_path_map[endpoint_id] == expected_relative_path

            # Check file exists at expected location
            endpoint_path = tmp_path / "connectors" / connector_id / "endpoints" / f"{endpoint_id}.json"
            assert endpoint_path.exists(), f"Endpoint not found at {endpoint_path}"

            # Verify content
            with open(endpoint_path) as f:
                saved = json.load(f)
            assert saved["endpoint_id"] == endpoint_id


class TestStreamTransformation:
    """Test stream endpoint_id -> endpoint path transformation."""

    def test_stream_source_endpoint_transformed(
        self, config_fetcher, sample_batch_job_data
    ):
        """Stream source.endpoint_id should be replaced with source.endpoint (path)."""
        connectors = sample_batch_job_data["data"]["connectors"]
        endpoints = sample_batch_job_data["data"]["endpoints"]
        streams = sample_batch_job_data["data"]["streams"]

        # Build the endpoint path map
        written_connector_ids = {c["connector_id"] for c in connectors}
        endpoint_path_map = {}
        for endpoint in endpoints:
            endpoint_id = endpoint["endpoint_id"]
            connector_id = endpoint["connector_id"]
            if connector_id in written_connector_ids:
                endpoint_path_map[endpoint_id] = f"{connector_id}/endpoints/{endpoint_id}.json"

        # Make a copy to avoid modifying fixture
        streams_copy = json.loads(json.dumps(streams))

        # Transform streams
        config_fetcher._transform_streams_with_endpoint_paths(streams_copy, endpoint_path_map)

        # Check source transformation
        stream = streams_copy[0]
        assert "endpoint_id" not in stream["source"], "endpoint_id should be removed"
        assert "endpoint" in stream["source"], "endpoint path should be added"

        # Verify path format: {connector_id}/endpoints/{endpoint_id}.json
        source_endpoint = stream["source"]["endpoint"]
        assert source_endpoint == "0b1b1d31-35ae-4047-a27f-151535fe5531/endpoints/5a4b9e21-441f-4bc7-9d5e-41917b4357e6.json"

    def test_stream_destination_endpoint_transformed(
        self, config_fetcher, sample_batch_job_data
    ):
        """Stream destinations[].endpoint_id should be replaced with endpoint path."""
        connectors = sample_batch_job_data["data"]["connectors"]
        endpoints = sample_batch_job_data["data"]["endpoints"]
        streams = sample_batch_job_data["data"]["streams"]

        # Build the endpoint path map
        written_connector_ids = {c["connector_id"] for c in connectors}
        endpoint_path_map = {}
        for endpoint in endpoints:
            endpoint_id = endpoint["endpoint_id"]
            connector_id = endpoint["connector_id"]
            if connector_id in written_connector_ids:
                endpoint_path_map[endpoint_id] = f"{connector_id}/endpoints/{endpoint_id}.json"

        # Make a copy
        streams_copy = json.loads(json.dumps(streams))

        # Transform streams
        config_fetcher._transform_streams_with_endpoint_paths(streams_copy, endpoint_path_map)

        # Check destination transformation
        stream = streams_copy[0]
        dest = stream["destinations"][0]
        assert "endpoint_id" not in dest, "endpoint_id should be removed"
        assert "endpoint" in dest, "endpoint path should be added"

        # Verify path format
        dest_endpoint = dest["endpoint"]
        assert dest_endpoint == "acc98990-c633-4005-90ba-f97001367694/endpoints/1e63d782-4b67-4b7e-b845-4b4de5e4f46e.json"


class TestPipelineConnectionsTransformation:
    """Test pipeline connections transformation to path-based format."""

    def test_pipeline_connections_use_connector_id(
        self, config_fetcher, sample_batch_job_data
    ):
        """Pipeline connections should use connector_id as connector name."""
        pipeline = sample_batch_job_data["data"]["pipeline"]
        connections = sample_batch_job_data["data"]["connections"]

        # Make a copy
        pipeline_copy = json.loads(json.dumps(pipeline))

        # Transform
        config_fetcher._transform_pipeline_connections(pipeline_copy, connections)

        # Check source connection format
        source_conn = pipeline_copy["connections"]["source"]["conn_1"]
        assert "connector" in source_conn
        assert "credentials" in source_conn

        # Connector should be the connector_id (not slug)
        # conn_1 -> connection b4904c77-... -> connector 0b1b1d31-...
        assert source_conn["connector"] == "0b1b1d31-35ae-4047-a27f-151535fe5531"
        # Credentials path uses secrets path from analitiq.yaml
        secrets_path = ANALITIQ_PATHS["secrets"]
        assert source_conn["credentials"] == f"{secrets_path}/b4904c77-0a4a-4a8d-a768-4a8b5f2f2414.json"

    def test_pipeline_destinations_use_connector_id(
        self, config_fetcher, sample_batch_job_data
    ):
        """Pipeline destination connections should use connector_id."""
        pipeline = sample_batch_job_data["data"]["pipeline"]
        connections = sample_batch_job_data["data"]["connections"]

        # Make a copy
        pipeline_copy = json.loads(json.dumps(pipeline))

        # Transform
        config_fetcher._transform_pipeline_connections(pipeline_copy, connections)

        # Check destination connection format
        dest_conn = pipeline_copy["connections"]["destinations"][0]["conn_2"]
        assert "connector" in dest_conn
        assert "credentials" in dest_conn

        # Connector should be the connector_id
        # conn_2 -> connection b452b1b2-... -> connector acc98990-...
        assert dest_conn["connector"] == "acc98990-c633-4005-90ba-f97001367694"
        # Credentials path uses secrets path from analitiq.yaml
        secrets_path = ANALITIQ_PATHS["secrets"]
        assert dest_conn["credentials"] == f"{secrets_path}/b452b1b2-34f9-4175-a47b-3dd6a4012230.json"


class TestCredentialsWriting:
    """Test credentials file writing."""

    def test_credentials_written_to_secrets_dir(
        self, config_fetcher, sample_batch_job_data, tmp_path
    ):
        """Credentials should be written to .secrets/{connection_id}.json"""
        connections = sample_batch_job_data["data"]["connections"]

        # Mock secrets (empty for this test)
        all_secrets = {}

        # Create .secrets directory
        (tmp_path / ".secrets").mkdir(exist_ok=True)

        # Write credentials
        config_fetcher._write_credentials(connections, all_secrets)

        # Check credentials files exist
        for conn in connections:
            conn_id = conn["connection_id"]
            cred_path = tmp_path / ".secrets" / f"{conn_id}.json"
            assert cred_path.exists(), f"Credentials not found at {cred_path}"


class TestFullIntegration:
    """Integration test for full fetch_and_write_all flow."""

    def test_full_transformation_flow(
        self, config_fetcher, sample_batch_job_data, tmp_path
    ):
        """Test the complete transformation flow with mocked Lambda/S3."""
        # Mock the Lambda and S3 calls
        config_fetcher.fetch_batch_job_data = MagicMock(
            return_value=sample_batch_job_data["data"]
        )
        config_fetcher.fetch_secrets_parallel = MagicMock(return_value={})

        # Run the full flow
        pipeline_path = config_fetcher.fetch_and_write_all()

        # Verify pipeline was written
        assert pipeline_path.exists()

        # Verify directory structure
        connectors_dir = tmp_path / "connectors"
        assert connectors_dir.exists()

        # Check connector directories use connector_id
        expected_connector_ids = [
            "acc98990-c633-4005-90ba-f97001367694",
            "0b1b1d31-35ae-4047-a27f-151535fe5531",
        ]
        for conn_id in expected_connector_ids:
            assert (connectors_dir / conn_id / "connector.json").exists()

        # Check endpoint files use endpoint_id
        expected_endpoints = [
            ("acc98990-c633-4005-90ba-f97001367694", "1e63d782-4b67-4b7e-b845-4b4de5e4f46e"),
            ("0b1b1d31-35ae-4047-a27f-151535fe5531", "5a4b9e21-441f-4bc7-9d5e-41917b4357e6"),
        ]
        for conn_id, ep_id in expected_endpoints:
            ep_path = connectors_dir / conn_id / "endpoints" / f"{ep_id}.json"
            assert ep_path.exists(), f"Endpoint not found at {ep_path}"

        # Verify stream transformation
        streams_dir = tmp_path / "streams"
        stream_files = list(streams_dir.glob("*.json"))
        assert len(stream_files) == 1

        with open(stream_files[0]) as f:
            stream = json.load(f)

        # Source should have endpoint (path), not endpoint_id
        assert "endpoint" in stream["source"]
        assert "endpoint_id" not in stream["source"]
        assert stream["source"]["endpoint"] == "0b1b1d31-35ae-4047-a27f-151535fe5531/endpoints/5a4b9e21-441f-4bc7-9d5e-41917b4357e6.json"

        # Destination should have endpoint (path), not endpoint_id
        dest = stream["destinations"][0]
        assert "endpoint" in dest
        assert "endpoint_id" not in dest
        assert dest["endpoint"] == "acc98990-c633-4005-90ba-f97001367694/endpoints/1e63d782-4b67-4b7e-b845-4b4de5e4f46e.json"

        # Verify pipeline connections transformation
        with open(pipeline_path) as f:
            pipeline = json.load(f)

        # Check connections are path-based with connector_id
        source_conn = pipeline["connections"]["source"]["conn_1"]
        assert source_conn["connector"] == "0b1b1d31-35ae-4047-a27f-151535fe5531"

        dest_conn = pipeline["connections"]["destinations"][0]["conn_2"]
        assert dest_conn["connector"] == "acc98990-c633-4005-90ba-f97001367694"