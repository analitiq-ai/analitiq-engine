#!/usr/bin/env python3
"""
Configuration fetcher for cloud deployments.

This script fetches all configuration via a single Lambda call and secrets from S3:
- Pipeline, streams, connectors, connections, endpoints via Lambda (_batch-job-data)
- Secrets from S3

It writes configuration to local filesystem using directory paths defined in
analitiq.yaml (single source of truth for all directory paths).

This script runs as the first step in AWS Batch container execution,
before the main pipeline runner is invoked.

Usage:
    python docker/config_fetcher.py

Environment Variables:
    PIPELINE_ID: UUID of the pipeline to fetch (optionally with version: uuid:v1.2.3)
    CLIENT_ID: UUID of the client
    STREAM_ID: UUID of the stream to process
    ENV: Environment name (dev, prod)
    AWS_REGION: AWS region (default: eu-central-1)
    SECRETS_BUCKET: S3 bucket for secrets (default: analitiq-secrets-{ENV})
    BATCH_JOB_DATA_LAMBDA: Lambda function name (default: _batch-job-data)
"""

import argparse
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
import yaml
from botocore.exceptions import ClientError, NoCredentialsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BATCH_JOB_DATA_LAMBDA = "_batch-job-data"


class ConfigFetcher:
    """
    Fetches configuration via Lambda and secrets from S3.

    Writes configuration to local filesystem using directory structure defined
    in analitiq.yaml (single source of truth for all directory paths).

    Stream endpoint paths are transformed from UUID to file paths:
    - source.endpoint_id (UUID) -> source.endpoint (e.g., "{connector_id}/endpoints/{endpoint_id}.json")
    - destinations[].endpoint_id (UUID) -> destinations[].endpoint (path)
    """

    def __init__(
        self,
        *,
        region: str = "eu-central-1",
    ):
        """
        Initialize config fetcher.

        All output paths are determined by analitiq.yaml (single source of truth).

        Args:
            region: AWS region
        """
        self.region = region

        # Get environment variables
        self.env = os.getenv("ENV", "dev")
        self.pipeline_id = os.getenv("PIPELINE_ID")
        self.client_id = os.getenv("CLIENT_ID")
        self.stream_id = os.getenv("STREAM_ID")

        # Lambda function name
        self.batch_job_data_lambda = os.getenv(
            "BATCH_JOB_DATA_LAMBDA", BATCH_JOB_DATA_LAMBDA
        )

        # S3 secrets bucket
        self.secrets_bucket = os.getenv(
            "SECRETS_BUCKET", f"analitiq-secrets-{self.env}"
        )

        # Initialize AWS clients
        self._s3: Optional[boto3.client] = None
        self._lambda: Optional[boto3.client] = None

        # Load paths from analitiq.yaml (single source of truth)
        self.paths = self._load_paths_from_analitiq_yaml()

        # Convert paths to Path objects for easier manipulation
        self.connectors_path = Path(self.paths["connectors"])
        self.connections_path = Path(self.paths["connections"])
        self.streams_path = Path(self.paths["streams"])
        self.pipelines_path = Path(self.paths["pipelines"])
        self.secrets_path = Path(self.paths["secrets"])

    def _load_paths_from_analitiq_yaml(self) -> Dict[str, str]:
        """
        Load directory paths from analitiq.yaml at project root.

        Returns:
            Dict with paths for connectors, streams, pipelines, secrets

        Raises:
            FileNotFoundError: If analitiq.yaml is not found
            ValueError: If required paths are missing
        """
        yaml_path = Path("analitiq.yaml")

        if not yaml_path.exists():
            raise FileNotFoundError("analitiq.yaml not found at project root")

        with open(yaml_path) as f:
            config = yaml.safe_load(f)

        paths_config = config.get("paths", {})

        required_paths = ["connectors", "connections", "streams", "pipelines", "secrets"]
        missing = [p for p in required_paths if p not in paths_config]
        if missing:
            raise ValueError(f"analitiq.yaml missing required paths: {missing}")

        logger.info(f"Loaded paths from analitiq.yaml")
        return paths_config

    @property
    def s3(self) -> boto3.client:
        """Get S3 client."""
        if self._s3 is None:
            self._s3 = boto3.client("s3", region_name=self.region)
        return self._s3

    @property
    def lambda_client(self) -> boto3.client:
        """Get Lambda client."""
        if self._lambda is None:
            self._lambda = boto3.client("lambda", region_name=self.region)
        return self._lambda

    def validate_environment(self) -> None:
        """Validate required environment variables."""
        required = {
            "PIPELINE_ID": self.pipeline_id,
            "CLIENT_ID": self.client_id,
        }

        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

    def fetch_batch_job_data(
        self, stream_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Fetch all pipeline data via batch-job-data Lambda.

        This single Lambda call returns:
        - pipeline configuration
        - connections
        - connectors
        - endpoints
        - streams

        Args:
            stream_ids: Optional list of specific stream IDs to fetch

        Returns:
            Dict containing pipeline, connections, connectors, endpoints, streams
        """
        try:
            payload: Dict[str, Any] = {
                "client_id": self.client_id,
                "pipeline_id": self.pipeline_id,
            }

            if self.stream_id:
                payload["stream_id"] = self.stream_id

            if stream_ids:
                payload["stream_ids"] = stream_ids

            response = self.lambda_client.invoke(
                FunctionName=self.batch_job_data_lambda,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )

            result = json.loads(response["Payload"].read())

            if not result.get("success"):
                error_msg = result.get("error", "Unknown error")
                raise RuntimeError(f"batch-job-data Lambda failed: {error_msg}")

            data = result.get("data", {})

            pipeline = data.get("pipeline")
            if not pipeline:
                raise ValueError(
                    f"Pipeline not found: {self.pipeline_id} "
                    f"(client_id={self.client_id})"
                )

            logger.info(
                f"Fetched batch job data for pipeline: {self.pipeline_id}"
            )
            logger.info(f"  Connections: {len(data.get('connections', []))}")
            logger.info(f"  Connectors: {len(data.get('connectors', []))}")
            logger.info(f"  Endpoints: {len(data.get('endpoints', []))}")
            logger.info(f"  Streams: {len(data.get('streams', []))}")

            return data

        except ClientError as e:
            raise RuntimeError(f"Failed to invoke batch-job-data Lambda: {e}")

    def fetch_secrets(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Fetch secrets from S3."""
        key = f"connections/{self.client_id}/{connection_id}"

        try:
            response = self.s3.get_object(
                Bucket=self.secrets_bucket,
                Key=key,
            )
            content = response["Body"].read().decode("utf-8")
            secrets = json.loads(content)
            logger.debug(f"Fetched secrets for connection: {connection_id}")
            return secrets

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "NoSuchKey":
                logger.warning(f"No secrets found for connection: {connection_id}")
                return None
            raise RuntimeError(f"Failed to fetch secrets for {connection_id}: {e}")

    def fetch_secrets_parallel(
        self, connection_ids: set, max_workers: int = 10
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch secrets for multiple connections in parallel.

        Args:
            connection_ids: Set of connection UUIDs
            max_workers: Maximum number of concurrent S3 requests

        Returns:
            Dict mapping connection_id -> secrets
        """
        results: Dict[str, Dict[str, Any]] = {}

        if not connection_ids:
            return results

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id = {
                executor.submit(self.fetch_secrets, cid): cid
                for cid in connection_ids
            }

            for future in as_completed(future_to_id):
                connection_id = future_to_id[future]
                try:
                    secrets = future.result()
                    if secrets is not None:
                        results[connection_id] = secrets
                except Exception as e:
                    logger.error(f"Failed to fetch secrets for {connection_id}: {e}")

        logger.info(
            f"Fetched {len(results)}/{len(connection_ids)} secrets in parallel"
        )
        return results

    def write_json(self, path: Path, data: Dict[str, Any]) -> None:
        """Write JSON data to file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        logger.debug(f"Wrote: {path}")

    def _build_connection_to_connector_map(
        self, connections: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """
        Build map from connection_id to connector_id.

        Args:
            connections: List of connection configs from batch-job-data

        Returns:
            Dict mapping connection_id -> connector_id
        """
        return {
            c["connection_id"]: c["connector_id"]
            for c in connections
            if c.get("connection_id") and c.get("connector_id")
        }

    def _write_connectors(
        self,
        connectors: List[Dict[str, Any]],
    ) -> set:
        """
        Write connector configs to disk using connector_id as directory name.

        Directory structure: {paths.connectors}/{connector_id}/connector.json

        Args:
            connectors: List of connector configs from batch-job-data

        Returns:
            Set of connector_ids that were written
        """
        written_connector_ids: set = set()

        for connector in connectors:
            connector_id = connector.get("connector_id")

            if not connector_id:
                logger.warning("Skipping connector: missing connector_id")
                continue

            written_connector_ids.add(connector_id)

            # Write connector config using connector_id as directory name
            connector_dir = self.connectors_path / connector_id
            connector_dir.mkdir(parents=True, exist_ok=True)
            connector_path = connector_dir / "connector.json"
            self.write_json(connector_path, connector)
            logger.info(f"Wrote connector: {connector_path}")

        return written_connector_ids

    def _write_endpoints(
        self,
        endpoints: List[Dict[str, Any]],
        written_connector_ids: set,
    ) -> Dict[str, str]:
        """
        Write endpoint configs to disk using connector_id and endpoint_id.

        Directory structure: {paths.connectors}/{connector_id}/endpoints/{endpoint_id}.json

        The returned path map contains paths relative to the connectors directory,
        which is what pipeline_config_prep.py expects for source.endpoint and
        destinations[].endpoint fields.

        Args:
            endpoints: List of endpoint configs from batch-job-data
            written_connector_ids: Set of connector_ids that were written

        Returns:
            Dict mapping endpoint_id -> relative path (e.g., "{connector_id}/endpoints/{endpoint_id}.json")
        """
        endpoint_path_map: Dict[str, str] = {}

        for endpoint in endpoints:
            endpoint_id = endpoint.get("endpoint_id")
            connector_id = endpoint.get("connector_id")

            if not endpoint_id or not connector_id:
                logger.warning(
                    f"Skipping endpoint {endpoint_id}: missing endpoint_id or connector_id"
                )
                continue

            if connector_id not in written_connector_ids:
                logger.warning(
                    f"Skipping endpoint {endpoint_id}: "
                    f"connector {connector_id} not found in written connectors"
                )
                continue

            # Build relative path for endpoint (relative to connectors directory)
            # This format matches what pipeline_config_prep.py expects
            relative_path = f"{connector_id}/endpoints/{endpoint_id}.json"
            endpoint_path_map[endpoint_id] = relative_path

            # Write endpoint config
            endpoints_dir = self.connectors_path / connector_id / "endpoints"
            endpoints_dir.mkdir(parents=True, exist_ok=True)
            endpoint_path = endpoints_dir / f"{endpoint_id}.json"
            self.write_json(endpoint_path, endpoint)
            logger.info(f"Wrote endpoint: {endpoint_path}")

        return endpoint_path_map

    def _write_credentials(
        self,
        connections: List[Dict[str, Any]],
        all_secrets: Dict[str, Dict[str, Any]],
    ) -> None:
        """
        Write merged credentials (connection config + secrets) to disk.

        Directory structure: {paths.secrets}/{connection_id}.json

        Args:
            connections: List of connection configs from batch-job-data
            all_secrets: Dict mapping connection_id -> secrets from S3
        """
        for connection in connections:
            connection_id = connection.get("connection_id")
            if not connection_id:
                continue

            # Get secrets and merge with connection config
            secrets = all_secrets.get(connection_id, {})
            credentials = {
                "host": connection.get("host"),
                "type": connection.get("type"),
            }
            # Include any additional config from connection
            if connection.get("config"):
                credentials.update(connection["config"])
            # Overlay with secrets (secrets take precedence)
            credentials.update(secrets)
            # Remove None values
            credentials = {k: v for k, v in credentials.items() if v is not None}

            # Write credentials file
            cred_path = self.secrets_path / f"{connection_id}.json"
            self.write_json(cred_path, credentials)
            logger.info(f"Wrote credentials: {cred_path}")

    def _write_connection_files(
        self,
        connections: List[Dict[str, Any]],
    ) -> None:
        """
        Write connection configs to disk using connection_id as filename.

        Directory structure: {paths.connections}/{connection_id}.json

        This is the ID-based format that PipelineConfigPrep expects.
        PipelineConfigPrep will merge these with secrets from {paths.secrets}.

        Args:
            connections: List of connection configs from batch-job-data
        """
        self.connections_path.mkdir(parents=True, exist_ok=True)

        for connection in connections:
            connection_id = connection.get("connection_id")
            if not connection_id:
                logger.warning("Skipping connection: missing connection_id")
                continue

            # Write connection config
            conn_path = self.connections_path / f"{connection_id}.json"
            self.write_json(conn_path, connection)
            logger.info(f"Wrote connection: {conn_path}")

    def _transform_streams_with_endpoint_paths(
        self,
        streams: List[Dict[str, Any]],
        endpoint_path_map: Dict[str, str],
    ) -> None:
        """
        Transform streams from endpoint_id to endpoint path format.

        Modifies streams in-place.

        Args:
            streams: List of stream configs (modified in-place)
            endpoint_path_map: Map of endpoint_id -> relative path
        """
        for stream in streams:
            # Transform source endpoint
            source = stream.get("source", {})
            source_endpoint_id = source.get("endpoint_id")

            if source_endpoint_id:
                endpoint_path = endpoint_path_map.get(source_endpoint_id)
                if endpoint_path:
                    source["endpoint"] = endpoint_path
                    del source["endpoint_id"]
                    logger.info(
                        f"Transformed source endpoint {source_endpoint_id} -> {endpoint_path}"
                    )
                else:
                    logger.warning(
                        f"No path found for source endpoint: {source_endpoint_id}"
                    )

            # Transform destination endpoints
            destinations = stream.get("destinations", [])
            for dest in destinations:
                dest_endpoint_id = dest.get("endpoint_id")

                if dest_endpoint_id:
                    endpoint_path = endpoint_path_map.get(dest_endpoint_id)
                    if endpoint_path:
                        dest["endpoint"] = endpoint_path
                        del dest["endpoint_id"]
                        logger.info(
                            f"Transformed dest endpoint {dest_endpoint_id} -> {endpoint_path}"
                        )
                    else:
                        logger.warning(
                            f"No path found for dest endpoint: {dest_endpoint_id}"
                        )

    def _collect_connection_ids(
        self, connections: List[Dict[str, Any]]
    ) -> set:
        """Collect all connection IDs from connections list."""
        return {
            c["connection_id"]
            for c in connections
            if c.get("connection_id")
        }

    def _get_pipeline_id_without_version(self) -> str:
        """Extract pipeline ID without version suffix."""
        if ":" in self.pipeline_id:
            return self.pipeline_id.split(":")[0]
        return self.pipeline_id

    def fetch_and_write_all(
        self, stream_ids: Optional[List[str]] = None
    ) -> Path:
        """
        Fetch all configuration and write to directories defined in analitiq.yaml.

        Fetches via single batch-job-data Lambda call:
        - Pipeline config
        - Connection configs
        - Connector configs
        - Endpoint configs
        - Stream configs

        Fetches from S3:
        - Secrets for each connection

        Transforms:
        - Connections from UUID format to path-based format
        - Streams endpoint_id to endpoint path format

        Args:
            stream_ids: Optional list of specific stream IDs to fetch

        Returns:
            Path to the pipeline config file
        """
        self.validate_environment()

        # Create output directories (paths from analitiq.yaml)
        self.pipelines_path.mkdir(parents=True, exist_ok=True)
        self.streams_path.mkdir(parents=True, exist_ok=True)
        self.connections_path.mkdir(parents=True, exist_ok=True)
        self.secrets_path.mkdir(parents=True, exist_ok=True)
        self.connectors_path.mkdir(parents=True, exist_ok=True)

        # Fetch all data via single Lambda call
        data = self.fetch_batch_job_data(stream_ids)

        pipeline = data["pipeline"]
        connections = data.get("connections", [])
        connectors = data.get("connectors", [])
        endpoints = data.get("endpoints", [])
        streams = data.get("streams", [])

        # Write connectors and get set of written connector_ids
        written_connector_ids = self._write_connectors(connectors)

        # Write endpoints and get path map (endpoint_id -> relative path)
        endpoint_path_map = self._write_endpoints(endpoints, written_connector_ids)

        # Fetch all secrets in parallel from S3
        connection_ids = self._collect_connection_ids(connections)
        all_secrets = self.fetch_secrets_parallel(connection_ids)

        # Write credentials (connection config + secrets merged)
        self._write_credentials(connections, all_secrets)

        # Write connection files (ID-based format for PipelineConfigPrep)
        # Pipeline connections remain as plain UUIDs, PipelineConfigPrep resolves them
        self._write_connection_files(connections)

        # Write transformed pipeline config
        pipeline_id = self._get_pipeline_id_without_version()
        pipeline_path = self.pipelines_path / f"{pipeline_id}.json"
        self.write_json(pipeline_path, pipeline)
        logger.info(f"Wrote pipeline: {pipeline_path}")

        # Transform streams: endpoint_id -> endpoint path
        self._transform_streams_with_endpoint_paths(streams, endpoint_path_map)

        # Write transformed stream configs
        for stream in streams:
            stream_id = stream.get("stream_id")
            stream_path = self.streams_path / f"{stream_id}.json"
            self.write_json(stream_path, stream)

        logger.info(f"Configuration written using paths from analitiq.yaml:")
        logger.info(f"  Pipelines: {self.pipelines_path}")
        logger.info(f"  Streams: {self.streams_path}")
        logger.info(f"  Connections: {self.connections_path}")
        logger.info(f"  Secrets: {self.secrets_path}")
        logger.info(f"  Connectors: {self.connectors_path}")
        logger.info(f"  Total streams: {len(streams)}")
        logger.info(f"  Total connections: {len(connections)}")
        logger.info(f"  Total connectors: {len(written_connector_ids)}")
        logger.info(f"  Total endpoints: {len(endpoint_path_map)}")

        return pipeline_path


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Fetch configuration via Lambda/S3 for pipeline execution. "
        "Output paths are determined by analitiq.yaml."
    )
    parser.add_argument(
        "--region",
        type=str,
        default=os.getenv("AWS_REGION", "eu-central-1"),
        help="AWS region (default: eu-central-1)",
    )
    parser.add_argument(
        "--stream-ids",
        type=str,
        nargs="*",
        help="Optional specific stream IDs to fetch (space-separated)",
    )

    args = parser.parse_args()

    try:
        fetcher = ConfigFetcher(region=args.region)
        pipeline_path = fetcher.fetch_and_write_all(
            stream_ids=args.stream_ids if args.stream_ids else None
        )

        # Print pipeline path for caller to use
        print(f"PIPELINE_PATH={pipeline_path}")
        return 0

    except NoCredentialsError:
        logger.error("AWS credentials not configured")
        return 1
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except RuntimeError as e:
        logger.error(f"Fetch error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())