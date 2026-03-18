#!/usr/bin/env python3
"""
Configuration fetcher for cloud deployments.

This script fetches all configuration via a single Lambda call and secrets from S3:
- Pipeline, streams, connectors, connections, endpoints via Lambda (_batch-job-data)
- Secrets from S3

It writes a SINGLE consolidated configuration file per pipeline:
- Consolidated file: {paths.pipelines}/{pipeline_id}.json
- Secrets: {paths.secrets}/{connection_id}.json (kept separate for security)
- States: {paths.state}/streams/{stream_id}/state.json (stream state checkpoints)

The consolidated file format:
{
    "pipeline": { ... },        # Pipeline object
    "connections": [ ... ],     # List of connection objects (no secrets)
    "connectors": [ ... ],      # List of connector metadata
    "endpoints": [ ... ],       # List of endpoint definitions
    "streams": [ ... ]          # List of stream configurations
    "states": { ... }           # Dict of stream_id -> state record (optional)
}

This script runs as the first step in AWS Batch container execution,
before the main pipeline runner is invoked.

Usage:
    python docker/config_fetcher.py

Environment Variables:
    PIPELINE_ID: UUID of the pipeline to fetch (optionally with version: uuid_v1)
    ORG_ID: UUID of the client
    STREAM_ID: UUID of the stream to process
    INVOCATION_ID: UUID of the batch job invocation (passed to Lambda)
    AWS_BATCH_JOB_ID: AWS Batch job ID (auto-injected by AWS Batch, passed to Lambda)
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

    Writes a SINGLE consolidated configuration file per pipeline containing
    all related data (pipeline, connections, connectors, endpoints, streams).
    Secrets are written separately to the secrets directory.

    Output structure:
    - {paths.pipelines}/{pipeline_id}.json  (consolidated config)
    - {paths.secrets}/{connection_id}.json  (secrets, kept separate)
    - {paths.state}/streams/{stream_id}/state.json  (stream state checkpoints)
    """

    def __init__(
        self,
        *,
        region: str = "eu-central-1",
    ):
        """
        Initialize config fetcher.

        Output paths are determined by analitiq.yaml (single source of truth).
        Only 'pipelines' and 'secrets' paths are required.

        Args:
            region: AWS region
        """
        self.region = region

        # Get environment variables
        self.env = os.getenv("ENV", "dev")
        self.pipeline_id = os.getenv("PIPELINE_ID")
        self.org_id = os.getenv("ORG_ID")
        self.stream_id = os.getenv("STREAM_ID")
        self.invocation_id = os.getenv("INVOCATION_ID")
        self.batch_job_id = os.getenv("AWS_BATCH_JOB_ID")

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
        self.pipelines_path = Path(self.paths["pipelines"])
        self.secrets_path = Path(self.paths["secrets"])
        self.state_path = Path(self.paths["state"])

    def _load_paths_from_analitiq_yaml(self) -> Dict[str, str]:
        """
        Load directory paths from analitiq.yaml at project root.

        Requires 'pipelines', 'secrets', and 'state' paths.

        Returns:
            Dict with paths for pipelines and secrets

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

        required_paths = ["pipelines", "secrets", "state"]
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
            "ORG_ID": self.org_id,
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
                "org_id": self.org_id,
                "pipeline_id": self.pipeline_id,
            }

            if self.invocation_id:
                payload["invocation_id"] = self.invocation_id

            if self.batch_job_id:
                payload["batch_job_id"] = self.batch_job_id

            if self.stream_id:
                payload["stream_id"] = self.stream_id

            if stream_ids:
                payload["stream_ids"] = stream_ids

            logger.info(f"Lambda payload: {json.dumps(payload)}")

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
                    f"(org_id={self.org_id})"
                )

            logger.info(
                f"Fetched batch job data for pipeline: {self.pipeline_id}"
            )

            return data

        except ClientError as e:
            raise RuntimeError(f"Failed to invoke batch-job-data Lambda: {e}")

    def fetch_secrets(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Fetch secrets from S3."""
        key = f"connections/{self.org_id}/{connection_id}"

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

    def _write_secrets(
        self,
        connections: List[Dict[str, Any]],
        all_secrets: Dict[str, Dict[str, Any]],
    ) -> None:
        """
        Write secrets to disk.

        Directory structure: {paths.secrets}/{connection_id}.json

        Secrets are kept separate from the consolidated config for security.
        PipelineConfigPrep will load secrets from here and merge with
        connection configs from the consolidated file.

        Args:
            connections: List of connection configs from batch-job-data
            all_secrets: Dict mapping connection_id -> secrets from S3
        """
        for connection in connections:
            connection_id = connection.get("connection_id")
            if not connection_id:
                continue

            # Get secrets for this connection
            secrets = all_secrets.get(connection_id, {})
            if not secrets:
                logger.debug(f"No secrets found for connection: {connection_id}")
                continue

            # Write secrets file
            secrets_path = self.secrets_path / f"{connection_id}.json"
            self.write_json(secrets_path, secrets)
            logger.info(f"Wrote secrets: {secrets_path}")

    def _write_states(self, states: Dict[str, Dict[str, Any]]) -> None:
        """
        Write stream state files to disk.

        Directory structure: {paths.state}/streams/{stream_id}/state.json

        Args:
            states: Dict mapping stream_id -> state record
        """
        for stream_id, state_data in states.items():
            state_dir = self.state_path / "streams" / stream_id
            state_file = state_dir / "state.json"
            self.write_json(state_file, state_data)
            logger.info(f"Wrote state: {state_file}")

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
        if "_" in self.pipeline_id:
            return self.pipeline_id.split("_")[0]
        return self.pipeline_id

    def fetch_and_write_all(
        self, stream_ids: Optional[List[str]] = None
    ) -> Path:
        """
        Fetch all configuration and write consolidated file + secrets.

        Fetches via single batch-job-data Lambda call:
        - Pipeline, connections, connectors, endpoints, streams

        Fetches from S3:
        - Secrets for each connection

        Writes:
        - Consolidated config: {paths.pipelines}/{pipeline_id}.json
        - Secrets: {paths.secrets}/{connection_id}.json
        - States: {paths.state}/streams/{stream_id}/state.json

        Args:
            stream_ids: Optional list of specific stream IDs to fetch

        Returns:
            Path to the consolidated pipeline config file
        """
        self.validate_environment()

        # Create output directories
        self.pipelines_path.mkdir(parents=True, exist_ok=True)
        self.secrets_path.mkdir(parents=True, exist_ok=True)

        # Fetch all data via single Lambda call
        data = self.fetch_batch_job_data(stream_ids)

        pipeline = data["pipeline"]
        connections = data.get("connections", [])
        connectors = data.get("connectors", [])
        endpoints = data.get("endpoints", [])
        streams = data.get("streams", [])
        states = data.get("states", {})

        # Fetch all secrets in parallel from S3
        connection_ids = self._collect_connection_ids(connections)
        all_secrets = self.fetch_secrets_parallel(connection_ids)

        # Write secrets files (separate from consolidated config)
        self._write_secrets(connections, all_secrets)

        # Write stream state files
        if states:
            self._write_states(states)

        # Write consolidated pipeline config
        pipeline_id = self._get_pipeline_id_without_version()
        consolidated = {
            "pipeline": pipeline,
            "connections": connections,
            "connectors": connectors,
            "endpoints": endpoints,
            "streams": streams,
        }
        pipeline_path = self.pipelines_path / f"{pipeline_id}.json"
        self.write_json(pipeline_path, consolidated)

        logger.info(f"Wrote consolidated config: {pipeline_path}")
        logger.info(f"  Pipeline: {pipeline.get('name', pipeline_id)}")
        logger.info(f"  Connections: {len(connections)}")
        logger.info(f"  Connectors: {len(connectors)}")
        logger.info(f"  Endpoints: {len(endpoints)}")
        logger.info(f"  Streams: {len(streams)}")
        logger.info(f"  Secrets: {len(all_secrets)}")
        logger.info(f"  States: {len(states)}")

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