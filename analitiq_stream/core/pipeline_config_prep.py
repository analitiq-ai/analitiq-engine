"""
Pipeline Configuration Preparation

This module provides the PipelineConfigPrep class that handles loading and merging
of pipeline configurations from either local filesystem or AWS S3, depending on
the deployment environment.
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pydantic import BaseModel, Field, field_validator, ValidationInfo

from analitiq_stream import Pipeline
from analitiq_stream.core.credentials import CredentialsManager
from analitiq_stream.models.state import PipelineConfig, SourceConfig, DestinationConfig


logger = logging.getLogger(__name__)


class PipelineConfigPrepSettings(BaseModel):
    """Configuration settings for PipelineConfigPrep."""

    env: str = Field(default="local", description="Environment: local, dev, or prod")
    pipeline_id: str = Field(..., description="Pipeline ID to load")

    # AWS region (unified across all AWS resources)
    aws_region: str = Field(default="eu-central-1", description="AWS region for all resources")

    # S3 configuration (required for dev/prod, ignored for local)
    s3_config_bucket: Optional[str] = Field(default=None, description="S3 bucket for configs")

    # Local mount point for configs (required for local, ignored for dev/prod)
    local_config_mount: Optional[str] = Field(default=None, description="Local config mount point")

    # Optional: AWS Secrets Manager integration (only for dev/prod)
    use_secrets_manager: bool = Field(default=False, description="Use AWS Secrets Manager for credentials")

    @field_validator('s3_config_bucket')
    @classmethod
    def validate_s3_bucket_for_aws(cls, v: Optional[str], info: ValidationInfo) -> Optional[str]:
        """Validate S3 bucket is provided for AWS environments."""
        env = info.data.get('env')
        if env in ['dev', 'prod'] and not v:
            return "analitiq-config"  # Default bucket name
        return v

    @field_validator('local_config_mount')
    @classmethod
    def validate_local_mount_for_local(cls, v: Optional[str], info: ValidationInfo) -> Optional[str]:
        """Validate local mount is provided for local environment."""
        env = info.data.get('env')
        if env == 'local' and not v:
            return "/config"  # Default mount point
        return v

    model_config = {"validate_assignment": True}


class PipelineConfigPrep:
    """
    Orchestrates loading and preparation of pipeline configurations from multiple sources.

    Supports both local filesystem and AWS S3 backends, with automatic environment detection
    and credential management.
    """

    def __init__(self, settings: Optional[PipelineConfigPrepSettings] = None):
        """
        Initialize PipelineConfigPrep.

        Args:
            settings: Configuration settings. If None, loads from environment variables.
        """
        self.settings = settings or self._load_settings_from_env()
        self.credentials_manager = CredentialsManager()

        # Initialize AWS clients if needed (only for non-local environments)
        self._s3_client: Optional[boto3.client] = None
        self._secrets_client: Optional[boto3.client] = None

        logger.info(f"Initialized PipelineConfigPrep for environment: {self.settings.env}")
        logger.info(f"Pipeline ID: {self.settings.pipeline_id}")
        if not self.is_local_mode:
            logger.info(f"AWS Region: {self.settings.aws_region}")

    @classmethod
    def _load_settings_from_env(cls) -> PipelineConfigPrepSettings:
        """Load settings from environment variables."""
        return PipelineConfigPrepSettings(
            env=os.getenv("ENV", "local"),
            pipeline_id=os.getenv("PIPELINE_ID", ""),
            aws_region=os.getenv("AWS_REGION", "eu-central-1"),
            s3_config_bucket=os.getenv("S3_CONFIG_BUCKET"),
            local_config_mount=os.getenv("LOCAL_CONFIG_MOUNT"),
            use_secrets_manager=os.getenv("USE_SECRETS_MANAGER", "false").lower() == "true"
        )

    @property
    def is_local_mode(self) -> bool:
        """Check if running in local mode."""
        return self.settings.env == "local"

    @property
    def s3_client(self) -> boto3.client:
        """Get or create S3 client."""
        if self.is_local_mode:
            raise RuntimeError("S3 client not available in local mode")

        if self._s3_client is None:
            self._s3_client = boto3.client('s3', region_name=self.settings.aws_region)
        return self._s3_client

    @property
    def secrets_client(self) -> boto3.client:
        """Get or create Secrets Manager client."""
        if self.is_local_mode:
            raise RuntimeError("Secrets Manager client not available in local mode")

        if self._secrets_client is None:
            self._secrets_client = boto3.client(
                'secretsmanager',
                region_name=self.settings.aws_region
            )
        return self._secrets_client

    def _get_pipeline_config_path(self) -> str:
        """Get path to pipeline configuration file."""
        if self.is_local_mode:
            return f"{self.settings.local_config_mount}/pipelines/{self.settings.pipeline_id}.json"
        else:
            return f"s3://{self.settings.s3_config_bucket}/pipelines/{self.settings.pipeline_id}.json"

    def _get_host_config_path(self, host_id: str) -> str:
        """Get path to host configuration file."""
        if self.is_local_mode:
            return f"{self.settings.local_config_mount}/hosts/{host_id}.json"
        else:
            return f"s3://{self.settings.s3_config_bucket}/hosts/{host_id}.json"

    def _get_endpoint_config_path(self, endpoint_id: str) -> str:
        """Get path to endpoint configuration file."""
        if self.is_local_mode:
            return f"{self.settings.local_config_mount}/endpoints/{endpoint_id}.json"
        else:
            return f"s3://{self.settings.s3_config_bucket}/endpoints/{endpoint_id}.json"

    def _load_local_json(self, file_path: str, expand_env_vars: bool = False) -> Dict[str, Any]:
        """Load JSON from local filesystem."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {file_path}")

        try:
            with open(path, "r") as f:
                config = json.load(f)

            # Expand environment variables if requested
            if expand_env_vars:
                config = self.credentials_manager._expand_environment_variables(config)

            logger.debug(f"Loaded local config from {file_path}")
            return config

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in file {file_path}: {e}")

    def _load_s3_json(self, s3_path: str, expand_env_vars: bool = False) -> Dict[str, Any]:
        """Load JSON from S3."""
        parsed_url = urlparse(s3_path)
        bucket = parsed_url.netloc
        key = parsed_url.path.lstrip('/')

        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            config = json.loads(content)

            # Expand environment variables if requested
            if expand_env_vars:
                config = self.credentials_manager._expand_environment_variables(config)

            logger.debug(f"Loaded S3 config from {s3_path}")
            return config

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                raise FileNotFoundError(f"Configuration file not found in S3: {s3_path}")
            elif error_code == 'NoSuchBucket':
                raise FileNotFoundError(f"S3 bucket not found: {bucket}")
            else:
                raise RuntimeError(f"Failed to load config from S3 {s3_path}: {e}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in S3 file {s3_path}: {e}")
        except NoCredentialsError:
            raise RuntimeError("AWS credentials not configured for S3 access")

    def load_json_config(self, path: str, expand_env_vars: bool = False) -> Dict[str, Any]:
        """
        Load JSON configuration from either local filesystem or S3.

        Args:
            path: File path (local) or S3 URI
            expand_env_vars: Whether to expand environment variables

        Returns:
            Parsed configuration dictionary
        """
        if path.startswith('s3://'):
            return self._load_s3_json(path, expand_env_vars)
        else:
            return self._load_local_json(path, expand_env_vars)

    def load_pipeline_config(self) -> PipelineConfig:
        """Load and validate the main pipeline configuration."""
        path = self._get_pipeline_config_path()
        logger.info(f"Loading pipeline config from: {path}")
        config_data = self.load_json_config(path)
        return PipelineConfig(**config_data)

    def load_host_config(self, host_id: str) -> Dict[str, Any]:
        """Load host configuration by ID."""
        path = self._get_host_config_path(host_id)
        logger.debug(f"Loading host config from: {path}")
        return self.load_json_config(path, expand_env_vars=True)

    def load_endpoint_config(self, endpoint_id: str) -> Dict[str, Any]:
        """Load endpoint configuration by ID."""
        path = self._get_endpoint_config_path(endpoint_id)
        logger.debug(f"Loading endpoint config from: {path}")
        return self.load_json_config(path)

    def merge_config(self, pipeline_config: PipelineConfig, config_type: str) -> Dict[str, Any]:
        """
        Load and merge endpoint schema with host credentials.

        Args:
            pipeline_config: The validated pipeline configuration
            config_type: Either 'src' or 'dst' to specify source or destination

        Returns:
            Merged configuration dictionary
        """
        # Get pipeline-level host_id from the source/destination config
        config_section = getattr(pipeline_config, config_type, None)
        if not config_section:
            raise ValueError(f"pipeline_config must contain '{config_type}' section")

        # we expect host_id to be at the pipeline level
        host_id = config_section.get('host_id')
        if not host_id:
            raise ValueError(f"pipeline_config.{config_type} must contain 'host_id'")

        # Get endpoint_id from the first stream (assuming all streams use same endpoint)
        if not pipeline_config.streams:
            raise ValueError("No streams configured")

        first_stream = next(iter(pipeline_config.streams.values()))
        stream_config = first_stream.get(config_type, {})
        endpoint_id = stream_config.get("endpoint_id")
        if not endpoint_id:
            raise ValueError(f"First stream must contain '{config_type}.endpoint_id'")

        # Load endpoint schema and host credentials
        endpoint_config = self.load_endpoint_config(endpoint_id)
        host_config = self.load_host_config(host_id)

        # Merge configurations (host credentials take precedence)
        merged_config = endpoint_config.copy()
        merged_config.update(host_config)

        logger.debug(f"Merged {config_type} config from endpoint {endpoint_id} and host {host_id}")
        return merged_config

    def create_pipeline(self) -> Pipeline:
        """
        Load configuration and create pipeline instance.

        Returns:
            Configured Pipeline instance
        """
        # Load and validate pipeline configuration
        pipeline_config = self.load_pipeline_config()

        # Load and merge configurations
        source_config = self.merge_config(pipeline_config, "src")
        destination_config = self.merge_config(pipeline_config, "dst")

        # Log pipeline information
        streams = pipeline_config.streams

        logger.info(f"Starting {pipeline_config.name} (ID: {pipeline_config.pipeline_id})")
        logger.info(f"Configured streams: {len(streams)}")
        logger.info(f"Source: {source_config.get('base_url', 'unknown')}")
        logger.info(f"Destination: {destination_config.get('base_url', 'unknown')}")

        # Create and return pipeline with dictionary config
        return Pipeline(
            pipeline_config=pipeline_config.model_dump(),
            source_config=source_config,
            destination_config=destination_config
        )

    def validate_environment(self) -> None:
        """
        Validate that the environment is properly configured.

        Raises:
            RuntimeError: If validation fails
        """
        if not self.settings.pipeline_id:
            raise RuntimeError("PIPELINE_ID environment variable is required")

        if not self.is_local_mode:
            # Validate AWS credentials are available
            try:
                self.s3_client.list_buckets()
                logger.debug("AWS S3 credentials validated")
            except NoCredentialsError:
                raise RuntimeError("AWS credentials not configured for S3 access")
            except ClientError as e:
                logger.warning(f"AWS S3 validation warning: {e}")
        else:
            # Validate local mount point exists
            mount_path = Path(self.settings.local_config_mount)
            if not mount_path.exists():
                raise RuntimeError(f"Local config mount point does not exist: {mount_path}")
            logger.debug(f"Local config mount validated: {mount_path}")

    def get_pipeline_info(self) -> Dict[str, Any]:
        """
        Get basic pipeline information without fully loading configs.

        Returns:
            Dictionary with pipeline metadata
        """
        try:
            pipeline_config = self.load_pipeline_config()
            return {
                "pipeline_id": pipeline_config.pipeline_id,
                "name": pipeline_config.name,
                "version": pipeline_config.version,
                "stream_count": len(pipeline_config.streams),
                "environment": self.settings.env,
                "config_source": "local" if self.is_local_mode else "s3"
            }
        except Exception as e:
            logger.error(f"Failed to load pipeline info: {e}")
            return {
                "pipeline_id": self.settings.pipeline_id,
                "environment": self.settings.env,
                "error": str(e)
            }