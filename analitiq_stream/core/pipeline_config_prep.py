"""
Pipeline Configuration Preparation

This module provides the PipelineConfigPrep class that handles loading and merging
of pipeline configurations from either local filesystem or AWS S3, depending on
the deployment environment.
"""
import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Literal
from urllib.parse import urlparse
import re
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pydantic import BaseModel, Field, field_validator, ValidationInfo

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

def validate_pipeline_config(config) -> bool:
    """Validate pipeline-level configuration fields."""
    required_fields = ["pipeline_id"]

    for field in required_fields:
        if field not in config:
            logger.error(f"Missing required pipeline field: {field}")
            return False

    # Validate pipeline_id format
    pipeline_id = config["pipeline_id"]
    if not isinstance(pipeline_id, str) or not pipeline_id.strip():
        logger.error("pipeline_id must be a non-empty string")
        return False

    # Validate engine_config if present
    if "engine_config" in config:
        engine_config = config["engine_config"]
        if not isinstance(engine_config, dict):
            logger.error("engine_config must be a dictionary")
            return False

        # Validate numeric values in engine config
        numeric_fields = ["batch_size", "max_concurrent_batches", "buffer_size"]
        for field in numeric_fields:
            if field in engine_config and not isinstance(engine_config[field], int):
                logger.error(f"engine_config.{field} must be an integer")
                return False

    def _validate_src_dest(key: str) -> bool:
        config_value = config[key]
        if not isinstance(config_value, dict):
            logger.error(f"{key} must be a dictionary")
            return False

        # For data destinations, validate refresh_mode if present
        if key == "dst" and "refresh_mode" in config_value:
            valid_refresh_modes = ["insert", "upsert", "truncate_insert"]
            if config_value["refresh_mode"] not in valid_refresh_modes:
                logger.error(f"Invalid dst.refresh_mode: {config_value['refresh_mode']}. Must be one of: {valid_refresh_modes}")
                return False

        # Validate batch_size if present
        if "batch_size" in config_value:
            if not isinstance(config_value["batch_size"], int) or config_value["batch_size"] <= 0:
                logger.error("dst.batch_size must be a positive integer")
                return False

        return True

    # Validate src and dst sections if present
    for section in ("src", "dst"):
        if section in config:
            if not _validate_src_dest(section):
                return False

    return True

def validate_transformations(transformations) -> bool:
    """Validate transformation configurations."""

    if not isinstance(transformations, list):
        logger.error("transformations must be a list")
        return False

    valid_transformation_types = [ # TODO make sure these match the Pydantic definitions
        "field_mapping",
        "value_transformation",
        "computed_field",
        "conditional_transformation",
    ]

    for i, transformation in enumerate(transformations):
        if not isinstance(transformation, dict):
            logger.error(f"Transformation {i} must be a dictionary")
            return False

        if "type" not in transformation:
            logger.error(f"Transformation {i} missing required field: type")
            return False

        if transformation["type"] not in valid_transformation_types:
            logger.error(f"Invalid transformation type: {transformation['type']}")
            return False

        # Validate specific transformation types
        if transformation["type"] == "field_mapping":
            if "mappings" not in transformation:
                logger.error(f"Field mapping transformation {i} missing mappings")
                return False

        elif transformation["type"] in ["value_transformation", "computed_field"]:
            if "field" not in transformation:
                logger.error(f"Transformation {i} missing required field: field")
                return False

    return True

def expand_required_vars(config: dict) -> dict:
    """
    Expands ${VAR} in values using environment variables.
    Fails if any variable is missing.
    """
    pattern = re.compile(r"\${([^}]+)}")

    expanded = {}
    for key, value in config.items():
        if isinstance(value, str):
            matches = pattern.findall(value)
            for var in matches:
                if var not in os.environ:
                    raise EnvironmentError(f"Missing required environment variable: {var}")
            expanded[key] = os.path.expandvars(value)
        else:
            expanded[key] = value
    return expanded

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

        # Validate environment
        self.validate_environment()

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

    def _get_config_path(self, config_type: str, id: str) -> str:
        """Get path to host or endpoints configuration file."""
        if self.is_local_mode:
            return f"{self.settings.local_config_mount}/{config_type}/{id}.json"
        else:
            return f"s3://{self.settings.s3_config_bucket}/{config_type}/{id}.json"

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
        config_dict = self.load_json_config(path)
        return config_dict

    def load_config(self, config_type: Literal["hosts", "endpoints"], id: str) -> Dict[str, Any]:
        """Load host configuration by ID."""
        path = self._get_config_path(config_type, id)
        logger.debug(f"Loading host config from: {path}")
        return self.load_json_config(path, expand_env_vars=True)

    def validate_config(self) -> bool:
        """Validate pipeline configuration."""
        try:
            # Validate pipeline-level configuration
            if not validate_pipeline_config():
                return False

            # Validate transformations if present
            if "transformations" in self.config:
                if not validate_transformations(self.config["transformations"]):
                    return False

            # Validate validation rules if present
            if "validation" in self.config:
                if not validate_validation_rules(self.config["validation"]):
                    return False

            logger.info("Configuration validation passed")
            return True

        except Exception as e:
            logger.error(f"Configuration validation failed: {str(e)}")
            return False

    def load_and_merge_config(self, pipeline_config: Dict) -> Dict[str, Any]:
        """
        Load and merge endpoint schema with host credentials.

        Args:
            pipeline_config: The validated pipeline configuration

        Returns:
            Merged configuration dictionary
        """
        # Get pipeline-level host_id from the source/destination config

        for config_type in ["src", "dst"]:
            config_section = pipeline_config[config_type]

            if not config_section:
                raise ValueError(f"pipeline_config must contain '{config_type}' section")

            # we expect host_id to be at the section level
            host_id = config_section.get('host_id')

            #load the host config
            host_config = self.load_config('hosts', host_id)
            if not host_config:
                raise ValueError(f"Could not load host config for host '{host_id}'.")

            # merge it into main config
            pipeline_config[config_type].update(host_config)

            logger.debug(f"Merged {config_type} config for host {host_id}.")

            for stream_id, steam_config in pipeline_config['streams'].items():
                # we expect endpoint_id to be at the section level
                endpoint_id = steam_config.get(config_type).get('endpoint_id')

                # Load endpoint schema and host credentials
                endpoint_config = self.load_config('endpoints', endpoint_id)

                # merge it into main config
                pipeline_config["streams"][stream_id][config_type].update(endpoint_config)

                logger.debug(f"Merged {config_type} config for stream {stream_id} for endpoint {endpoint_id}.")

        return pipeline_config

    def create_config(self):
        """
        Load configuration.

        Returns:
            Configured Pipeline instance
        """
        # Load and validate pipeline configuration
        pipeline_config = self.load_pipeline_config()

        validate_pipeline_config(pipeline_config)

        # Load and merge configurations
        pipeline_config = self.load_and_merge_config(pipeline_config)

        # Log pipeline information
        streams = pipeline_config.get("streams", {})

        logger.info(f"Source: {pipeline_config.get('src').get('host')}")
        logger.info(f"Destination: {pipeline_config.get('dst').get('host')}")
        logger.info(f"Configured streams: {len(streams)}")

        expanded_config = expand_required_vars(pipeline_config)

        return PipelineConfig(**expanded_config
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



if __name__ == "__main__":
    # Test basic functionality
    import os
    os.environ["PIPELINE_ID"] = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    os.environ["ENV"] = "local"
    os.environ["LOCAL_CONFIG_MOUNT"] = "/Users/kirillandriychuk/Documents/Projects/analitiq-core/examples/wise_to_sevdesk"

    prep = PipelineConfigPrep()
    pipeline_config = prep.create_config()
    print(json.dumps(pipeline_config.__dict__, indent=4, default=str))
