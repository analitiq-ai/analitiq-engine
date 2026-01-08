"""
Pipeline Configuration Preparation

This module provides the PipelineConfigPrep class that handles loading and merging
of pipeline and stream configurations from local filesystem.

Architecture:

Configuration is ALWAYS loaded from local filesystem.
Directory paths are defined in analitiq.yaml (single source of truth):
- Pipelines: {paths.pipelines}/{pipeline_id}.json
- Streams: {paths.streams}/{stream_id}.json
- Connections: {paths.connections}/{connection_id}.json
- Secrets: {paths.secrets}/{connection_id}.json
- Endpoints: {paths.connectors}/{connector_name}/endpoints/{endpoint}.json

In cloud environments (dev/prod):
1. config_fetcher.py runs first to populate directories from Lambda/S3
2. PipelineConfigPrep then loads from directories defined in analitiq.yaml

The ENV variable controls storage backends (state, logs, DLQ, metrics), NOT config loading.

Loading Flow:
1. Load paths from analitiq.yaml
2. Load pipeline config from {paths.pipelines}
3. Load stream configs from {paths.streams}
4. For each connection (referenced by connection_id in pipeline):
   - Load connection config from {paths.connections}/{connection_id}.json
   - Load secrets from {paths.secrets}/{connection_id}.json
   - Expand ${placeholder} values with secrets
5. For each stream source/destination:
   - Load endpoint from {paths.connectors}/{path}
"""
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from src.config import load_analitiq_config
from src.core.credentials import CredentialsManager
from src.models.pipeline import PipelineConfig, PipelineConnectionsConfig, RuntimeConfig
from src.models.stream import StreamConfig, SourceConfig, DestinationConfig, MappingConfig
from src.models.stream_state import StreamState
from src.secrets import (
    SecretsResolver,
    ConnectionConfig,
    LocalFileSecretsResolver,
    InMemorySecretsResolver,
    SecretNotFoundError,
)


logger = logging.getLogger(__name__)


class PipelineConfigPrepSettings(BaseModel):
    """Configuration settings for PipelineConfigPrep.

    Pipeline and stream configs are ALWAYS loaded from local filesystem.
    Directory paths are defined in analitiq.yaml (single source of truth).

    In cloud environments (dev/prod), config_fetcher.py pre-fetches configs
    from Lambda/S3 and writes them to directories defined in analitiq.yaml
    before PipelineConfigPrep runs.

    The ENV variable controls:
    - State storage backend (local filesystem vs S3)
    - Log storage backend (local filesystem vs S3)
    - DLQ storage backend (local filesystem vs S3)
    - Metrics storage backend (local filesystem vs S3)
    """

    env: str = Field(default="local", description="Environment: local, dev, or prod")
    pipeline_id: str = Field(..., description="Pipeline ID to load")
    client_id: Optional[str] = Field(default=None, description="Client ID for cloud storage paths")

    # AWS region (used for state/logs/DLQ storage in cloud environments)
    aws_region: str = Field(default="eu-central-1", description="AWS region for cloud storage")

    # Late-binding secrets option
    late_binding_secrets: bool = Field(
        default=False,
        description="When True, secrets are resolved at connection time (late-binding). "
                    "When False, secrets are resolved at config load time (eager)."
    )

    model_config = {"validate_assignment": True}


def validate_pipeline_config(config: Dict[str, Any]) -> bool:
    """Validate pipeline-level configuration fields."""
    required_fields = ["pipeline_id"]

    for field in required_fields:
        if field not in config:
            logger.error(f"Missing required pipeline field: {field}")
            return False

    pipeline_id = config["pipeline_id"]
    if not isinstance(pipeline_id, str) or not pipeline_id.strip():
        logger.error("pipeline_id must be a non-empty string")
        return False

    return True


def validate_stream_config(config: Dict[str, Any]) -> bool:
    """Validate stream configuration fields.

    Accepts either endpoint (path) or endpoint_id (UUID) format.
    """
    required_fields = ["stream_id", "source", "destinations"]

    for field in required_fields:
        if field not in config:
            logger.error(f"Missing required stream field: {field}")
            return False

    source = config.get("source", {})
    if not source.get("connection_ref"):
        logger.error("source.connection_ref is required")
        return False
    if not source.get("endpoint") and not source.get("endpoint_id"):
        logger.error("source.endpoint or source.endpoint_id is required")
        return False

    destinations = config.get("destinations", [])
    if not destinations:
        logger.error("At least one destination is required")
        return False

    for i, dest in enumerate(destinations):
        if not dest.get("connection_ref"):
            logger.error(f"destinations[{i}].connection_ref is required")
            return False
        if not dest.get("endpoint") and not dest.get("endpoint_id"):
            logger.error(f"destinations[{i}].endpoint or endpoint_id is required")
            return False

    return True


def expand_required_vars(config: Dict[str, Any], strict: bool = False) -> Dict[str, Any]:
    """
    Expands ${VAR} in values using environment variables.

    Args:
        config: Configuration dict to expand
        strict: If True, raises error for missing vars. If False, leaves them unexpanded.
    """
    pattern = re.compile(r"\${([^}]+)}")

    def expand_value(value: Any) -> Any:
        if isinstance(value, str):
            matches = pattern.findall(value)
            if strict:
                for var in matches:
                    if var not in os.environ:
                        raise EnvironmentError(f"Missing required environment variable: {var}")
                return os.path.expandvars(value)
            else:
                # Non-strict: only expand vars that exist, leave others as-is
                def replace_if_exists(match: re.Match) -> str:
                    var = match.group(1)
                    if var in os.environ:
                        return os.environ[var]
                    return match.group(0)  # Keep original placeholder
                return pattern.sub(replace_if_exists, value)
        elif isinstance(value, dict):
            return {k: expand_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [expand_value(item) for item in value]
        return value

    return expand_value(config)


def find_unexpanded_placeholders(config: Any, path: str = "") -> List[tuple]:
    """
    Recursively find all unexpanded ${...} placeholders in a configuration.
    """
    pattern = re.compile(r"\$\{([^}]+)\}")
    unexpanded = []

    if isinstance(config, dict):
        for key, value in config.items():
            current_path = f"{path}.{key}" if path else key
            unexpanded.extend(find_unexpanded_placeholders(value, current_path))
    elif isinstance(config, list):
        for i, item in enumerate(config):
            current_path = f"{path}[{i}]"
            unexpanded.extend(find_unexpanded_placeholders(item, current_path))
    elif isinstance(config, str):
        matches = pattern.findall(config)
        for match in matches:
            unexpanded.append((path, f"${{{match}}}", config))

    return unexpanded


def validate_no_unexpanded_placeholders(config: Dict[str, Any], config_name: str = "config") -> None:
    """
    Validate that a configuration has no unexpanded ${...} placeholders.
    """
    unexpanded = find_unexpanded_placeholders(config)

    if unexpanded:
        error_lines = [
            f"Found {len(unexpanded)} unexpanded placeholder(s) in {config_name}. "
        ]
        for path, placeholder, value in unexpanded:
            display_value = value if len(value) < 80 else value[:77] + "..."
            error_lines.append(f"  - {path}: {placeholder} (value: {display_value})")

        raise ValueError("\n".join(error_lines))


class ResolvedConnection:
    """
    Container for a resolved connection configuration.

    Supports both eager and late-binding secret resolution:
    - Eager: config contains fully-expanded secrets, connection_config_wrapper is None
    - Late-binding: config contains raw template, connection_config_wrapper holds the wrapper
    """

    def __init__(
        self,
        connection_id: str,
        connection_type: str,
        config: Dict[str, Any],
        connection_config_wrapper: Optional[ConnectionConfig] = None,
    ):
        self.connection_id = connection_id
        self.connection_type = connection_type  # "api" or "database"
        self.config = config
        self.connection_config_wrapper = connection_config_wrapper

    @property
    def is_late_binding(self) -> bool:
        """Check if this connection uses late-binding secrets."""
        return self.connection_config_wrapper is not None

    async def resolve_config(self) -> Dict[str, Any]:
        """
        Get the fully-resolved configuration.

        For late-binding connections, this resolves secrets just-in-time.
        For eager connections, this returns the already-resolved config.

        Returns:
            Fully resolved configuration dictionary
        """
        if self.connection_config_wrapper is not None:
            return await self.connection_config_wrapper.resolve()
        return self.config


class PipelineConfigPrep:
    """
    Orchestrates loading and preparation of pipeline and stream configurations.

    Configuration is ALWAYS loaded from local filesystem.
    Directory paths are defined in analitiq.yaml (single source of truth):
    - Pipelines from: {paths.pipelines}/{pipeline_id}.json
    - Streams from: {paths.streams}/{stream_id}.json
    - Connections from: {paths.connections}/{connection_id}.json
    - Secrets from: {paths.secrets}/{connection_id}.json
    - Endpoints from: {paths.connectors}/{connector_name}/endpoints/{endpoint}.json

    Connection format in pipeline config:
    - connections.source.{alias}: "connection_id" (string)
    - connections.destinations[].{alias}: "connection_id" (string)

    In cloud environments (dev/prod), config_fetcher.py pre-populates these
    directories by fetching from Lambda/S3 before this class runs.

    The ENV variable only affects storage backends (state, logs, DLQ, metrics),
    not config loading.
    """

    def __init__(self, settings: Optional[PipelineConfigPrepSettings] = None):
        """
        Initialize PipelineConfigPrep.

        Args:
            settings: Configuration settings. If None, loads from environment variables.
        """
        self.settings = settings or self._load_settings_from_env()
        self.credentials_manager = CredentialsManager()

        # Load paths from analitiq.yaml (single source of truth)
        self._paths = self._load_paths_from_analitiq_yaml()

        # Cache for resolved connections and endpoints
        self._resolved_connections: Dict[str, ResolvedConnection] = {}
        self._resolved_endpoints: Dict[str, Dict[str, Any]] = {}

        # Secrets resolver for late-binding (initialized lazily)
        self._secrets_resolver: Optional[SecretsResolver] = None
        self._secrets_dir: Optional[Path] = None

        # Validate environment
        self.validate_environment()

        logger.info(f"Initialized PipelineConfigPrep for environment: {self.settings.env}")
        logger.info(f"Pipeline ID: {self.settings.pipeline_id}")
        logger.info(f"Using paths from analitiq.yaml: pipelines={self._paths['pipelines']}, "
                    f"streams={self._paths['streams']}, connectors={self._paths['connectors']}")

    def _load_paths_from_analitiq_yaml(self) -> Dict[str, Path]:
        """
        Load directory paths from analitiq.yaml (single source of truth).

        Returns:
            Dict with Path objects for connectors, streams, pipelines, secrets

        Raises:
            FileNotFoundError: If analitiq.yaml is not found
            ValueError: If required paths are missing
        """
        config = load_analitiq_config()
        paths_config = config.get("paths", {})

        required_paths = ["connectors", "connections", "endpoints", "streams", "pipelines", "secrets"]
        missing = [p for p in required_paths if p not in paths_config]
        if missing:
            raise ValueError(f"analitiq.yaml missing required paths: {missing}")

        # Return Path objects (relative paths resolved from current working directory)
        return {
            "connectors": Path(paths_config["connectors"]),
            "connections": Path(paths_config["connections"]),
            "endpoints": Path(paths_config["endpoints"]),
            "streams": Path(paths_config["streams"]),
            "pipelines": Path(paths_config["pipelines"]),
            "secrets": Path(paths_config["secrets"]),
        }

    @classmethod
    def _load_settings_from_env(cls) -> PipelineConfigPrepSettings:
        """Load settings from environment variables.

        Pipeline/stream configs are loaded from paths defined in analitiq.yaml.
        In cloud environments, config_fetcher.py populates these directories first.
        """
        env = os.getenv("ENV", "local")
        client_id = os.getenv("CLIENT_ID")

        # Validate CLIENT_ID for cloud environments (needed for state/logs paths)
        if env != "local" and not client_id:
            raise RuntimeError(
                f"CLIENT_ID environment variable is required for cloud environment '{env}'"
            )

        return PipelineConfigPrepSettings(
            env=env,
            pipeline_id=os.getenv("PIPELINE_ID", ""),
            client_id=client_id,
            aws_region=os.getenv("AWS_REGION", "eu-central-1"),
        )

    @property
    def is_cloud_env(self) -> bool:
        """Check if running in a cloud environment (dev/prod).

        This affects storage backends (state, logs, DLQ) but NOT config loading.
        Config is always loaded from local filesystem.
        """
        return self.settings.env in ("dev", "prod")

    @property
    def secrets_resolver(self) -> SecretsResolver:
        """
        Get the secrets resolver for late-binding secret resolution.

        Returns:
            SecretsResolver instance appropriate for the current environment
        """
        if self._secrets_resolver is None:
            if self._secrets_dir is None:
                raise RuntimeError(
                    "Secrets directory not configured. "
                    "Call create_config() first or set _secrets_dir manually."
                )
            self._secrets_resolver = LocalFileSecretsResolver(self._secrets_dir)
            logger.debug(f"Created LocalFileSecretsResolver for: {self._secrets_dir}")
        return self._secrets_resolver

    # =========================================================================
    # Secrets Methods
    # =========================================================================

    def _expand_secrets_in_config(
        self,
        config: Dict[str, Any],
        secrets: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Expand secret placeholders in configuration."""
        pattern = re.compile(r"\$\{([^}]+)\}")

        def expand_value(value: Any) -> Any:
            if isinstance(value, str):
                def replace_placeholder(match: re.Match) -> str:
                    key = match.group(1)
                    if key in secrets:
                        return str(secrets[key])
                    return match.group(0)
                return pattern.sub(replace_placeholder, value)
            elif isinstance(value, dict):
                return {k: expand_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [expand_value(item) for item in value]
            return value

        return expand_value(config)

    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two configurations (override takes precedence)."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        return result

    # =========================================================================
    # Local File Loading Methods
    # =========================================================================

    def _get_local_path(self, config_type: str, config_id: str) -> Path:
        """Get path to local configuration file using paths from analitiq.yaml."""
        return self._paths[config_type] / f"{config_id}.json"

    def _load_local_json(
        self,
        file_path: str,
        expand_env_vars: bool = False,
        strict_expansion: bool = False
    ) -> Dict[str, Any]:
        """
        Load JSON from local filesystem.

        Args:
            file_path: Path to JSON file
            expand_env_vars: Whether to expand ${VAR} placeholders
            strict_expansion: If True, raise error on missing env vars; if False, keep placeholders
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {file_path}")

        try:
            with open(path, "r") as f:
                config = json.load(f)

            if expand_env_vars:
                config = self._expand_env_vars_in_config(config, strict=strict_expansion)

            return config

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in file {file_path}: {e}")

    def _expand_env_vars_in_config(self, config: Any, strict: bool = False) -> Any:
        """
        Expand ${VAR} placeholders in config using environment variables.

        Args:
            config: Configuration value (dict, list, str, or primitive)
            strict: If True, raise error on missing vars; if False, keep placeholders
        """
        pattern = re.compile(r"\$\{([^}]+)\}")

        def expand_value(value: Any) -> Any:
            if isinstance(value, str):
                def replace_match(match: re.Match) -> str:
                    var_name = match.group(1)
                    env_value = os.environ.get(var_name)
                    if env_value is not None:
                        return env_value
                    elif strict:
                        raise EnvironmentError(f"Missing required environment variable: {var_name}")
                    else:
                        return match.group(0)  # Keep placeholder
                return pattern.sub(replace_match, value)
            elif isinstance(value, dict):
                return {k: expand_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [expand_value(item) for item in value]
            return value

        return expand_value(config)

    def _load_local_pipeline(self) -> Dict[str, Any]:
        """Load pipeline configuration from local filesystem."""
        path = self._get_local_path("pipelines", self.settings.pipeline_id)
        logger.info(f"Loading pipeline config from: {path}")
        return self._load_local_json(path)

    def _load_local_streams(self) -> List[Dict[str, Any]]:
        """Load all stream configurations from local filesystem.

        Streams are loaded from paths.streams directory (defined in analitiq.yaml).
        In cloud environments, config_fetcher.py populates this directory with
        streams that belong to the requested pipeline - no additional filtering needed.
        """
        streams_dir = self._paths["streams"]
        streams = []

        if streams_dir.exists():
            for stream_file in streams_dir.glob("*.json"):
                try:
                    stream_config = self._load_local_json(str(stream_file))
                    streams.append(stream_config)
                except Exception as e:
                    logger.warning(f"Failed to load stream {stream_file}: {e}")

        logger.info(f"Loaded {len(streams)} streams from {streams_dir}")
        return streams

    # NOTE: UUID-based connection loading removed.
    # Connections are now loaded via _resolve_connection_path_based() from connectors/

    def _load_local_secret(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """
        Load secret for a connection from local secrets directory.

        Secrets are stored in: {paths.secrets}/{connection_id}.json
        """
        secrets_path = self._paths["secrets"] / f"{connection_id}.json"
        if not secrets_path.exists():
            # Try without .json extension for backwards compatibility
            secrets_path = self._paths["secrets"] / connection_id
            if not secrets_path.exists():
                logger.debug(f"No local secret found at: {secrets_path}")
                return None

        try:
            with open(secrets_path, "r") as f:
                secrets = json.load(f)
            logger.debug(f"Loaded local secret from: {secrets_path}")
            return secrets
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in secret file {secrets_path}: {e}")

    # =========================================================================
    # Connection Loading Methods (ID-Based)
    # =========================================================================

    def _load_connection_config(self, connection_id: str) -> Dict[str, Any]:
        """
        Load connection configuration by ID from connections directory.

        Args:
            connection_id: Connection identifier (filename without .json extension)

        Returns:
            Connection configuration dictionary

        Raises:
            FileNotFoundError: If connection file doesn't exist
        """
        connection_path = self._paths["connections"] / f"{connection_id}.json"
        if not connection_path.exists():
            raise FileNotFoundError(
                f"Connection file not found: {connection_path}. "
                f"Connection ID: {connection_id}"
            )
        return self._load_local_json(str(connection_path))

    def _determine_connection_type(self, config: Dict[str, Any], connection_id: str) -> str:
        """
        Determine connection type from config fields.

        Rules:
        - If 'connector_type' or 'type' equals 'api' -> API connection
        - If 'connector_type' or 'type' equals 'database' -> Database connection
        - If 'provider' or 'driver' field exists -> Database connection
        - Otherwise raise ValueError

        Args:
            config: Connection configuration dictionary
            connection_id: Connection identifier (for error messages)

        Returns:
            Connection type: "api" or "database"

        Raises:
            ValueError: If connection type cannot be determined
        """
        # Check connector_type first (new standard), then type (legacy)
        conn_type = config.get("connector_type") or config.get("type")
        if conn_type == "api":
            return "api"
        if conn_type == "database":
            return "database"
        # Legacy: database connections identified by provider or driver field
        if "provider" in config or "driver" in config:
            return "database"
        raise ValueError(
            f"Cannot determine connection type for '{connection_id}'. "
            f"Connections must have 'connector_type' (api or database) or "
            f"'type' field, or database connections must have 'driver' field."
        )

    def _normalize_database_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize database connection fields to match expected schema.

        Transformations:
        - provider -> driver
        - dbname -> database
        - port (string) -> port (int)

        Note: username is kept as-is (not renamed to user) since
        DatabaseConfig now uses 'username' as the standard field name.

        Args:
            config: Raw database connection configuration

        Returns:
            Normalized configuration dictionary
        """
        result = config.copy()

        # Rename legacy field names
        if "provider" in result and "driver" not in result:
            result["driver"] = result.pop("provider")
        if "dbname" in result and "database" not in result:
            result["database"] = result.pop("dbname")

        # Convert port to integer (required, no default)
        if "port" in result and isinstance(result["port"], str):
            try:
                result["port"] = int(result["port"])
            except ValueError:
                raise ValueError(f"Invalid port value: {result['port']}")

        return result

    def _resolve_connection_by_id(
        self,
        connection_ref: str,
        connection_id: str,
        client_id: str = ""
    ) -> ResolvedConnection:
        """
        Resolve a connection by its ID.

        Loads connection config from connections/{connection_id}.json,
        expands secrets from .secrets/{connection_id}.json.

        Args:
            connection_ref: Reference alias from pipeline (e.g., "conn_1")
            connection_id: Connection identifier (filename without .json)
            client_id: Client ID (for logging and late-binding resolver)

        Returns:
            ResolvedConnection object with expanded configuration
        """
        cache_key = f"id:{connection_id}"
        if cache_key in self._resolved_connections:
            return self._resolved_connections[cache_key]

        # Load connection config from connections directory
        config = self._load_connection_config(connection_id)

        # Determine connection type
        connection_type = self._determine_connection_type(config, connection_id)

        # Normalize database connections
        if connection_type == "database":
            config = self._normalize_database_connection(config)

        # Store secrets directory for the resolver
        self._secrets_dir = self._paths["secrets"]

        # Late-binding: return ConnectionConfig wrapper without expanding secrets
        if self.settings.late_binding_secrets:
            config_wrapper = ConnectionConfig(
                raw_config=config,
                connection_id=connection_id,
                resolver=self.secrets_resolver,
                client_id=client_id or None,
            )

            resolved = ResolvedConnection(
                connection_ref,
                connection_type,
                config,  # Store raw config (unexpanded)
                connection_config_wrapper=config_wrapper,
            )
            self._resolved_connections[cache_key] = resolved

            logger.info(
                f"Resolved connection (late-binding): {connection_ref} -> {connection_id}"
            )
            return resolved

        # Eager resolution: load secrets and expand placeholders
        secrets = self._load_local_secret(connection_id)
        if secrets:
            config = self._expand_secrets_in_config(config, secrets)

        # Expand any remaining environment variables
        config = expand_required_vars(config)

        resolved = ResolvedConnection(connection_ref, connection_type, config)
        self._resolved_connections[cache_key] = resolved

        logger.info(f"Resolved connection: {connection_ref} -> {connection_id}")
        return resolved

    # =========================================================================
    # Endpoint Loading Methods
    # =========================================================================

    def _load_endpoint_config(self, endpoint_id: str) -> Dict[str, Any]:
        """
        Load endpoint configuration by ID from endpoints directory.

        Args:
            endpoint_id: Endpoint identifier (filename without .json extension)

        Returns:
            Endpoint configuration dictionary

        Raises:
            FileNotFoundError: If endpoint file doesn't exist
        """
        endpoint_path = self._paths["endpoints"] / f"{endpoint_id}.json"
        if not endpoint_path.exists():
            raise FileNotFoundError(
                f"Endpoint file not found: {endpoint_path}. "
                f"Endpoint ID: {endpoint_id}"
            )
        return self._load_local_json(str(endpoint_path))

    # =========================================================================
    # Connector Endpoint Loading Methods (Path-based)
    # =========================================================================

    def _get_connectors_base_path(self, raw_pipeline: Dict[str, Any]) -> Path:
        """
        Get the base path for connectors directory from analitiq.yaml.

        Connectors are always loaded from local directory.
        Path is defined in analitiq.yaml (single source of truth).

        Returns:
            Path to connectors directory

        Raises:
            FileNotFoundError: If connectors directory doesn't exist
        """
        # Use path from analitiq.yaml (already loaded in __init__)
        connectors_path = self._paths["connectors"]

        # Validate the path exists
        if not connectors_path.exists():
            raise FileNotFoundError(
                f"Connectors directory not found: {connectors_path}. "
                f"Check analitiq.yaml configuration or run config_fetcher.py first."
            )

        logger.info(f"Using connectors directory: {connectors_path}")
        return connectors_path

    def _load_endpoint_by_path(self, endpoint_path: str, connectors_base: Path) -> Dict[str, Any]:
        """
        Load endpoint from path relative to connectors directory.

        Args:
            endpoint_path: Path to endpoint file (e.g., "wise/endpoints/transfers.json")
            connectors_base: Base path to connectors directory
        """
        full_path = connectors_base / endpoint_path
        if not full_path.exists():
            raise FileNotFoundError(
                f"Endpoint not found: {full_path}. "
                f"Path: {endpoint_path}"
            )
        return self._load_local_json(str(full_path))

    # =========================================================================
    # Endpoint Resolution
    # =========================================================================
    # NOTE: Connections are resolved via _resolve_connection_by_id() from connections/ directory.
    # Endpoints are resolved via _resolve_endpoint_flexible() from connectors/ directory.

    def _resolve_endpoint_flexible(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve endpoint from config using either ID or path format.

        Supports two formats:
        - endpoint_id: Load from endpoints/{endpoint_id}.json
        - endpoint: Load from connectors/{path}

        Args:
            config: Dict containing either:
                - endpoint_id: endpoint identifier string
                - endpoint: path string like "wise/endpoints/transfers.json"

        Returns:
            Resolved endpoint configuration dict

        Raises:
            ValueError: If neither endpoint_id nor endpoint is specified
        """
        endpoint_id = config.get("endpoint_id")
        endpoint_path = config.get("endpoint")

        if not endpoint_id and not endpoint_path:
            raise ValueError(
                "Stream config must specify 'endpoint_id' or 'endpoint'"
            )

        # Prefer endpoint_id if both are specified
        if endpoint_id:
            cache_key = f"id:{endpoint_id}"
            if cache_key in self._resolved_endpoints:
                return self._resolved_endpoints[cache_key]

            endpoint = self._load_endpoint_config(endpoint_id)
            self._resolved_endpoints[cache_key] = endpoint
            logger.info(f"Resolved endpoint by ID: {endpoint_id}")
            return endpoint

        # Fall back to path-based resolution
        cache_key = f"path:{endpoint_path}"
        if cache_key in self._resolved_endpoints:
            return self._resolved_endpoints[cache_key]

        if not hasattr(self, '_connectors_base') or not self._connectors_base:
            raise ValueError(
                f"Connectors directory not configured. Check analitiq.yaml or connectors_path setting."
            )

        endpoint = self._load_endpoint_by_path(endpoint_path, self._connectors_base)
        self._resolved_endpoints[cache_key] = endpoint
        logger.info(f"Resolved endpoint by path: {endpoint_path}")
        return endpoint

    # =========================================================================
    # Configuration Loading and Assembly
    # =========================================================================

    def load_pipeline_config(self) -> Tuple[PipelineConfig, List[StreamConfig]]:
        """
        Load and assemble complete pipeline and stream configurations.

        Configuration is always loaded from local filesystem. In cloud environments,
        config_fetcher.py pre-populates the local_config_mount directory.

        Connections use ID-based format where the value is a connection_id string
        that maps to a file in the connections directory.

        Returns:
            Tuple of (PipelineConfig, list of StreamConfig)
        """
        # Load raw pipeline config from local filesystem
        raw_pipeline = self._load_local_pipeline()

        if not validate_pipeline_config(raw_pipeline):
            raise ValueError("Pipeline configuration validation failed")

        client_id = raw_pipeline.get("client_id", "")

        # Get connectors base path - always required for loading connectors/endpoints
        connectors_base = self._get_connectors_base_path(raw_pipeline)

        # Store connectors_base for endpoint resolution
        self._connectors_base = connectors_base

        # Get connections from pipeline
        raw_connections = raw_pipeline.get("connections", {})
        source_connections = raw_connections.get("source", {})
        dest_connections = raw_connections.get("destinations", [])

        # Validate all connections use ID-based format (string)
        for alias, value in source_connections.items():
            if not isinstance(value, str):
                raise ValueError(
                    f"Connection '{alias}' must be a string (connection_id) "
                    f"that maps to a file in the connections directory."
                )

        for dest_dict in dest_connections:
            for alias, value in dest_dict.items():
                if not isinstance(value, str):
                    raise ValueError(
                        f"Connection '{alias}' must be a string (connection_id)."
                    )

        # Resolve all connections (load from connections/ dir, secrets from .secrets/)
        for alias, connection_id in source_connections.items():
            self._resolve_connection_by_id(alias, connection_id, client_id)

        for dest_dict in dest_connections:
            for alias, connection_id in dest_dict.items():
                self._resolve_connection_by_id(alias, connection_id, client_id)

        # Build connections_config with plain connection IDs (no prefix)
        connections_config = PipelineConnectionsConfig(
            source=dict(source_connections),
            destinations=[dict(dest_dict) for dest_dict in dest_connections]
        )

        # Load stream configs from local filesystem
        raw_streams = self._load_local_streams()

        # Handle version - convert string to int if needed
        version = raw_pipeline.get("version", 1)
        if isinstance(version, str):
            version = int(float(version))

        # Build PipelineConfig
        pipeline_config = PipelineConfig(
            version=version,
            client_id=client_id,
            pipeline_id=raw_pipeline["pipeline_id"],
            name=raw_pipeline.get("name", ""),
            description=raw_pipeline.get("description"),
            status=raw_pipeline.get("status", "draft"),
            is_active=raw_pipeline.get("is_active", False),
            tags=raw_pipeline.get("tags", []),
            connections=connections_config,
            runtime=RuntimeConfig(**raw_pipeline.get("runtime", {})) if raw_pipeline.get("runtime") else RuntimeConfig(),
            function_catalog=raw_pipeline.get("function_catalog"),
            created_at=raw_pipeline.get("created_at"),
            updated_at=raw_pipeline.get("updated_at"),
        )

        # Build StreamConfigs with resolved connections and endpoints
        stream_configs = []
        for raw_stream in raw_streams:
            if not validate_stream_config(raw_stream):
                logger.warning(f"Skipping invalid stream: {raw_stream.get('stream_id', 'unknown')}")
                continue

            stream_config = self._build_stream_config(
                raw_stream,
                connections_config,
                client_id
            )
            stream_configs.append(stream_config)

        logger.info(f"Loaded pipeline '{pipeline_config.name}' with {len(stream_configs)} streams")
        return pipeline_config, stream_configs

    def _build_stream_config(
        self,
        raw_stream: Dict[str, Any],
        connections_config: PipelineConnectionsConfig,
        client_id: str
    ) -> StreamConfig:
        """
        Build a StreamConfig from raw stream data with resolved connections and endpoints.

        Connections must already be resolved via _resolve_connection_path_based() in load_pipeline_config().
        Endpoints are resolved via _resolve_endpoint_flexible() from local connectors directory.
        """
        stream_id = raw_stream["stream_id"]

        # Resolve source connection
        source_ref = raw_stream["source"]["connection_ref"]
        try:
            source_connection_id = connections_config.get_connection_uuid(source_ref)
        except KeyError:
            raise ValueError(f"Unknown connection_ref '{source_ref}' in stream {stream_id}")

        # Get connection from cache - all connections must be pre-resolved
        # Internal cache uses "id:{uuid}" keys
        cache_key = f"id:{source_connection_id}"
        source_connection = self._resolved_connections.get(cache_key)
        if not source_connection:
            raise ValueError(
                f"Connection '{source_ref}' (id={source_connection_id}) not resolved. "
                f"Ensure pipeline uses path-based connection format with 'connector' key."
            )

        # Resolve source endpoint from local connectors directory
        source_endpoint = self._resolve_endpoint_flexible(raw_stream["source"])

        # Build enriched source config
        source_data = raw_stream["source"].copy()
        source_data["_connection"] = source_connection.config
        source_data["_endpoint"] = source_endpoint

        # Resolve destinations
        destinations_data = []
        for dest in raw_stream["destinations"]:
            dest_ref = dest["connection_ref"]
            try:
                dest_connection_id = connections_config.get_connection_uuid(dest_ref)
            except KeyError:
                raise ValueError(f"Unknown connection_ref '{dest_ref}' in stream {stream_id}")

            # Get connection from cache - all connections must be pre-resolved
            # Internal cache uses "id:{uuid}" keys
            dest_cache_key = f"id:{dest_connection_id}"
            dest_connection = self._resolved_connections.get(dest_cache_key)
            if not dest_connection:
                raise ValueError(
                    f"Connection '{dest_ref}' (id={dest_connection_id}) not resolved. "
                    f"Ensure pipeline uses path-based connection format with 'connector' key."
                )

            # Resolve destination endpoint from local connectors directory
            dest_endpoint = self._resolve_endpoint_flexible(dest)

            dest_data = dest.copy()
            dest_data["_connection"] = dest_connection.config
            dest_data["_endpoint"] = dest_endpoint
            destinations_data.append(dest_data)

        # Handle version - convert string to int if needed
        version = raw_stream.get("version", 1)
        if isinstance(version, str):
            version = int(float(version))

        # Build StreamConfig
        return StreamConfig(
            version=version,
            stream_id=stream_id,
            pipeline_id=raw_stream.get("pipeline_id", self.settings.pipeline_id),
            client_id=client_id,
            status=raw_stream.get("status", "draft"),
            is_enabled=raw_stream.get("is_enabled", True),
            source=SourceConfig(**self._normalize_source_config(source_data)),
            destinations=[
                DestinationConfig(**self._normalize_destination_config(d))
                for d in destinations_data
            ],
            mapping=MappingConfig(**raw_stream.get("mapping", {})) if raw_stream.get("mapping") else MappingConfig(),
            tags=raw_stream.get("tags"),
            runtime=raw_stream.get("runtime"),
            created_at=raw_stream.get("created_at"),
            updated_at=raw_stream.get("updated_at"),
        )

    def _normalize_source_config(self, source_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize source configuration to match SourceConfig model.

        Uses endpoint path (e.g., 'wise/endpoints/transfers.json') which is stored
        in endpoint_id field for model compatibility.
        """
        replication = source_data.get("replication", {})

        # Handle either endpoint_id or endpoint (path)
        endpoint_ref = source_data.get("endpoint_id") or source_data.get("endpoint", "")

        result = {
            "connection_ref": source_data["connection_ref"],
            "endpoint_id": endpoint_ref,
            "primary_key": source_data.get("primary_key", []),
            "replication": {
                "method": replication.get("method", "incremental"),
                "cursor_field": replication.get("cursor_field", []),
            },
        }

        # Add optional fields only if present
        if source_data.get("source_schema_fingerprint"):
            result["source_schema_fingerprint"] = source_data["source_schema_fingerprint"]
        if replication.get("safety_window_seconds") is not None:
            result["replication"]["safety_window_seconds"] = replication["safety_window_seconds"]
        if replication.get("tie_breaker_fields"):
            result["replication"]["tie_breaker_fields"] = replication["tie_breaker_fields"]
        if source_data.get("parameters"):
            result["parameters"] = source_data["parameters"]

        return result

    def _normalize_destination_config(self, dest_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize destination configuration to match DestinationConfig model.

        Uses endpoint path (e.g., 'postgres/endpoints/transfers.json') which is stored
        in endpoint_id field for model compatibility.
        """
        write = dest_data.get("write", {})

        # Handle either endpoint_id or endpoint (path)
        endpoint_ref = dest_data.get("endpoint_id") or dest_data.get("endpoint", "")

        result = {
            "connection_ref": dest_data["connection_ref"],
            "endpoint_id": endpoint_ref,
            "write": {
                "mode": write.get("mode", "upsert"),
            },
        }

        # Add optional fields only if present
        if dest_data.get("target_schema_fingerprint"):
            result["target_schema_fingerprint"] = dest_data["target_schema_fingerprint"]
        if write.get("conflict_keys"):
            result["write"]["conflict_keys"] = write["conflict_keys"]
        if write.get("idempotency_key"):
            result["write"]["idempotency_key"] = write["idempotency_key"]
        if dest_data.get("batching"):
            batching = dest_data["batching"]
            result["batching"] = {
                "supported": batching.get("supported", False),
                "size": batching.get("size", 1),
            }

        return result

    def create_config(self) -> Tuple[
        PipelineConfig,
        List[StreamConfig],
        Dict[str, "ResolvedConnection"],
        Dict[str, Dict[str, Any]]
    ]:
        """
        Load and return validated pipeline and stream configurations.

        Returns:
            Tuple of (PipelineConfig, list of StreamConfig, resolved_connections dict, resolved_endpoints dict)
            - resolved_connections maps connection_id to ResolvedConnection objects
              (use await resolved.resolve_config() to get fully-expanded config)
            - resolved_endpoints maps endpoint_id to its resolved config dict
        """
        pipeline_config, stream_configs = self.load_pipeline_config()

        # Validate no unexpanded placeholders in connections (only for eager resolution)
        for conn_id, resolved in self._resolved_connections.items():
            if not resolved.is_late_binding:
                validate_no_unexpanded_placeholders(
                    resolved.config,
                    f"connection '{conn_id}'"
                )

        # Strip internal key prefixes for external consumers
        # Internal keys use "id:{uuid}" format, but consumers expect plain UUIDs
        resolved_connections = {}
        for key, conn in self._resolved_connections.items():
            clean_key = key.split(":", 1)[1] if ":" in key else key
            resolved_connections[clean_key] = conn

        # Strip "path:" prefix from endpoint keys for external consumers
        resolved_endpoints = {}
        for key, endpoint in self._resolved_endpoints.items():
            clean_key = key.split(":", 1)[1] if ":" in key else key
            resolved_endpoints[clean_key] = endpoint

        if self.settings.late_binding_secrets:
            logger.info("Configuration loaded with late-binding secrets enabled")
        else:
            logger.info("Configuration validation passed - no unexpanded placeholders found")

        return pipeline_config, stream_configs, resolved_connections, resolved_endpoints

    def validate_environment(self) -> None:
        """Validate that the environment is properly configured.

        Configuration is always loaded from local filesystem. In cloud environments,
        config_fetcher.py populates directories defined in analitiq.yaml before this runs.
        """
        if not self.settings.pipeline_id:
            raise RuntimeError("PIPELINE_ID environment variable is required")

        # Validate pipelines directory exists (most critical for loading pipeline config)
        pipelines_path = self._paths["pipelines"]
        if not pipelines_path.exists():
            raise RuntimeError(
                f"Pipelines directory not found: {pipelines_path}. "
                f"Check analitiq.yaml configuration or run config_fetcher.py first."
            )
        logger.debug(f"Pipelines directory validated: {pipelines_path}")

    def get_resolved_connection(self, connection_ref: str, connections_map: Dict[str, str]) -> ResolvedConnection:
        """Get a resolved connection by its reference alias."""
        connection_id = connections_map.get(connection_ref)
        if not connection_id:
            raise ValueError(f"Unknown connection reference: {connection_ref}")
        if connection_id not in self._resolved_connections:
            raise ValueError(f"Connection not resolved: {connection_id}")
        return self._resolved_connections[connection_id]
