"""
Path-based configuration loader.

Loads pipeline, stream, and connector configurations from local filesystem paths.
This is the core configuration loading mechanism that works for both local
development and cloud deployments (after config fetcher populates local files).
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.config.exceptions import (
    ConfigNotFoundError,
    ConfigValidationError,
    ConnectorNotFoundError,
    EndpointNotFoundError,
)

logger = logging.getLogger(__name__)


class PathBasedConfigLoader:
    """
    Loads configurations from user-specified paths.

    This loader reads JSON configuration files from a directory structure:
    - pipelines/{pipeline_id}.json
    - streams/{stream_id}.json
    - connectors/{connector_name}/connector.json
    - connectors/{connector_name}/endpoints/{endpoint_name}.json

    The loader is environment-agnostic - it only knows about local file paths.
    """

    def __init__(self, base_path: str | Path):
        """
        Initialize path-based config loader.

        Args:
            base_path: Base directory for resolving relative paths
        """
        self.base_path = Path(base_path)
        self._connectors_path: Optional[Path] = None
        self._streams_path: Optional[Path] = None

    @property
    def connectors_path(self) -> Path:
        """Get the connectors directory path."""
        if self._connectors_path is None:
            return self.base_path / "connectors"
        return self._connectors_path

    @connectors_path.setter
    def connectors_path(self, path: str | Path) -> None:
        """Set the connectors directory path."""
        p = Path(path)
        if not p.is_absolute():
            p = (self.base_path / p).resolve()
        self._connectors_path = p

    @property
    def streams_path(self) -> Path:
        """Get the streams directory path."""
        if self._streams_path is None:
            return self.base_path / "streams"
        return self._streams_path

    @streams_path.setter
    def streams_path(self, path: str | Path) -> None:
        """Set the streams directory path."""
        p = Path(path)
        if not p.is_absolute():
            p = (self.base_path / p).resolve()
        self._streams_path = p

    def _load_json(self, path: Path) -> Dict[str, Any]:
        """
        Load JSON file.

        Args:
            path: Path to JSON file

        Returns:
            Parsed JSON as dictionary

        Raises:
            ConfigNotFoundError: If file doesn't exist
            ConfigValidationError: If file contains invalid JSON
        """
        if not path.exists():
            raise ConfigNotFoundError(str(path))

        try:
            with open(path) as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigValidationError(f"Invalid JSON in {path}: {e}")
        except PermissionError as e:
            raise ConfigNotFoundError(f"Permission denied: {path}")
        except OSError as e:
            raise ConfigNotFoundError(f"Error reading {path}: {e}")

    def load_pipeline(self, pipeline_path: str | Path) -> Tuple[Dict[str, Any], Path]:
        """
        Load pipeline configuration and extract path settings.

        Args:
            pipeline_path: Path to pipeline JSON file

        Returns:
            Tuple of (pipeline config dict, resolved pipeline file path)

        Raises:
            ConfigNotFoundError: If pipeline file doesn't exist
            ConfigValidationError: If file contains invalid JSON
        """
        path = Path(pipeline_path)
        if not path.is_absolute():
            path = (self.base_path / path).resolve()

        pipeline = self._load_json(path)

        # Extract connectors_path from pipeline config
        if "connectors_path" in pipeline:
            connectors_path = pipeline["connectors_path"]
            if not Path(connectors_path).is_absolute():
                # Resolve relative to pipeline file's directory
                connectors_path = (path.parent / connectors_path).resolve()
            self._connectors_path = Path(connectors_path)
            logger.debug(f"Using connectors path from pipeline: {self._connectors_path}")

        # Extract streams_path from pipeline config
        if "streams_path" in pipeline:
            streams_path = pipeline["streams_path"]
            if not Path(streams_path).is_absolute():
                streams_path = (path.parent / streams_path).resolve()
            self._streams_path = Path(streams_path)
            logger.debug(f"Using streams path from pipeline: {self._streams_path}")
        else:
            # Default to pipeline directory / streams
            self._streams_path = path.parent / "streams"

        logger.info(f"Loaded pipeline from {path}")
        return pipeline, path

    def load_connector(self, connector_name: str) -> Dict[str, Any]:
        """
        Load connector template from connectors/{name}/connector.json.

        Args:
            connector_name: Name of the connector (e.g., "wise", "postgres")

        Returns:
            Connector configuration dictionary

        Raises:
            ConnectorNotFoundError: If connector doesn't exist
        """
        connector_path = self.connectors_path / connector_name / "connector.json"
        if not connector_path.exists():
            raise ConnectorNotFoundError(
                connector_name,
                f"Expected at: {connector_path}"
            )

        connector = self._load_json(connector_path)
        logger.debug(f"Loaded connector template: {connector_name}")
        return connector

    def load_endpoint(self, endpoint_ref: str) -> Dict[str, Any]:
        """
        Load endpoint from path relative to connectors directory.

        Args:
            endpoint_ref: Path to endpoint file (e.g., "wise/endpoints/transfers.json")

        Returns:
            Endpoint configuration dictionary

        Raises:
            EndpointNotFoundError: If endpoint doesn't exist
        """
        endpoint_path = self.connectors_path / endpoint_ref
        if not endpoint_path.exists():
            raise EndpointNotFoundError(
                endpoint_ref,
                f"Expected at: {endpoint_path}"
            )

        endpoint = self._load_json(endpoint_path)
        logger.debug(f"Loaded endpoint: {endpoint_ref}")
        return endpoint

    def load_credentials(self, credentials_path: str, base: Optional[Path] = None) -> Dict[str, Any]:
        """
        Load credentials from specified path.

        Args:
            credentials_path: Path to credentials file (can be relative)
            base: Base path for resolving relative paths (defaults to self.base_path)

        Returns:
            Credentials dictionary

        Raises:
            ConfigNotFoundError: If credentials file doesn't exist
        """
        base_path = base or self.base_path

        # Resolve relative paths
        if credentials_path.startswith("./"):
            path = base_path / credentials_path[2:]
        elif credentials_path.startswith("../"):
            path = (base_path / credentials_path).resolve()
        elif not Path(credentials_path).is_absolute():
            path = base_path / credentials_path
        else:
            path = Path(credentials_path)

        if not path.exists():
            raise ConfigNotFoundError(f"Credentials file not found: {path}")

        return self._load_json(path)

    def load_streams(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """
        Load all stream configurations for a pipeline.

        Args:
            pipeline_id: Pipeline ID to filter streams

        Returns:
            List of stream configuration dictionaries
        """
        streams: List[Dict[str, Any]] = []

        if not self.streams_path.exists():
            logger.warning(f"Streams directory does not exist: {self.streams_path}")
            return streams

        for stream_file in self.streams_path.glob("*.json"):
            try:
                stream = self._load_json(stream_file)
                if stream.get("pipeline_id") == pipeline_id:
                    streams.append(stream)
            except (ConfigNotFoundError, ConfigValidationError) as e:
                logger.warning(f"Failed to load stream {stream_file}: {e}")

        logger.info(f"Loaded {len(streams)} streams for pipeline {pipeline_id}")
        return streams

    def list_available_connectors(self) -> List[str]:
        """
        List all available connectors.

        Returns:
            List of connector names
        """
        if not self.connectors_path.exists():
            return []

        connectors = []
        for item in self.connectors_path.iterdir():
            if item.is_dir() and (item / "connector.json").exists():
                connectors.append(item.name)

        return sorted(connectors)

    def __repr__(self) -> str:
        """String representation."""
        return f"PathBasedConfigLoader({self.base_path})"
