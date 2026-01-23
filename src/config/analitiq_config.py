"""
Analitiq configuration loader.

Loads configuration from analitiq.yaml file.
"""

import logging
from pathlib import Path
from typing import Any, List, Optional

import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ConsolidatedConfigValidator(BaseModel):
    """
    Pydantic validator for consolidated pipeline configuration.

    Validates that the consolidated config contains the minimum required
    number of connections, connectors, endpoints, and streams.
    """

    pipeline: dict[str, Any] = Field(..., description="Pipeline configuration")
    connections: List[dict[str, Any]] = Field(
        ..., min_length=2, description="At least 2 connections required"
    )
    connectors: List[dict[str, Any]] = Field(
        ..., min_length=2, description="At least 2 connectors required"
    )
    endpoints: List[dict[str, Any]] = Field(
        ..., min_length=2, description="At least 2 endpoints required"
    )
    streams: List[dict[str, Any]] = Field(
        ..., min_length=1, description="At least 1 stream required"
    )


def validate_consolidated_config(config: dict[str, Any]) -> ConsolidatedConfigValidator:
    """
    Validate a consolidated pipeline configuration.

    Args:
        config: The consolidated config dictionary to validate.

    Returns:
        Validated ConsolidatedConfigValidator instance.

    Raises:
        pydantic.ValidationError: If validation fails.
    """
    return ConsolidatedConfigValidator.model_validate(config)

# Default config file name
DEFAULT_CONFIG_FILE = "analitiq.yaml"


def load_analitiq_config(config_path: Optional[str] = None) -> dict[str, Any]:
    """
    Load the analitiq.yaml configuration file.

    Args:
        config_path: Path to config file. If None, searches for analitiq.yaml
                    in current directory and parent directories.

    Returns:
        Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If config file not found.
        yaml.YAMLError: If config file is invalid YAML.
    """
    if config_path:
        path = Path(config_path)
    else:
        # Search for config file in current and parent directories
        path = _find_config_file()

    if not path or not path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path or DEFAULT_CONFIG_FILE}"
        )

    with open(path) as f:
        config = yaml.safe_load(f)

    logger.info(f"Loaded configuration from: {path}")
    return config


def _find_config_file() -> Optional[Path]:
    """Search for analitiq.yaml in current and parent directories."""
    current = Path.cwd()

    for _ in range(10):  # Limit search depth
        config_path = current / DEFAULT_CONFIG_FILE
        if config_path.exists():
            return config_path
        if current.parent == current:
            break
        current = current.parent

    return None
