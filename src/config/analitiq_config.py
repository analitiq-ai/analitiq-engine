"""
Analitiq configuration loader.

Loads configuration from analitiq.yaml file.
"""

import logging
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)


def validate_consolidated_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate a consolidated pipeline configuration has minimum required elements."""
    required = {"pipeline", "connections", "connectors", "endpoints", "streams"}
    missing = required - config.keys()
    if missing:
        raise ValueError(f"Missing required config keys: {missing}")

    minimums = {"connections": 2, "connectors": 2, "endpoints": 2, "streams": 1}
    for key, min_count in minimums.items():
        actual = len(config[key])
        if actual < min_count:
            raise ValueError(f"'{key}' requires at least {min_count} items, got {actual}")

    return config


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
