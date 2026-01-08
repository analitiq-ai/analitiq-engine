"""
Connector synchronization module.

Downloads and updates connectors from a GitHub repository based on configuration.
"""

import json
import logging
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import yaml

logger = logging.getLogger(__name__)

# Default config file name
DEFAULT_CONFIG_FILE = "analitiq.yaml"


@dataclass
class ConnectorSpec:
    """Specification for a single connector to sync."""

    name: str
    version: str = "latest"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConnectorSpec":
        """Create ConnectorSpec from dictionary."""
        if isinstance(data, str):
            return cls(name=data)
        return cls(
            name=data["name"],
            version=data.get("version", "latest"),
        )


@dataclass
class ConnectorConfig:
    """Configuration for connector repository and sync settings."""

    repository: str
    local_path: str
    sync: list[ConnectorSpec] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConnectorConfig":
        """Create ConnectorConfig from dictionary."""
        sync_list = [
            ConnectorSpec.from_dict(item) for item in data.get("sync", [])
        ]
        return cls(
            repository=data.get("repository", ""),
            local_path=data.get("local_path", "./connectors"),
            sync=sync_list,
        )


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


class ConnectorSync:
    """
    Synchronizes connectors from a GitHub repository.

    Downloads and updates connectors based on the configuration in analitiq.yaml.
    """

    def __init__(
        self,
        config: Optional[ConnectorConfig] = None,
        config_path: Optional[str] = None,
        base_path: Optional[Path] = None,
    ):
        """
        Initialize ConnectorSync.

        Args:
            config: Pre-loaded ConnectorConfig. If None, loads from config_path.
            config_path: Path to analitiq.yaml. If None, searches automatically.
            base_path: Base path for resolving relative paths. Defaults to cwd.
        """
        self.base_path = base_path or Path.cwd()

        if config:
            self.config = config
            local_path_str = config.local_path
        else:
            raw_config = load_analitiq_config(config_path)
            connectors_config = raw_config.get("connectors", {})
            # Read local_path from paths.connectors
            local_path_str = raw_config.get("paths", {}).get("connectors", "./connectors")
            connectors_config["local_path"] = local_path_str
            self.config = ConnectorConfig.from_dict(connectors_config)

        # Resolve local path
        local_path = Path(local_path_str)
        if not local_path.is_absolute():
            self.local_path = (self.base_path / local_path).resolve()
        else:
            self.local_path = local_path

        # Parse repository URL
        self.repo_url = self.config.repository
        self.repo_owner, self.repo_name = self._parse_github_url(self.repo_url)

    def _parse_github_url(self, url: str) -> tuple[str, str]:
        """
        Parse GitHub URL to extract owner and repo name.

        Args:
            url: GitHub repository URL.

        Returns:
            Tuple of (owner, repo_name).
        """
        parsed = urlparse(url)
        path_parts = parsed.path.strip("/").split("/")

        if len(path_parts) >= 2:
            owner = path_parts[0]
            repo = path_parts[1].replace(".git", "")
            return owner, repo

        raise ValueError(f"Invalid GitHub URL: {url}")

    def sync_all(self) -> dict[str, str]:
        """
        Synchronize all connectors specified in the configuration.

        Returns:
            Dictionary mapping connector names to their sync status.
        """
        results = {}

        if not self.config.sync:
            logger.info("No connectors specified for sync")
            return results

        logger.info(f"Syncing {len(self.config.sync)} connectors from {self.repo_url}")

        for spec in self.config.sync:
            try:
                status = self.sync_connector(spec)
                results[spec.name] = status
                logger.info(f"Connector '{spec.name}': {status}")
            except Exception as e:
                results[spec.name] = f"error: {e}"
                logger.error(f"Failed to sync connector '{spec.name}': {e}")

        return results

    def sync_connector(self, spec: ConnectorSpec) -> str:
        """
        Synchronize a single connector.

        Args:
            spec: Connector specification with name and version.

        Returns:
            Status string: 'up-to-date', 'updated', 'downloaded', or error message.
        """
        connector_path = self.local_path / spec.name
        requested_version = spec.version

        # Check if connector exists locally
        if connector_path.exists():
            local_version = self._get_local_version(connector_path)

            if requested_version == "latest":
                # For 'latest', check against remote latest version
                remote_version = self._get_remote_latest_version(spec.name)
                if local_version == remote_version:
                    return f"up-to-date (v{local_version})"
                else:
                    self._download_connector(spec.name, remote_version)
                    return f"updated: v{local_version} -> v{remote_version}"
            else:
                # Specific version requested
                if local_version == requested_version:
                    return f"up-to-date (v{local_version})"
                else:
                    self._download_connector(spec.name, requested_version)
                    return f"updated: v{local_version} -> v{requested_version}"
        else:
            # Connector doesn't exist locally - download it
            if requested_version == "latest":
                version = self._get_remote_latest_version(spec.name)
            else:
                version = requested_version

            self._download_connector(spec.name, version)
            return f"downloaded (v{version})"

    def _get_local_version(self, connector_path: Path) -> Optional[str]:
        """
        Get version from local connector.json.

        Args:
            connector_path: Path to connector directory.

        Returns:
            Version string or None if not found.
        """
        connector_json = connector_path / "connector.json"
        if not connector_json.exists():
            return None

        try:
            with open(connector_json) as f:
                data = json.load(f)
            return data.get("version")
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to read connector.json: {e}")
            return None

    def _get_remote_latest_version(self, connector_name: str) -> str:
        """
        Get the latest version of a connector from the remote repository.

        This checks the connector.json in the main/master branch.

        Args:
            connector_name: Name of the connector.

        Returns:
            Version string.
        """
        # Use GitHub raw content URL to fetch connector.json
        for branch in ["main", "master"]:
            raw_url = (
                f"https://raw.githubusercontent.com/{self.repo_owner}/{self.repo_name}"
                f"/{branch}/connectors/{connector_name}/connector.json"
            )

            try:
                import urllib.request

                with urllib.request.urlopen(raw_url, timeout=30) as response:
                    data = json.loads(response.read().decode())
                    return data.get("version", "unknown")
            except Exception:
                continue

        raise RuntimeError(
            f"Could not fetch latest version for connector '{connector_name}'"
        )

    def _download_connector(self, connector_name: str, version: str) -> None:
        """
        Download a connector from the GitHub repository.

        Uses git sparse checkout to download only the specified connector directory.

        Args:
            connector_name: Name of the connector to download.
            version: Version tag or 'main'/'master' for latest.
        """
        connector_dest = self.local_path / connector_name

        # Create parent directory if needed
        self.local_path.mkdir(parents=True, exist_ok=True)

        # Remove existing connector if present
        if connector_dest.exists():
            shutil.rmtree(connector_dest)

        # Determine git ref to checkout
        git_ref = self._version_to_git_ref(connector_name, version)

        # Use sparse checkout to download only the connector directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            try:
                # Initialize git repo with sparse checkout
                self._run_git(["init"], cwd=temp_path)
                self._run_git(
                    ["remote", "add", "origin", self.repo_url], cwd=temp_path
                )
                self._run_git(
                    ["config", "core.sparseCheckout", "true"], cwd=temp_path
                )

                # Configure sparse checkout to only get the connector directory
                sparse_file = temp_path / ".git" / "info" / "sparse-checkout"
                sparse_file.parent.mkdir(parents=True, exist_ok=True)
                sparse_file.write_text(f"connectors/{connector_name}/\n")

                # Fetch and checkout
                self._run_git(["fetch", "--depth=1", "origin", git_ref], cwd=temp_path)
                self._run_git(["checkout", git_ref], cwd=temp_path)

                # Copy connector to destination
                source_connector = temp_path / "connectors" / connector_name
                if source_connector.exists():
                    shutil.copytree(source_connector, connector_dest)
                    logger.info(
                        f"Downloaded connector '{connector_name}' (v{version}) "
                        f"to {connector_dest}"
                    )
                else:
                    raise FileNotFoundError(
                        f"Connector '{connector_name}' not found in repository"
                    )

            except subprocess.CalledProcessError as e:
                raise RuntimeError(
                    f"Git operation failed while downloading '{connector_name}': {e}"
                )

    def _version_to_git_ref(self, connector_name: str, version: str) -> str:
        """
        Convert version string to git ref (tag or branch).

        Args:
            connector_name: Name of the connector.
            version: Version string like '1.0.0' or 'latest'.

        Returns:
            Git ref string.
        """
        if version in ("latest", "main", "master"):
            # Try main first, fall back to master
            return "main"

        # Check for connector-specific tag (e.g., wise-v1.0.0)
        connector_tag = f"{connector_name}-v{version}"

        # Check for global version tag (e.g., v1.0.0)
        global_tag = f"v{version}"

        # Try connector-specific tag first
        if self._tag_exists(connector_tag):
            return connector_tag

        # Fall back to global tag
        if self._tag_exists(global_tag):
            return global_tag

        # If no tag found, assume it's a branch or commit
        return version

    def _tag_exists(self, tag: str) -> bool:
        """Check if a tag exists in the remote repository."""
        try:
            result = subprocess.run(
                ["git", "ls-remote", "--tags", self.repo_url, tag],
                capture_output=True,
                text=True,
                timeout=30,
            )
            return bool(result.stdout.strip())
        except Exception:
            return False

    def _run_git(
        self,
        args: list[str],
        cwd: Optional[Path] = None,
    ) -> subprocess.CompletedProcess:
        """
        Run a git command.

        Args:
            args: Git command arguments.
            cwd: Working directory for the command.

        Returns:
            CompletedProcess result.

        Raises:
            subprocess.CalledProcessError: If command fails.
        """
        cmd = ["git"] + args
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode != 0:
            logger.debug(f"Git command failed: {' '.join(cmd)}")
            logger.debug(f"stderr: {result.stderr}")
            raise subprocess.CalledProcessError(
                result.returncode, cmd, result.stdout, result.stderr
            )

        return result

    def check_status(self) -> dict[str, dict[str, Any]]:
        """
        Check the status of all configured connectors without downloading.

        Returns:
            Dictionary with connector status information.
        """
        status = {}

        for spec in self.config.sync:
            connector_path = self.local_path / spec.name
            info: dict[str, Any] = {
                "requested_version": spec.version,
                "exists_locally": connector_path.exists(),
            }

            if connector_path.exists():
                local_version = self._get_local_version(connector_path)
                info["local_version"] = local_version

                try:
                    if spec.version == "latest":
                        remote_version = self._get_remote_latest_version(spec.name)
                        info["remote_version"] = remote_version
                        info["needs_update"] = local_version != remote_version
                    else:
                        info["needs_update"] = local_version != spec.version
                except Exception as e:
                    info["error"] = str(e)
                    info["needs_update"] = None
            else:
                info["local_version"] = None
                info["needs_update"] = True

            status[spec.name] = info

        return status


def sync_connectors(
    config_path: Optional[str] = None,
    base_path: Optional[Path] = None,
    force: bool = False,
) -> dict[str, str]:
    """
    Convenience function to sync connectors.

    Args:
        config_path: Path to analitiq.yaml configuration file.
        base_path: Base path for resolving relative paths.
        force: If True, re-download all connectors even if up-to-date.

    Returns:
        Dictionary mapping connector names to their sync status.
    """
    syncer = ConnectorSync(config_path=config_path, base_path=base_path)
    return syncer.sync_all()


def check_connector_status(
    config_path: Optional[str] = None,
    base_path: Optional[Path] = None,
) -> dict[str, dict[str, Any]]:
    """
    Convenience function to check connector status without downloading.

    Args:
        config_path: Path to analitiq.yaml configuration file.
        base_path: Base path for resolving relative paths.

    Returns:
        Dictionary with connector status information.
    """
    syncer = ConnectorSync(config_path=config_path, base_path=base_path)
    return syncer.check_status()
