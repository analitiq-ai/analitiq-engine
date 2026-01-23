"""Local filesystem storage backend implementation."""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import aiofiles
import aiofiles.os

from .base import BaseStorageBackend


logger = logging.getLogger(__name__)


class LocalFileStorage(BaseStorageBackend):
    """
    Storage backend for local filesystem.

    Writes files to the local filesystem. Useful for:
    - Local testing and development
    - Scenarios where data stays on the same machine
    - Debugging destination handlers
    """

    def __init__(self) -> None:
        """Initialize local file storage."""
        super().__init__()
        self._base_path: Path | None = None

    @property
    def storage_type(self) -> str:
        """Return the storage type identifier."""
        return "local"

    async def connect(self, config: Dict[str, Any]) -> None:
        """
        Initialize local file storage with configuration.

        Args:
            config: Configuration dictionary with keys:
                - path: Base path for file storage (required)
                - create_dirs: Whether to create directories if missing (default: True)
        """
        path = config.get("path")
        if not path:
            raise ValueError("LocalFileStorage requires 'path' in configuration")

        self._base_path = Path(path)
        self._config = config

        # Optionally create base directory
        create_dirs = config.get("create_dirs", True)
        if create_dirs:
            self._base_path.mkdir(parents=True, exist_ok=True)

        self._connected = True
        logger.info(f"LocalFileStorage connected to: {self._base_path}")

    async def disconnect(self) -> None:
        """Disconnect from local file storage."""
        if self._connected:
            self._connected = False
            logger.info("LocalFileStorage disconnected")

    async def write_file(
        self,
        path: str,
        data: bytes,
        content_type: Optional[str] = None,
    ) -> str:
        """
        Write data to a file at the specified path.

        Args:
            path: Relative path from base_path
            data: File contents
            content_type: Ignored for local storage

        Returns:
            Absolute path where file was written
        """
        if not self._connected or self._base_path is None:
            raise IOError("Storage not connected")

        full_path = self._base_path / path

        # Ensure parent directory exists
        full_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(full_path, "wb") as f:
            await f.write(data)

        logger.debug(f"Wrote {len(data)} bytes to: {full_path}")
        return str(full_path)

    async def append_to_file(
        self,
        path: str,
        data: bytes,
    ) -> int:
        """
        Append data to an existing file.

        Args:
            path: Relative path from base_path
            data: Data to append

        Returns:
            Number of bytes written
        """
        if not self._connected or self._base_path is None:
            raise IOError("Storage not connected")

        full_path = self._base_path / path

        # Ensure parent directory exists
        full_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiofiles.open(full_path, "ab") as f:
            await f.write(data)

        logger.debug(f"Appended {len(data)} bytes to: {full_path}")
        return len(data)

    async def file_exists(self, path: str) -> bool:
        """
        Check if a file exists at the given path.

        Args:
            path: Relative path from base_path

        Returns:
            True if file exists
        """
        if not self._connected or self._base_path is None:
            return False

        full_path = self._base_path / path
        return await aiofiles.os.path.exists(full_path)

    async def read_file(self, path: str) -> bytes:
        """
        Read file contents from the given path.

        Args:
            path: Relative path from base_path

        Returns:
            File contents as bytes
        """
        if not self._connected or self._base_path is None:
            raise IOError("Storage not connected")

        full_path = self._base_path / path

        if not await aiofiles.os.path.exists(full_path):
            raise FileNotFoundError(f"File not found: {full_path}")

        async with aiofiles.open(full_path, "rb") as f:
            return await f.read()

    async def delete_file(self, path: str) -> bool:
        """
        Delete a file at the given path.

        Args:
            path: Relative path from base_path

        Returns:
            True if file was deleted, False if it didn't exist
        """
        if not self._connected or self._base_path is None:
            raise IOError("Storage not connected")

        full_path = self._base_path / path

        if not await aiofiles.os.path.exists(full_path):
            return False

        await aiofiles.os.remove(full_path)
        logger.debug(f"Deleted file: {full_path}")
        return True

    async def health_check(self) -> bool:
        """
        Verify local storage is accessible.

        Returns:
            True if base path exists and is writable
        """
        if not self._connected or self._base_path is None:
            return False

        try:
            # Check if path exists and is a directory
            if not self._base_path.exists():
                return False
            if not self._base_path.is_dir():
                return False

            # Check if writable by attempting to create a temp file
            test_file = self._base_path / ".health_check"
            test_file.write_bytes(b"ok")
            test_file.unlink()
            return True

        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False
