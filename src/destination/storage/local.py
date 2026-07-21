"""Local filesystem storage backend implementation."""

import contextlib
import logging
import uuid
from pathlib import Path
from typing import Any

import aiofiles  # type: ignore[import-untyped]  # types-aiofiles not installed
import aiofiles.os  # type: ignore[import-untyped]  # types-aiofiles not installed

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

    async def connect(self, config: dict[str, Any]) -> None:
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

    def _require_path(self, path: str) -> Path:
        if not self._connected or self._base_path is None:
            raise OSError("Storage not connected")
        return self._base_path / path

    async def write_file(
        self,
        path: str,
        data: bytes,
        content_type: str | None = None,
    ) -> str:
        """
        Write data to a file at the specified path.

        The data goes to a temp file in the same directory and is moved
        into place with an atomic rename, so a process crash mid-write
        never leaves a truncated file at the final path. A replayed batch
        rewriting its committed file therefore lands all-or-nothing
        (issue #306).

        Args:
            path: Relative path from base_path
            data: File contents
            content_type: Ignored for local storage

        Returns:
            Absolute path where file was written
        """
        full_path = self._require_path(path)

        # Ensure parent directory exists
        full_path.parent.mkdir(parents=True, exist_ok=True)

        # Same directory keeps the rename on one filesystem; the extra
        # suffix keeps the temp file out of extension globs. The unique
        # component keeps overlapping retries of the same batch (an ACK
        # timeout racing the first attempt) on separate temp files, so
        # neither can keep writing into an inode the other already
        # renamed into place.
        tmp_path = full_path.with_name(f"{full_path.name}.{uuid.uuid4().hex[:8]}.tmp")
        try:
            async with aiofiles.open(tmp_path, "wb") as f:
                await f.write(data)
            await aiofiles.os.replace(tmp_path, full_path)
        except BaseException:
            with contextlib.suppress(OSError):
                await aiofiles.os.remove(tmp_path)
            raise

        logger.debug(f"Wrote {len(data)} bytes to: {full_path}")
        return str(full_path)

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
        exists: bool = await aiofiles.os.path.exists(full_path)
        return exists

    async def read_file(self, path: str) -> bytes:
        """
        Read file contents from the given path.

        Args:
            path: Relative path from base_path

        Returns:
            File contents as bytes
        """
        full_path = self._require_path(path)

        if not await aiofiles.os.path.exists(full_path):
            raise FileNotFoundError(f"File not found: {full_path}")

        async with aiofiles.open(full_path, "rb") as f:
            data: bytes = await f.read()
            return data

    async def delete_file(self, path: str) -> bool:
        """
        Delete a file at the given path.

        Args:
            path: Relative path from base_path

        Returns:
            True if file was deleted, False if it didn't exist
        """
        full_path = self._require_path(path)

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
