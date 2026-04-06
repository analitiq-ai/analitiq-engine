"""Abstract base class for storage backends.

Storage backends handle writing files to different storage systems
(local filesystem, S3, GCS, etc.).
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional


class BaseStorageBackend(ABC):
    """
    Abstract base class for all storage backends.

    Storage backends handle the actual file operations for file-based
    destinations. They provide a consistent interface across different
    storage systems.
    """

    def __init__(self) -> None:
        """Initialize the storage backend."""
        self._config: Dict[str, Any] = {}
        self._connected: bool = False

    @property
    @abstractmethod
    def storage_type(self) -> str:
        """
        Return the storage type identifier.

        Returns:
            Storage type (e.g., 'local', 's3', 'gcs')
        """
        pass

    @abstractmethod
    async def connect(self, config: Dict[str, Any]) -> None:
        """
        Initialize storage connection/credentials.

        Args:
            config: Storage configuration (paths, credentials, etc.)

        Raises:
            ConnectionError: If storage cannot be accessed
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Close storage connection.

        Should be idempotent - safe to call multiple times.
        """
        pass

    @abstractmethod
    async def write_file(
        self,
        path: str,
        data: bytes,
        content_type: Optional[str] = None,
    ) -> str:
        """
        Write data to a file at the specified path.

        Args:
            path: Path where the file should be written
            data: File contents as bytes
            content_type: Optional MIME content type

        Returns:
            Final path/URI where the file was written

        Raises:
            IOError: If write fails
        """
        pass

    @abstractmethod
    async def append_to_file(
        self,
        path: str,
        data: bytes,
    ) -> int:
        """
        Append data to an existing file.

        If the file doesn't exist, it will be created.

        Args:
            path: Path to the file
            data: Data to append

        Returns:
            Number of bytes written

        Raises:
            IOError: If append fails
        """
        pass

    @abstractmethod
    async def file_exists(self, path: str) -> bool:
        """
        Check if a file exists at the given path.

        Args:
            path: Path to check

        Returns:
            True if file exists, False otherwise
        """
        pass

    @abstractmethod
    async def read_file(self, path: str) -> bytes:
        """
        Read file contents from the given path.

        Args:
            path: Path to read

        Returns:
            File contents as bytes

        Raises:
            FileNotFoundError: If file doesn't exist
            IOError: If read fails
        """
        pass

    @abstractmethod
    async def delete_file(self, path: str) -> bool:
        """
        Delete a file at the given path.

        Args:
            path: Path to delete

        Returns:
            True if file was deleted, False if it didn't exist
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Verify storage is accessible.

        Returns:
            True if storage is healthy, False otherwise
        """
        pass

    def build_path(
        self,
        base_path: str,
        stream_id: str,
        batch_seq: int,
        extension: str,
        partition_template: Optional[str] = None,
    ) -> str:
        """
        Build a file path with optional partitioning.

        Default format: {base_path}/{stream_id}/{batch_seq}{extension}
        With partition: {base_path}/{partitions}/{stream_id}_{batch_seq}{extension}

        Args:
            base_path: Base directory path
            stream_id: Stream identifier
            batch_seq: Batch sequence number
            extension: File extension including dot (e.g., '.jsonl')
            partition_template: Optional partition template with placeholders
                               (e.g., 'year={year}/month={month}')

        Returns:
            Constructed file path
        """
        now = datetime.utcnow()

        if partition_template:
            # Replace partition placeholders
            partitions = partition_template.format(
                year=now.year,
                month=f"{now.month:02d}",
                day=f"{now.day:02d}",
                hour=f"{now.hour:02d}",
            )
            return f"{base_path}/{partitions}/{stream_id}_{batch_seq}{extension}"

        return f"{base_path}/{stream_id}/{batch_seq}{extension}"
