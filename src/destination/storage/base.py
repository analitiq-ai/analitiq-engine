"""Abstract base class for storage backends.

Storage backends handle writing files to different storage systems
(local filesystem, S3, GCS, etc.).
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any


class BaseStorageBackend(ABC):
    """
    Abstract base class for all storage backends.

    Storage backends handle the actual file operations for file-based
    destinations. They provide a consistent interface across different
    storage systems.
    """

    def __init__(self) -> None:
        """Initialize the storage backend."""
        self._config: dict[str, Any] = {}
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
    async def connect(self, config: dict[str, Any]) -> None:
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
        content_type: str | None = None,
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
        timestamp: datetime,
        partition_template: str | None = None,
        content_hash: str = "",
    ) -> str:
        """
        Build a file path with optional partitioning.

        Without hash:
          {base_path}/{stream_id}/{batch_seq}{extension}
        With hash:
          {base_path}/{stream_id}/{batch_seq}_{content_hash}{extension}
        With partition (without hash):
          {base_path}/{partitions}/{stream_id}_{batch_seq}{extension}
        With partition and hash:
          {base_path}/{partitions}/{stream_id}_{batch_seq}_{content_hash}{extension}

        The content_hash suffix ensures that two batches with the same sequence
        number but different content (as happens on a same-RUN_ID restart, where
        batch_seq resets while the source resumes from the committed cursor) land
        on distinct files instead of overwriting each other.

        The partition placeholders resolve from ``timestamp`` -- the engine's
        replay-stable per-batch instant -- never from the write-time wall clock.
        A wall-clock read would place a replayed batch that crosses an hour/day
        boundary under a different partition directory, breaking the content-
        addressed replay-overwrite guarantee (issue #353): same batch must map
        to the same path so the rewrite overwrites in place.

        Args:
            base_path: Base directory path
            stream_id: Stream identifier
            batch_seq: Batch sequence number
            extension: File extension including dot (e.g., '.jsonl')
            timestamp: Timezone-aware UTC instant the partition placeholders
                       resolve from (the engine's per-batch ``emitted_at``).
                       Unused when ``partition_template`` is None.
            partition_template: Optional partition template with placeholders
                               (e.g., 'year={year}/month={month}')
            content_hash: First 16 hex chars of SHA-256 of the serialized batch;
                          omit (or pass "") to use the legacy batch_seq-only stem.

        Returns:
            Constructed file path
        """
        stem = f"{batch_seq}_{content_hash}" if content_hash else str(batch_seq)

        if partition_template:
            # Read the calendar fields off the UTC projection of the instant,
            # so partitioning is UTC by construction rather than by caller
            # convention -- a non-UTC (but still aware) timestamp buckets into
            # the same directory as its UTC equivalent. Derived from the
            # replay-stable batch instant, so a retried batch always resolves
            # the same dir.
            utc = timestamp.astimezone(timezone.utc)
            partitions = partition_template.format(
                year=utc.year,
                month=f"{utc.month:02d}",
                day=f"{utc.day:02d}",
                hour=f"{utc.hour:02d}",
            )
            return f"{base_path}/{partitions}/{stream_id}_{stem}{extension}"

        return f"{base_path}/{stream_id}/{stem}{extension}"
