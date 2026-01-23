"""Abstract base class for record formatters.

Formatters serialize records to different output formats (JSONL, CSV, Parquet).
They are used by file-based destination handlers to convert records before writing.
"""

from abc import ABC, abstractmethod
from typing import Any, BinaryIO, Dict, List, Optional


class BaseFormatter(ABC):
    """
    Abstract base class for all record formatters.

    Formatters handle serialization of record batches to specific formats.
    They are stateless and can be shared across multiple writes.
    """

    def __init__(self) -> None:
        """Initialize the formatter with default configuration."""
        self._config: Dict[str, Any] = {}

    @property
    @abstractmethod
    def format_name(self) -> str:
        """
        Return the format identifier.

        Returns:
            Format name (e.g., 'jsonl', 'csv', 'parquet')
        """
        pass

    @property
    @abstractmethod
    def file_extension(self) -> str:
        """
        Return the file extension including the dot.

        Returns:
            File extension (e.g., '.jsonl', '.csv', '.parquet')
        """
        pass

    @property
    def is_binary(self) -> bool:
        """
        Whether this format produces binary output.

        Returns:
            True for binary formats (Parquet), False for text formats (JSONL, CSV)
        """
        return False

    @property
    def content_type(self) -> str:
        """
        Return the MIME content type for this format.

        Returns:
            MIME type string
        """
        return "application/octet-stream"

    def configure(self, config: Dict[str, Any]) -> None:
        """
        Configure the formatter with format-specific options.

        Args:
            config: Configuration dictionary with format-specific options
                   (e.g., delimiter for CSV, compression for Parquet)
        """
        self._config = config

    @abstractmethod
    def serialize_batch(
        self,
        records: List[Dict[str, Any]],
        schema: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """
        Serialize a batch of records to bytes.

        Args:
            records: List of record dictionaries to serialize
            schema: Optional JSON Schema describing the record structure

        Returns:
            Serialized bytes ready to be written to storage
        """
        pass

    @abstractmethod
    def write_batch_to_stream(
        self,
        records: List[Dict[str, Any]],
        stream: BinaryIO,
        schema: Optional[Dict[str, Any]] = None,
        append: bool = True,
    ) -> int:
        """
        Write a batch of records directly to a stream.

        This method is optimized for streaming writes where the full
        serialized content doesn't need to be held in memory.

        Args:
            records: List of record dictionaries to write
            stream: Binary stream to write to
            schema: Optional JSON Schema describing the record structure
            append: Whether to append to existing content (affects headers in CSV)

        Returns:
            Number of bytes written
        """
        pass

    def serialize_single(
        self,
        record: Dict[str, Any],
        schema: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """
        Serialize a single record to bytes.

        Default implementation wraps the record in a list and calls serialize_batch.
        Subclasses may override for more efficient single-record serialization.

        Args:
            record: Single record dictionary to serialize
            schema: Optional JSON Schema describing the record structure

        Returns:
            Serialized bytes
        """
        return self.serialize_batch([record], schema)
