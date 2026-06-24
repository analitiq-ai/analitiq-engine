"""Abstract base class for record formatters.

Formatters serialize records to different output formats (JSONL, CSV, Parquet).
They are used by file-based destination handlers to convert records before writing.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


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
