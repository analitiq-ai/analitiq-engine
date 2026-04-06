"""JSON Lines (JSONL) formatter implementation.

JSONL is a simple format where each line is a valid JSON object.
It's ideal for streaming and append operations.
"""

import json
from typing import Any, BinaryIO, Dict, List, Optional

from .base import BaseFormatter


class JsonLinesFormatter(BaseFormatter):
    """
    Formatter for JSON Lines (JSONL) format.

    Each record is serialized as a single JSON object on its own line.
    This format is ideal for:
    - Streaming writes (each record can be written independently)
    - Append operations (no need to parse existing content)
    - Human readability and debugging
    """

    @property
    def format_name(self) -> str:
        """Return the format identifier."""
        return "jsonl"

    @property
    def file_extension(self) -> str:
        """Return the file extension."""
        return ".jsonl"

    @property
    def is_binary(self) -> bool:
        """JSONL is a text format encoded as UTF-8 bytes."""
        return False

    @property
    def content_type(self) -> str:
        """Return the MIME content type."""
        return "application/x-ndjson"

    def serialize_batch(
        self,
        records: List[Dict[str, Any]],
        schema: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """
        Serialize a batch of records to JSONL format.

        Args:
            records: List of record dictionaries
            schema: Optional schema (not used for JSONL but kept for interface)

        Returns:
            UTF-8 encoded bytes with one JSON object per line
        """
        if not records:
            return b""

        # Get configuration options
        ensure_ascii = self._config.get("ensure_ascii", False)
        sort_keys = self._config.get("sort_keys", False)

        lines = []
        for record in records:
            line = json.dumps(
                record,
                ensure_ascii=ensure_ascii,
                sort_keys=sort_keys,
                separators=(",", ":"),  # Compact format
                default=str,  # Handle non-serializable types
            )
            lines.append(line)

        # Join with newlines and add trailing newline
        return ("\n".join(lines) + "\n").encode("utf-8")

    def write_batch_to_stream(
        self,
        records: List[Dict[str, Any]],
        stream: BinaryIO,
        schema: Optional[Dict[str, Any]] = None,
        append: bool = True,
    ) -> int:
        """
        Write a batch of records directly to a stream.

        Args:
            records: List of record dictionaries
            stream: Binary stream to write to
            schema: Optional schema (not used for JSONL)
            append: Whether appending (doesn't affect JSONL format)

        Returns:
            Number of bytes written
        """
        if not records:
            return 0

        data = self.serialize_batch(records, schema)
        stream.write(data)
        return len(data)

    def serialize_single(
        self,
        record: Dict[str, Any],
        schema: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """
        Serialize a single record to JSONL format.

        Optimized for single-record serialization without list overhead.

        Args:
            record: Single record dictionary
            schema: Optional schema (not used)

        Returns:
            UTF-8 encoded bytes with single JSON object and newline
        """
        ensure_ascii = self._config.get("ensure_ascii", False)
        sort_keys = self._config.get("sort_keys", False)

        line = json.dumps(
            record,
            ensure_ascii=ensure_ascii,
            sort_keys=sort_keys,
            separators=(",", ":"),
            default=str,
        )
        return (line + "\n").encode("utf-8")
