"""CSV formatter implementation.

CSV is a widely-supported tabular format, ideal for spreadsheet compatibility.
"""

import csv
import io
from typing import Any, BinaryIO, Dict, List, Optional

from .base import BaseFormatter


class CsvFormatter(BaseFormatter):
    """
    Formatter for CSV (Comma-Separated Values) format.

    Each record is serialized as a row with values in column order.
    The first batch includes a header row with column names.

    Configuration options:
    - delimiter: Field delimiter (default: ',')
    - quotechar: Quote character (default: '"')
    - quoting: Quote behavior (default: csv.QUOTE_MINIMAL)
    - include_header: Whether to include header row (default: True)
    """

    @property
    def format_name(self) -> str:
        """Return the format identifier."""
        return "csv"

    @property
    def file_extension(self) -> str:
        """Return the file extension."""
        return ".csv"

    @property
    def is_binary(self) -> bool:
        """CSV is a text format encoded as UTF-8 bytes."""
        return False

    @property
    def content_type(self) -> str:
        """Return the MIME content type."""
        return "text/csv"

    def _get_csv_options(self) -> Dict[str, Any]:
        """Get CSV writer options from configuration."""
        return {
            "delimiter": self._config.get("delimiter", ","),
            "quotechar": self._config.get("quotechar", '"'),
            "quoting": self._config.get("quoting", csv.QUOTE_MINIMAL),
        }

    def _get_fieldnames(
        self,
        records: List[Dict[str, Any]],
        schema: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """
        Get column names from schema or first record.

        Args:
            records: List of records
            schema: Optional JSON Schema

        Returns:
            List of column names in consistent order
        """
        # Try to get field order from schema
        if schema and "properties" in schema:
            return list(schema["properties"].keys())

        # Fall back to keys from first record
        if records:
            return list(records[0].keys())

        return []

    def serialize_batch(
        self,
        records: List[Dict[str, Any]],
        schema: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """
        Serialize a batch of records to CSV format.

        Args:
            records: List of record dictionaries
            schema: Optional JSON Schema for column ordering

        Returns:
            UTF-8 encoded bytes with CSV data
        """
        if not records:
            return b""

        fieldnames = self._get_fieldnames(records, schema)
        csv_options = self._get_csv_options()
        include_header = self._config.get("include_header", True)

        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames,
            extrasaction="ignore",  # Ignore extra fields not in fieldnames
            **csv_options,
        )

        if include_header:
            writer.writeheader()

        for record in records:
            # Convert non-string values to strings
            row = {k: self._format_value(v) for k, v in record.items()}
            writer.writerow(row)

        return output.getvalue().encode("utf-8")

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
            schema: Optional JSON Schema for column ordering
            append: Whether appending (affects header inclusion)

        Returns:
            Number of bytes written
        """
        if not records:
            return 0

        fieldnames = self._get_fieldnames(records, schema)
        csv_options = self._get_csv_options()

        # Only include header if not appending or explicitly configured
        include_header = self._config.get("include_header", True) and not append

        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames,
            extrasaction="ignore",
            **csv_options,
        )

        if include_header:
            writer.writeheader()

        for record in records:
            row = {k: self._format_value(v) for k, v in record.items()}
            writer.writerow(row)

        data = output.getvalue().encode("utf-8")
        stream.write(data)
        return len(data)

    def _format_value(self, value: Any) -> str:
        """
        Format a value for CSV output.

        Args:
            value: Any value to format

        Returns:
            String representation
        """
        if value is None:
            return ""
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, (list, dict)):
            import json
            return json.dumps(value)
        return str(value)
