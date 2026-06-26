"""Parquet formatter implementation.

Parquet is a columnar storage format optimized for analytics workloads.
Requires pyarrow: poetry install -E analytics
"""

from typing import Any

from .base import BaseFormatter


class ParquetFormatter(BaseFormatter):
    """
    Formatter for Apache Parquet format.

    Parquet is a columnar format that provides:
    - Efficient compression
    - Column pruning for selective reads
    - Predicate pushdown for filtering
    - Schema preservation

    Configuration options:
    - compression: Compression codec (snappy, gzip, zstd, none). Default: snappy
    - row_group_size: Number of rows per row group. Default: 10000
    - version: Parquet version ('1.0', '2.4', '2.6'). Default: '2.6'

    Requires: pyarrow (install with: poetry install -E analytics)
    """

    @property
    def format_name(self) -> str:
        """Return the format identifier."""
        return "parquet"

    @property
    def file_extension(self) -> str:
        """Return the file extension."""
        return ".parquet"

    @property
    def content_type(self) -> str:
        """Return the MIME content type."""
        return "application/vnd.apache.parquet"

    def _ensure_pyarrow(self) -> None:
        """Ensure pyarrow is available."""
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            raise ImportError(
                "ParquetFormatter requires pyarrow. "
                "Install with: poetry install -E analytics"
            ) from None

    def serialize_batch(
        self,
        records: list[dict[str, Any]],
        schema: dict[str, Any] | None = None,
    ) -> bytes:
        """
        Serialize a batch of records to Parquet format.

        Args:
            records: List of record dictionaries
            schema: Optional JSON Schema (unused; PyArrow infers column
                types from the prepared records)

        Returns:
            Parquet file bytes
        """
        self._ensure_pyarrow()
        import pyarrow as pa
        import pyarrow.parquet as pq

        if not records:
            return b""

        # Convert records to a PyArrow Table; PyArrow infers column types.
        prepared_records = self._prepare_records(records)
        table = pa.Table.from_pylist(prepared_records)

        # Get configuration options
        compression = self._config.get("compression", "snappy")
        if compression == "none":
            compression = None

        row_group_size = self._config.get("row_group_size", 10000)
        version = self._config.get("version", "2.6")

        # Write to buffer
        buffer = pa.BufferOutputStream()
        pq.write_table(
            table,
            buffer,
            compression=compression,
            row_group_size=row_group_size,
            version=version,
        )

        data: bytes = buffer.getvalue().to_pybytes()
        return data

    def _prepare_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Prepare records for Parquet conversion.

        Converts complex types to JSON strings where needed.

        Args:
            records: Raw records

        Returns:
            Prepared records
        """
        import json

        prepared = []
        for record in records:
            row = {}
            for key, value in record.items():
                # Convert complex objects to JSON strings
                if isinstance(value, dict):
                    row[key] = json.dumps(value)
                else:
                    row[key] = value
            prepared.append(row)

        return prepared
