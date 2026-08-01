"""Parquet formatter implementation.

Parquet is a columnar storage format optimized for analytics workloads.
Requires pyarrow: ``analitiq-cdk[arrow]``.
"""

from typing import Any

from .._extras import reraise_for_missing_extra
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

    Requires: pyarrow (install with: ``analitiq-cdk[arrow]``)
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

    @staticmethod
    def _ensure_pyarrow() -> None:
        """Fail with the extra to install, not a bare import trace.

        The engine extra this once named ships pandas and numpy, never
        pyarrow, so the instruction was unactionable even before the
        module moved. ``reraise_for_missing_extra`` is what every other
        lazy Arrow entry in the CDK uses, and it re-raises a *broken*
        pyarrow install untouched rather than mislabelling it.
        """
        try:
            import pyarrow  # noqa: F401
        except ImportError as exc:
            reraise_for_missing_extra(
                exc,
                feature="cdk.formatters.ParquetFormatter",
                extra="arrow",
                modules=("pyarrow",),
            )

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

    @staticmethod
    def _prepare_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
